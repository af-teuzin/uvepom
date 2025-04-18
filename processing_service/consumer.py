from fastapi import HTTPException
import redis
import json
import os
import time
import signal
import sys
import asyncio
from dotenv import load_dotenv
import importlib
import logging
import db.db as db

PROCESSORS = {}
INSERTORS = {}

def load_processors():
    # Define all known processors
    processor_configs = [
        ("checkout", "kiwify", "services.checkout.kiwify.kiwify")
    ]
    for platform_type, platform, module_path in processor_configs:
        try:
            module = importlib.import_module(module_path)
            PROCESSORS[(platform_type, platform)] = module.Processor
        except ImportError as e:
            logging.error(f"Failed to load processor: {e}")

def load_insertors():
    # Define all known insertors
    insertor_configs = [
        ("checkout", "kiwify", "services.checkout.checkout")
    ]
    for platform_type, platform, module_path in insertor_configs:
        try:
            module = importlib.import_module(module_path)
            INSERTORS[(platform_type, platform)] = module.Insertor
        except ImportError as e:
            logging.error(f"Failed to load insertor: {e}")

# Call this during application startup
load_processors()
load_insertors()

# Load environment variables
load_dotenv()

# Global flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    """Handle Ctrl+C or other termination signals"""
    global running
    logging.info('Shutting down gracefully...')
    running = False

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def process_message(message_data):
    start_time = time.time()
    try:
        raw_payload = message_data[b'payload']
        payload = json.loads(raw_payload.decode('utf-8'))
        
        platform_type = payload.get('platform_type')
        platform_name = payload.get('platform_name')
        raw_data_id = payload.get('raw_data_id')
        logging.info(f"Processing message: {platform_type}/{platform_name}, ID: {raw_data_id}")
        
        key = (platform_type, platform_name)
    
        # Find processor
        if key in PROCESSORS:
            processor_class = PROCESSORS[key]
        else:
            try:
                module_path = f"services.{platform_type}.{platform_name}"
                processor_module = importlib.import_module(module_path)
                processor_class = processor_module.Processor
                PROCESSORS[key] = processor_class
            except ImportError as e:
                logging.error(f"No processor found for {platform_type}/{platform_name}: {e}")
                return False, raw_data_id
        
        # Process the message
        processor = processor_class()
        normalized_payload = processor.normalize_payload(payload)
        
        # Insert into database
        insertor = INSERTORS.get(key, processor_class)()
        inserted = await insertor.insert_into_db(normalized_payload, db_pool)
        
        processing_time = time.time() - start_time
        
        if inserted:
            logging.info(f"Successfully processed event in {processing_time:.4f}s")
            return True, raw_data_id
        else:
            logging.error(f"Failed to insert event in {processing_time:.4f}s")
            return False, raw_data_id
    except Exception as e:
        processing_time = time.time() - start_time
        logging.error(f"Error processing message: {e} (took {processing_time:.4f}s)")
        return False, payload.get('raw_data_id') if 'payload' in locals() and payload else None

async def start_consumer(stream_name="webhooks", consumer_group="webhook_processors", consumer_name="processing_service"):
    # Initialize database pool
    start_time = time.time()
    global db_pool 
    db_pool = await db.init_db_pool()
    logging.info(f"Database connection pool initialized in {time.time() - start_time:.4f}s")
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD", ""),
        decode_responses=False
    )
    
    try:
        redis_client.xgroup_create(stream_name, consumer_group, id='0', mkstream=True)
        logging.info(f"Created consumer group '{consumer_group}' for stream '{stream_name}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logging.info(f"Consumer group '{consumer_group}' already exists")
        else:
            logging.error(f"Error creating consumer group: {e}")
            return
    
    logging.info(f"Starting consumer '{consumer_name}' in group '{consumer_group}'...")
    
    # Process messages in a loop with asyncio
    while running:
        try:
            # Read new messages
            messages = redis_client.xreadgroup(
                consumer_group, 
                consumer_name,
                {stream_name: '>'},
                count=10,  # Process 10 messages at a time
                block=1000  # Block for 1 second before checking running flag again
            )
            
            # If no messages, continue loop
            if not messages:
                continue
            
            # Process messages
            for stream_data in messages:
                stream, message_list = stream_data
                
                for message in message_list:
                    msg_id, data = message
                    
                    success, raw_data_id = await process_message(data)
                    
                    if success:
                        # Acknowledge successful processing
                        redis_client.xack(stream_name, consumer_group, msg_id)
                        await db.update_raw_data(raw_data_id, "processed", db_pool)
                    else:
                        # Message will remain pending for retry
                        logging.info(f"Message will be retried later")
                        if raw_data_id:
                            await db.update_raw_data(raw_data_id, "processing_failed", db_pool)
                
        except Exception as e:
            logging.error(f"Error reading from stream: {e}")
            await asyncio.sleep(1)
    
    logging.info("Consumer stopped gracefully")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_time = time.time()
    logging.info("Starting consumer service...")
    try:
        asyncio.run(start_consumer())
    finally:
        logging.info(f"Consumer service ran for {time.time() - start_time:.2f} seconds") 