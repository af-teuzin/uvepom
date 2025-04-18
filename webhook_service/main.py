import fastapi
import uvicorn
import os
from dotenv import load_dotenv
import time
from fastapi import BackgroundTasks
import redis
import json
import uuid
import db.db as db
# Load environment variables from .env file
load_dotenv()

app = fastapi.FastAPI()
db_pool = None
redis_client = None

# Custom JSON encoder to handle UUID objects
class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

@app.on_event("startup")
async def startup():
    global db_pool, redis_client
    # Initialize the connection pool at startup
    db_pool = await db.init_db_pool()
    print("Database connection pool initialized")
    
    # Initialize Redis connection
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASSWORD", ""),
        decode_responses=False
    )
    print("Redis connection initialized")

async def publish_to_redis_stream(payload: dict, stream_name: str = "webhooks"):
    """Publish message to Redis Stream with guaranteed delivery"""
    if redis_client is None:
        print("Redis client not initialized")
        return False
    
    try:
        # Convert payload to bytes for Redis using custom encoder for UUID
        message_data = {
            "payload": json.dumps(payload, cls=UUIDEncoder).encode('utf-8')
        }
        
        # Add to stream with * to auto-generate ID
        message_id = redis_client.xadd(
            name=stream_name,
            fields=message_data,
            maxlen=10000,  # Limit stream size
            approximate=True
        )
        print(f"Message published to Redis Stream: {stream_name}, ID: {message_id}")
        return True
    except Exception as e:
        print(f"Error publishing to Redis Stream: {e}")
        return False

async def process_webhook_data(payload: dict, platform_name: str):
    """Process webhook data in the background"""
    start_time = time.time()
    
    global db_pool
    if db_pool is None:
        print("DB pool not initialized in background task")
        return
    
    try:
        # 1. Store in database
        db_start = time.time()
        raw_data_id = await db.insert_raw_data(payload, platform_name, db_pool)
        db_time = time.time() - db_start
        
        if raw_data_id is None:
            print("Failed to insert data in background task")
            return
        
        # 2. Add database ID to payload
        payload["raw_data_id"] = raw_data_id
        
        # 3. Publish to Redis Stream
        stream_start = time.time()
        success = await publish_to_redis_stream(payload)
        if success == True:
            stream_time = time.time() - stream_start
            status = "forwarded"
        else:
            status = "forwarding_failed"
            stream_time = 0
        
        await db.update_raw_data(raw_data_id, status, db_pool)
        total_time = time.time() - start_time
        print(f"Background task metrics: Total: {total_time:.4f}s, DB: {db_time:.4f}s, Stream: {stream_time:.4f}s, Success: {success}")
    except Exception as e:
        print(f"Error in background task: {e}")

@app.post("/{platform_type}/{platform_name}")
async def read_root(request: fastapi.Request, platform_type: str, platform_name: str, background_tasks: BackgroundTasks):
    total_start_time = time.time()
    
    # Measure JSON parsing time
    json_start = time.time()
    payload = await request.json()
    json_time = time.time() - json_start
    
    # Add platform info to payload
    payload["platform_type"] = platform_type
    payload["platform_name"] = platform_name
    
    # Schedule background processing
    background_tasks.add_task(process_webhook_data, payload, platform_name)
    
    total_time = time.time() - total_start_time
    print(f"Response metrics: Total: {total_time:.4f}s, JSON: {json_time:.4f}s")
    
    # Return immediately with 200 status
    return fastapi.Response(status_code=200)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)