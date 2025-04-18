import psycopg
import os
import json
from psycopg_pool import ConnectionPool
from db.classes.classes import Transaction, AbandonedCart
from dataclasses import asdict
import logging
# Create a global connection pool that will be reused
pool = None

async def init_db_pool():
    global pool
    if pool is None:
        pool = ConnectionPool(
            conninfo=f"host={os.getenv('DB_HOST')} "
            f"port={os.getenv('DB_PORT')} "
            f"dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')}",
            min_size=5,
            max_size=20
        )
    return pool

async def connect_to_db():
    # Now returns the pool instead of a single connection
    try:
        return await init_db_pool()
    except Exception as e:
        print(f"Error initializing connection pool: {e}")
        return None

async def insert_raw_data(payload: dict, platform: str, pool):
    if pool is None:
        print("Database pool is None")
        return None
        
    sql = """INSERT INTO hytallo_soares.raw_data(
            payload, platform
            ) VALUES (
    %s, %s)
    RETURNING id
    """
    try:
        # Get a connection from the pool
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                # Use prepared statement
                cursor.execute(sql, (json.dumps(payload), platform))
                result = cursor.fetchone()[0]
                return result
    except Exception as e:
        print(f"Error inserting data: {e}")
        return None

async def update_raw_data(raw_data_id, status, pool):
    if pool is None:
        print("Database pool is None")
        return False
    sql = """UPDATE hytallo_soares.raw_data
            SET status = %s, updated_at = NOW()
            WHERE id = %s
            """
    try:
        # Get a connection from the pool
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                # Use prepared statement
                cursor.execute(sql, (status, raw_data_id))
                conn.commit()
                return True
    except Exception as e:
        print(f"Error updating data: {e}")
        return False
    
async def insert_transaction(transaction: Transaction, pool):
    sql = """INSERT INTO hytallo_soares.transactions_v3(
        transaction_id,
        created_at,
        updated_at,
        order_date,
        currency,
        status,
        payment_method,
        user_email,
        user_name,
        user_phone,
        user_country,
        user_ip,
        user_city,
        user_region,
        user_postal_code,
        product_id,
        product_name,
        product_type,
        product_price,
        transaction_value,
        transaction_fee_total,
        transaction_net_value,
        installments,
        quantity,
        offer_id,
        offer_name,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        utm_target,
        sck,
        src,
        cycle,
        producer_name,
        affiliate,
        subscription,
        affiliate_commission,
        audit_original_payment_method,
        audit_original_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id, product_id) DO UPDATE SET
        updated_at = NOW(),
        status = EXCLUDED.status"""
    try:
        transaction_dict = asdict(transaction)
        values = list(transaction_dict.values())
        
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, values)
                return True
    except Exception as e:
        logging.error(f"Error inserting transaction: {e}")
        logging.error(f"Transaction ID: {transaction.transaction_id}")
        return False
    

async def insert_abandoned_cart(abandoned_cart: AbandonedCart, pool):
    sql = """INSERT INTO hytallo_soares.abandoned_carts(
        user_email,
        user_phone,
        user_doc,
        user_name,
        product_id,
        product_name,
        created_at,
        user_ip,
        user_city,
        user_region,
        user_postal_code,
        transaction_value,
        offer_id,
        offer_name,
        status,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        utm_target,
        sck,
        src,
        producer_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    try:
        cart_dict = asdict(abandoned_cart)
        values = list(cart_dict.values())
        
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, values)
                return True
    except Exception as e:
        logging.error(f"Error inserting abandoned cart: {e}")
        return False