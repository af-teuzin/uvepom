import psycopg
import os
import json
from psycopg_pool import ConnectionPool

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