import time
import logging
import db.db as db

class Insertor:
    async def insert_into_db(self, payload, pool):
        """Insert normalized payload into database"""
        start_time = time.time()
        try:
            if payload.status == "abandoned":
                result = await db.insert_abandoned_cart(payload, pool)
            else:
                result = await db.insert_transaction(payload, pool)
            logging.info(f"Database insert completed in {time.time() - start_time:.4f}s")
            return result
            
        except Exception as e:
            logging.error(f"Error in database insert: {e} (took {time.time() - start_time:.4f}s)")
            return False