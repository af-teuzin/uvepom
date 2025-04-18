import logging
from google_ads import google_ads
from google_ads.credentials import setup_google_ads_credentials
import fastapi
import db.db as db
import uvicorn
from facebook_ads import facebook_ads
import asyncio
from datetime import datetime, timedelta
from fastapi import BackgroundTasks, HTTPException

logging.basicConfig(level=logging.INFO)

app = fastapi.FastAPI()

db_pool = None
since_date = (datetime.now().date() - timedelta(days=460)).strftime('%Y-%m-%d')
until_date = (datetime.now().date() - timedelta(days=430)).strftime('%Y-%m-%d')

@app.on_event("startup")
async def startup():
    global db_pool
    
    # Initialize Google Ads credentials
    logging.info("Initializing Google Ads credentials")
    setup_google_ads_credentials()
    
    # Initialize database connection pool
    db_pool = await db.init_db_pool()
    logging.info("Database connection pool initialized")

@app.post("/retrieve_ad_metrics")
async def retrieve_ad_metrics(background_tasks: BackgroundTasks):
    try:
        facebook_ad_accounts = await db.retrieve_facebook_ad_accounts(db_pool)
        
        # Adiciona as tarefas em segundo plano
        background_tasks.add_task(
            google_ads.retrieve_ad_metrics,
            db_pool,
            since_date,
            until_date
        )
        
        background_tasks.add_task(
           facebook_ads.retrieve_ad_metrics,
           db_pool,
           since_date,
           until_date,
           facebook_ad_accounts
        )
        
        logging.info(f"Scheduled ad metrics retrieval for date range {since_date} to {until_date}")
        return {"status": "success", "message": "Iniciando recuperação de métricas de anúncios"}
    except Exception as e:
        logging.error(f"Error scheduling ad metrics retrieval: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao agendar recuperação de métricas: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/test_google_ads")
async def test_google_ads_connection():
    """
    Test endpoint for Google Ads API connection
    """
    try:
        result = google_ads.test_connection()
        if result:
            return {"status": "success", "message": "Google Ads API connection successful"}
        else:
            return {"status": "error", "message": "Failed to connect to Google Ads API"}
    except Exception as e:
        logging.error(f"Error testing Google Ads connection: {e}")
        raise HTTPException(status_code=500, detail=f"Error testing Google Ads connection: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
