import psycopg
import os
import json
from psycopg_pool import ConnectionPool
from db.classes import AdMetrics, AdAccounts
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
            min_size=2,
            max_size=4
        )
    return pool

async def retrieve_google_ad_accounts(pool):
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT aa.id as account_id, aa.ad_account_id as ref_account_id FROM hytallo_soares.ads_accounts aa JOIN hytallo_soares.credentials c ON aa.credential_id = c.id WHERE c.platform_id = 2")
            return cursor.fetchall()

async def retrieve_facebook_ad_accounts(pool):
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT aa.id as account_id, aa.ad_account_id as ref_account_id, c.ref_account_id  as bm_id, c.access_token as access_token 
                    FROM hytallo_soares.ads_accounts aa 
                    JOIN hytallo_soares.credentials c ON aa.credential_id = c.id 
                    WHERE c.platform_id = 1""")
            return cursor.fetchall()


async def insert_facebook_ad_accounts(pool, ad_accounts: AdAccounts):
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""INSERT INTO hytallo_soares.ads_accounts (ref_account_id, bm_id, credential_id) VALUES (%s)
                   RETURNING id""",
                   (asdict(ad_accounts)))
            return cursor.fetchone()


async def insert_facebook_ad_metrics_batch(pool, ad_metrics_list):
    """Inserir múltiplas métricas de anúncios do Facebook em um único comando de banco de dados"""
    if not ad_metrics_list:
        return 0  # Nada para inserir
        
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            # Construir a query base para inserção
            base_query = """INSERT INTO hytallo_soares.ad_metrics (
                           ads_accounts_id, campaign_id, campaign_name, 
                           ad_group_id, ad_group_name, ad_id, ad_name, 
                           impressions, clicks, cost, currency, date,
                           page_view, initiate_checkout, reach,
                           "3s_video_view", "50_video_view", "75_video_view") 
                       VALUES """
                       
            value_template = """(%(ads_accounts_id_{})s, %(campaign_id_{})s, %(campaign_name_{})s,
                              %(ad_group_id_{})s, %(ad_group_name_{})s, %(ad_id_{})s, %(ad_name_{})s,
                              %(impressions_{})s, %(clicks_{})s, %(cost_{})s, %(currency_{})s, %(date_{})s,
                              %(page_view_{})s, %(initiate_checkout_{})s, %(reach_{})s,
                              %(three_second_video_view_{})s, %(fifty_video_view_{})s, %(seventy_five_video_view_{})s)"""
            
            values_parts = []
            params = {}
            
            # Construir a parte de valores e parâmetros para cada métrica
            for i, ad_metric in enumerate(ad_metrics_list):
                metric_dict = asdict(ad_metric)
                values_parts.append(value_template.format(i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i))
                
                # Adicionar parâmetros com índice para evitar colisões
                for key, value in metric_dict.items():
                    params[f"{key}_{i}"] = value
            
            # Juntar tudo em uma única query
            full_query = base_query + ", ".join(values_parts) + """
                       ON CONFLICT (ads_accounts_id, campaign_id, ad_group_id, ad_id, date) DO UPDATE SET 
                       impressions = EXCLUDED.impressions, 
                       clicks = EXCLUDED.clicks, 
                       cost = EXCLUDED.cost,
                       page_view = EXCLUDED.page_view,
                       initiate_checkout = EXCLUDED.initiate_checkout,
                       reach = EXCLUDED.reach,
                       "3s_video_view" = EXCLUDED."3s_video_view",
                       "50_video_view" = EXCLUDED."50_video_view",
                       "75_video_view" = EXCLUDED."75_video_view",
                       campaign_name = EXCLUDED.campaign_name,
                       ad_group_name = EXCLUDED.ad_group_name,
                       ad_name = EXCLUDED.ad_name"""
            
            # Executar a query
            cursor.execute(full_query, params)
            conn.commit()
            
            # Retornar o número de registros inseridos
            return len(ad_metrics_list)
            
async def insert_google_ad_metrics_batch(pool, ad_metrics_list):
    """Inserir múltiplas métricas de anúncios do Google em um único comando de banco de dados"""
    if not ad_metrics_list:
        return 0  # Nada para inserir
    
    # Log para ajudar na depuração
    logging.info(f"Attempting to insert batch of {len(ad_metrics_list)} Google Ads metrics")
    
    try:
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                # Construir a query base para inserção
                base_query = """INSERT INTO hytallo_soares.ad_metrics (
                            ads_accounts_id, campaign_id, campaign_name, 
                            ad_group_id, ad_group_name, ad_id, ad_name, 
                            impressions, clicks, cost, currency, date,
                            conversions) 
                        VALUES """
                        
                value_template = """(%(ads_accounts_id_{})s, %(campaign_id_{})s, %(campaign_name_{})s,
                                %(ad_group_id_{})s, %(ad_group_name_{})s, %(ad_id_{})s, %(ad_name_{})s,
                                %(impressions_{})s, %(clicks_{})s, %(cost_{})s, %(currency_{})s, %(date_{})s,
                                %(conversions_{})s)"""
                
                values_parts = []
                params = {}
                
                # Construir a parte de valores e parâmetros para cada métrica
                for i, ad_metric in enumerate(ad_metrics_list):
                    # Converter para string para garantir compatibilidade
                    ad_metric.ads_accounts_id = str(ad_metric.ads_accounts_id)
                    
                    metric_dict = asdict(ad_metric)
                    values_parts.append(value_template.format(i, i, i, i, i, i, i, i, i, i, i, i, i))
                    
                    # Adicionar parâmetros com índice para evitar colisões
                    for key, value in metric_dict.items():
                        params[f"{key}_{i}"] = value
                
                # Juntar tudo em uma única query
                full_query = base_query + ", ".join(values_parts) + """
                        ON CONFLICT (ads_accounts_id, campaign_id, ad_group_id, ad_id, date) DO UPDATE SET 
                        impressions = EXCLUDED.impressions, 
                        clicks = EXCLUDED.clicks, 
                        cost = EXCLUDED.cost,
                        campaign_name = EXCLUDED.campaign_name,
                        ad_group_name = EXCLUDED.ad_group_name,
                        ad_name = EXCLUDED.ad_name,
                        conversions = EXCLUDED.conversions"""
                
                # Executar a query
                cursor.execute(full_query, params)
                conn.commit()
                
                # Retornar o número de registros inseridos
                return len(ad_metrics_list)
    except Exception as e:
        logging.error(f"Error in batch insertion: {e}")
        # Log detailed parameters for debugging
        for i, metric in enumerate(ad_metrics_list[:2]):  # Log only first 2 to avoid flooding
            logging.error(f"Sample metric {i}: {asdict(metric)}")
        return 0
