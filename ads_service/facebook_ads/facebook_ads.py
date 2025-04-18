import logging
import requests
from datetime import datetime, timedelta
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import db.db as db
from db.classes import AdAccounts, AdMetrics
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constantes de configuração
MAX_WORKERS = 10  # Número máximo de workers para processamento paralelo
BATCH_SIZE = 100  # Tamanho do lote para inserção em banco de dados

# Facebook API settings
logging.info("Facebook API settings configured. Preparing to send request.")
base_url = 'https://graph.facebook.com/v20.0'

def fetch_ad_data(ad_account_id, ad_account_access_token, since_date, until_date):
    logging.info(f"Fetching data for ad account {ad_account_id} using access token.")
    time_range_str = json.dumps({'since': since_date, 'until': until_date})
    fields = 'account_id, account_name, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_name, impressions, inline_link_clicks, spend, purchase_roas, actions, action_values, date_start, reach, video_p50_watched_actions, video_p75_watched_actions, account_currency'
    params = {
        'time_increment': 1,
        'level': 'ad',
        'fields': fields,
        'limit': 100,
        'access_token': ad_account_access_token,
        'time_range': time_range_str
    }
    try:
        response = requests.get(f'{base_url}/act_{ad_account_id}/insights', params=params)
        if response.status_code == 200:
            logging.info(f"Successfully fetched ads data for ad account {ad_account_id}")
            data = response.json().get('data', [])
        else:
            logging.error(f"Failed to fetch ads data for ad account {ad_account_id}. Status code: {response.status_code}, Response: {response.text}")
            data = []
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception occurred while fetching ads data for ad account {ad_account_id}: {e}")
        data = []

    # Handle pagination
    paging = response.json().get('paging', {})
    while 'next' in paging:
        try:
            response = requests.get(paging['next'])
            if response.status_code == 200:
                    data.extend(response.json().get('data', []))
                    paging = response.json().get('paging', {})
            else:
                logging.error(f"Pagination failed for ad account {ad_account_id}. Status code: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception during pagination for ad account {ad_account_id}: {e}")
            break

    return data

async def fetch_ad_accounts(db_pool, bm_id, access_token, credential_id):
    url = f"{base_url}/{bm_id}/owned_ad_accounts"
    params = {
        'access_token': access_token,
        'fields': 'id,name,account_id'
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            logging.error(f"Failed to fetch ad accounts for BM {bm_id}. Status code: {response.status_code}")
            return []
            
        accounts_data = response.json().get('data', [])
        
        # Handle pagination
        paging = response.json().get('paging', {})
        while 'next' in paging:
            try:
                response = requests.get(paging['next'])
                if response.status_code == 200:
                    accounts_data.extend(response.json().get('data', []))
                    paging = response.json().get('paging', {})
                else:
                    break
            except requests.exceptions.RequestException:
                break
        
        # Insert accounts into database
        ad_accounts = []
        for account in accounts_data:
            ad_account = AdAccounts(
                ref_account_id=account['id'].replace('act_', ''),
                bm_id=bm_id,
                credential_id=credential_id
            )
            await db.insert_facebook_ad_accounts(db_pool, ad_account)
            ad_accounts.append(account)
            
        return ad_accounts
    except Exception as e:
        logging.error(f"Error fetching ad accounts for BM {bm_id}: {e}")
        return []

async def insert_ad_data(db_pool, data, ads_accounts_id):
    logging.info(f"Inserting {len(data)} ad metrics records for account {ads_accounts_id}")
    
    # Processar registros em lotes para maior eficiência
    start_time = datetime.now()
    
    # Lista para armazenar os objetos AdMetrics
    metrics_batch = []
    total_inserted = 0
    
    # Função para processar um único registro (usada em paralelo)
    def process_record(record):
        try:
            # Extract values with proper error handling
            impressions = int(record.get('impressions', 0))
            clicks = int(record.get('inline_link_clicks', 0))
            cost = float(record.get('spend', 0))
            
            # Get actions data
            actions = record.get('actions', [])
            page_view = None
            initiate_checkout = None
            video_view = None
            
            for action in actions:
                if action.get('action_type') == 'landing_page_view':
                    page_view = int(action.get('value', 0))
                elif action.get('action_type') == 'initiate_checkout':
                    initiate_checkout = int(action.get('value', 0))
                elif action.get('action_type') == 'video_view':
                    video_view = int(action.get('value', 0))
            
            # Extrair dados de visualização de vídeo com tratamento de erro
            reach = 0
            fifty_video_view = None
            seventy_five_video_view = None
            
            try:
                reach = int(record.get('reach', 0))
            except (TypeError, ValueError):
                reach = 0
                
            # Obter visualizações de 50% do vídeo, se disponível
            try:
                video_p50 = record.get('video_p50_watched_actions', [])
                if video_p50 and len(video_p50) > 0:
                    fifty_video_view = int(video_p50[0].get('value', 0))
            except (TypeError, ValueError, IndexError):
                fifty_video_view = None
                
            # Obter visualizações de 75% do vídeo, se disponível
            try:
                video_p75 = record.get('video_p75_watched_actions', [])
                if video_p75 and len(video_p75) > 0:
                    seventy_five_video_view = int(video_p75[0].get('value', 0))
            except (TypeError, ValueError, IndexError):
                seventy_five_video_view = None
            
            # Create AdMetrics object
            return AdMetrics(
                id=0, 
                ads_accounts_id=ads_accounts_id,
                campaign_id=record.get('campaign_id', ''),
                campaign_name=record.get('campaign_name', ''),
                ad_group_id=record.get('adset_id', ''),
                ad_group_name=record.get('adset_name', ''),
                ad_id=record.get('ad_id', ''),
                ad_name=record.get('ad_name', ''),
                impressions=impressions,
                clicks=clicks,
                cost=cost,
                currency=record.get('account_currency', 'BRL'),  # Usar a moeda da conta se disponível
                date=record.get('date_start', ''),
                page_view=page_view,
                initiate_checkout=initiate_checkout,
                reach=reach,
                three_second_video_view=video_view,
                fifty_video_view=fifty_video_view,
                seventy_five_video_view=seventy_five_video_view,
                conversions=None
            )
        except Exception as e:
            logging.error(f"Error processing record: {e}")
            return None
    
    # Processar registros em paralelo usando um ThreadPoolExecutor
    processed_records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviar todos os registros para processamento
        future_to_record = {executor.submit(process_record, record): record for record in data}
        
        # Coletar resultados à medida que são concluídos
        for future in as_completed(future_to_record):
            try:
                metric = future.result()
                if metric:
                    processed_records.append(metric)
            except Exception as e:
                logging.error(f"Exception during record processing: {e}")
    
    logging.info(f"Processed {len(processed_records)} records in {(datetime.now() - start_time).total_seconds():.2f} seconds")
    
    # Inserir registros em lotes
    batch_start_time = datetime.now()
    for i in range(0, len(processed_records), BATCH_SIZE):
        batch = processed_records[i:i+BATCH_SIZE]
        if batch:
            try:
                inserted = await db.insert_facebook_ad_metrics_batch(db_pool, batch)
                total_inserted += inserted
                logging.info(f"Batch inserted: {inserted} records. Total so far: {total_inserted}/{len(processed_records)}")
            except Exception as e:
                logging.error(f"Error during batch insertion: {e}")
    
    total_time = (datetime.now() - start_time).total_seconds()
    batch_time = (datetime.now() - batch_start_time).total_seconds()
    logging.info(f"Account {ads_accounts_id}: Inserted {total_inserted} records in {total_time:.2f} seconds (processing: {(total_time - batch_time):.2f}s, insertion: {batch_time:.2f}s)")
    
    return total_inserted

async def retrieve_ad_metrics(db_pool, since_date, until_date, facebook_ad_accounts):
    logging.info(f"Retrieving Facebook ad metrics from {since_date} to {until_date}")
    start_time = datetime.now()
    
    try:
        # Processar até 3 contas simultaneamente
        sem = asyncio.Semaphore(3)  # Limite de 3 processamentos concorrentes
        tasks = []
        
        for account in facebook_ad_accounts:
            # Criar uma tarefa para cada conta, mas limitar a execução concorrente
            tasks.append(process_account(sem, db_pool, account, since_date, until_date))
            
        # Aguardar a conclusão de todas as tarefas
        await asyncio.gather(*tasks)
            
    except Exception as e:
        logging.error(f"Error in retrieve_ad_metrics: {e}")
    finally:
        total_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"Facebook ad metrics retrieval completed in {total_time:.2f} seconds")

async def process_account(semaphore, db_pool, account, since_date, until_date):
    """Processa uma única conta com controle de concorrência"""
    async with semaphore:  # Limita o número de contas processadas simultaneamente
        account_start = datetime.now()
        account_id = account[0]  # ads_accounts_id from database
        ref_account_id = account[1]  # Facebook's ad account ID
        bm_id = account[2]  # Business Manager ID
        access_token = account[3]  # Access token
        
        logging.info(f"Processing ad account {ref_account_id} (DB ID: {account_id})")
        
        # Fetch ad data from Facebook
        ad_data = fetch_ad_data(ref_account_id, access_token, since_date, until_date)
        
        # Insert the data into the database
        if ad_data:
            inserted = await insert_ad_data(db_pool, ad_data, account_id)
            account_time = (datetime.now() - account_start).total_seconds()
            logging.info(f"Account {ref_account_id}: Successfully processed {inserted} records in {account_time:.2f} seconds")
        else:
            logging.warning(f"No data retrieved for account {ref_account_id}")