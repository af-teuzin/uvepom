from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from db.classes import AdMetrics
from datetime import datetime, timedelta
import os
import logging
import db.db as db
from google.oauth2 import service_account
from google.ads.googleads.oauth2 import ServiceAccountCreds
from pathlib import Path
from .credentials import check_credentials_exist, setup_google_ads_credentials

logging.basicConfig(level=logging.INFO)

# Versão específica da API para evitar incompatibilidades
API_VERSION = "v11"

def init_google_ads_client():
    """
    Inicializa o cliente do Google Ads usando o caminho do arquivo YAML.
    Verifica se os arquivos de credenciais existem primeiro.
    """
    current_dir = Path(__file__).parent.absolute()
    yaml_path = current_dir / "google_ads.yaml"
    
    # Verify if credentials exist
    if not check_credentials_exist():
        logging.info("Credential files not found. Generating from environment variables...")
        setup_google_ads_credentials()
    
    # Load the client from the YAML file
    logging.info(f"Loading Google Ads client from yaml file: {yaml_path}")
    return GoogleAdsClient.load_from_storage(yaml_path)

# Fix the path to use a relative path based on the current script's location
async def retrieve_ad_metrics(pool, since_date, until_date):
    ad_accounts = await db.retrieve_google_ad_accounts(pool)
    
    # Inicializar o cliente com o caminho correto da conta de serviço
    client = init_google_ads_client()
    google_ads_service = client.get_service("GoogleAdsService")

    for ad_account in ad_accounts:
        ref_account_id = str(ad_account[1])
        account_id = str(ad_account[0])
        logging.info(f"Processing Google Ads account {ref_account_id} (DB ID: {account_id})")
        
        query = f"""
            SELECT
            customer.descriptive_name,
            customer.id,
            campaign.name,
            campaign.id,
            ad_group.name,
            ad_group.id,
            ad_group_ad.ad.name,
            ad_group_ad.ad.id,
            metrics.clicks,
            metrics.impressions,
            metrics.cost_micros,
            segments.date,
            metrics.conversions
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{since_date}' AND '{until_date}'
            """

        try:
            # Usar search em vez de search_stream para evitar o erro '_FailureOutcome'
            response = google_ads_service.search(customer_id=ref_account_id, query=query)
            
            # Coletar métricas para inserção em lote
            metrics_batch = []
            
            for row in response:
                try:
                    # Extrair campos com segurança
                    campaign_id = str(row.campaign.id) if hasattr(row, 'campaign') else ''
                    campaign_name = str(row.campaign.name) if hasattr(row, 'campaign') else ''
                    ad_group_id = str(row.ad_group.id) if hasattr(row, 'ad_group') else ''
                    ad_group_name = str(row.ad_group.name) if hasattr(row, 'ad_group') else ''
                    ad_id = str(row.ad_group_ad.ad.id) if hasattr(row, 'ad_group_ad') and hasattr(row.ad_group_ad, 'ad') else ''
                    ad_name = str(row.ad_group_ad.ad.name) if hasattr(row, 'ad_group_ad') and hasattr(row.ad_group_ad, 'ad') else ''
                    
                    impressions = row.metrics.impressions if hasattr(row, 'metrics') else 0
                    clicks = row.metrics.clicks if hasattr(row, 'metrics') else 0
                    cost_micros = row.metrics.cost_micros if hasattr(row, 'metrics') else 0
                    date = row.segments.date if hasattr(row, 'segments') else ''
                    conversions = row.metrics.conversions if hasattr(row, 'metrics') else 0
                    # Criar objeto AdMetrics
                    ad_metric = AdMetrics(
                        id=0,
                        ads_accounts_id=account_id,
                        campaign_id=campaign_id,
                        campaign_name=campaign_name,
                        ad_group_id=ad_group_id,
                        ad_group_name=ad_group_name,
                        ad_id=ad_id,
                        ad_name=ad_name,
                        impressions=impressions,
                        clicks=clicks,
                        cost=float(cost_micros) / 1e6,
                        currency="BRL",
                        date=date,
                        page_view=None,
                        initiate_checkout=None,
                        reach=None,
                        three_second_video_view=None,
                        fifty_video_view=None,
                        seventy_five_video_view=None,
                        conversions=conversions
                    )
                    
                    metrics_batch.append(ad_metric)
                except Exception as row_error:
                    logging.error(f"Error processing row: {row_error}")
            
            # Inserir métricas em lote
            if metrics_batch:
                inserted = await db.insert_google_ad_metrics_batch(pool, metrics_batch)
                logging.info(f"Successfully inserted {inserted} Google Ads metrics for account {ref_account_id}")
            else:
                logging.warning(f"No metrics to insert for account {ref_account_id}")
                
        except Exception as e:
            logging.error(f"Error processing Google Ads account {ref_account_id}: {e}")
    
    logging.info("Google Ads metrics retrieval completed")

# Tester function
def test_connection():
    """
    Função para testar a conexão com a API do Google Ads.
    Executa uma consulta simples para verificar se a autenticação está funcionando.
    """
    try:
        # Generate credentials if they don't exist yet
        if not check_credentials_exist():
            setup_google_ads_credentials()
        
        client = init_google_ads_client()
        logging.info("Cliente Google Ads inicializado com sucesso!")
        
        # Obter o serviço GoogleAdsService
        google_ads_service = client.get_service("GoogleAdsService")
        
        # ID da conta do cliente para teste (usando login_customer_id do ambiente)
        customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        
        # Consulta simples para testar a conexão - pegando apenas o nome da conta
        query = """
            SELECT
                customer.descriptive_name
            FROM
                customer
            LIMIT 1
        """
        
        # Executar a consulta
        response = google_ads_service.search(customer_id=customer_id, query=query)
        
        # Verificar a resposta
        for row in response:
            logging.info(f"Conexão bem-sucedida! Nome da conta: {row.customer.descriptive_name}")
            return True
        
        logging.info("Conexão bem-sucedida, mas não retornou dados.")
        return True
    except Exception as e:
        logging.error(f"Erro ao testar conexão: {e}")
        return False

# Para executar o teste diretamente
if __name__ == "__main__":
    test_connection()