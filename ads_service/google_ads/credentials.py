import os
import json
from pathlib import Path
import logging

def setup_google_ads_credentials():
    """
    Creates Google Ads credential files from environment variables.
    This function should be called during application startup.
    """
    logging.info("Setting up Google Ads credentials from environment variables...")
    current_dir = Path(__file__).parent.absolute()
    
    # Create service account key file dynamically
    try:
        service_account_data = {
            "type": "service_account",
            "project_id": os.getenv("GOOGLE_PROJECT_ID"),
            "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            "private_key": os.getenv("GOOGLE_PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
            "universe_domain": "googleapis.com"
        }

        key_path = current_dir / "service-account-key.json"
        with open(key_path, "w") as f:
            json.dump(service_account_data, f, indent=2)
        
        logging.info(f"Created service account key file at {key_path}")
        
        # Create Google Ads YAML config
        yaml_content = f"""# Developer token
developer_token: {os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')}

# Use proto plus
use_proto_plus: True

# Service Account configuration
json_key_file_path: {key_path}
impersonated_email: {os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')}

login_customer_id: {os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')}
"""

        yaml_path = current_dir / "google_ads.yaml"
        with open(yaml_path, "w") as f:
            f.write(yaml_content)
        
        logging.info(f"Created Google Ads YAML config at {yaml_path}")
        
        # Verify credentials are present
        credentials_ok = all([
            os.getenv("GOOGLE_PROJECT_ID"),
            os.getenv("GOOGLE_PRIVATE_KEY_ID"),
            os.getenv("GOOGLE_PRIVATE_KEY"),
            os.getenv("GOOGLE_CLIENT_EMAIL"),
            os.getenv("GOOGLE_CLIENT_ID"),
            os.getenv("GOOGLE_CLIENT_X509_CERT_URL"),
            os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
            os.getenv("GOOGLE_ADS_IMPERSONATED_EMAIL"),
            os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        ])
        
        if not credentials_ok:
            logging.warning("Some Google Ads credentials are missing. Authentication may fail.")
        else:
            logging.info("All required Google Ads credentials are present.")
        
        return True
    
    except Exception as e:
        logging.error(f"Error setting up Google Ads credentials: {e}")
        return False

def check_credentials_exist():
    """
    Check if the credential files exist
    """
    current_dir = Path(__file__).parent.absolute()
    key_path = current_dir / "service-account-key.json"
    yaml_path = current_dir / "google_ads.yaml"
    
    return key_path.exists() and yaml_path.exists() 