import datetime
from db.classes.classes import Transaction, StatusEnum, AbandonedCart
from db.db import insert_transaction, insert_abandoned_cart
import requests
from utils.phone_formatting import process_phone_number
import json
import logging
def format_datetime(datetime_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")

def retrieve_ip_info(ip: str) -> dict:
    logging.info(f"Retrieving IP info for {ip}")
    try:
        response = requests.get(f"https://ipinfo.io/{ip}/json")
        data = response.json()
        logging.info(f"Response: {data}")
        return {
            "country": data.get("country", ""),
            "city": data.get("city", ""),
            "region": data.get("region", ""),
            "postal_code": data.get("postal", ""),
        }
    except Exception as e:
        logging.error(f"Error retrieving IP info: {e}")
        return {
            "country": "",
            "city": "",
            "region": "",
            "postal_code": "",
        }

def retrieve_affiliate_info(commissioned_stores: list) -> dict:
    for commission in commissioned_stores:
        if commission.get("type") == "affiliate":
            return {
                "id": commission.get("id"),
                "name": commission.get("custom_name"),
                "email": commission.get("email"),
                "commission": commission.get("value"),
            }
    return None


class Processor:
    def normalize_payload(self, payload: dict) -> dict:
        """Normalize payload based on type"""
        if payload.get("order_status") is None:
            return self.normalize_abandoned_cart(payload)
        else:
            return self.normalize_transaction(payload)
    
    def normalize_abandoned_cart(self, payload: dict) -> AbandonedCart:
        """Extract and normalize abandoned cart data from webhook payload"""
        payload = payload
        if payload.get("ip"):
            ip_info = retrieve_ip_info(payload.get("ip"))
        else:
            ip_info = {
                "country": "",
                "city": "",
                "region": "",
                "postal_code": "",
            }
        return AbandonedCart(
            user_email=payload.get("email") if payload.get("email") else None,
            user_phone=process_phone_number(payload.get("phone")) if payload.get("phone") else None,
            user_doc=payload.get("document") if payload.get("document") else None,
            user_name=payload.get("name") if payload.get("name") else None,
            product_id=payload.get("product_id") if payload.get("product_id") else None,
            product_name=payload.get("product_name") if payload.get("product_name") else None,
            created_at = datetime.datetime.now().isoformat(),
            user_ip = payload.get("ip") if payload.get("ip") else None,
            user_city = ip_info.get("city") if ip_info.get("city") else None,
            user_region = ip_info.get("region") if ip_info.get("region") else None,
            user_postal_code = ip_info.get("postal_code") if ip_info.get("postal_code") else None,
            transaction_value=None,
            offer_id=None,
            offer_name=payload.get("offer_name") if payload.get("offer_name") else None,
            status=StatusEnum.ABANDONED.value,
            utm_source=payload.get("utm_source") if payload.get("utm_source") else None,
            utm_medium=payload.get("utm_medium") if payload.get("utm_medium") else None,
            utm_campaign=payload.get("utm_campaign") if payload.get("utm_campaign") else None,
            utm_content=payload.get("utm_content") if payload.get("utm_content") else None,
            utm_term=payload.get("utm_term") if payload.get("utm_term") else None,
            utm_target=payload.get("utm_target") if payload.get("utm_target") else None,
            sck=payload.get("sck") if payload.get("sck") else None,
            src=payload.get("src") if payload.get("src") else None,
            producer_name=payload.get("producer_name") if payload.get("producer_name") else None,
        )
    
    def normalize_transaction(self, payload: dict) -> Transaction:
        """Extract and normalize transaction data from webhook payload"""
        transaction = payload
        customer = transaction.get("Customer", {})
        product = transaction.get("Product", {})
        tracking = transaction.get("TrackingParameters", {})
        commissions = transaction.get("Commissions", {})
        affiliate_info = retrieve_affiliate_info(commissions.get("commissioned_stores", []))
        subscription = transaction.get("Subscription", {}) if 'Subscription' in transaction else None
        # Handle missing or null values with defaults
        status_value = transaction.get("order_status", "waiting_payment")
        if not status_value or status_value not in [e.value for e in StatusEnum]:
            status_value = "status_not_mapped"
        
        if customer.get("ip", ""):
            ip_info = retrieve_ip_info(customer.get("ip", ""))
            logging.info(f"IP info: {ip_info}")
        else:
            ip_info = {
                "country": "",
                "city": "",
                "region": "",
                "postal_code": "",
            }
            logging.info(f"No IP provided, using default IP info: {ip_info}")
        return Transaction(
            transaction_id=transaction.get("order_ref", ""),
            created_at=format_datetime(transaction.get("created_at", datetime.datetime.now().isoformat())),
            updated_at=datetime.datetime.now().isoformat(),
            order_date=format_datetime(transaction.get("created_at", datetime.datetime.now().isoformat())),
            currency=commissions.get("product_base_price_currency", "BRL"),
            status=StatusEnum(status_value).value,
            payment_method=transaction.get("payment_method", ""),  # Default to OTHER if not mapped
            user_email=customer.get("email", ""),
            user_name=customer.get("full_name", ""),
            user_phone=process_phone_number(customer.get("mobile", "")),
            user_country=ip_info.get("country", None),
            user_ip=customer.get("ip", None),
            user_city=ip_info.get("city", None),
            user_region=ip_info.get("region", None),
            user_postal_code=ip_info.get("postal_code", None),
            product_id=product.get("product_id", ""),
            product_name=product.get("product_name", ""),
            product_type=transaction.get("product_type", ""),
            product_price=float(commissions.get("product_base_price", 0)) / 100,
            transaction_value=float(commissions.get("charge_amount", 0)) / 100,
            transaction_fee_total=float(commissions.get("kiwify_fee", 0)) / 100,
            transaction_net_value=float(commissions.get("my_commission", 0)) / 100,
            installments=transaction.get("installments", None),
            quantity=transaction.get("quantity", None),
            offer_id=product.get("product_offer_id", None),
            offer_name=product.get("product_offer_name", None),
            utm_source=tracking.get("utm_source", None),
            utm_medium=tracking.get("utm_medium", None),
            utm_campaign=tracking.get("utm_campaign", None),
            utm_content=tracking.get("utm_content", None),
            utm_term=tracking.get("utm_term", None),
            utm_target=tracking.get("utm_target", None),
            sck=tracking.get("sck", None),
            src=tracking.get("src", None),
            cycle=None,  # No cycle information in the payload
            producer_name=product.get("producer_name", None),
            affiliate=json.dumps(affiliate_info) if affiliate_info else None,
            subscription=json.dumps(subscription) if subscription else None,
            affiliate_commission=float(affiliate_info.get("commission", 0)) / 100 if affiliate_info else None,
            audit_original_payment_method=transaction.get("payment_method", ""),
            audit_original_status=transaction.get("order_status", ""),
        )
