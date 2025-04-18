from dataclasses import dataclass
from enum import Enum
from typing import Optional

@dataclass
class StatusEnum(str, Enum):
    PAID = "paid"
    WAITING_PAYMENT = "waiting_payment"
    REFUNDED = "refunded"
    COMPLETED = "completed"
    REFUSED = "refused"
    CANCELLED = "refused"
    EXPIRED = "expired"
    ABANDONED = "abandoned"
@dataclass
class AbandonedCart:
    user_email: str
    user_phone: Optional[str] = None
    user_doc: Optional[str] = None
    user_name: Optional[str] = None
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    created_at: Optional[str] = None
    user_ip: Optional[str] = None
    user_city: Optional[str] = None
    user_region: Optional[str] = None
    user_postal_code: Optional[str] = None
    transaction_value: Optional[float] = None
    offer_id: Optional[str] = None
    offer_name: Optional[str] = None
    status: StatusEnum = StatusEnum.ABANDONED.value
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None
    utm_term: Optional[str] = None
    utm_target: Optional[str] = None
    sck: Optional[str] = None
    src: Optional[str] = None
    producer_name: Optional[str] = None
    

@dataclass
class Transaction:
    transaction_id: str
    created_at: str
    updated_at: str
    order_date: str
    currency: str
    status: StatusEnum
    payment_method: str
    user_email: str
    user_name: str
    user_phone: str
    user_country: str
    user_ip: str
    user_city: str
    user_region: str
    user_postal_code: str
    product_id: str
    product_name: str
    product_type: str
    product_price: float
    transaction_value: float
    transaction_fee_total: float
    transaction_net_value: float
    installments: int
    quantity: int
    offer_id: Optional[str] = None
    offer_name: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_content: Optional[str] = None
    utm_term: Optional[str] = None
    utm_target: Optional[str] = None
    sck: Optional[str] = None
    src: Optional[str] = None
    cycle: Optional[int] = None
    producer_name: Optional[str] = None
    affiliate: Optional[dict] = None
    subscription: Optional[dict] = None
    affiliate_commission: Optional[float] = None
    audit_original_payment_method: Optional[str] = None
    audit_original_status: Optional[str] = None