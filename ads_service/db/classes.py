from dataclasses import dataclass
from typing import Optional

@dataclass
class AdMetrics:
    id: int
    ads_accounts_id: int
    campaign_id: str
    campaign_name: str
    ad_group_id: str
    ad_group_name: str
    ad_id: str
    ad_name: str
    impressions: int
    clicks: int
    cost: float
    currency: str
    date: str
    page_view: Optional[int]
    initiate_checkout: Optional[int]
    reach: Optional[int]
    three_second_video_view: Optional[int]
    fifty_video_view: Optional[int]
    seventy_five_video_view: Optional[int]
    conversions: Optional[int]
    
    
@dataclass
class AdAccounts:
    ref_account_id: int
    bm_id: int
    credential_id: int