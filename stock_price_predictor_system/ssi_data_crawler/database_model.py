from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(kw_only=True)
class Market:
    ID: int
    Symbol: str
    Name: str
    EnName: str
    CreateDate: datetime
    UpdateDate: Optional[datetime] = None
    DeleteDate: Optional[datetime] = None


@dataclass(kw_only=True)
class SecurityType:
    ID: int
    Symbol: str
    Name: str
    EnName: str
    CreateDate: datetime
    UpdateDate: Optional[datetime] = None
    DeleteDate: Optional[datetime] = None


from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass(kw_only=True)
class Security:
    ID: int
    Symbol: str
    Name: Optional[str] = None
    EnName: Optional[str] = None
    ListedShare: Optional[int] = None
    MarketCapitalization: Optional[int] = None
    Market_ID: int
    SecurityType_ID: Optional[int] = None
    CreateDate: datetime
    UpdateDate: Optional[datetime] = None
    DelistDate: Optional[datetime] = None

    @classmethod
    def get_key_list(cls):
        return [field.name for field in cls.__dataclass_fields__.values()]
