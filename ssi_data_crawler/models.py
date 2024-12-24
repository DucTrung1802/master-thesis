from ssi_fc_data.model.model import *
from dataclasses import dataclass
from enum import Enum
from typing import List


class Market(Enum):
    """
    Represents different financial markets in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
        BOND: Bond Market.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange
    UPCOM = "UPCOM"  # Unlisted Public Company Market
    DER = "DER"  # Derivatives Market
    BOND = "BOND"  # Bond Market


class Exchange(Enum):
    """
    Represents different stock exchanges in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange


# Base Models
@dataclass
class BaseOutputModel:
    message: str
    status: int
    totalRecord: int


# POST AccessToken Models
@dataclass
class AccessTokenInputModel(accessToken):
    pass


@dataclass
class AccessTokenDataModel:
    accessToken: str


@dataclass
class AccessTokenOutputModel(BaseOutputModel):
    data: AccessTokenDataModel


# GET Securities Models
@dataclass
class SecuritiesInputModel(securities):
    pass


@dataclass
class SecuritiesDataModel:
    market: Market
    symbol: str
    StockName: str
    StockEnName: str


@dataclass
class SecuritiesOutputModel(BaseOutputModel):
    data: List[SecuritiesDataModel]
