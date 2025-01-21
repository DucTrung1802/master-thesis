from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional


class Market_3(Enum):
    """
    Represents 03 base financial markets in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange
    UPCOM = "UPCOM"  # Unlisted Public Company Market


class Market_4(Enum):
    """
    Represents Market_3.
    Add DER.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
    """

    HOSE = Market_3.HOSE.value
    HNX = Market_3.HNX.value
    UPCOM = Market_3.UPCOM.value

    DER = "DER"  # Derivatives Market


class Market_5(Enum):
    """
    Represents Market_4.
    Add BOND.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
        BOND: Bond Market.
    """

    HOSE = Market_4.HOSE.value
    HNX = Market_4.HNX.value
    UPCOM = Market_4.UPCOM.value
    DER = Market_4.DER.value

    BOND = "BOND"  # Bond Market


class Exchange_2(Enum):
    """
    Represents 02 base financial markets in Vietnam.

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
    """

    HOSE = "HOSE"  # Ho Chi Minh City Stock Exchange
    HNX = "HNX"  # Hanoi Stock Exchange


class Exchange_5(Enum):
    """
    Represents Exchange_2.
    Add HNXBOND, UPCOM, DER

    Attributes:
        HOSE: Ho Chi Minh City Stock Exchange.
        HNX: Hanoi Stock Exchange.
        HNXBOND: Hanoi Stock Bond Market.
        UPCOM: Unlisted Public Company Market.
        DER: Derivatives Market.
    """

    HOSE = Exchange_2.HOSE.value
    HNX = Exchange_2.HNX.value

    HNXBOND = "HNXBOND"
    UPCOM = "UPCOM"
    DER = "DER"


class SecurityType(Enum):
    ST = "Stock"
    CW = "Covered Warrant"
    FU = "Futures"
    EF = "ETF"
    BO = "BOND"
    OF = "OEF"
    MF = "Mutual Fund"


class TradingSession(Enum):
    ATO = "Opening Call Auction"
    LO = "Continuous Trading"
    ATC = "Closing All Auction"
    PT = "Putthrough"
    C = "Market Close"
    BREAK = "Lunch Break"
    HALT = "Market Halt"


@dataclass(kw_only=True)
class BaseInputModel:
    pageIndex: int = field(default=1, metadata={"ge": 1, "le": 10})
    pageSize: Literal[10, 20, 50, 100, 1000] = 10


# Base Models
@dataclass(kw_only=True)
class BaseOutputModel:
    message: str
    status: int
    totalRecord: int


# POST AccessToken Models
@dataclass
class AccessTokenInputModel:
    consumerID: str
    consumerSecret: str


@dataclass
class AccessTokenDataModel:
    accessToken: str


@dataclass
class AccessTokenOutputModel(BaseOutputModel):
    data: AccessTokenDataModel


# GET Securities Models
@dataclass
class SecuritiesInputModel(BaseInputModel):
    market: Optional[str] = None


@dataclass
class SecuritiesDataModel:
    market: Market_4
    symbol: str
    stockName: str
    stockEnName: str


@dataclass
class SecuritiesOutputModel(BaseOutputModel):
    data: List[SecuritiesDataModel]


# GET SecuritiesDetails Models
@dataclass
class SecuritiesDetailsInputModel(BaseInputModel):
    market: Market_4
    symbol: str


@dataclass
class SecuritiesDetailsDataRepeatedInfoModel:
    isin: str
    symbol: str
    symbolName: str
    symbolEngName: str
    secType: SecurityType
    marketId: Market_5
    exchange: Market_5
    issuer: str
    lotSize: int
    issueDate: datetime
    maturityDate: datetime
    firstTradingDate: datetime
    lastTradingDate: datetime
    contractMultiplier: str
    settlMethod: str
    underlying: str
    putOrCall: str
    exercisePrice: float
    exerciseStyle: str
    excerciseRatio: float
    listedShare: int
    tickPrice1: int
    tickIncrement1: int
    tickPrice2: int
    tickIncrement2: int
    tickPrice3: int
    tickIncrement3: int
    tickPrice4: int
    tickIncrement4: int


@dataclass
class SecuritiesDetailsDataModel:
    RType: str
    reportDate: str  # dd/MM/yyyy
    totalNoSym: int
    repeatedinfoList: List[SecuritiesDetailsDataRepeatedInfoModel]


@dataclass
class SecuritiesDetailsOutputModel(BaseOutputModel):
    data: List[SecuritiesDetailsDataModel]


# GET IndexComponents
@dataclass
class IndexComponentsInputModel(BaseInputModel):
    indexCode: str


@dataclass
class IndexComponentsDataIndexComponentModel:
    isin: str
    stockSymbol: str


@dataclass
class IndexComponentsDataModel:
    indexCode: str
    indexName: str
    exchange: Exchange_2
    totalSymbolNo: int
    indexComponent: List[IndexComponentsDataIndexComponentModel]


@dataclass
class IndexComponentsOutputModel(BaseOutputModel):
    data: List[IndexComponentsDataModel]


# GET IndexList
@dataclass
class IndexListInputModel(BaseInputModel):
    exchange: Exchange_5


@dataclass
class IndexListDataModel:
    indexCode: str
    indexName: str
    exchange: Exchange_5


@dataclass
class IndexListOutputModel(BaseOutputModel):
    data: List[IndexListDataModel]


# GET DailyOhlc
@dataclass
class DailyOhlcInputModel(BaseInputModel):
    symbol: str
    fromDate: datetime
    toDate: datetime
    ascending: bool


@dataclass
class DailyOhlcDataModel:
    symbol: str
    market: Market_5
    tradingDate: datetime
    time: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int
    value: float


@dataclass
class DailyOhlcOutputModel(BaseOutputModel):
    data: List[DailyOhlcDataModel]


# GET IntradayOhlc
@dataclass
class IntradayOhlcInputModel(BaseInputModel):
    symbol: str
    fromDate: datetime
    toDate: datetime
    ascending: bool
    resollution: int = 1


@dataclass
class IntradayOhlcDataModel:
    symbol: str
    market: Market_3
    tradingDate: datetime
    time: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int


@dataclass
class IntradayOhlcOutputModel(BaseOutputModel):
    data: List[IntradayOhlcDataModel]


# GET DailyIndex
@dataclass
class DailyIndexInputModel(BaseInputModel):
    indexId: str
    fromDate: datetime
    toDate: datetime
    ascending: bool


@dataclass
class DailyIndexDataModel:
    indexCode: str
    indexValue: float
    tradingDate: datetime
    time: datetime
    change: float
    ratioChange: float
    totalTrade: int
    totalMatchVol: int
    totalMatchVal: int
    typeIndex: str
    indexName: str
    advances: int
    noChanges: int
    declines: int
    ceilings: int
    floors: int
    totalDealVol: int
    totalDealVal: int
    totalVol: int
    totalVal: int
    tradingSession: TradingSession


@dataclass
class DailyIndexOutputModel(BaseOutputModel):
    data: List[DailyIndexDataModel]


# GET DailyStockPrice
@dataclass
class DailyStockPriceInputModel(BaseInputModel):
    symbol: str
    fromDate: datetime
    toDate: datetime
    market: Market_5


@dataclass
class DailyStockPriceDataModel:
    symbol: str
    tradingDate: datetime
    time: datetime
    priceChange: int
    perPriceChange: float
    ceilingPrice: int
    floorPrice: int
    refPrice: int
    openPrice: int
    highestPrice: int
    lowestPrice: int
    closePrice: int
    averagePrice: int
    closePriceAdjusted: int
    totalMatchVol: int
    totalMatchVal: int
    totalDealVal: int
    totalDealVol: int
    foreignBuyVolTotal: int
    foreignCurrentRoom: int
    foreignSellVolTotal: int
    foreignBuyValTotal: int
    foreignSellValTotal: int
    totalBuyTrade: int
    totalBuyTradeVol: int
    totalSellTrade: int
    totalSellTradeVol: int
    netBuySellVol: int
    netBuySellVal: int
    totalTradedVol: int
    totalTradedValue: int


@dataclass
class DailyStockPriceOutputModel(BaseOutputModel):
    data: List[DailyStockPriceDataModel]
