from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Literal, Optional
from .enums import *


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
    market: int
    symbol: str
    stockName: str
    stockEnName: str


@dataclass
class SecuritiesOutputModel(BaseOutputModel):
    data: List[SecuritiesDataModel]


# GET SecuritiesDetails Models
@dataclass
class SecuritiesDetailsInputModel(BaseInputModel):
    market: int
    symbol: str


@dataclass
class SecuritiesDetailsDataRepeatedInfoModel:
    isin: str
    symbol: str
    symbolName: str
    symbolEngName: str
    secType: SecurityType
    marketId: int
    exchange: int
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
    exchange: int
    totalSymbolNo: int
    indexComponent: List[IndexComponentsDataIndexComponentModel]


@dataclass
class IndexComponentsOutputModel(BaseOutputModel):
    data: List[IndexComponentsDataModel]


# GET IndexList
@dataclass
class IndexListInputModel(BaseInputModel):
    exchange: int


@dataclass
class IndexListDataModel:
    indexCode: str
    indexName: str
    exchange: int


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
    market: int
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
    market: int
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
    market: int


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
