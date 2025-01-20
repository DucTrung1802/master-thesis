from pydantic import BaseModel, ConfigDict, Field, field_validator
import datetime
from enum import Enum
from typing import List, Literal


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


class BaseInputModel(BaseModel):
    pageIndex: int = Field(default=1, ge=1, le=10)
    pageSize: Literal[10, 20, 50, 100, 1000] = 10


# Base Models
class BaseOutputModel(BaseModel):
    message: str
    status: int
    totalRecord: int


# POST AccessToken Models
class AccessTokenInputModel(BaseModel):
    consumerID: str
    consumerSecret: str


class AccessTokenDataModel(BaseModel):
    accessToken: str


class AccessTokenOutputModel(BaseOutputModel):
    data: AccessTokenDataModel


# GET Securities Models
class SecuritiesInputModel(BaseInputModel):
    market: str = Field(...)

    @field_validator("market")
    def validate_market(cls, value):
        if value not in [m.value for m in Market_5]:
            raise ValueError(
                f"'{value}' is not a valid Market_5 value. Allowed values: {[m.value for m in Market_5]}"
            )
        return value


class SecuritiesDataModel(BaseModel):
    market: Market_4
    symbol: str
    stockName: str
    stockEnName: str


class SecuritiesOutputModel(BaseOutputModel):
    data: List[SecuritiesDataModel]


# GET SecuritiesDetails Models
class SecuritiesDetailsInputModel(BaseInputModel):
    market: Market_4
    symbol: str


class SecuritiesDetailsDataRepeatedInfoModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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


class SecuritiesDetailsDataModel(BaseModel):
    RType: str
    reportDate: str  # dd/MM/yyyy
    totalNoSym: int
    repeatedinfoList: List[SecuritiesDetailsDataRepeatedInfoModel]


class SecuritiesDetailsOutputModel(BaseOutputModel):
    data: List[SecuritiesDetailsDataModel]


# GET IndexComponents
class IndexComponentsInputModel(BaseInputModel):
    indexCode: str


class IndexComponentsDataIndexComponentModel(BaseModel):
    isin: str
    stockSymbol: str


class IndexComponentsDataModel(BaseModel):
    indexCode: str
    indexName: str
    exchange: Exchange_2
    totalSymbolNo: int
    indexComponent: List[IndexComponentsDataIndexComponentModel]


class IndexComponentsOutputModel(BaseOutputModel):
    data: List[IndexComponentsDataModel]


# GET IndexList
class IndexListInputModel(BaseInputModel):
    exchange: Exchange_5


class IndexListDataModel(BaseModel):
    indexCode: str
    indexName: str
    exchange: Exchange_5


class IndexListOutputModel(BaseOutputModel):
    data: List[IndexListDataModel]


# GET DailyOhlc
class DailyOhlcInputModel(BaseInputModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbol: str
    fromDate: datetime
    toDate: datetime
    ascending: bool


class DailyOhlcDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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


class DailyOhlcOutputModel(BaseOutputModel):
    data: List[DailyOhlcDataModel]


# GET IntradayOhlc
class IntradayOhlcInputModel(BaseInputModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbol: str
    fromDate: datetime
    toDate: datetime
    ascending: bool
    resollution: int = 1


class IntradayOhlcDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbol: str
    market: Market_3
    tradingDate: datetime
    time: datetime
    open: int
    high: int
    low: int
    close: int
    volume: int


class IntradayOhlcOutputModel(BaseOutputModel):
    data: List[IntradayOhlcDataModel]


# GET DailyIndex
class DailyIndexInputModel(BaseInputModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    indexId: str
    fromDate: datetime
    toDate: datetime
    ascending: bool


class DailyIndexDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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


class DailyIndexOutputModel(BaseOutputModel):
    data: List[DailyIndexDataModel]


# GET DailyStockPrice
class DailyStockPriceInputModel(BaseInputModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    symbol: str
    fromDate: datetime
    toDate: datetime
    market: Market_5


class DailyStockPriceDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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


class DailyStockPriceOutputModel(BaseModel):
    data: List[DailyStockPriceDataModel]
