from enum import Enum


class MarketCode(Enum):
    HNX = 1
    HOSE = 2
    UPCOM = 3
    DERIVATIVES = 4
    BOND = 5

    @classmethod
    def get_market_code(cls, market_name: str) -> int:
        try:
            return cls[market_name.upper()].value
        except KeyError:
            raise ValueError(f"Invalid market name: {market_name}")


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
