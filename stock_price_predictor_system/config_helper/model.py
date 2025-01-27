from pydantic import BaseModel, Field


class GeneralConfig(BaseModel):
    app_name: str = Field(default="StockPricePredictorSystem")
    author: str = Field(default="Trung Ly Duc")
    version: str = Field(default="0.1.0")


class SsiCrawlerInfoConfig(BaseModel):
    auth_type: str = Field(default="Bearer")
    consumerID: str = Field(...)
    consumerSecret: str = Field(...)
    url: str = Field(default="https://fc-data.ssi.com.vn/")
    stream_url: str = Field(default="https://fc-datahub.ssi.com.vn")


class RelationalDatabaseConfig(BaseModel):
    server_name: str = Field(...)
    login: str = Field(...)
    password: str = Field(...)


class TimeSeriesDatabaseConfig(BaseModel):
    url: str = Field(...)
    org: str = Field(...)
    token: str = Field(...)


class ConfigModel(BaseModel):
    general: GeneralConfig
    ssi_crawler_info: SsiCrawlerInfoConfig
    relational_database: RelationalDatabaseConfig
    time_series_database: TimeSeriesDatabaseConfig
