from pydantic import BaseModel, Field


class GeneralConfig(BaseModel):
    app_name: str = Field(default="StockPricePredictorSystem")
    author: str = Field(default="Trung Ly Duc")
    version: str = Field(default="0.1.0")


class SqlServerConfig(BaseModel):
    server_name: str = Field(...)
    login: str = Field(...)
    password: str = Field(...)


class Config(BaseModel):
    general: GeneralConfig
    sql_server: SqlServerConfig
