from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from .enums import *


@dataclass
class InfluxdbAuthentication:
    url: str
    org: str
    token: str


@dataclass
class PointComponent:
    measurement: str
    tags: Dict
    fields: Dict
    time: datetime


@dataclass
class WriteComponent:
    bucket: str
    point_component_list: List[PointComponent]


@dataclass
class TimeInterval:
    value: int
    time_unit: TimeUnit

    def format_time(self):
        return f"-{abs(self.value)}{self.time_unit.value}"


@dataclass
class ReadComponent:
    bucket: str
    start_time: datetime | TimeInterval
    end_time: datetime = None
    measurement: str = None
    tags: Dict = None
    fields: Dict = None
    aggregate_interval: TimeInterval = None
    aggregate_function: AggregateFunction = None
