from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List


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
