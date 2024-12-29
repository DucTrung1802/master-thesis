from dataclasses import dataclass


@dataclass
class Context:
    className: str
    methodName: str
