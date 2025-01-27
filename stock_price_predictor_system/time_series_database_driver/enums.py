from enum import Enum


class TimeUnit(Enum):
    SECOND = "s"
    MINUTE = "m"
    HOUR = "h"
    DAY = "d"


class AggregateFunction(Enum):
    MEAN = "mean"
    MEDIAN = "median"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    DERIVATIVE = "derivative"
    NONNEGATIVE_DERIVATIVE = "nonnegative derivative"
    DISTINCT = "distinct"
    COUNT = "count"
    INCREASE = "increase"
    SKEW = "skew"
    SPREAD = "spread"
    STDDEV = "stddev"
    FIRST = "first"
    LAST = "last"
    UNIQUE = "unique"
    SORT = "sort"
