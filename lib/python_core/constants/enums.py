from enum import Enum


class Date_STUCT(Enum):
    YYYY = "%Y"
    YYYY_MM = "%Y-%m"
    YYYY_MM_DD = "%Y-%m-%d"


class Time_STUCT(Enum):
    HH = "%H"
    HH_MM = "%H:%M"
    HH_MM_SS = "%H:%M:%S"
