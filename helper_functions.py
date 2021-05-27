from datetime import datetime


def to_datetime(str_date) -> datetime:
    return datetime.strptime(str_date, "%y.%d.%m")


def from_datetime(datetime_date: datetime) -> str:
    return datetime.strftime(datetime_date, "%y.%d.%m")

