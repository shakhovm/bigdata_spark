from helper_functions import to_datetime


def compare_dates(x, y):
    if to_datetime(x["start_date"]) > to_datetime(y["start_date"]):
        x["start_date"] = y["start_date"]
    if to_datetime(x["end_date"]) < to_datetime(y["end_date"]):
        x["end_date"] = y["end_date"]
    x["video_stats"] += y["video_stats"]
    x["total_views"] += y["total_views"]
    return x


def to_appropriate(x):
    x[1]["channel_name"] = x[0]
    return x[1]

