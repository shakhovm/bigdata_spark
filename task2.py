from datetime import timedelta

from helper_functions import to_datetime, from_datetime


def devide_by_weeks(x):
    current_date = to_datetime(x["trending_date"])
    current_weekday = current_date.weekday()
    start_day = current_date - timedelta(days=current_weekday)
    end_day = current_date + timedelta(days=6 - current_weekday)
    return (from_datetime(start_day), x["category_id"], x["video_id"]), \
           {
               "start_day": from_datetime(start_day),
               "end_day": from_datetime(end_day),
               "first_date": x["trending_date"],
               "last_date": x["trending_date"],
               "first_views": int(x["views"]),
               "last_views": int(x["views"]),
               "video_count": 1
           }


def compare_dates(x, y):
    if to_datetime(x["first_date"]) > to_datetime(y["first_date"]):
        x["first_date"] = y["first_date"]
        x["first_views"] = y["first_views"]
    if to_datetime(x["last_date"]) < to_datetime(y["last_date"]):
        x["last_date"] = y["last_date"]
        x["last_views"] = y["last_views"]
    x["video_count"] += 1
    return x


def count_videos(x, y):
    x["video_id"] += y["video_id"]
    x["number_of_videos"] += 1
    x["total_views"] += y["total_views"]
    return x


def to_appropriate_format(x):
    return x[1]