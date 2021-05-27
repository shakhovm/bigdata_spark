from helper_functions import to_datetime, from_datetime
from datetime import timedelta


def videos_count(x: dict, y: dict):
    x["number_of_videos"] += y["number_of_videos"]
    x["video_ids"] += y["video_ids"]
    return x


def tags_count(x, y):
    if len(x["tags"]) < 10:
        x["tags"] += y["tags"]
    return x


def to_appropriate_format(x):
    start_date = x[0][:2] + '.01' + x[0][2:]
    x[1]["start_date"] = start_date
    return x[1]