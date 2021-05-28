from datetime import datetime, timedelta


def to_datetime(str_date) -> datetime:
    return datetime.strptime(str_date, "%y.%d.%m")


def from_datetime(datetime_date: datetime) -> str:
    return datetime.strftime(datetime_date, "%y.%d.%m")


def trending_days(x, y):
    x["trending_days"] += y["trending_days"]

    if to_datetime(x["latest_date"]) < to_datetime(y["latest_date"]):
        x["latest_date"] = y["latest_date"]
        x["latest_views"] = y["latest_views"]
        x["latest_likes"] = y["latest_likes"]
        x["latest_dislikes"] = y["latest_dislikes"]
    return x


def dct_creator(x):
    dct = {
        "id": x["video_id"],
        "title": x["title"],
        "channel_title": x["channel_title"],
        "description": x["description"],
        "latest_views": x["views"],
        "latest_likes": x["likes"],
        "latest_dislikes": x["dislikes"],
        "latest_date": x["trending_date"],
        "trending_days": [
            {
                "date": x["trending_date"],
                "views": x["views"],
                "likes": x["likes"],
                "dislikes": x["dislikes"]
            }
        ]
    }

    return x["video_id"], dct


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


def compare_dates_and_add(x, y):
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


def add_trending_days(x, y):
    x["trending_days"] += y["trending_days"]
    return x


def add_video_days(x, y):
    x["total_trending_days"] += y["total_trending_days"]
    x["video_days"] += y["video_days"]
    return x


def add_videos(x, y):

    if len(x["videos"]) < 10:
        x["videos"] += y["videos"]
    return x


