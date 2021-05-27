from helper_functions import to_datetime


def add_trending_days(x, y):
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
