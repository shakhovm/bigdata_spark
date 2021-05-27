def add_trending_days(x, y):
    x["trending_days"] += y["trending_days"]
    return x


def add_video_days(x, y):
    x["total_trending_days"] += y["total_trending_days"]
    x["video_days"] += y["video_days"]
    return x
