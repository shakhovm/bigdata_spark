
def add_videos(x, y):

    if len(x["videos"]) < 10:
        x["videos"] += y["videos"]
    return x
