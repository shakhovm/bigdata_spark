import helper_functions


class Queries:
    def __init__(self, df):
        self.df = df
        self.df_rdd = df.rdd

    def task1(self):
        popular_ids = set(self.df.groupBy("video_id")
                          .count()
                          .sort("count", ascending=False)
                          .limit(10)
                          .rdd.map(lambda x: x["video_id"]).collect())
        popular_ids_df = self.df_rdd.filter(lambda x: x["video_id"] in popular_ids) \
            .map(helper_functions.dct_creator)\
            .reduceByKey(helper_functions.trending_days)

        # print(type(popular_ids_df.take(10)))
        return list(map(lambda x: x[1], popular_ids_df.take(10)))

    def task2(self):
        df = self.df_rdd.map(helper_functions.devide_by_weeks)\
            .reduceByKey(helper_functions.compare_dates) \
            .filter(lambda x: x[1]["last_date"] != x[1]["first_date"]) \
            .map(lambda x: ((x[0][0], x[0][1]),
                            {
                                "category_id": x[0][1],
                                "video_id": [x[0][2]],
                                "number_of_videos": 1,
                                "end_day": x[1]["end_day"],
                                "start_day": x[1]["start_day"],
                                "total_views": x[1]["last_views"] - x[1]["first_views"]
                            })).reduceByKey(helper_functions.count_videos) \
            .map(lambda x: (x[0][0], x[1])) \
            .reduceByKey(lambda x, y: x if x["total_views"] > y["total_views"] else y) \
            .map(lambda x: x[1])
        return df.collect()

    def task3(self):
        df = self.df_rdd.map(
            lambda x: ((x["trending_date"][:2] + x["trending_date"][-3:], x["video_id"]), x["tags"].split('|'))) \
            .reduceByKey(lambda x, y: x) \
            .flatMapValues(lambda x: x) \
            .map(lambda x: ((x[0][0], x[1]), {
            "video_ids": [x[0][1]],
            "number_of_videos": 1

        })) \
            .reduceByKey(helper_functions.videos_count). \
            sortBy(lambda x: -x[1]["number_of_videos"]) \
            .map(lambda x: (x[0][0], {"tags": [{
            "tag": x[0][1],
            "number_of_videos": x[1]["number_of_videos"],
            "video_ids": x[1]["video_ids"]
        }]})) \
            .reduceByKey(helper_functions.tags_count)
        return df.take(10)

    def task4(self):
        df = self.df_rdd.map(lambda x: (x["video_id"], (x["channel_title"], x["trending_date"],
                                                        x["views"])))
        df = df.reduceByKey(lambda x, y: x if x[2] > y[2] else y) \
            .map(lambda x: (x[1][0], {"video_stats": [{"video_id": x[0],
                                                       "views": int(x[1][2])}],
                                      "start_date": x[1][1],
                                      "end_date": x[1][1],
                                      "total_views": int(x[1][2])})) \
            .reduceByKey(helper_functions.compare_dates_and_add).sortBy(lambda x: -x[1]["total_views"])\
            .map(helper_functions.to_appropriate)
        return df.take(20)

    def task5(self):
        df = self.df_rdd.map(lambda x: (x["video_id"], {
            "channel_title": x["channel_title"],
            "trending_days": 1,
            "video_title": x["title"]
        })).reduceByKey(helper_functions.add_trending_days).map(lambda x: (x[1]["channel_title"], {
            "video_days": [{"video_id": x[0], "video_title": x[1]["video_title"],
                            "trending_days": x[1]["trending_days"]}],
            "channel_name": x[1]["channel_title"],
            "total_trending_days": x[1]["trending_days"]

        })).reduceByKey(helper_functions.add_video_days).sortBy(lambda x: -x[1]["total_trending_days"])
        return list(map(lambda x: x[1], df.take(10)))

    def task6(self):
        df = self.df_rdd.filter(lambda x: int(x["views"]) >= 100000) \
            .map(lambda x: (x["video_id"], ((int(x["likes"]) + 1) / (int(x["dislikes"]) + 1), x["category_id"],
                                            x["title"], int(x["views"])))) \
            .reduceByKey(lambda x, y: x if x > y else y).sortBy(lambda x: -x[1][0]) \
            .map(lambda x: (x[1][1], {
            "category_id": x[1][1],
            "videos": [
                {
                    "video_id": x[0],
                    "video_title": x[1][2],
                    "radio_likes_dislikes": x[1][0],
                    "views": x[1][3]
                }
            ]
        })).reduceByKey(helper_functions.add_videos)
        return list(map(lambda x: x[1], df.collect()))
