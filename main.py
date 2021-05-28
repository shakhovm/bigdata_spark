from pyspark import SparkContext
from pyspark.sql import SparkSession
from queries import Queries
import json
import sys
import os

if __name__ == "__main__":
    files = [
        "USvideos.csv",
        "RUvideos.csv",
        "KRvideos.csv",
        "JPvideos.csv",
        "INvideos.csv",
        "GBvideos.csv",
        "FRvideos.csv",
        "DEvideos.csv",
        "CAvideos.csv",
    ]
    files = list(map(lambda x: sys.argv[1] + "/" + x, files))
    sc = SparkContext(
        master="local",
        appName="BigDataSpark"
    )

    ss = SparkSession.builder.appName("First app").getOrCreate()

    # if not os.path.exists(out_dir):
    #     os.mkdir(out_dir)
    for file in files:
        df = ss.read.csv(file, sep=",", header=True). \
            filter("char_length(video_id) == 11 and video_id != 'ABOUT WIRED' and tags is not null "
                   "and views is not null and trending_date is not null")  # Troubles with nulls ;)

        q = Queries(df)
        for i in range(1, 7):
            if i == 1:
                results = q.task1()
            elif i == 2:
                results = q.task2()
            elif i == 3:
                results = q.task3()
            elif i == 4:
                results = q.task4()
            elif i == 5:
                results = q.task5()
            else:
                results = q.task6()

            results = json.dumps(results, indent=4)
            df = ss.read.json(sc.parallelize([results]))
            df.coalesce(1).write.format('json').mode("overwrite").save(sys.argv[2] + \
                                                                       f"/{file[:-4]}" + f"/task{i}")
            # with open(out_dir + f"/task{i}.json", 'w', encoding='utf-8') as f:
            #     json.dump(results, f, indent=2)
            print(f"task {i} ended!")
