from pyspark.sql import SparkSession

from pathlib import Path


def generate_l2_fav_streaming_day(input_df, app_list):
    spark = SparkSession.builder.getOrCreate()
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()))

    week_list = input_df.select("start_of_week").distinct().collect()
    week_list = list(map(lambda x: x[0], week_list))

    for each_app in app_list:
        for each_week in week_list:
            df = spark.sql("""
                select
                    access_method_num,
                    start_of_week,
                    day_of_week as fav_{each_app}_streaming_day_of_week,
                    download_kb_traffic_{each_app}_sum 
                from input_df
                where {each_app}_by_download_rank = 1
                and start_of_week = to_date('{each_week}')
                and download_kb_traffic_{each_app}_sum > 0
            """.format(each_app=each_app,
                       each_week=each_week.strftime("%Y-%m-%d")))

            ctx.catalog.save("l2_streaming_fav_{}_streaming_day_of_week_feature"
                             .format(each_app), df)

    return None
