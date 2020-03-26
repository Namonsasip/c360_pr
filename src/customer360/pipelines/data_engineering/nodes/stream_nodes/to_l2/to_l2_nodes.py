import os

from customer360.utilities.spark_util import get_spark_session
from pathlib import Path

conf = os.getenv("CONF", None)

def generate_l2_fav_streaming_day(input_df, app_list):
    spark = get_spark_session()
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()), env=conf)

    for each_app in app_list:
        df = spark.sql("""
            select
                access_method_num,
                subscription_identifier,
                start_of_week,
                day_of_week as fav_{each_app}_streaming_day_of_week,
                download_kb_traffic_{each_app}_sum 
            from input_df
            where {each_app}_by_download_rank = 1
            and download_kb_traffic_{each_app}_sum > 0
        """.format(each_app=each_app))

        ctx.catalog.save("l2_streaming_fav_{}_streaming_day_of_week_feature"
                         .format(each_app), df)

    return None
