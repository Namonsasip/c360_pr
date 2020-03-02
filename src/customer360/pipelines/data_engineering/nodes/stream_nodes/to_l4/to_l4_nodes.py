from pyspark.sql import SparkSession
from pathlib import Path
import json

from customer360.utilities.config_parser import \
    l4_rolling_ranked_window, \
    join_l4_rolling_ranked_table


def generate_l4_fav_streaming_day(input_df, template_config, app_list):
    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()))

    spark = SparkSession.builder.getOrCreate()

    for each_app in app_list:
        config_str = json.dumps(template_config).replace("{app}", each_app)
        config = json.loads(config_str)

        df_map = l4_rolling_ranked_window(input_df, config)

        for window in ["last_week", "last_two_week", "last_four_week", "last_twelve_week"]:
            df_map[window].createOrReplaceTempView("input_table")

            # If there is no record for that window, display null
            df_map[window] = spark.sql("""
                select {partition_cols},
                    case when sum_download_kb_traffic_{app}_sum_weekly_{window} > 0 
                         then fav_{app}_streaming_day_of_week 
                         else null end as fav_{app}_streaming_day_of_week
                from input_table
            """.format(partition_cols=','.join(config["partition_by"]),
                       app=each_app,
                       window=window))

        df = join_l4_rolling_ranked_table(df_map, config)

        ctx.catalog.save("l4_streaming_fav_{}_streaming_day_of_week_feature"
                         .format(each_app), df)

    return None
