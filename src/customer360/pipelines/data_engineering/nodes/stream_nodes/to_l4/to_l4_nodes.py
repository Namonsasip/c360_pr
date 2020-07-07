import json
from customer360.utilities.spark_util import get_spark_session
from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from customer360.utilities.config_parser import \
    l4_rolling_ranked_window, l4_rolling_window, join_l4_rolling_ranked_table
import os
from customer360.utilities.re_usable_functions import check_empty_dfs
from pathlib import Path
from kedro.context.context import load_context

conf = os.getenv("CONF", None)


def generate_l4_fav_streaming_day(input_df, template_config, app_list):
    """
    :param input_df:
    :param template_config:
    :param app_list:
    :return:
    """

    CNTX = load_context(Path.cwd(), env=conf)
    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == "l4_streaming_fav_steamtv_esport_streaming_day_of_week_feature") \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    input_df = input_df.filter(f.col("start_of_week") > max_date)

    if check_empty_dfs([input_df]):
        return None

    input_df.createOrReplaceTempView("input_df")

    from customer360.run import ProjectContext
    ctx = ProjectContext(str(Path.cwd()), env=conf)

    spark = get_spark_session()

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


def streaming_two_output_function(input_df: DataFrame,
                                  config_one: dict,
                                  config_two: dict,
                                  config_three: dict,
                                  ) -> [DataFrame, DataFrame]:
    """
    :param input_df:
    :param config_one:
    :param config_two:
    :param config_three:
    :return:
    """
    input_first_pass_df = l4_rolling_window(input_df, config_one)
    input_second_pass = l4_rolling_ranked_window(input_first_pass_df, config_two)
    input_third_pass = l4_rolling_ranked_window(input_first_pass_df, config_three)

    return [input_second_pass, input_third_pass]


def streaming_three_output_function(input_df: DataFrame,
                                    config_one: dict,
                                    config_two: dict,
                                    config_three: dict,
                                    config_fourth: dict,
                                    ) -> [DataFrame, DataFrame]:
    """
    :param input_df:
    :param config_one:
    :param config_two:
    :param config_three:
    :param  config_fourth:
    :return:
    """
    input_first_pass_df = l4_rolling_window(input_df, config_one)
    input_second_pass = l4_rolling_ranked_window(input_first_pass_df, config_two)
    input_third_pass = l4_rolling_ranked_window(input_first_pass_df, config_three)
    input_fourth_pass = l4_rolling_ranked_window(input_first_pass_df, config_fourth)

    return [input_second_pass, input_third_pass, input_fourth_pass]
