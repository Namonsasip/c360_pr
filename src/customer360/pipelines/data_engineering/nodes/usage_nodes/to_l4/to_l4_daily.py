import logging, os
from pathlib import Path

from customer360.utilities.config_parser import l4_rolling_window, rolling_window_for_metadata
from pyspark.sql.functions import monotonically_increasing_id
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.spark_util import get_spark_session
from kedro.context import load_context

conf = os.getenv("CONF", None)


def l4_rolling_window_daily_manual(df_input: DataFrame, config: dict):

    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    metadata_last_date = metadata.filter(F.col("table_name") == "l4_usage_prepaid_postpaid_daily_features") \
        .filter(F.col("target_max_data_load_date") == '2021-08-09') \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd')))

    spark = get_spark_session()
    group_cols = config["partition_by"]
    read_from = config.get("read_from")
    logging.info("read_from --> " + read_from)

    re_df = rolling_window_for_metadata(metadata_last_date, read_from, config, group_cols, spark, df_input)
    return re_df

def split_and_run_daily(data_frame, dict_obj) -> DataFrame:
    """
    :param data_frame: 
    :param dict_obj: 
    :return: 
    """

    if check_empty_dfs([data_frame]):
        return data_frame

    unique_ids = data_frame.select("subscription_identifier").distinct()
    unique_ids = unique_ids.withColumn("id", monotonically_increasing_id())

    min_max_id = unique_ids.select("id").agg(F.min("id").alias("min_id")
                                             , F.max("id").alias("max_id")).collect()

    min_id = min_max_id[0][0]
    max_id = min_max_id[0][1]

    mid_point = (min_id + max_id) / 2

    unique_ids_1 = unique_ids.filter(F.col("id") <= mid_point).drop("id")
    unique_ids_2 = unique_ids.filter(F.col("id") > mid_point).drop("id")

    join_key = ['subscription_identifier']

    first_df_to_prepare = data_frame.join(unique_ids_1, join_key)
    second_df_to_prepare = data_frame.join(unique_ids_2, join_key)

    final_1 = l4_rolling_window(first_df_to_prepare, dict_obj)
    final_2 = l4_rolling_window(second_df_to_prepare, dict_obj)

    return union_dataframes_with_missing_cols(final_1, final_2)
