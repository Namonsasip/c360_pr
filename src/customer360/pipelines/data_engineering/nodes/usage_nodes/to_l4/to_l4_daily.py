from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_window_by_metadata
from pyspark.sql.functions import monotonically_increasing_id
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_daily_l4(df_input: DataFrame, config: dict, target_table: str):

    df_input = df_input.filter(F.col('event_partition_date') <= '2021-07-25')
    rt_df = l4_rolling_window_by_metadata(df_input, config, target_table)

    return rt_df


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
