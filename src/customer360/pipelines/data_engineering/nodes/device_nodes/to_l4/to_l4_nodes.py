from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql
from customer360.utilities.config_parser import l4_rolling_window
from pathlib import Path
from kedro.context.context import load_context
from pyspark.sql import DataFrame, functions as f
import os

conf = os.getenv("CONF", None)

def device_l4_rolling_window(input_df: DataFrame,
                             rolling_window_dict_first: dict,
                             rolling_window_dict_second: dict) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == "l4_device_summary_features") \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    rolling_df_first = rolling_df_first.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_device_summary_features_first", rolling_df_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    rolling_df_second = rolling_df_second.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_device_summary_features_second", rolling_df_second)

    rolling_df_first = CNTX.catalog.load("l4_device_summary_features_first")
    rolling_df_second = CNTX.catalog.load("l4_device_summary_features_second")

    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second])
    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df
