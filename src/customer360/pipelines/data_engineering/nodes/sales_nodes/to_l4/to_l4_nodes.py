from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    get_spark_session, union_dataframes_with_missing_cols
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.config_parser import node_from_config
from pyspark.sql import DataFrame, functions as f
import os
from pathlib import Path
from kedro.context.context import load_context

conf = os.getenv("CONF", None)


def sales_l4_rolling_window(input_df: DataFrame,
                            rolling_window_dict_first: dict,
                            rolling_window_dict_second: dict,
                            rolling_window_dict_third: dict,
                            ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :param rolling_window_dict_third:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    join_key = ["national_id_card", "access_method_num", "subscription_identifier", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == "l4_sales_number_and_volume_transaction_weekly") \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    rolling_df_first = rolling_df_first.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_first", rolling_df_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    rolling_df_second = rolling_df_second.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_second", rolling_df_second)

    rolling_df_third = l4_rolling_window(input_df, rolling_window_dict_third)
    rolling_df_third = rolling_df_third.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_number_and_volume_transaction_weekly_third", rolling_df_third)

    rolling_df_first = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_first")
    rolling_df_second = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_second")
    rolling_df_third = CNTX.catalog.load("l4_sales_number_and_volume_transaction_weekly_third")

    merged_df = rolling_df_first.join(rolling_df_second, join_key) \
        .join(rolling_df_third, join_key)

    return merged_df
