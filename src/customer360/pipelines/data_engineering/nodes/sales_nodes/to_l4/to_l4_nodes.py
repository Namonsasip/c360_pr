from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    get_spark_session, union_dataframes_with_missing_cols, gen_max_sql, execute_sql
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
                            rolling_window_dict_fourth: dict,
                            rolling_window_dict_fifth: dict,
                            rolling_window_dict_sixth: dict,
                            table_name: str
                            ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :param rolling_window_dict_third:
    :param rolling_window_dict_fourth:
    :param rolling_window_dict_fifth:
    :param table_name
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == table_name) \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    rolling_df_first = rolling_df_first.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_1", rolling_df_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    rolling_df_second = rolling_df_second.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_2", rolling_df_second)

    rolling_df_third = l4_rolling_window(input_df, rolling_window_dict_third)
    rolling_df_third = rolling_df_third.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_3", rolling_df_third)

    rolling_df_fourth = l4_rolling_window(input_df, rolling_window_dict_fourth)
    rolling_df_fourth = rolling_df_fourth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_4", rolling_df_fourth)

    rolling_df_fifth = l4_rolling_window(input_df, rolling_window_dict_fifth)
    rolling_df_fifth = rolling_df_fifth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_5", rolling_df_fifth)

    rolling_df_sixth = l4_rolling_window(input_df, rolling_window_dict_sixth)
    rolling_df_sixth = rolling_df_sixth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_sales_temp_6", rolling_df_sixth)

    rolling_df_first = CNTX.catalog.load("l4_sales_temp_1")
    rolling_df_second = CNTX.catalog.load("l4_sales_temp_2")
    rolling_df_third = CNTX.catalog.load("l4_sales_temp_3")
    rolling_df_fourth = CNTX.catalog.load("l4_sales_temp_4")
    rolling_df_fifth = CNTX.catalog.load("l4_sales_temp_5")
    rolling_df_sixth = CNTX.catalog.load("l4_sales_temp_6")

    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second, rolling_df_third,
                                                   rolling_df_fourth, rolling_df_fifth,rolling_df_sixth])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df
