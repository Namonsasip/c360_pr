import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs,  \
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def loyalty_build_weekly_features(input_df: DataFrame,
                                  first_dict: dict,
                                  second_dict: dict) -> DataFrame:
    """
    :param input_df:
    :param first_dict:
    :param second_dict:
    :return:
    """
    first_df = l4_rolling_window(input_df, first_dict)
    second_df = l4_rolling_window(input_df, second_dict)

    group_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_week"]

    union_df = union_dataframes_with_missing_cols([first_df, second_df])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)
    return merged_df


def l4_loyalty_point_balance_statuses_features(input_df: DataFrame,
                                               first_dict: dict,
                                               second_dict: dict) -> DataFrame:
    """
    :param input_df:
    :param first_dict:
    :param second_dict:
    :return:
    """
    first_df = l4_rolling_window(input_df, first_dict)
    second_df = l4_rolling_window(input_df, second_dict)

    group_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_month"]

    union_df = union_dataframes_with_missing_cols([first_df, second_df])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)
    return merged_df
