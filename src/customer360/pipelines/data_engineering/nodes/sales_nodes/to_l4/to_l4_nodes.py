from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, \
     union_dataframes_with_missing_cols, gen_max_sql, execute_sql
from customer360.utilities.config_parser import l4_rolling_window
from pyspark.sql import DataFrame
import os

conf = os.getenv("CONF", None)


def sales_l4_rolling_window(input_df: DataFrame,
                            rolling_window_dict_first: dict,
                            rolling_window_dict_second: dict,
                            rolling_window_dict_third: dict,
                            rolling_window_dict_fourth: dict,
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

    group_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_week"]

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    rolling_df_third = l4_rolling_window(input_df, rolling_window_dict_third)

    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second, rolling_df_third])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df
