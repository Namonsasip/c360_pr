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


def device_l4_rolling_window(input_df: DataFrame,
                            rolling_window_dict_first: dict,
                            rolling_window_dict_second: dict,
                            ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    # CNTX = load_context(Path.cwd(), env=conf)
    join_key = ["national_id_card", "access_method_num", "subscription_identifier", "start_of_week"]

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)

    merged_df = rolling_df_first.join(rolling_df_second, join_key)

    return merged_df
