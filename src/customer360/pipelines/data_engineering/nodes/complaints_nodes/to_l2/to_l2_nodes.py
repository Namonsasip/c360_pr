import os

from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from pathlib import Path
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from pyspark.sql import DataFrame

conf = os.getenv("CONF", None)
def dac_for_complaints_to_l2_pipeline(input_df: DataFrame, target_table_name: str):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="event_partition_date",
                                                       target_table_name=target_table_name,
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df

#def dac_for_complaints_to_l2_pipeline_from_l2(input_df: DataFrame, target_table_name: str):
#    ################################# Start Implementing Data availability checks #############################
#    if check_empty_dfs([input_df]):
#        return get_spark_empty_df()
#
#    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="start_of_week",
#                                                       target_table_name=target_table_name)
#
#    if check_empty_dfs([input_df]):
#        return get_spark_empty_df()
#
#    ################################# End Implementing Data availability checks ###############################
#
#    return input_df
