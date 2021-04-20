from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types  import *
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check

# def touchpoints_to_l3_pipeline_contact_test(input_df, cust_df):
#     if check_empty_dfs([input_df, cust_df]):
#         return get_spark_empty_df()
#     output_df = input_df.join(cust_df['start_of_month', 'subscription_identifier'], 'left')
#         input_df.start_of_month,
#         input_df.subscription_identifier,
#         "touchpoints_sum_contact_myais"
#         "touchpoints_total_days_countact_myais"
#     )
#
#   output_df = input_df.join(cust_df, (input_df.#somethime# == cust.df.subscription_identifier &
#                                       input_df.start_of_month == cust_df.start_of_month)
#                                       ,'left').select(
#   )
#
#     return output_df


def dac_for_touchpoints_to_l3_pipeline_from_l1(input_df: DataFrame, target_table_name: str, exception_partition=None):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly", par_col="event_partition_date",
                                                       target_table_name=target_table_name,
                                                       missing_data_check_flg='Y',
                                                       exception_partitions=exception_partition)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df
