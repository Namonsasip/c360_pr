from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check, union_dataframes_with_missing_cols

from customer360.utilities.spark_util import get_spark_empty_df


def merge_with_customer_prepaid_df(source_df: DataFrame,
                                   cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    source_df = data_non_availability_and_missing_check(
        df=source_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            source_df.select(
                F.max(F.col("start_of_month")).alias("max_date")),
            cust_df.select(
                F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    source_df = source_df.filter(F.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([source_df, cust_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'start_of_month', 'subscription_identifier']
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols)

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null and start_of_month is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df


def merge_with_customer_postpaid_df(source_df: DataFrame) -> DataFrame:
    """
    :param source_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    source_df = data_non_availability_and_missing_check(
        df=source_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
        missing_data_check_flg='N')

    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    source_df = source_df.withColumnRenamed("sub_id", "subscription_identifier") \
        .where("subscription_identifier is not null and start_of_month is not null")

    return source_df
