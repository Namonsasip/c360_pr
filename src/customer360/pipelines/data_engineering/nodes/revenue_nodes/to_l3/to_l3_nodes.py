from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check


def merge_with_customer_prepaid_df(source_df: DataFrame,
                                   cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df]):
        return source_df

    source_df = data_non_availability_and_missing_check(df=source_df, grouping="monthly",
                                                        par_col="start_of_month",
                                                        target_table_name="l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                                                        missing_data_check_flg='N')

    if check_empty_dfs([source_df]):
        return source_df

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'partition_month', 'subscription_identifier']
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.where("charge_type = 'Pre-paid'")

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null")

    final_df = final_df.where("start_of_month is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df


def merge_with_customer_postpaid_df(source_df: DataFrame,
                                    cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([source_df]):
        return source_df

    source_df = data_non_availability_and_missing_check(df=source_df, grouping="monthly",
                                                        par_col="start_of_month",
                                                        target_table_name="l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                                                        missing_data_check_flg='N')

    if check_empty_dfs([source_df]):
        return source_df

    ################################# End Implementing Data availability checks ###############################

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['partition_month', 'subscription_identifier']
    join_key = ['subscription_identifier', 'start_of_month']

    cust_df = cust_df.where("charge_type = 'Post-paid'")

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    source_df = source_df.withColumnRenamed("sub_id", "subscription_identifier")

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null")

    final_df = final_df.where("start_of_month is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df
