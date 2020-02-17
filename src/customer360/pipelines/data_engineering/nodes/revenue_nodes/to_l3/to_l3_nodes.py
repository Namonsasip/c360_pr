from pyspark.sql import DataFrame


def merge_with_customer_prepaid_df(source_df: DataFrame,
                                   cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'start_of_month', 'crm_sub_id']
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols) \
        .withColumnRenamed("crm_sub_id", "subscription_identifier")

    final_df = source_df.join(cust_df, join_key)

    return final_df


def merge_with_customer_postpaid_df(source_df: DataFrame,
                                    cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['partition_month', 'subscription_identifier']
    join_key = ['subscription_identifier', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    source_df = source_df.withColumnRenamed("sub_id", "subscription_identifier")

    final_df = source_df.join(cust_df, join_key)

    return final_df
