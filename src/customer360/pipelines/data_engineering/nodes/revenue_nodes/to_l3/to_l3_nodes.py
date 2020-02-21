from pyspark.sql import DataFrame


def merge_with_customer_prepaid_df(source_df: DataFrame,
                                   cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'partition_month', 'subscription_identifier']
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.where("charge_type = 'Pre-paid'")

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

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

    cust_df = cust_df.where("charge_type = 'Post-paid'")

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    source_df = source_df.withColumnRenamed("sub_id", "subscription_identifier")

    final_df = source_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df
