from pyspark.sql import DataFrame


def merge_with_customer_df(source_df: DataFrame,
                           cust_df: DataFrame) -> DataFrame:
    """

    :param source_df:
    :param cust_df:
    :return:
    """
    # This code will populate a subscriber id to the data set.
    cust_df_cols = ['access_method_num', 'start_of_month', 'crm_sub_id']
    join_key = ['access_method_num', 'start_of_month']
    final_df = source_df.join(cust_df.select(cust_df_cols), join_key)

    return final_df
