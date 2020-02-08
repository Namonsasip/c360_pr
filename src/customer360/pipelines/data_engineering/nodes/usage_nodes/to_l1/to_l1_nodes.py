from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F


def gen_max_sql(data_frame, table_name, group):
    grp_str = ', '.join(group)
    col_to_iterate = ["max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group]
    all_cols = ', '.join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(grp_str, all_cols, "from", table_name, grp_str)
    return final_str


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


def merge_incoming_outgoing_calls_with_customer_dim(out_going_df: DataFrame
                                                    , in_going_df: DataFrame
                                                    , cust_df: DataFrame) -> DataFrame:
    """

    :param out_going_df:
    :param in_going_df:
    :param cust_df:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['access_method_num', 'event_partition_date']

    final_df = union_dataframes_with_missing_cols([out_going_df, in_going_df])
    final_df = final_df.drop(*drop_cols)

    final_df_str = gen_max_sql(final_df, 'incoming_outgoing_call', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='incoming_outgoing_call', sql_str=final_df_str)

    final_df = merge_with_customer_df(final_df, cust_df)

    return final_df


def merge_prepaid_postpaid_data_usage(prepaid: DataFrame
                                      , postpaid: DataFrame
                                      , cust_df: DataFrame) -> DataFrame:
    """

    :param prepaid:
    :param postpaid:
    :param cust_df:
    :return:
    """
    group_cols = ['access_method_num', 'event_partition_date']
    final_df = union_dataframes_with_missing_cols([prepaid, postpaid])

    final_df_str = gen_max_sql(final_df, 'incoming_outgoing_data', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='incoming_outgoing_data', sql_str=final_df_str)

    final_df = merge_with_customer_df(final_df, cust_df)

    return final_df


def merge_roaming_incoming_outgoing_calls(outgoing: DataFrame
                                          , incoming: DataFrame
                                          , cust_df: DataFrame) -> DataFrame:
    """

    :param outgoing:
    :param incoming:
    :param cust_df:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['access_method_num', 'event_partition_date']
    final_df = union_dataframes_with_missing_cols([outgoing, incoming])
    final_df = final_df.drop(*drop_cols)

    final_df_str = gen_max_sql(final_df, 'roaming_incoming_outgoing_data', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)

    final_df = merge_with_customer_df(final_df, cust_df)

    return final_df


def build_data_for_prepaid_postpaid(prepaid: DataFrame
                                    , postpaid: DataFrame) -> DataFrame:
    """

    :param prepaid:
    :param postpaid:
    :return:
    """
    prepaid = prepaid.select("access_method_num", "number_of_call", 'day_id')
    postpaid = postpaid.where("call_type_cd = 5") \
        .select("access_method_num", F.col("no_transaction").alias("number_of_call"), 'day_id')

    final_df = union_dataframes_with_missing_cols(prepaid, postpaid)

    return final_df

