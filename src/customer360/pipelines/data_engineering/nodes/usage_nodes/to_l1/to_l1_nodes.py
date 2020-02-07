from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F


def gen_max_sql(data_frame, table_name, group):
    grp_str = ', '.join(group)
    col_to_iterate = ["max(" + x + ")" + " as " + x for x in data_frame.columns if x not in group]
    all_cols = ', '.join(col_to_iterate)
    final_str = "select {0}, {1} {2} {3} group by {4}".format(grp_str, all_cols, "from", table_name, grp_str)
    return final_str


def merge_incoming_outgoing_calls(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['msisdn', 'event_partition_date', 'start_of_week', 'start_of_month']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df_str = gen_max_sql(final_df, 'incoming_outgoing_call', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='incoming_outgoing_call', sql_str=final_df_str)

    return final_df


def merge_prepaid_postpaid_data_usage(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ["access_method_num"]
    group_cols = ['msisdn', 'event_partition_date', 'start_of_week', 'start_of_month']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df_str = gen_max_sql(final_df, 'incoming_outgoing_data', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='incoming_outgoing_data', sql_str=final_df_str)

    return final_df


def merge_roaming_incoming_outgoing_calls(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['msisdn', 'event_partition_date', 'start_of_week', 'start_of_month']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df_str = gen_max_sql(final_df, 'roaming_incoming_outgoing_data', group_cols)
    final_df = execute_sql(data_frame=final_df, table_name='roaming_incoming_outgoing_data', sql_str=final_df_str)

    return final_df


def build_data_for_prepaid_postpaid(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    prepaid = df1.select("access_method_num", "number_of_call", 'day_id')
    postpaid = df2.where("call_type_cd = 5") \
        .select("access_method_num", F.col("no_transaction").alias("number_of_call"), 'day_id')

    return union_dataframes_with_missing_cols(prepaid, postpaid)
