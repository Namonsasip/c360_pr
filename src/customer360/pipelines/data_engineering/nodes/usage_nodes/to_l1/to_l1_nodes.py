from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols


def merge_incoming_outgoing_calls(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['msisdn', 'event_partition_date']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df = final_df.groupBy(group_cols).max()

    for col in final_df.columns:
        new_col = col.replace('max(', '').replace(')', '')
        final_df = final_df.withColumnRenamed(col, new_col)

    return final_df


def merge_prepaid_postpaid_data_usage(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ["access_method_num"]
    group_cols = ['msisdn', 'event_partition_date']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df = final_df.groupBy(group_cols).max()

    for col in final_df.columns:
        new_col = col.replace('max(', '').replace(')', '')
        final_df = final_df.withColumnRenamed(col, new_col)

    return final_df


def merge_roaming_incoming_outgoing_calls(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """
    This function will union two dataframe and will return final_df
    :param df1:
    :param df2:
    :return:
    """
    drop_cols = ['called_no', 'caller_no', 'day_id']
    group_cols = ['msisdn', 'event_partition_date']
    final_df = union_dataframes_with_missing_cols([df1, df2])
    final_df = final_df.drop(*drop_cols)

    final_df = final_df.groupBy(group_cols).max()

    for col in final_df.columns:
        new_col = col.replace('max(', '').replace(')', '')
        final_df = final_df.withColumnRenamed(col, new_col)

    return final_df
