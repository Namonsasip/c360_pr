from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import SparkSession


def union_dataframes_with_missing_cols(df_input_or_list, *args):
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list] + list(args)

    col_list = set()
    for df in df_list:
        for column in df.columns:
            col_list.add(column)

    def add_missing_cols(dataframe, col_list):
        missing_cols = [column for column in col_list if column not in dataframe.columns]
        for column in missing_cols:
            dataframe = dataframe.withColumn(column, F.lit(None))
        return dataframe.select(*sorted(col_list))

    df_list_updated = [add_missing_cols(df, col_list) for df in df_list]
    return reduce(DataFrame.union, df_list_updated)


def execute_sql(data_frame, table_name, sql_str):
    """

    :param data_frame:
    :param table_name:
    :param sql_str:
    :return:
    """
    ss = SparkSession.builder.getOrCreate()
    data_frame.registerTempTable(table_name)
    return ss.sql(sql_str)


def add_start_of_week_and_month(input_df, date_column="day_id"):
    input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col(date_column))))
    input_df = input_df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col(date_column))))

    return input_df
