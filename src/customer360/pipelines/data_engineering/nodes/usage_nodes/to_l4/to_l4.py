from pyspark.sql import DataFrame


def merge_all_usage_outputs(df1: DataFrame, df2: DataFrame, df3: DataFrame, df4: DataFrame) -> DataFrame:
    """
    :param df1:
    :param df2:
    :param df3:
    :return:
    """
    join_key = ["subscriber_identifier", "start_of_week"]

    final_df = df1.join(df2, join_key)
    final_df = final_df.join(df3, join_key)
    final_df = final_df.join(df4, join_key)

    return final_df
