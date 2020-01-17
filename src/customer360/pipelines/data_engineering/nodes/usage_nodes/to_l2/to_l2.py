from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

def prepare_prepaid_call_data(df: DataFrame) -> DataFrame:
    """
    This function prepares data set for l2 features
    :param df:
    :return:
    """
    df = df.withColumn("week_of_year", F.weekofyear(F.col("day_id"))) \
        .withColumn("year", F.year(F.col("day_id"))) \
        .withColumn("month_of_year", F.month("day_id"))

    # Add last date of call within week group
    win = Window.partitionBy("access_method_num", "month_of_year", "week_of_year", "year")\
        .orderBy("access_method_num", "month_of_year", "week_of_year", "year")

    df = df.withColumn("last_call_dt", F.to_date(F.max(F.col("day_id")).over(win)))

    return df


def prepare_postpaid_call_data(df: DataFrame) -> DataFrame:
    """
    This function prepares data set for l2 features
    :param df:
    :return:
    """
    df = df.withColumn("week_of_year", F.weekofyear(F.col("usg_date"))) \
        .withColumn("year", F.year(F.col("usg_date"))) \
        .withColumn("month_of_year", F.month("usg_date"))

    # Add last date of call within week group
    win = Window.partitionBy("access_method_id", "registered_date", "month_of_year", "week_of_year", "year") \
        .orderBy("access_method_id", "registered_date", "month_of_year", "week_of_year", "year")

    df = df.withColumn("last_call_dt", F.to_date(F.max(F.col("usg_date")).over(win)))

    return df

def prepare_prepaid_gprs_data(df: DataFrame) -> DataFrame:
    """
    This function prepares data set for l2 features
    :param df:
    :return:
    """
    df = df.withColumn("week_of_year", F.weekofyear(F.col("call_start_dt"))) \
        .withColumn("year", F.year(F.col("call_start_dt"))) \
        .withColumn("month_of_year", F.month("call_start_dt"))

    # Add last date of call within week group
    win = Window.partitionBy("access_method_num", "month_of_year", "week_of_year", "year") \
        .orderBy("access_method_num", "month_of_year", "week_of_year", "year")

    df = df.withColumn("last_data_dt", F.to_date(F.max(F.col("call_start_dt")).over(win)))

    return df