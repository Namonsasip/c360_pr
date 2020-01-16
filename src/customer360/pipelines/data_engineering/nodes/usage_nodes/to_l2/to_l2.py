from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_prepaid_call_data(df: DataFrame) -> DataFrame:
    """
    This function prepares data set for l2 features
    :param df:
    :return:
    """
    df = df.withColumn("week_of_year", F.weekofyear(F.col("day_id"))) \
           .withColumn("year", F.year(F.col("day_id")))

    return df