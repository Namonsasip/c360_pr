from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


def get_spark_session() -> SparkSession:
    """
    :return:
    """
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    # Increase allow size
    spark.conf.set("spark.driver.maxResultSize", "8g")

    spark.conf.set("spark.sql.parquet.mergeSchema", "true")

    return spark


def get_spark_empty_df(schema=None) -> DataFrame:
    """
    :return:
    """
    if schema is None:
        schema = StructType([])
    spark = get_spark_session()
    src = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return src
