from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


def get_spark_session() -> SparkSession:
    """
    :return:
    """
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # pyarrow is not working so disable it for now
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

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
