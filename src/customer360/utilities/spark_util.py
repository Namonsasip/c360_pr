from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    return spark


def get_spark_empty_df() -> DataFrame:
    """
    :return:
    """
    schema = StructType([])
    spark = get_spark_session()
    src = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return src
