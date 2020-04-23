from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrame

def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # pyarrow is not working so disable it for now
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

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
