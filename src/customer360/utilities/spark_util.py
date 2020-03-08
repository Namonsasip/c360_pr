from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()
