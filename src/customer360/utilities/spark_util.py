from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # pyarrow is not working so disable it for now
    spark.conf.set("spark.sql.execution.arrow.enabled", "false")

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    return spark
