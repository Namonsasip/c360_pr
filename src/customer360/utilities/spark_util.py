import getpass
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *

from pathlib import Path
from kedro.context.context import load_context

conf = os.getenv("CONF", "base")
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")
PROJECT_NAME = "project-samudra"


def get_spark_session() -> SparkSession:
    """
    Purpose: To create a spark session with below properties
    :return:
    """
    if running_environment.lower() == 'on_premise':
        CNTX = load_context(Path.cwd(), env=conf)
        parameter = CNTX.catalog.load("params:spark_conf")
        spark_conf = SparkConf().setAll(parameter.items())

        spark_session_conf = (
            SparkSession.builder.appName(
                "{}_{}".format(PROJECT_NAME, getpass.getuser())
            )
                .enableHiveSupport()
                .config(conf=spark_conf)
        )
        spark = spark_session_conf.master("yarn").getOrCreate()

    else:
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        # pyarrow is not working so disable it for now
        spark.conf.set("spark.sql.execution.arrow.enabled", "false")
        # Dont delete this line. This allow spark to only overwrite the partition
        # saved to parquet instead of entire table folder
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
       # spark.conf.set("spark.sql.parquet.mergeSchema", "true")

    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_spark_empty_df(schema=None) -> DataFrame:
    """
    Purpose: This is a helper function which is used to create and return an empty dataset.
    It can be used at multiple places in the code wherever required.
    :return:
    """
    if schema is None:
        schema = StructType([])
    spark = get_spark_session()
    src = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return src
