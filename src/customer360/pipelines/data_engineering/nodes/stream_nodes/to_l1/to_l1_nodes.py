import os
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from pyspark.sql import functions as f, DataFrame, Window
from pyspark.sql.types import *


conf = os.getenv("CONF", None)


def dac_for_streaming_to_l1_intermediate_pipeline(input_df: DataFrame, cust_df: DataFrame, target_table_name: str):

    # ################################# Start Implementing Data availability checks #############################
    # if check_empty_dfs([input_df, cust_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df()]
    #
    # input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
    #                                                    target_table_name=target_table_name)
    #
    # cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
    #                                                    target_table_name=target_table_name)
    #
    # if check_empty_dfs([input_df, cust_df]):
    #     return [get_spark_empty_df(), get_spark_empty_df()]
    #
    # min_value = union_dataframes_with_missing_cols(
    #     [
    #         input_df.select(
    #             f.max(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
    #         cust_df.select(
    #             f.max(f.col("event_partition_date")).alias("max_date")),
    #     ]
    # ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date
    #
    # input_df = input_df.filter(f.to_date((f.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    # cust_df = cust_df.filter(f.col("event_partition_date") <= min_value)
    #
    # ################################# End Implementing Data availability checks ###############################

    return [input_df, cust_df]


def dac_for_streaming_to_l1_pipeline(input_df: DataFrame, target_table_name: str):

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_for_streaming_to_l1_pipeline_from_l0(input_df: DataFrame, target_table_name: str):

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def application_duration(streaming_df: DataFrame, application_df: DataFrame) -> DataFrame:

    w_recent_partition = Window.partitionBy("application_id").orderBy(f.col("partition_month").desc())
    w_lag_stream = Window.partitionBy("msisdn", "partition_date").orderBy(f.col("begin_time"))

    application_df = (application_df
                      .withColumn("rank", f.row_number().over(w_recent_partition))
                      .where(f.col("rank") == 1)
                      .withColumnRenamed("application_name", "application")
                      ).alias("application_df")

    joined_df = (streaming_df.alias("streaming_df")
                 .join(application_df, f.col("streaming_df.app_id") == f.col("application_df.application_id"), "left_outer")
                 .withColumn("lead_begin_time", f.lead(f.col("begin_time")).over(w_lag_stream))
                 .withColumn("duration", f.col("lead_begin_time") - f.col("begin_time"))  # duration in seconds
                 .select(streaming_df.columns + ["application", "duration"])
                 )

    return joined_df
