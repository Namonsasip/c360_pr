import os, logging

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , add_event_week_and_month_from_yyyymmdd, union_dataframes_with_missing_cols, add_start_of_week_and_month
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def loyalty_number_of_services_for_each_category(customer_prof: DataFrame
                                                 , input_df: DataFrame
                                                 ) -> DataFrame:
    """
    :param customer_prof:
    :param input_df:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df, grouping="daily", par_col="partition_date",
        target_table_name="l1_loyalty_number_of_services_daily")

    input_df = add_event_week_and_month_from_yyyymmdd(input_df=input_df
                                                      , column="partition_date")

    customer_prof = data_non_availability_and_missing_check(
        df=customer_prof, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_loyalty_number_of_services_daily")

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
            customer_prof.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    drop_cols = ["event_partition_date", "start_of_week", "start_of_month"]

    input_df = input_df.filter(f.col("event_partition_date") <= min_value).drop(*drop_cols) \

    customer_prof = customer_prof.filter(f.col("event_partition_date") <= min_value)

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    join_key = ["access_method_num", "event_partition_date", "start_of_week", "start_of_month"]
    customer_cols = ["access_method_num", "subscription_identifier", "event_partition_date",
                     "start_of_week", "start_of_month"]
    customer_prof = customer_prof.select(customer_cols)

    selective_df = input_df.where("upper(group_project) = 'PRIVILEGE'") \
        .select(f.col("mobile_no").alias("access_method_num"), "project_id", "response_date")

    dated_df = add_start_of_week_and_month(input_df=selective_df, date_column="response_date")

    dated_df = dated_df.withColumnRenamed("response_date", "loyalty_privilige_registered_date") \
        .withColumn("flag_with_orignal_data", f.lit(1))

    return_df = customer_prof.join(dated_df, join_key, 'left')

    return return_df


def loyalty_number_of_rewards_redeemed_for_each_category(customer_prof: DataFrame
                                                         , input_df: DataFrame) -> DataFrame:
    """
    :param customer_prof:
    :param input_df:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_loyalty_number_of_rewards_daily")

    input_df = add_event_week_and_month_from_yyyymmdd(input_df=input_df
                                                      , column="partition_date")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_rewards_daily")

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
            customer_prof.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    drop_cols = ["event_partition_date", "start_of_week", "start_of_month"]

    input_df = input_df.filter(f.col("event_partition_date") <= min_value).drop(*drop_cols)

    customer_prof = customer_prof.filter(f.col("event_partition_date") <= min_value)

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    join_key = ["access_method_num", "event_partition_date", "start_of_week", "start_of_month"]
    customer_cols = ["access_method_num", "subscription_identifier", "event_partition_date",
                     "start_of_week", "start_of_month"]
    customer_prof = customer_prof.select(customer_cols)

    input_df = input_df.where("msg_event_id = 13") \
        .select(f.col("mobile_no").alias("access_method_num"), "project_id", "response_date")
    input_df = add_start_of_week_and_month(input_df, "response_date") \
        .withColumnRenamed("response_date", "loyalty_rewards_registered_date") \
        .withColumn("flag_with_orignal_data", f.lit(1))

    return_df = customer_prof.join(input_df, join_key, 'left')

    return return_df


def loyalty_number_of_points_spend_for_each_category(customer_prof: DataFrame,
                                                     input_df: DataFrame) -> DataFrame:
    """
    :param customer_prof:
    :param input_df:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_loyalty_number_of_points_spend_daily")

    # Additional logic to handle increment for this dataset
    input_df = input_df.withColumn("filtered_date",
                                   f.to_date(f.col("partition_date").cast(StringType()), 'yyyyMMdd')) \
        .filter(f.col("tran_date").isNotNull()) \
        .filter(f.col("filtered_date") == f.to_date(f.col("tran_date")))

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_points_spend_daily")

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("filtered_date")).alias("max_date")),
            customer_prof.select(
                f.max(f.col("event_partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    drop_cols = ["event_partition_date", "start_of_week", "start_of_month"]

    input_df = input_df.filter(f.col("filtered_date") <= min_value).drop(*drop_cols)

    customer_prof = customer_prof.filter(f.col("event_partition_date") <= min_value)

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    join_key = ["access_method_num", "event_partition_date", "start_of_week", "start_of_month"]
    customer_cols = ["access_method_num", "subscription_identifier", "event_partition_date",
                     "start_of_week", "start_of_month"]
    customer_prof = customer_prof.select(customer_cols)

    selective_df = input_df.where(
        "point_tran_type_id in (15,35) and refund_session_id is null and project_id is not null") \
        .select(f.col("msisdn").alias("access_method_num"), "tran_date", "project_id", "points")

    date_curated_df = add_start_of_week_and_month(selective_df, "tran_date")

    grouped_df = date_curated_df.groupBy(["access_method_num", "event_partition_date", "start_of_week",
                                          "start_of_month", "project_id"]) \
        .agg(f.sum("points").alias("loyalty_points_spend")) \
        .withColumn("flag_with_orignal_data", f.lit(1))

    return_df = grouped_df.join(customer_prof, join_key, 'left')

    return return_df
