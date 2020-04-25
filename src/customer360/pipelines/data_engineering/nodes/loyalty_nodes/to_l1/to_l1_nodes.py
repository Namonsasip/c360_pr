import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)

def loyalty_number_of_services_for_each_category(customer_prof: DataFrame
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
                                                       target_table_name="l1_loyalty_number_of_services_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_services_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.select(f.col("mobile_no").alias("access_method_num"), "response_date", "project_id",
                               "register_date") \
        .withColumn("event_partition_date", f.to_date(f.col("response_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("response_date"))))

    return_df = customer_prof.join(input_df, join_key)

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
                                                       target_table_name="l1_loyalty_number_of_rewards_redeemed_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_rewards_redeemed_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.where("msg_event_id = 13") \
        .select(f.col("mobile_no").alias("access_method_num"), "response_date", "project_id") \
        .withColumn("event_partition_date", f.to_date(f.col("response_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("response_date"))))

    return_df = customer_prof.join(input_df, join_key)

    return return_df


def loyalty_number_of_points_spend_for_each_category(customer_prof: DataFrame
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
                                                       target_table_name="l1_loyalty_number_of_rewards_redeemed_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_points_spend_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.where("point_tran_type_id in (15,35) and refund_session_id is null") \
        .select(f.col("mobile_no").alias("access_method_num"), "tran_date", "project_id") \
        .withColumn("event_partition_date", f.to_date(f.col("tran_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("tran_date"))))\
        .agg(f.sum("points").alias("loyalty_points_spend")) \
        .select("access_method_num", "event_partition_date", "start_of_week", "loyalty_points_spend")

    return_df = customer_prof.join(input_df, join_key)

    return return_df


def loyalty_number_of_points_balance(customer_prof: DataFrame
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
                                                       target_table_name="l1_loyalty_priv_point_ba_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_priv_point_ba_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.select(f.col("mobile_no").alias("access_method_num"), "response_date", "mobile_status_date"
                               , "mobile_segment") \
        .withColumn("event_partition_date", f.to_date(f.col("response_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("response_date"))))

    return_df = customer_prof.join(input_df, join_key)

    return return_df
