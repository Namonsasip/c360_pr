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

    customer_prof = customer_prof.select("national_id_card"
                                         "access_method_num",
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
                                                       target_table_name="l1_loyalty_number_of_rewards_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_rewards_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("national_id_card"
                                         "access_method_num",
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
                                                       target_table_name="l1_loyalty_number_of_points_spend_daily")

    customer_prof = data_non_availability_and_missing_check(df=customer_prof, grouping="daily",
                                                            par_col="event_partition_date",
                                                            target_table_name="l1_loyalty_number_of_points_spend_daily")

    if check_empty_dfs([input_df, customer_prof]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("national_id_card"
                                         "access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.where("point_tran_type_id in (15,35) and refund_session_id is null and project_id is not null") \
        .select(f.col("msisdn").alias("access_method_num"), "tran_date", "project_id", "points") \
        .withColumn("event_partition_date", f.to_date(f.col("tran_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("tran_date")))) \
        .groupBy(["access_method_num", "event_partition_date", "start_of_week", "project_id"]) \
        .agg(f.sum("points").alias("loyalty_points_spend")) \
        .select("access_method_num", "event_partition_date", "start_of_week", "project_id", "loyalty_points_spend")

    return_df = customer_prof.join(input_df, join_key)

    return return_df


def loyalty_number_of_points_balance(customer_prof: DataFrame
                                     , input_df: DataFrame
                                     , l0_loyalty_priv_point_bonus_ba: DataFrame
                                     , l0_loyalty_priv_point_ba: DataFrame
                                     ) -> DataFrame:
    """
    :param customer_prof:
    :param input_df:
    :param l0_loyalty_priv_point_bonus_ba:
    :param l0_loyalty_priv_point_ba:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    # No check for weekly data to be used at daily level for this special case
    if check_empty_dfs([input_df, customer_prof, l0_loyalty_priv_point_bonus_ba]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_loyalty_priv_point_ba_daily")

    customer_prof = data_non_availability_and_missing_check(
        df=customer_prof, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_loyalty_priv_point_ba_daily")

    l0_loyalty_priv_point_bonus_ba = data_non_availability_and_missing_check(
        df=l0_loyalty_priv_point_bonus_ba,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_loyalty_priv_point_bonus_ba_daily")

    if check_empty_dfs([input_df, customer_prof, l0_loyalty_priv_point_bonus_ba]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]
    ################################# End Implementing Data availability checks ###############################
    customer_prof = customer_prof.select("national_id_card"
                                         "access_method_num",
                                         "subscription_identifier",
                                         "event_partition_date",
                                         "start_of_week")
    join_key = ["access_method_num", "event_partition_date", "start_of_week"]

    input_df = input_df.withColumn("event_partition_date", f.to_date(f.col("response_date"))) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', f.col("response_date")))) \
        .withColumn("loyalty_register_program_points_date",
                    f.when((f.col("msg_event_id").isin(33, 34))
                           & (f.upper(f.col("aunjai_flag").like('REGISTER%'))), f.col("event_partition_date"))
                    .otherwise(f.lit(None))
                    ) \
        .select(f.col("mobile_no").alias("access_method_num"), "mobile_status_date"
                , "mobile_segment", "billing_account", "loyalty_register_program_points"
                , "start_of_month", "event_partition_date")

    l0_loyalty_priv_point_bonus_ba = l0_loyalty_priv_point_bonus_ba\
        .select("billing_account", "points", "modified_date"
                , f.to_date(f.col("expired_date")).alias("bonus_ba_expired_date")) \
        .withColumn("event_partition_date", f.to_date(f.col("modified_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("modified_date")))) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', f.col("modified_date"))))

    l0_loyalty_priv_point_ba = l0_loyalty_priv_point_ba.select("billing_account", "points", "modified_date",
                                                               "expired_date") \
        .withColumn("ba_expired_date", f.to_date(f.col("expired_date"))) \
        .withColumn("event_partition_date", f.to_date(f.col("modified_date"))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.col("modified_date")))) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', f.col("modified_date"))))

    return_df = customer_prof.join(input_df, join_key)

    return [return_df, l0_loyalty_priv_point_bonus_ba, l0_loyalty_priv_point_ba]
