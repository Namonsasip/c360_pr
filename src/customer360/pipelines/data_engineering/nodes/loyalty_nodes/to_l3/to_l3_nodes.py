import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check \
    , union_dataframes_with_missing_cols
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def build_loyalty_point_balance_statuses_monthly(
        l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily: DataFrame,
        l1_loyalty_priv_point_bonus_ba_daily: DataFrame,
        l1_loyalty_priv_point_ba_daily: DataFrame,
        l3_loyalty_point_balance_statuses_monthly: dict) -> DataFrame:
    """
    :param l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily:
    :param l1_loyalty_priv_point_bonus_ba_daily:
    :param l1_loyalty_priv_point_ba_daily:
    :param l3_loyalty_point_balance_statuses_monthly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily
                           , l1_loyalty_priv_point_bonus_ba_daily
                           , l1_loyalty_priv_point_ba_daily
                        ]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_loyalty_point_balance_statuses_monthly",
    )

    l1_loyalty_priv_point_bonus_ba_daily = data_non_availability_and_missing_check(
        df=l1_loyalty_priv_point_bonus_ba_daily,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_loyalty_point_balance_statuses_monthly")

    l1_loyalty_priv_point_ba_daily = data_non_availability_and_missing_check(
        df=l1_loyalty_priv_point_ba_daily,
        grouping="monthly",
        par_col="event_partition_date",
        target_table_name="l3_loyalty_point_balance_statuses_monthly")

    if check_empty_dfs([input_df, l1_loyalty_priv_point_bonus_ba_daily, l1_loyalty_priv_point_ba_daily]):
        return get_spark_empty_df()

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            l1_loyalty_priv_point_bonus_ba_daily.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            l1_loyalty_priv_point_ba_daily.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("start_of_month") <= min_value)
    l1_loyalty_priv_point_bonus_ba_daily = l1_loyalty_priv_point_bonus_ba_daily.filter(
        f.col("start_of_month") <= min_value)
    l1_loyalty_priv_point_ba_daily = l1_loyalty_priv_point_ba_daily.filter(f.col("start_of_month") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    win_ba_bonus = Window.partitionBy("billing_account", "start_of_month").orderBy(f.col("event_partition_date").desc())

    win_input_df = Window.partitionBy("billing_account", "national_id_card", "access_method_num",
                                      "subscription_identifier", "start_of_month").orderBy(
        f.col("event_partition_date").desc())

    l1_loyalty_priv_point_bonus_ba_daily = l1_loyalty_priv_point_bonus_ba_daily \
        .withColumn("rnk", f.row_number().over(win_ba_bonus)) \
        .withColumnRenamed("points", "bonus_points") \
        .filter(f.col("rnk = 1"))

    l1_loyalty_priv_point_ba_daily = l1_loyalty_priv_point_ba_daily \
        .withColumn("rnk", f.row_number().over(win_ba_bonus)) \
        .withColumnRenamed("points", "ba_points") \
        .filter(f.col("rnk = 1"))

    input_df = input_df \
        .withColumn("loyalty_register_program_points_date", f.max("loyalty_register_program_points_date").over(win_input_df))\
        .withColumn("rnk", f.row_number().over(win_input_df)) \
        .filter(f.col("rnk = 1"))

    points_merged_monthly = l1_loyalty_priv_point_bonus_ba_daily.join(l1_loyalty_priv_point_ba_daily,
                                                                      ["billing_account", "start_of_month"], "outer")

    points_merged_monthly = points_merged_monthly.withColumn("final_points",
                                                             f.coalesce(f.col("bonus_points", 0)) +
                                                             f.coalesce(f.col("ba_points"), 0)) \
        .withColumn("expired_date", f.when(f.col("ba_expired_date") > f.col("bonus_ba_expired_date")
                                           , f.col("ba_expired_date")).otherwise(f.col("bonus_ba_expired_date"))) \
        .select("billing_account", "start_of_month", "final_points", "expired_date")

    return_df = input_df.join(points_merged_monthly, ["billing_account", "start_of_month"], how="left")

    return_df = node_from_config(return_df, l3_loyalty_point_balance_statuses_monthly)

    return return_df
