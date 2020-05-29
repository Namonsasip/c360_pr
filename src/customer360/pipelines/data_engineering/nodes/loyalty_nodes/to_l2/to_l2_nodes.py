import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check\
    , union_dataframes_with_missing_cols
from customer360.utilities.spark_util import get_spark_empty_df
from pyspark.sql.types import *

conf = os.getenv("CONF", None)


def build_loyalty_number_of_services_weekly(l1_loyalty_number_of_services_daily: DataFrame,
                                            l0_loyalty_priv_project: DataFrame,
                                            l0_loyalty_priv_category: DataFrame,
                                            l2_loyalty_number_of_services_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_number_of_services_daily:
    :param l0_loyalty_priv_project:
    :param l0_loyalty_priv_category:
    :param l2_loyalty_number_of_services_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_number_of_services_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_number_of_services_daily, grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_loyalty_number_of_services_weekly",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    win_category = Window.partitionBy("category_id").orderBy(f.col("partition_date").desc())
    win_project = Window.partitionBy("project_id").orderBy(f.col("start_date").desc(), f.col("stop_date").desc())

    l0_loyalty_priv_category = l0_loyalty_priv_category.withColumn("rnk", f.row_number().over(win_category)) \
        .filter(f.col("rnk") == 1) \
        .select("category_id", f.col("category").alias("category_text"))

    l0_loyalty_priv_project = l0_loyalty_priv_project.withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="left")

    return_df = node_from_config(return_df, l2_loyalty_number_of_services_weekly)

    return return_df


def build_loyalty_number_of_rewards_redeemed_weekly(l1_loyalty_number_of_rewards_redeemed_daily: DataFrame,
                                                    l0_loyalty_priv_project: DataFrame,
                                                    l0_loyalty_priv_category: DataFrame,
                                                    l2_loyalty_number_of_rewards_redeemed_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_number_of_rewards_redeemed_daily:
    :param l0_loyalty_priv_project:
    :param l0_loyalty_priv_category:
    :param l2_loyalty_number_of_services_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_number_of_rewards_redeemed_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_number_of_rewards_redeemed_daily,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_loyalty_number_of_rewards_redeemed_weekly",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    win_category = Window.partitionBy("category_id").orderBy(f.col("partition_date").desc())
    win_project = Window.partitionBy("project_id").orderBy(f.col("start_date").desc(), f.col("stop_date").desc())

    l0_loyalty_priv_category = l0_loyalty_priv_category.withColumn("rnk", f.row_number().over(win_category)) \
        .filter(f.col("rnk") == 1) \
        .select("category_id", f.col("category").alias("category_text"))

    l0_loyalty_priv_project = l0_loyalty_priv_project.where("project_type_id = 6 and "
                                                            "lower(project_subtype) like '%redeem%'") \
        .withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="left")

    return_df = node_from_config(return_df, l2_loyalty_number_of_rewards_redeemed_weekly)

    return return_df


def build_loyalty_number_of_points_spend_weekly(l1_loyalty_number_of_points_spend_daily: DataFrame,
                                                l0_loyalty_priv_project: DataFrame,
                                                l0_loyalty_priv_category: DataFrame,
                                                l2_loyalty_number_of_rewards_redeemed_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_number_of_rewards_redeemed_daily:
    :param l0_loyalty_priv_project:
    :param l0_loyalty_priv_category:
    :param l2_loyalty_number_of_services_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_number_of_points_spend_daily, l0_loyalty_priv_project,
                        l0_loyalty_priv_category]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_number_of_points_spend_daily,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_loyalty_number_of_points_spend_weekly",
                                                       missing_data_check_flg='Y')

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    win_category = Window.partitionBy("category_id").orderBy(f.col("partition_date").desc())
    win_project = Window.partitionBy("project_id").orderBy(f.col("start_date").desc(), f.col("stop_date").desc())

    l0_loyalty_priv_category = l0_loyalty_priv_category.withColumn("rnk", f.row_number().over(win_category)) \
        .filter(f.col("rnk") == 1) \
        .select("category_id", f.col("category").alias("category_text"))

    l0_loyalty_priv_project = l0_loyalty_priv_project \
        .withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="inner")

    return_df = node_from_config(return_df, l2_loyalty_number_of_rewards_redeemed_weekly)

    return return_df


def build_loyalty_point_balance_statuses_weekly(
        l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily: DataFrame,
        l2_loyalty_point_balance_statuses_weekly: dict) -> DataFrame:
    """
    :param l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily:
    :param l2_loyalty_point_balance_statuses_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=l1_loyalty_drm_t_aunjai_point_collection_with_customers_for_point_bal_daily,
        grouping="weekly",
        par_col="event_partition_date",
        target_table_name="l3_loyalty_point_balance_statuses_monthly",
    )

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    win_input_df = Window.partitionBy("national_id_card", "access_method_num",
                                      "subscription_identifier", "start_of_week").orderBy(
        f.col("event_partition_date").desc())

    input_df = input_df \
        .withColumn("loyalty_register_program_points_date", f.max("loyalty_register_program_points_date").over(win_input_df)) \
        .withColumn("rnk", f.row_number().over(win_input_df)) \
        .filter(f.col("rnk = 1"))

    return_df = node_from_config(input_df, l2_loyalty_point_balance_statuses_weekly)

    return return_df
