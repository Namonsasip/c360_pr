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

    l0_loyalty_priv_project = l0_loyalty_priv_project \
        .withColumn("rnk", f.row_number().over(win_project)) \
        .filter(f.col("rnk") == 1) \
        .select("project_id", f.col("category").alias("category_id"))

    proj_cat_joined = l0_loyalty_priv_project.join(l0_loyalty_priv_category, ['category_id'], 'left')

    return_df = input_df.join(proj_cat_joined, ['project_id'], how="inner")

    return_df = node_from_config(return_df, l2_loyalty_number_of_rewards_redeemed_weekly)

    return return_df


def build_loyalty_point_balance_statuses_weekly(l1_loyalty_priv_point_ba_daily: DataFrame,
                                                l0_loyalty_priv_point_ba: DataFrame,
                                                l2_loyalty_priv_point_ba_weekly: dict) -> DataFrame:
    """

    :param l1_loyalty_priv_point_ba_daily:
    :param l0_loyalty_priv_point_ba:
    :param l2_loyalty_priv_point_ba_weekly:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([l1_loyalty_priv_point_ba_daily, l0_loyalty_priv_point_ba]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=l1_loyalty_priv_point_ba_daily,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_loyalty_priv_point_ba_weekly",
                                                       missing_data_check_flg='Y')

    loyalty_priv_point_ba = data_non_availability_and_missing_check(df=l1_loyalty_priv_point_ba_daily,
                                                                    grouping="weekly",
                                                                    par_col="partition_date",
                                                                    target_table_name="l2_loyalty_priv_point_ba_weekly")

    loyalty_priv_point_ba = loyalty_priv_point_ba.withColumn("start_of_week",
                                                             f.to_date(f.col("partition_date").StringType()), 'yyyyMMdd')

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                f.max(f.col("start_of_week")).alias("max_date")),
            loyalty_priv_point_ba.select(
                f.max(f.col("start_of_week")).alias("max_date"))
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(f.col("start_of_week") <= min_value)

    loyalty_priv_point_ba = loyalty_priv_point_ba.filter(f.col("start_of_week") <= min_value)

    if check_empty_dfs([input_df, loyalty_priv_point_ba]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    proj_cat_joined = input_df.join(loyalty_priv_point_ba, ['billing_account', 'start_of_week'])

    return_df = node_from_config(proj_cat_joined, l2_loyalty_priv_point_ba_weekly)

    return return_df
