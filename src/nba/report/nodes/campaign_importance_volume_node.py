from datetime import date
from datetime import timedelta
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session
from pyspark.sql import functions as F
import logging


def create_l0_campaign_history_master_active(input_campaign_master: DataFrame, output: Dict[str, Any]) -> DataFrame:
    spark = get_spark_session()
    stmt = "Drop table if exists " + output['table']
    spark.sql(stmt)
    return input_campaign_master


def create_l5_campaign_distinct_contact_response(child_response_full: DataFrame,
                                                 child_response_params: Dict[str, Any]) -> DataFrame:
    child_response = child_response_full.fillna(0, subset=child_response_params["source_cols"])
    # source_cols: ['contact_trans_m0', 'contact_trans_m1', 'contact_trans_m2',
    # 'trans_resp_m0', 'trans_resp_m1', 'trans_resp_m2',
    # 'trans_nonres_m0', 'trans_nonres_m1', 'trans_nonres_m2', 'trans_inform_m0', 'trans_inform_m1', 'trans_inform_m2',
    # 'trans_null_m0', 'trans_null_m1', 'trans_null_m2']
    # #agg_cols: ['contact_trans_last_3mth', 'trans_response_last_3mth', 'trans_no_response_last_3mth', 'trans_inform_last_3mth','trans_null_last_3mth']
    child_response_agg = child_response
    iter_var = 0
    for agg_col in child_response_params["agg_cols"]:
        child_response_agg = child_response_agg.withColumn(agg_col, F.lit(
            F.col(child_response_params["source_cols"][(iter_var * 3) + 0]) +
            F.col(child_response_params["source_cols"][(iter_var * 3) + 1]) +
            F.col(child_response_params["source_cols"][(iter_var * 3) + 2])))
        iter_var = iter_var + 1
    # Example if each iterate:
    # child_response_agg = child_response_agg.withColumn('contact_last_3mth', F.lit(F.col('contact_trans_m0')+
    #                                                                               F.col('contact_trans_m1')+
    #                                                                               F.col('contact_trans_m2))
    # print("==========================SUCCESS=========================")
    return child_response_agg


def create_l5_response_percentage_report(distinct_child_response_agg: DataFrame,
                                         campaign_master: DataFrame,
                                         report_param: Dict[str, Any]) -> DataFrame:
    # excluding the offer_type != information because the new column includes the non tracking information campaigns
    campaign_mst_exclude_information = campaign_master.where('offer_type <> "information" and month_id = "2020-02-29"')
    # campaign_mst_exclude_information = campaign_master.where('month_id = "2020-02-29"')
    campaign_agg_no_info = distinct_child_response_agg.alias('agg') \
        .join(campaign_mst_exclude_information.alias('mst'),
              F.col('agg.' + report_param['join_cols']) == F.col('mst.' + report_param['join_cols']), 'left') \
        .select("agg.*")
    response_rate_df = campaign_agg_no_info \
        .withColumn(report_param['response_rate_cols'],
                    F.lit(F.col('trans_response_last_3mth') / F.col('contact_trans_last_3mth') * 100)) \
        .fillna(0, subset=report_param['response_rate_cols'])
    return response_rate_df


def filter_campaigns_by_category(base_campaigns: DataFrame,
                                 campaign_amount_per_category: int,
                                 sub_criteria: str) -> DataFrame:
    # Different category has different amount of contacts
    # The contacts_trans_last_3mth is the total times a campaign has contacted customers in the last 3 months
    # For churn-related and cross sell/upsell, the number of contacts are very high, so we filter only the campaigns that
    # have exceptionally high contacts, currently higher than 1M contacts for low response and 100K contacts for high response

    df_churn = base_campaigns.where('campaign_category = "Churn Prevention" '
                                    'or campaign_category = "Retention"') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)
    if sub_criteria == 'low_response':
        df_churn = df_churn.where('contact_trans_last_3mth > 1000000')

    df_cross_upsell = base_campaigns.where('contact_trans_last_3mth > 1000000'
                                           ' and (campaign_category = "Cross/Up sell"'
                                           'or campaign_category = "Cross Sell")') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)
    if sub_criteria == 'low_response':
        df_cross_upsell = df_cross_upsell.where('contact_trans_last_3mth > 1000000')

    df_handset = base_campaigns.where('campaign_category = "Handset"') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)

    df_pre_to_post = base_campaigns.where('campaign_category = "Convert Pre to Post"') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)

    df_top_up = base_campaigns.where('campaign_category = "Top up"') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)

    df_vas = base_campaigns.where('campaign_category = "VAS"') \
        .orderBy('contact_trans_last_3mth', ascending=False) \
        .limit(campaign_amount_per_category)

    # Union all dataframes into one combined dataframe with all categories
    total_focus_campaigns = df_churn.union(df_cross_upsell) \
        .union(df_handset) \
        .union(df_pre_to_post) \
        .union(df_top_up) \
        .union(df_vas)
    return total_focus_campaigns


def create_focus_campaign_by_volume_low_response(response_percentage_report: DataFrame,
                                                 percentage_threshold_low: float,
                                                 percentage_threshold_high: float,
                                                 campaign_amount_per_category: int,
                                                 focus_campaign_param: Dict[str, Any]) -> DataFrame:
    # Exclude information campaigns and non-tracking campaigns
    df_not_inform_trackable_low_response = response_percentage_report.select(focus_campaign_param['focus_cols']) \
        .where("response_rate_percentage <= " + str(percentage_threshold_low) +
               " and trans_inform_last_3mth = 0 and trans_null_last_3mth = 0 ") \
        .orderBy('contact_trans_last_3mth', ascending=False)
    df_not_inform_trackable_high_response = response_percentage_report.select(focus_campaign_param['focus_cols']) \
        .where("response_rate_percentage >= " + str(percentage_threshold_high) +
               " and trans_inform_last_3mth = 0 and trans_null_last_3mth = 0 ") \
        .orderBy('contact_trans_last_3mth', ascending=False)

    # Select (amount) campaigns by categories and union into one dataframe
    # Add 2 criteria columns
    # main_criteria: High contact volume
    # sub_criteria: Reason to focus on the campaigns, whether low or high response
    total_focus_campaigns_low_response = filter_campaigns_by_category(df_not_inform_trackable_low_response,
                                                                      campaign_amount_per_category,
                                                                      'low_response')
    total_focus_campaigns_low_response = total_focus_campaigns_low_response.withColumn("main_criteria",
                                                                                       F.lit("High_contact")) \
        .withColumn("sub_criteria", F.lit("Low_response"))

    total_focus_campaigns_high_response = filter_campaigns_by_category(df_not_inform_trackable_high_response,
                                                                       campaign_amount_per_category,
                                                                       'high_response')
    total_focus_campaigns_high_response = total_focus_campaigns_high_response.withColumn("main_criteria",
                                                                                         F.lit("High_contact")) \
        .withColumn("sub_criteria", F.lit("High_response"))

    return total_focus_campaigns_low_response, total_focus_campaigns_high_response


def create_focus_campaign_by_volume_high_response(response_percentage_report: DataFrame,
                                                  percentage_threshold: float,
                                                  campaign_amount_per_category: int,
                                                  focus_campaign_param: Dict[str, Any]) -> DataFrame:
    # Exclude information campaigns and non-tracking campaigns
    df_not_inform_trackable = response_percentage_report.select(focus_campaign_param['focus_cols']) \
        .where("response_rate_percentage >= " + str(percentage_threshold) +
               " and trans_inform_last_3mth = 0 and trans_null_last_3mth = 0 ") \
        .orderBy('contact_trans_last_3mth', ascending=False)

    # Different category has different amount of contacts
    # The contacts_trans_last_3mth is the total times a campaign has contacted customers in the last 3 months
    # For churn-related and cross sell/upsell, the number of contacts are very high, so we filter only the campaigns that
    # have exceptionally high contacts, currently higher than 1M contacts
    df_churn = df_not_inform_trackable.where('contact_trans_last_3mth > 1000000'
                                             ' and (campaign_category = "Churn Prevention" '
                                             'or campaign_category = "Retention")') \
        .limit(campaign_amount_per_category)
    df_cross_upsell = df_not_inform_trackable.where('contact_trans_last_3mth > 1000000'
                                                    ' and (campaign_category = "Cross/Up sell"'
                                                    'or campaign_category = "Cross Sell")') \
        .limit(campaign_amount_per_category)
    df_handset = df_not_inform_trackable.where('campaign_category = "Handset"') \
        .limit(campaign_amount_per_category)  # NOTE: There are only 3 campaigns
    df_pre_to_post = df_not_inform_trackable.where('campaign_category = "Convert Pre to Post"') \
        .limit(campaign_amount_per_category)  # NOTE: There is only 1 campaign
    df_top_up = df_not_inform_trackable.where('campaign_category = "Top up"') \
        .limit(campaign_amount_per_category)  # NOTE: There are only 7 campaigns
    df_vas = df_not_inform_trackable.where('campaign_category = "VAS"') \
        .limit(campaign_amount_per_category)  # NOTE: There are only 8 campaigns

    # To be decided: write these into seperate tables or one table, or keep both
    total_focus_campaigns = df_churn.union(df_cross_upsell) \
        .union(df_handset) \
        .union(df_pre_to_post) \
        .union(df_top_up) \
        .union(df_vas)
    total_focus_campaigns = total_focus_campaigns.withColumn("main_criteria", F.lit("High_contact")) \
        .withColumn("sub_criteria", F.lit("High_response"))
    return total_focus_campaigns
