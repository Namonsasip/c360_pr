from datetime import date
from datetime import timedelta
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session
from pyspark.sql import functions as F
import logging



def create_l0_campaign_history_master_active(input_campaign_master:DataFrame, output:Dict[str, Any]) -> DataFrame:
    return input_campaign_master


def create_l5_campaign_distinct_contact_response(child_response_full:DataFrame,
                                                 child_response_params:Dict[str, Any]) -> DataFrame:
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
                                                           F.col(child_response_params["source_cols"][(iter_var*3)+0]) +
                                                           F.col(child_response_params["source_cols"][(iter_var*3)+1]) +
                                                           F.col(child_response_params["source_cols"][(iter_var*3)+2])))
        iter_var = iter_var + 1
    # Example if each iterate:
    # child_response_agg = child_response_agg.withColumn('contact_last_3mth', F.lit(F.col('contact_trans_m0')+
    #                                                                               F.col('contact_trans_m1')+
    #                                                                               F.col('contact_trans_m2))
    # print("==========================SUCCESS=========================")
    return child_response_agg


def create_l5_response_percentage_report(distinct_child_response_agg:DataFrame,
                                         campaign_master:DataFrame,
                                         report_param:Dict[str, Any]) -> DataFrame:
    # excluding the offer_type != information because the new column includes the non tracking information campaigns
    campaign_mst_exclude_information = campaign_master.where('offer_type <> "information" and month_id = "2020-02-29"')
    # campaign_mst_exclude_information = campaign_master.where('month_id = "2020-02-29"')
    campaign_agg_no_info = distinct_child_response_agg.alias('agg')\
        .join(campaign_mst_exclude_information.alias('mst'),
              F.col('agg.'+report_param['join_cols']) == F.col('mst.'+report_param['join_cols']), 'left')\
        .select("agg.*")
    response_rate_df = campaign_agg_no_info\
        .withColumn(report_param['response_rate_cols'], F.lit(F.col('trans_response_last_3mth') / F.col('contact_trans_last_3mth')*100))\
        .fillna(0, subset=report_param['response_rate_cols'])
    return response_rate_df

