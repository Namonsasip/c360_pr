from datetime import date
from datetime import timedelta
from datetime import datetime
from typing import Dict, Any
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session
from pyspark.sql import functions as F
spark = get_spark_session()


def create_l0_campaign_history_master_active(input_campaign_master:DataFrame, output:Dict[str, Any]) -> DataFrame:
    stmt = "TRUNCATE TABLE "+output["schema"]+"."+output["table"]
    print(stmt)
    spark.sql(stmt)
    return input_campaign_master


def create_l5_campaign_distinct_contact_response(child_response_full:DataFrame,
                                                 child_response_params:Dict[str, Any]) -> DataFrame:
    child_response = child_response_full.select(child_response_params["source_cols"])

    stmt = "TRUNCATE TABLE "+child_response_params["schema"]+"."+child_response_params["table"]
    print(stmt)
    spark.sql(stmt)
    child_response = child_response.fillna(0, subset=child_response_params["source_cols"][5:])
    print(child_response_params["agg_cols"])
    # #source_cols: ['campaign_category','child_code','campaign_type','tools','tracking_flag','contact_trans_m0',
    #               'contact_trans_m1','contact_trans_m2','contact_sub_m0','contact_sub_m1','contact_sub_m2',
    #               'response_m0','response_m1','response_m2']
    # #agg_cols: ['contact_last_3mth','contact_sub_last_3mth','response_last_3mth']
    # Improvement: would be nice if I can iterate this
    child_response_agg = child_response\
        .withColumn(child_response_params["agg_cols"][0], F.lit(F.col(child_response_params["source_cols"][5]) +
                                               F.col(child_response_params["source_cols"][6]) +
                                               F.col(child_response_params["source_cols"][7])))\
        .withColumn(child_response_params["agg_cols"][1], F.lit(F.col(child_response_params["source_cols"][8]) +
                                                   F.col(child_response_params["source_cols"][9]) +
                                                   F.col(child_response_params["source_cols"][10])))\
        .withColumn(child_response_params["agg_cols"][2], F.lit(F.col(child_response_params["source_cols"][11]) +
                                                F.col(child_response_params["source_cols"][12]) +
                                                F.col(child_response_params["source_cols"][13])))
    output_child_response_agg_cols = child_response_params["source_cols"][:5]
    output_child_response_agg_cols.extend(child_response_params["agg_cols"])
    output_child_response_agg = child_response_agg.select(output_child_response_agg_cols)
    print("==========================SUCCESS=========================")
    return output_child_response_agg


def create_l5_response_percentage_report(distinct_child_response_agg:DataFrame,
                                         campaign_master:DataFrame,
                                         report_param:Dict[str, Any]) -> DataFrame:
    stmt = "TRUNCATE TABLE " + report_param["schema"] + "." + report_param["table"]
    print(stmt)
    try:
        spark.sql(stmt)
        print("Truncated the old table: "+report_param["schema"] + "." + report_param["table"])
    except:
        print("No existing table: "+report_param["schema"] + "." + report_param["table"])
    campaign_mst_exclude_information = campaign_master.where('offer_type <> "information" and month_id = "2020-02-29"')
    campaign_agg_no_info = distinct_child_response_agg.alias('agg')\
        .join(campaign_mst_exclude_information.alias('mst'),
              F.col('agg.'+report_param['join_cols']) == F.col('mst.'+report_param['join_cols']), 'left')\
        .select("agg.*")
    response_rate_df = campaign_agg_no_info\
        .withColumn(report_param['response_rate_cols'], F.lit(F.col('response_last_3mth') / F.col('contact_last_3mth') * 100))\
        .fillna(0, subset=report_param['response_rate_cols'])
    return response_rate_df
