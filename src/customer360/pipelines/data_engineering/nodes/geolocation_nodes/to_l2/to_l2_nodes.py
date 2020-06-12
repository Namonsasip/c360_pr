import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics

def l2_geo_time_spent_by_location_weekly(df,sql):
    df=node_from_config(df,sql)
    return df

def l2_geo_area_from_ais_store_weekly(df,sql):

    df =node_from_config(df,sql)

    return df

def l2_geo_area_from_competitor_store_weekly(df,sql):
    df =node_from_config(df,sql)
    return df


##==============================Update 2020-06-12 by Thatt529==========================================##

###total_distance_km###
def l2_geo_total_distance_km_weekly(df,sql):
    df = node_from_config(df,sql)
    return df

###Traffic_fav_location###
def   l2_geo_use_traffic_home_work_weekly(df,sql):
    l2_df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))).drop( 'event_partition_date')
    l2_df_2 =node_from_config(l2_df,sql)
    return l2_df_2

###Number_of_base_station###
def l2_geo_data_count_location_weekly(L1_DF,sql):
    L2_DF = L1_DF.withColumn("START_OF_WEEK", F.to_date(F.date_trunc('WEEK', "EVENT_PARTITION_DATE"))).drop(
        'EVENT_PARTITION_DATE')
    df = node_from_config(L2_DF, sql)
    return df


###feature_sum_voice_location###
def l2_geo_call_home_work_location_weekly(df,sql):
    l2_df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))).drop( 'event_partition_date')
    l2_df_2 =node_from_config(l2_df,sql)
    return l2_df_2

