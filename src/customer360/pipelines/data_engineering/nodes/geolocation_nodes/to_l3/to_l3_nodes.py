import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics
from customer360.utilities.spark_util import get_spark_session

def l3_geo_time_spent_by_location_monthly(df,sql):
    df=node_from_config(df,sql)
    return df

def l3_geo_area_from_ais_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

def l3_geo_area_from_competitor_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

##==============================Update 2020-06-12 by Thatt529==========================================##

###total_distance_km###
def l3_geo_total_distance_km_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

###Traffic_fav_location###
def l3_geo_use_Share_traffic_monthly(df,sql):
    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "start_of_week"))).drop( 'start_of_week')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2

###feature_sum_voice_location###
def l3_geo_call_location_home_work_monthly(df,sql):
    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date"))).drop( 'event_partition_date')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2

## ==============================Update 2020-06-15 by Thatt529==========================================##

###Top_3_cells_on_voice_usage###
def l3_geo_top3_cells_on_voice_usage(df,sql):
    ### config
    spark = get_spark_session()

    df = node_from_config(df, sql)
    df.createOrReplaceTempView('top3_cells_on_voice_usage')
    sql_query = """
        select
        imsi
        ,latitude
        ,longitude
        ,total_call
        ,row_number() over (partition by imsi,start_of_month order by total_call desc) as rnk
        ,start_of_month
        from top3_cells_on_voice_usage
        """
    df = spark.sql(sql_query)
    df.cache()
    df = df.where("rnk <= 3")

    return df

# 47 The favourite location
def l3_the_favourite_locations_monthly(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')
    sql_query = """
    select
    mobile_no
    ,start_of_month
    ,lac	
    ,ci
    ,sum(vol_3g) as vol_3g
    ,sum(vol_4g) as vol_4g
    ,sum(vol_5g) as vol_5g
    from l1_df_the_favourite_location_daily
    group by 1,2,3,4
    order by 2,1,3,4
    """
    l3 = spark.sql(sql_query)
    return l3