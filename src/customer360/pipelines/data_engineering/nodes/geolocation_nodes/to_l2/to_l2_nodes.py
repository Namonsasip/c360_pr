import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics
from customer360.utilities.spark_util import get_spark_session


def l2_geo_time_spent_by_location_weekly(df,sql):
    df=node_from_config(df,sql)
    return df


def l2_geo_area_from_ais_store_weekly(df, sql):
    df = node_from_config(df, sql)

    return df

def l2_geo_area_from_competitor_store_weekly(df,sql):
    df =node_from_config(df,sql)
    return df


def l2_geo_cust_subseqently_distance_weekly(df, sql):

    # Add start_of_week
    df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))

    # Add week_type
    df = df.withColumn("week_type", F.when(
        (F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
                                                                  .otherwise('weekday').cast(StringType())
                                                                  )

    print(df.printSchema())

    # start_of_week, weekday= , weekend=
    df_week_type = df.groupBy('imsi', 'start_of_week', 'week_type') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'week_type', 'distance_km')

    print("Group by pass with week_type")

    df_week = df.groupBy('imsi', 'start_of_week') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'distance_km')

    print("Group by pass")

    # Left join weekday and weekend
    df_finish_week = df_week.join(df_week_type, [df_week.imsi == df_week_type.imsi,
                                                 df_week_type.week_type == 'weekday',
                                                 df_week.start_of_week == df_week_type.start_of_week], 'left') \
        .select(df_week.imsi, df_week.start_of_week, df_week.distance_km,
                df_week_type.distance_km.alias('weekday_distance_km'))

    df_finish_week_2 = df_finish_week.join(df_week_type, [df_finish_week.imsi == df_week_type.imsi,
                                                          df_week_type.week_type == 'weekend',
                                                          df_finish_week.start_of_week == df_week_type.start_of_week],
                                           'left') \
        .select(df_finish_week.imsi, df_finish_week.start_of_week, df_finish_week.distance_km,
                df_finish_week.weekday_distance_km, df_week_type.distance_km.alias('weekend_distance_km'))
    # | imsi | start_of_week | distance_km | weekday_distance_km | weekend_distance_km |

    print('Start use node from config')

    df = df_finish_week_2.groupBy('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km') \
        .agg({'imsi': 'count'}).withColumnRenamed('count(imsi)', 'count_row') \
        .select('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km')

    # df = node_from_config(df_finish_week_2, sql)

    return df

##==============================Update 2020-06-12 by Thatt529==========================================##

###total_distance_km###
def l2_geo_total_distance_km_weekly(df, sql):
    df = node_from_config(df, sql)
    return df

###Traffic_fav_location###
def   l2_geo_use_traffic_home_work_weekly(df, sql):
    l2_df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))).drop('event_partition_date')
    l2_df_2 = node_from_config(l2_df, sql)
    return l2_df_2

###Number_of_base_station###
def l2_geo_data_count_location_weekly(L1_DF,sql):
    L2_DF = L1_DF.withColumn("START_OF_WEEK", F.to_date(F.date_trunc('WEEK', "event_partition_date"))).drop(
        'event_partition_date')
    df = node_from_config(L2_DF, sql)
    return df


###feature_sum_voice_location###
def l2_geo_call_home_work_location_weekly(df,sql):
    l2_df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))).drop( 'event_partition_date')
    l2_df_2 =node_from_config(l2_df,sql)
    return l2_df_2

## ==============================Update 2020-06-15 by Thatt529==========================================##

###Top_3_cells_on_voice_usage###
def l2_geo_top3_cells_on_voice_usage(df,sql):
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
    ,row_number() over (partition by imsi,start_of_week order by total_call desc) as rnk
    ,start_of_week
    from top3_cells_on_voice_usage
    """
    df = spark.sql(sql_query)
    df.cache()
    df = df.where("rnk <= 3")

    return df





### 47 l1_the_favourite_locations_daily ====================\
def l2_the_favourite_locations_weekly(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')
    sql_query = """
    select
    mobile_no
    ,start_of_week
    ,lac	
    ,ci
    ,sum(vol_3g) as vol_3g
    ,sum(vol_4g) as vol_4g
    ,sum(vol_5g) as vol_5g
    from l1_df_the_favourite_location_daily
    group by 1,2,3,4
    order by 2,1,3,4
    """
    l2 = spark.sql(sql_query)
    return l2


#27 Same favourite location for weekend and weekday
def l2_same_favourite_location_weekend_weekday_weekly(l0_geo_cust_cell_visit_time_df):
    ### config
    spark = get_spark_session()


    # Assign day_of_week to weekday or weekend
    geo_df = l0_geo_cust_cell_visit_time_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "time_in")))\
        .withColumn("start_of_month",F.to_date(F.date_trunc('month',"time_in")))

    l0_geo_cust_cell_visit_time_df.createOrReplaceTempView('l0_geo_cust_cell_visit_time_df')
    geo_df.createOrReplaceTempView('geo_df')
    sql_query = """
    select
    imsi
    ,start_of_week
    ,case when dayofweek(time_in) = 2 or dayofweek(time_in) = 3 or 
    dayofweek(time_in) = 4 or dayofweek(time_in) = 5 or dayofweek(time_in) = 6 then "weekday"
    else "weekend"
    end as weektype
    ,location_id
    ,sum(duration) as duration_sumtime
    from geo_df
    where imsi is not NULL
    group by 1,2,3,4 order by 1,2,3,4,5 desc
    """
    l2 = spark.sql(sql_query)
    return l2

