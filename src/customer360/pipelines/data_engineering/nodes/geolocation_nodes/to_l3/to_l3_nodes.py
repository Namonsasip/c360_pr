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

def l3_geo_area_from_ais_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

def l3_geo_area_from_competitor_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

# Test for Home && Work location_id Monthly
# --------------------------------------------------------------------------------------------------------------------------------------------
def l3_geo_home_duration_location_id_monthly(home_duration_dayily, sql):
    """
    :param home_duration_dayily:
    :param sql:
    :return:
    """
    # Add column Weekend and Weekday
    home_duration_dayily_with_weektype = home_duration_dayily.withColumn("week_type",
                                   F.when((F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
                                   .otherwise('weekday').cast(StringType())
                                   )
    home_duration_dayily_with_weektype.cache()
    # imsi | location_id | latitude | longitude | duration | event_partition_date | start_of_week | start_of_month | rank
    home_duration_dayily_with_weektype.createOrReplaceTempView('home_duration_dayily_with_weektype')

    # Get spark session
    spark = get_spark_session()

    df_weekend = spark.sql("""
        select
        imsi,
        location_id, latitude, longitude
        ,start_of_month
        sum(duration) as duration
        ,row_number() over ( partition by imsi order by sum(duration) desc) as rank
        from home_duration_dayily_with_weektype
        where week_type = 'weekend'
        group by imsi, location_id, latitude, longitude, start_of_month
    """)
    # imsi | location_id | latitude | longitude | duration | week_type | start_of_month | rank
    df_weekend.cache()
    df_weekend = df_weekend[df_weekend['rank']==1]
    df_weekend.createOrReplaceTempView('df_weekend')

    df_weekday = spark.sql("""
        select
        imsi,
        location_id, latitude, longitude
        ,start_of_month
        sum(duration) as duration
        ,row_number() over ( partition by imsi order by sum(duration) desc) as rank
        from home_duration_dayily_with_weektype
        where week_type = 'weekday'
        group by imsi, location_id, latitude, longitude, start_of_month
    """)
    df_weekday.cache()
    df_weekday = df_weekday[df_weekday['rank'] == 1]
    df_weekday.createOrReplaceTempView('df_weekday')

    df = spark.sql("""
        select
        a.imsi as imsi,
        a.location_id as home_weekday_loation_id,
        a.latitude as home_weekday_latitude,
        a.longitude as home_weekday_longitude,
        b.location_id as home_weekend_loation_id,
        b.latitude as home_weekend_latitude,
        b.longitude as home_weekend_longitude
        from df_weekday a
        inner join df_weekend b
        on a.imsi = b.imsi and a.start_of_month = b.start_of_month
    """)

    df2 = node_from_config(df, sql)
    return df2


def l3_geo_work_duration_location_id_monthly(work_duration_dayily, sql):
    """
    :param home_duration_dayily:
    :param sql:
    :return:
    """
    work_duration_dayily.cache()
    # imsi | location_id | latitude | longitude | duration | event_partition_date | start_of_week | start_of_month | rank
    work_duration_dayily.createOrReplaceTempView('work_duration_dayily')

    # Get spark session
    spark = get_spark_session()

    df = spark.sql("""
        select
        imsi,
        location_id, latitude, longitude, start_of_month
        ,sum(duration) as duration
        ,row_number() over ( partition by imsi order by sum(duration) desc) as rank
        from work_duration_dayily
        group by imsi, location_id, latitude, longitude, start_of_month
    """)
    # imsi | location_id | latitude | longitude | duration | week_type | start_of_month | rank

    df2 = node_from_config(df, sql)
    return df2
# --------------------------------------------------------------------------------------------------------------------------------------------