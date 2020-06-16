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


def l2_geo_time_spent_by_location_weekly(df, sql):
    df = node_from_config(df, sql)
    return df


def l2_geo_area_from_ais_store_weekly(df, sql):
    df = node_from_config(df, sql)

    return df


def l2_geo_area_from_competitor_store_weekly(df, sql):
    df = node_from_config(df, sql)
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
