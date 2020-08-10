from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, l2_massive_processing
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def distance_calculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) +
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) *
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast(DecimalType(13, 2))


def add_week_type_statement(param_col: str) -> Column:
    return (F.when((F.dayofweek(F.col(param_col)) == 1) | (F.dayofweek(F.col(param_col)) == 7), 'weekend')
            .otherwise('weekday')).cast(StringType())


def l2_geo_time_spent_by_store_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_time_spent_by_store_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = node_from_config(input_df, param_config)

    return output_df


def l2_geo_count_visit_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_count_visit_by_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    output_df = node_from_config(input_df, param_config)

    return output_df


def l2_geo_time_spent_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=input_df,
                                                 grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_time_spent_by_location_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    output_df = l2_massive_processing(input_df, param_config)

    return output_df


def l2_geo_total_distance_km_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_total_distance_km_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = input_df.withColumn("week_type", add_week_type_statement("event_partition_date"))

    input_df = input_df.groupBy('imsi', 'start_of_week', 'start_of_month', 'week_type').agg(
        F.sum('distance_km').alias('distance_km')
    )

    output_df = input_df.groupBy('imsi', 'start_of_week', 'start_of_month').agg(
        F.sum(F.when((F.col('week_type') == 'weekday'), F.col('distance_km')).otherwise(0)).alias('distance_km_weekday'),
        F.sum(F.when((F.col('week_type') == 'weekend'), F.col('distance_km')).otherwise(0)).alias('distance_km_weekend'),
        F.sum(F.col('distance_km')).alias('distance_km_total')
    )

    return output_df


def l2_geo_data_session_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_data_session_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = node_from_config(input_df, param_config)
    return output_df


def int_l2_geo_top3_voice_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_top3_voice_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    win = Window().partitionBy('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month') \
        .orderBy(F.col('total_call').desc())

    output_df = input_df.withColumn('rank', F.row_number().over(win)).where('rank <= 3')
    output_df = output_df.groupBy('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month') \
        .agg(F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_location_1st'),
             F.max(F.when((F.col('rank') == 1), F.col('latitude'))).alias('top_voice_latitude_1st'),
             F.max(F.when((F.col('rank') == 1), F.col('longitude'))).alias('top_voice_longitude_1st'),
             F.max(F.when((F.col('rank') == 2), F.col('location'))).alias('top_voice_location_2nd'),
             F.max(F.when((F.col('rank') == 2), F.col('latitude'))).alias('top_voice_latitude_2nd'),
             F.max(F.when((F.col('rank') == 2), F.col('longitude'))).alias('top_voice_longitude_2nd'),
             F.max(F.when((F.col('rank') == 3), F.col('location'))).alias('top_voice_location_3rd'),
             F.max(F.when((F.col('rank') == 3), F.col('latitude'))).alias('top_voice_latitude_3rd'),
             F.max(F.when((F.col('rank') == 3), F.col('longitude'))).alias('top_voice_longitude_3rd')
             )

    return output_df


def l2_geo_top3_voice_location_weekly(input_df: DataFrame, config_param: str) -> DataFrame:
    # Add column distance between 1st and 2nd, 3rd
    output_df = input_df.withColumn('distance_2nd_voice_location', F.when((F.col('top_voice_latitude_1st').isNull()) |
                                                                          (F.col('top_voice_latitude_2nd').isNull()), 0)
                                    .otherwise(distance_calculate_statement('top_voice_latitude_1st',
                                                                            'top_voice_latitude_1st',
                                                                            'top_voice_latitude_2nd',
                                                                            'top_voice_latitude_2nd'))) \
        .withColumn('distance_3rd_voice_location', F.when((F.col('top_voice_latitude_1st').isNull()) |
                                                          (F.col('top_voice_latitude_3rd').isNull()), 0)
                    .otherwise(distance_calculate_statement('top_voice_latitude_1st',
                                                            'top_voice_latitude_1st',
                                                            'top_voice_latitude_3rd',
                                                            'top_voice_latitude_3rd'))) \
        .select('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month',
                'top_voice_location_1st', 'top_voice_location_2nd', 'top_voice_location_3rd',
                'distance_2nd_voice_location', 'distance_3rd_voice_location')

    return output_df
