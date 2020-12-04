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
    # input_df = input_df.filter('start_of_week = "2020-06-29"')
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_time_spent_by_store_weekly",
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = node_from_config(input_df, param_config)

    return output_df


def l2_geo_count_visit_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # input_df = input_df.filter('start_of_week = "2020-06-29"')
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_count_visit_by_location_weekly",
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    output_df = node_from_config(input_df, param_config)

    return output_df


def l2_geo_time_spent_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # input_df = input_df.filter('start_of_week = "2020-06-29"')
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_time_spent_by_location_weekly",
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    output_df = l2_massive_processing(input_df, param_config)

    return output_df


def l2_geo_total_distance_km_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    # input_df = input_df.filter('start_of_week = "2020-06-29"')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_total_distance_km_weekly",
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = input_df.withColumn("week_type", add_week_type_statement("event_partition_date"))

    input_df = input_df.groupBy('imsi', 'start_of_week', 'week_type').agg(
        F.sum('distance_km').alias('distance_km')
    )

    output_df = input_df.groupBy('imsi', 'start_of_week').agg(
        F.sum(F.when((F.col('week_type') == 'weekday'), F.col('distance_km'))
              .otherwise(0)).cast(DecimalType(13, 2)).alias('distance_km_weekday'),
        F.sum(F.when((F.col('week_type') == 'weekend'), F.col('distance_km'))
              .otherwise(0)).cast(DecimalType(13, 2)).alias('distance_km_weekend'),
        F.sum(F.col('distance_km')).cast(DecimalType(13, 2)).alias('distance_km_total')
    )

    return output_df


def l2_geo_data_session_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_data_session_location_weekly",
                                                       missing_data_check_flg='Y')
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
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = node_from_config(input_df, param_config)

    win = Window().partitionBy('subscription_identifier', 'mobile_no', 'imsi', 'start_of_week') \
        .orderBy(F.col('total_call').desc(), F.col('total_call_minute').desc(), F.col('location_id').asc())

    output_df = input_df.withColumn('rank', F.row_number().over(win)).where('rank <= 3')
    output_df = output_df.groupBy('subscription_identifier', 'mobile_no', 'imsi', 'start_of_week').agg(
        F.sum(F.col('total_call')).alias('total_call'),
        F.sum(F.col('total_call_minute')).alias('total_call_minute'),
        F.max(F.when((F.col('rank') == 1), F.col('location_id'))).alias('top_voice_location_id_1st'),
        F.max(F.when((F.col('rank') == 1), F.col('latitude'))).alias('top_voice_latitude_1st'),
        F.max(F.when((F.col('rank') == 1), F.col('longitude'))).alias('top_voice_longitude_1st'),
        F.max(F.when((F.col('rank') == 2), F.col('location_id'))).alias('top_voice_location_id_2nd'),
        F.max(F.when((F.col('rank') == 2), F.col('latitude'))).alias('top_voice_latitude_2nd'),
        F.max(F.when((F.col('rank') == 2), F.col('longitude'))).alias('top_voice_longitude_2nd'),
        F.max(F.when((F.col('rank') == 3), F.col('location_id'))).alias('top_voice_location_id_3rd'),
        F.max(F.when((F.col('rank') == 3), F.col('latitude'))).alias('top_voice_latitude_3rd'),
        F.max(F.when((F.col('rank') == 3), F.col('longitude'))).alias('top_voice_longitude_3rd')
    )

    return output_df


def l2_geo_top3_voice_location_weekly(input_df: DataFrame, config_param: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="start_of_week",
                                                       target_table_name="l2_geo_top3_voice_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

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
                                                            'top_voice_latitude_3rd')))

    return output_df


def l2_geo_count_data_session_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_count_data_session_by_location_weekly",
                                                       missing_data_check_flg='Y')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = node_from_config(input_df, param_config)
    output_df = output_df.groupBy('subscription_identifier', 'mobile_no', 'imsi', 'start_of_week').agg(
        F.max(F.when(F.col('week_type') == 'weekday', F.col('count_location_id'))).alias('count_location_id_weekday'),
        F.max(F.when(F.col('week_type') == 'weekday',
                     F.col('count_distinct_location_id'))).alias('count_distinct_location_id_weekday'),
        F.max(F.when(F.col('week_type') == 'weekend', F.col('count_location_id'))).alias('count_location_id_weekend'),
        F.max(F.when(F.col('week_type') == 'weekend',
                     F.col('count_distinct_location_id'))).alias('count_distinct_location_id_weekend'),
        F.sum('count_location_id').alias('count_location_id'),
        F.sum('count_distinct_location_id').alias('count_distinct_location_id')
    )

    return output_df


def int_l2_customer_profile_imsi_daily_feature(cust_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([cust_df]):
        return get_spark_empty_df()

    cust_df = data_non_availability_and_missing_check(df=cust_df,
                                                      grouping="weekly",
                                                      par_col="event_partition_date",
                                                      target_table_name="int_l2_customer_profile_imsi_daily_feature",
                                                      missing_data_check_flg='Y')

    if check_empty_dfs([cust_df]):
        return get_spark_empty_df()

    window = Window().partitionBy('subscription_identifier', 'start_of_week') \
        .orderBy(F.col('event_partition_date').desc())

    output_df = cust_df.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').drop('_rank')

    return output_df

