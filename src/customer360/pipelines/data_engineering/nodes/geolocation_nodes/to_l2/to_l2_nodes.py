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
                                                       missing_data_check_flg='N')
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
                                                       missing_data_check_flg='N')
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
                                                       missing_data_check_flg='N')
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
                                                       missing_data_check_flg='N')
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

    input_df = node_from_config(input_df, param_config)

    win = Window().partitionBy('access_method_num', 'start_of_week') \
        .orderBy(F.col('total_call').desc(), F.col('total_call_minute').desc(), F.col('location_id').asc())

    output_df = input_df.withColumn('rank', F.row_number().over(win)).where('rank <= 3')
    output_df = output_df.groupBy('access_method_num', 'start_of_week').agg(
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


def l2_geo_most_frequently_used_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_most_frequently_used_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    def window_statement(col_name: str) -> list:
        window = Window().partitionBy('start_of_week').orderBy(
            F.col(col_name).desc(), F.col('location_id').asc()
        )
        window_week_type = Window().partitionBy('start_of_week', 'week_type').orderBy(
            F.col(col_name).desc(), F.col('location_id').asc()
        )
        return [window, window_week_type]

    output_df = input_df.groupBy('start_of_week', 'start_of_month',
                                 'location_id', 'latitude', 'longitude', 'week_type').agg(
        F.countDistinct('imsi').alias('number_customer'),
        F.sum('no_of_call').alias('no_of_call'),
        F.sum('total_minute').alias('total_minute'),
        F.sum('call_traffic').alias('call_traffic'),
        F.sum('vol_all').alias('vol_all'),
        F.sum('vol_5g').alias('vol_5g'),
        F.sum('vol_4g').alias('vol_4g'),
        F.sum('vol_3g').alias('vol_3g')
    )

    return output_df


def l2_geo_count_data_session_by_location_weekly(input_df: DataFrame, param_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="weekly",
                                                       par_col="event_partition_date",
                                                       target_table_name="l2_geo_count_data_session_by_location_weekly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    output_df = node_from_config(input_df, param_config)
    output_df = output_df.groupBy('imsi', 'start_of_week').agg(
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
                                                      target_table_name="int_l2_customer_profile_imsi_daily_feature")

    if check_empty_dfs([cust_df]):
        return get_spark_empty_df()

    def __divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = cust_df
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = param_config.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col('start_of_week').isin(*[curr_item]))
        output_df = small_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))\
            .withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col('event_partition_date'))))
        window = Window().partitionBy('subscription_identifier', 'start_of_week', 'start_of_month') \
            .orderBy(F.col('event_partition_date').desc())
        output_df = output_df.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').drop('_rank')
        CNTX.catalog.save(param_config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('start_of_week').isin(*[first_item]))
    return_df = return_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))\
            .withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col('event_partition_date'))))
    window = Window().partitionBy('subscription_identifier', 'start_of_week', 'start_of_month') \
        .orderBy(F.col('event_partition_date').desc())
    return_df = return_df.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').drop('_rank')

    return return_df

