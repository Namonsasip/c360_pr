from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def distance_calculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) + \
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) * \
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast(DecimalType(13, 2))


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

    output_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))) \
        .drop('event_partition_date')
    output_df = node_from_config(output_df, param_config)

    return output_df


def l2_geo_time_spent_by_location_weekly(df, sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_time_spent_by_location_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    df = massive_processing_time_spent_weekly(df, sql, "l2_geo_time_spent_by_location_weekly", 'start_of_week')
    print('DEBUG : ------------------------------------------------> l2_geo_time_spent_by_location_weekly')
    df.show(10)

    return df


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

    # Add week_type
    df = input_df.withColumn("week_type", F.when(
        (F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
                             .otherwise('weekday').cast(StringType())
                             )

    # start_of_week, weekday= , weekend=
    df_week_type = df.groupBy('imsi', 'start_of_week', 'week_type') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'week_type', 'distance_km')

    df_week = df.groupBy('imsi', 'start_of_week') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'distance_km')

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

    df = df_finish_week_2.groupBy('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km') \
        .agg({'imsi': 'count'}).withColumnRenamed('count(imsi)', 'count_row') \
        .select('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km')

    # df = node_from_config(df_finish_week_2, sql)

    return df


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
                                                            'top_voice_latitude_3rd')))\
        .select('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month',
                'top_voice_location_1st', 'top_voice_location_2nd', 'top_voice_location_3rd',
                'distance_2nd_voice_location', 'distance_3rd_voice_location')

    return output_df


def massive_processing_time_spent_weekly(data_frame: DataFrame, sql, output_df_catalog, partition_col) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select(partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(partition_col).isin(*[curr_item]))
        small_df = node_from_config(small_df, sql)
        # small_df = add_start_of_week_and_month(small_df, "time_in")
        # small_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
        # output_df = ss.sql(sql)
        CNTX.catalog.save(output_df_catalog, small_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(partition_col).isin(*[first_item]))
    return_df = node_from_config(return_df, sql)
    # return_df = add_start_of_week_and_month(return_df, "time_in")
    # return_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
    # return_df = ss.sql(sql)
    return return_df