from pyspark.sql import DataFrame, Column, Window
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql,\
    __divide_chunks, check_empty_dfs, data_non_availability_and_missing_check

from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def int_l3_geo_top3_visit_exclude_hw_monthly(input_df: DataFrame, homework_df: DataFrame,
                                             param_config: str) -> DataFrame:
    if check_empty_dfs([input_df, homework_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_geo_top_visit_exclude_homework_monthly",
                                                       missing_data_check_flg='N')

    homework_df = data_non_availability_and_missing_check(df=homework_df,
                                                          grouping="monthly",
                                                          par_col="start_of_month",
                                                          target_table_name="l3_geo_top_visit_exclude_homework_monthly",
                                                          missing_data_check_flg='N')

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(F.max(
                F.to_date(
                    F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM')))
            ).alias("max_date")),
            homework_df.select(F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(F.col("start_of_month") <= min_value)
    homework_df = homework_df.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([input_df, homework_df]):
        return get_spark_empty_df()

    input_df = input_df.withColumn("start_of_month", F.to_date(
        F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM'))))

    window = Window().partitionBy('imsi', 'location_id', 'partition_weektype') \
        .orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)

    # Group by
    input_df = input_df.groupBy('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude',
                                'partition_weektype').agg(
        F.sum(F.col('duration')).alias('duration'),
        F.sum(F.col('days')).alias('days'),
        F.countDistinct(F.col('hour')).alias('hours')
    ).withColumn('duration_3m', F.sum("duration").over(window))\
        .withColumn('days_3m', F.sum('days').over(window))\
        .withColumn('hours_3m', F.sum('hours').over(window))

    window_row = Window().partitionBy('imsi', 'start_of_month', 'partition_weektype').orderBy(
        F.col('location_id').asc(),
        F.col('duration_3m').desc(),
        F.col('days_3m').desc(),
        F.col('hours_3m').desc()
    )

    output_df = input_df.withColumn('row_num_weektype', F.row_number().over(window_row))\
        .where('row_num_weektype <= 6').drop('row_num_weektype')

    result_df = output_df.join(homework_df,
                               (input_df.imsi == homework_df.imsi) &
                               (input_df.start_of_month == homework_df.start_of_month) &
                               (input_df.location_id != homework_df.work_location_id) &
                               (input_df.location_id != homework_df.home_location_id_weekday) &
                               (input_df.location_id != homework_df.home_location_id_weekend)
                               , 'inner').select(
        input_df.imsi, input_df.start_of_month, input_df.location_id, input_df.latitude, input_df.longitude,
        'partition_weektype', 'duration_3m', 'days_3m', 'hours_3m'
    )

    return result_df


def l3_geo_top3_visit_exclude_hw_monthly(input_df: DataFrame, param_config: str) -> DataFrame:

    window = Window().partitionBy('imsi', 'start_of_month', 'partition_weektype').orderBy(
        F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc(), F.col('hours_3m').desc()
    )

    window_all = Window().partitionBy('imsi', 'start_of_month').orderBy(
        F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc(), F.col('hours_3m').desc()
    )

    window_sum = Window().partitionBy('imsi', 'start_of_month', 'location_id')
    result_df = input_df.withColumn('duration_3m_all', F.sum('duration_3m').over(window_sum))\
        .withColumn('days_3m_all', F.sum('days_3m').over(window_sum))\
        .withColumn('hours_3m_all', F.sum('hours_3m').over(window_sum))

    result_df = result_df.withColumn('row_num', F.row_number().over(window))\
        .withColumn('row_num_all', F.row_number().over(window_all))\
        .where('row_num <= 3 and row_num_all <= 3')

    result_df = result_df.groupBy('imsi', 'start_of_month').agg(
        F.max(F.when((F.col('row_num_all') == 1), F.col('location_id'))).alias('top_location_1st'),
        F.max(F.when((F.col('row_num_all') == 1), F.col('latitude'))).alias('top_latitude_1st'),
        F.max(F.when((F.col('row_num_all') == 1), F.col('longitude'))).alias('top_longitude_1st'),
        F.max(F.when((F.col('row_num_all') == 2), F.col('location_id'))).alias('top_location_2nd'),
        F.max(F.when((F.col('row_num_all') == 2), F.col('latitude'))).alias('top_latitude_2nd'),
        F.max(F.when((F.col('row_num_all') == 2), F.col('longitude'))).alias('top_longitude_2nd'),
        F.max(F.when((F.col('row_num_all') == 3), F.col('location_id'))).alias('top_location_3rd'),
        F.max(F.when((F.col('row_num_all') == 3), F.col('latitude'))).alias('top_latitude_3rd'),
        F.max(F.when((F.col('row_num_all') == 3), F.col('longitude'))).alias('top_longitude_3rd'),
        F.max(F.when(((F.col('row_num') == 1) & F.col('partition_weektype') == 'WEEKDAY'),
                     F.col('location_id'))).alias('top_location_1st_weekday'),
        F.max(F.when(((F.col('row_num') == 2) & F.col('partition_weektype') == 'WEEKDAY'),
                     F.col('location_id'))).alias('top_location_2nd_weekday'),
        F.max(F.when(((F.col('row_num') == 3) & F.col('partition_weektype') == 'WEEKDAY'),
                     F.col('location_id'))).alias('top_location_3rd_weekday'),
        F.max(F.when(((F.col('row_num') == 1) & F.col('partition_weektype') == 'WEEKEND'),
                     F.col('location_id'))).alias('top_location_1st_weekend'),
        F.max(F.when(((F.col('row_num') == 2) & F.col('partition_weektype') == 'WEEKEND'),
                     F.col('location_id'))).alias('top_location_2nd_weekend'),
        F.max(F.when(((F.col('row_num') == 3) & F.col('partition_weektype') == 'WEEKEND'),
                     F.col('location_id'))).alias('top_location_3rd_weekend')
    ).withColumn('same_fav_weekday_and_weekend',
                 F.when(F.col('top_location_1st_weekday') == F.col('top_location_1st_weekend'), 'Y').otherwise('N'))

    return result_df


def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]
    input_df = input_df.where('{0}={1}'.format(par_col, str(max_date)))
    return input_df


def massive_processing_for_int_home_work_monthly(input_df: DataFrame, config_home: str, config_work: str
                                                 ) -> DataFrame:

    CNTX = load_context(Path.cwd(), env=os.getenv("CONF", "base"))

    # ----- Data Availability Checks -----
    if check_empty_dfs(input_df):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df,
                                                       grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l3_geo_home_work_location_id_monthly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs(input_df):
        return get_spark_empty_df()
    # ----- Transformation -----

    data_frame = input_df
    dates_list = data_frame.select('partition_month').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(__divide_chunks(mvv_array, 1))
    add_list = mvv_new

    # Set first dataframe to merge
    if len(add_list) != 1:
        last_item = add_list[0]
        logging.info("First date to run for {0}".format(str(last_item)))
        small_df_last = data_frame.filter(F.col('partition_month').isin(*[last_item]))

        # Add a column: start_of_month
        small_df_last = small_df_last.withColumn("start_of_month", F.to_date(
            F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM'))))
        # Work
        output_df_work = node_from_config(small_df_last, config_work)
        CNTX.catalog.save(config_work["output_catalog"], output_df_work)

        # Home
        output_df_home = node_from_config(small_df_last, config_home)
        CNTX.catalog.save(config_home["output_catalog"], output_df_home)
        add_list.remove(last_item)

    elif len(add_list) == 1:
        last_item = add_list[0]
        small_df_last = data_frame.filter(F.col('partition_month').isin(*[last_item]))

        # Add a column: start_of_month
        small_df_last = small_df_last.withColumn("start_of_month", F.to_date(
            F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM'))))

        output_df_work = node_from_config(small_df_last, config_work)
        output_df_home = node_from_config(small_df_last, config_home)

        return [output_df_home, output_df_work]

    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col('partition_month').isin(*[curr_item]))

        # Add a columns: start_of_month
        small_df = small_df.withColumn("start_of_month", F.to_date(
            F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM'))))

        output_df_work = node_from_config(small_df, config_work)
        CNTX.catalog.save(config_work["output_catalog"], output_df_work)
        output_df_home = node_from_config(small_df, config_home)
        CNTX.catalog.save(config_home["output_catalog"], output_df_home)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('partition_month').isin(*[first_item]))

    # Add a columns: start_of_month
    return_df = return_df.withColumn("start_of_month", F.to_date(
        F.date_trunc('month', F.to_date((F.col('partition_month')).cast(StringType()), 'yyyyMM'))))

    output_df_work = node_from_config(return_df, config_work)
    output_df_home = node_from_config(return_df, config_home)

    return [output_df_home, output_df_work]


def int_geo_work_location_id_monthly(work_monthly: DataFrame):
    w_work = Window().partitionBy('imsi', 'location_id') \
        .orderBy(F.col("Month").cast("long")) \
        .rangeBetween(-(86400 * 89), 0)

    work_last_3m = work_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")) \
        .withColumn("duration_3m", F.sum("duration").over(w_work)) \
        .withColumn("days_3m", F.sum('days').over(w_work)) \
        .withColumn("hours_3m", F.sum("hours").over(w_work))

    work_last_3m = work_last_3m.dropDuplicates(['imsi', 'start_of_month', 'location_id', 'duration_3m', 'days_3m']) \
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m', 'hours_3m')

    w_work_num_row = Window().partitionBy('imsi', 'start_of_month') \
        .orderBy(F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc())

    result_df = work_last_3m.withColumn('row_num', F.row_number().over(w_work_num_row))
    result_df = result_df.where('row_num = 1').drop('row_num')
    result_df = result_df.select('imsi', 'start_of_month',
                                 (F.col('location_id').alias('work_location_id')),
                                 (F.col('latitude').alias('work_latitude')),
                                 (F.col('longitude').alias('work_longitude')))

    return [result_df, work_last_3m]


def int_geo_home_location_id_monthly(home_monthly: DataFrame) -> DataFrame:
    w_home = Window().partitionBy('imsi', 'location_id', 'partition_weektype') \
        .orderBy(F.col("Month").cast("long")) \
        .rangeBetween(-(86400 * 89), 0)

    home_last_3m = home_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")) \
        .withColumn("duration_3m", F.sum("duration").over(w_home)) \
        .withColumn("days_3m", F.sum('days').over(w_home))

    home_last_3m = home_last_3m.select('imsi', 'start_of_month', 'partition_weektype', 'location_id', 'latitude',
                                       'longitude', 'duration_3m', 'days_3m').dropDuplicates()

    w_num_row = Window().partitionBy('imsi', 'partition_weektype', 'start_of_month') \
        .orderBy(F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc())
    home_last_3m = home_last_3m.withColumn('row_num', F.row_number().over(w_num_row))
    home_last_3m = home_last_3m.where('row_num = 1').drop('row_num')

    home_last_3m = home_last_3m.groupBy('imsi', 'start_of_month').agg(
        F.max(F.when((F.col('partition_weektype') == 'WEEKDAY'), F.col('location_id'))).alias('home_location_id_weekday'),
        F.max(F.when((F.col('partition_weektype') == 'WEEKDAY'), F.col('latitude'))).alias('home_latitude_weekday'),
        F.max(F.when((F.col('partition_weektype') == 'WEEKDAY'), F.col('longitude'))).alias('home_longitude_weekday'),
        F.max(F.when((F.col('partition_weektype') == 'WEEKEND'), F.col('location_id'))).alias('home_location_id_weekend'),
        F.max(F.when((F.col('partition_weektype') == 'WEEKEND'), F.col('latitude'))).alias('home_latitude_weekend'),
        F.max(F.when((F.col('partition_weektype') == 'WEEKEND'), F.col('longitude'))).alias('home_longitude_weekend')
    )

    return home_last_3m


def l3_geo_home_work_location_id_monthly(home_df: DataFrame, work_df: DataFrame) -> DataFrame:
    if check_empty_dfs([home_df, work_df]):
        return get_spark_empty_df()

    home_df = data_non_availability_and_missing_check(df=home_df,
                                                      grouping="monthly",
                                                      par_col="start_of_month",
                                                      target_table_name="l3_geo_home_work_location_id_monthly",
                                                      missing_data_check_flg='N')

    work_df = data_non_availability_and_missing_check(df=work_df,
                                                      grouping="monthly",
                                                      par_col="start_of_month",
                                                      target_table_name="l3_geo_home_work_location_id_monthly",
                                                      missing_data_check_flg='N')

    min_value = union_dataframes_with_missing_cols(
        [
            home_df.select(F.max(F.col("start_of_month")).alias("max_date")),
            work_df.select(F.max(F.col("start_of_month")).alias("max_date"))
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    home_df = home_df.filter(F.col("start_of_month") <= min_value)
    work_df = work_df.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([home_df, work_df]):
        return get_spark_empty_df()

    home_df_temp = home_df.select('imsi', 'start_of_month').distinct()
    work_df_temp = work_df.select('imsi', 'start_of_month').distinct()
    list_imsi = home_df_temp.union(work_df_temp).distinct()

    final_df = list_imsi.join(home_df, ['imsi', 'start_of_month'], 'left').select(
        list_imsi.start_of_month, list_imsi.imsi,
        'home_location_id_weekday',
        'home_latitude_weekday',
        'home_longitude_weekday',
        'home_location_id_weekend',
        'home_latitude_weekend',
        'home_longitude_weekend'
    )

    final_df = final_df.join(work_df, ['imsi', 'start_of_month'], 'left').select(
        final_df.start_of_month, final_df.imsi,
        'work_location_id',
        'work_latitude',
        'work_longitude',
        'home_location_id_weekday',
        'home_latitude_weekday',
        'home_longitude_weekday',
        'home_location_id_weekend',
        'home_latitude_weekend',
        'home_longitude_weekend'
    )

    return final_df


def l3_geo_home_weekday_city_citizens_monthly(home_df: DataFrame, master_df: DataFrame, param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([home_df, master_df]):
        return get_spark_empty_df()

    master_df = get_max_date_from_master_data(master_df, 'partition_date')

    join_df = home_df.join(master_df, [home_df.home_location_id_weekday == master_df.location_id], 'left').select(
        home_df.start_of_month, 'imsi',
        'home_location_id_weekday', 'region_name', 'province_name', 'district_name', 'sub_district_name'
    )

    output_df = node_from_config(join_df, param_config)
    return output_df


def score_calculate_statement(window, weight: list) -> Column:
    weight = [0.7, 0.2, 0.1] if len(weight) == 0 else weight
    return (weight[0] * (F.col('duration_3m') / F.sum('duration_3m').over(window)) +
            weight[1] * (F.col('hours_3m') / F.sum('hours_3m').over(window)) +
            weight[2] * (F.col('days_3m') / F.sum('days_3m').over(window)))


def distance_calculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) +
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) *
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast(DecimalType(13, 2))


def l3_geo_work_area_center_average_monthly(work_df_3m: DataFrame, work_df: DataFrame, param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([work_df_3m, work_df]):
        return get_spark_empty_df()

    w = Window().partitionBy('imsi', 'start_of_month')

    # Calculate score
    work_df_3m = work_df_3m.withColumn('score', score_calculate_statement(w, [0.7, 0.2, 0.1]))

    # Calculate average lat and long
    work_center_average = work_df_3m.groupBy('imsi', 'start_of_month').agg(
        (F.sum(F.col('latitude') * F.col('score')) / F.sum(F.col('score'))).alias('avg_work_latitude'),
        (F.sum(F.col('longitude') * F.col('score')) / F.sum(F.col('score'))).alias('avg_work_longitude')
    )

    w_order = Window().partitionBy('imsi', 'start_of_month').orderBy(
        F.col('location_id').desc(), F.col('score').desc()
    )

    work_df_rank = work_df_3m.withColumn('rank', F.row_number().over(w_order))
    work_df_rank = work_df_rank.where('rank > 6')

    work_join_df = work_df_rank\
        .join(work_center_average, [work_center_average.imsi == work_df_rank.imsi,
                             work_center_average.start_of_month == work_df_rank.start_of_month], 'left') \
        .select(work_df_rank.imsi, work_df_rank.start_of_month, 'avg_work_latitude',
                'avg_work_longitude', 'latitude', 'longitude')

    # Calculate radius
    work_radius = work_join_df.groupBy('imsi', 'start_of_month').agg(
        F.max(distance_calculate_statement('avg_work_latitude', 'avg_work_longitude',
                                           'latitude', 'longitude')).alias('radius')
    )

    result_df = work_center_average.join(work_radius, ['imsi', 'start_of_month'], 'inner').select(
        work_center_average.imsi, work_center_average.start_of_month, 'avg_work_latitude', 'avg_work_longitude', 'radius'
    )

    # Calculate difference from home_work_location_id
    result_df = result_df.join(work_df, [result_df.imsi == work_df.imsi,
                                         result_df.start_of_month == work_df.start_of_month], 'left').select(
        result_df.imsi, result_df.start_of_month, 'avg_work_latitude', 'avg_work_longitude',
        'work_latitude', 'work_longitude', 'radius'
    )

    result_df = result_df.withColumn('diff_distance', F.when(
        (result_df.work_latitude.isNull()) | (result_df.work_longitude.isNull()), 0.0)
                                                    .otherwise(
        distance_calculate_statement('avg_work_latitude', 'avg_work_longitude', 'work_latitude', 'work_longitude')))

    return result_df


def int_l3_geo_use_traffic_favorite_location_monthly(data_df: DataFrame,
                                                     homework_df: DataFrame,
                                                     top3visit_df: DataFrame,
                                                     param_config: str):
    # Use column: vol_all and call_traffic
    homework_data_df = data_df.join(homework_df, [data_df.start_of_month == homework_df.start_of_month], 'inner') \
        .select('mobile_no', 'start_of_month',
                data_df.location_id.alias('home_location'),
                'vol_all', 'total_minute', 'call_traffic')

    join_df = data_df.join(top3visit_df, [], 'left') \
        .select('mobile_no', 'start_of_month', 'location_id', 'latitude', 'longitude', 'total_minute', 'call_traffic')

    output_df = join_df
    # """SELECT
    #         IMSI,
    #         start_of_month,
    #         sum(Home_traffic_KB) as Home_traffic_KB,
    #         sum(Work_traffic_KB) as Work_traffic_KB,
    #         sum(Top1_location_traffic_KB) as Top1_location_traffic_KB,
    #         sum(Top2_location_traffic_KB) as Top2_location_traffic_KB,
    #         sum(share_Home_traffic_KB) as share_Home_traffic_KB,
    #         sum(share_Work_traffic_KB) as share_Work_traffic_KB,
    #         sum(share_Top1_location_traffic_KB) as share_Top1_location_traffic_KB,
    #         sum(share_Top2_location_traffic_KB) as share_Top2_location_traffic_KB
    #     from ( select
    #                 IMSI,
    #                 event_partition_date,
    #                 start_of_month,
    #                 Home_traffic_KB,
    #                 Work_traffic_KB,
    #                 Top1_location_traffic_KB,
    #                 Top2_location_traffic_KB,
    #                 ((Home_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Home_traffic_KB,
    #                 ((Work_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Work_traffic_KB,
    #                 ((Top1_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top1_location_traffic_KB,
    #                 ((Top2_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top2_location_traffic_KB
    #             FROM  GEO_TEMP_04 )
    #     group by IMSI, start_of_month
    # """

    return [output_df, output_df]


def l3_geo_use_traffic_favorite_location_monthly(input_df: DataFrame, input_df2: DataFrame, param_config):
    output_df = input_df

    return output_df


def int_l3_geo_visit_ais_store_location_monthly(input_df: DataFrame,
                                                homework_df: DataFrame,
                                                top3_df: DataFrame):
    # ----- Data Availability Checks -----
    if check_empty_dfs([input_df, homework_df, top3_df]):
        return get_spark_empty_df()

    join_homework_df = input_df.join(homework_df, [input_df.imsi == homework_df.imsi,
                                          input_df.start_of_month == homework_df.start_of_month], 'inner').select(
        input_df.imsi, input_df.start_of_month, 'landmark_name_th', 'landmark_sub_name_en',
        'landmark_latitude', 'landmark_longitude', 'last_visit', 'num_visit', 'duration',
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'home_latitude_weekday',
                                     'home_longitude_weekday').alias('distance_near_home_weekday'),
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'home_latitude_weekend',
                                     'home_longitude_weekend').alias('distance_near_home_weekend'),
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'work_latitude',
                                     'work_longitude').alias('distance_near_work')
    )

    join_top3_df = input_df.join(top3_df, [input_df.imsi == top3_df.imsi,
                                           input_df.start_of_month == top3_df.start_of_month], 'inner').select(
        input_df.imsi, input_df.start_of_month, 'landmark_name_th', 'landmark_sub_name_en',
        'landmark_latitude', 'landmark_longitude', 'last_visit', 'num_visit', 'duration',
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'top_latitude_1st',
                                     'top_longitude_1st').alias('distance_near_1st'),
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'top_latitude_2nd',
                                     'top_longitude_2nd').alias('distance_near_2nd'),
        distance_calculate_statement('landmark_latitude',
                                     'landmark_longitude',
                                     'top_latitude_3rd',
                                     'top_longitude_3rd').alias('distance_near_3rd')
    )

    return [join_homework_df, join_top3_df]


def _row_number(col_name: str) -> Column:
    return F.row_number().over(Window().partitionBy('imsi', 'start_of_month').orderBy(
        F.col(f'distance_near_{col_name}').asc(), F.col('landmark_name_th').asc())
    )


def l3_geo_visit_ais_store_location_monthly(homework_df: DataFrame,
                                            top3_df: DataFrame,
                                            param_config: str) -> DataFrame:
    # ----- Data Availability Checks -----
    if check_empty_dfs([homework_df, top3_df]):
        return get_spark_empty_df()

    homework_df = homework_df.withColumn('rank_near_home_weekday', _row_number('home_weekday'))\
        .withColumn('rank_near_home_weekend', _row_number('home_weekend'))\
        .withColumn('rank_near_work', _row_number('work'))

    top3_df = top3_df.withColumn('rank_near_1st', _row_number('1st'))\
        .withColumn('rank_near_2nd', _row_number('2nd'))\
        .withColumn('rank_near_3rd', _row_number('3rd'))

    output_df = top3_df.join(homework_df, ['imsi', 'start_of_month', 'landmark_name_th', 'landmark_sub_name_en',
                                           'landmark_latitude', 'landmark_longitude'], 'inner').select(
        top3_df.imsi, top3_df.start_of_month, top3_df.landmark_name_th, top3_df.landmark_sub_name_en,
        top3_df.landmark_latitude, top3_df.landmark_longitude, 'last_visit', 'num_visit', 'duration',
        'distance_near_home_weekday', 'distance_near_home_weekend', 'distance_near_work',
        'distance_near_1st', 'distance_near_2nd', 'distance_near_3rd',
        'rank_near_home_weekday', 'rank_near_home_weekend', 'rank_near_work',
        'rank_near_1st', 'rank_near_2nd', 'rank_near_3rd'
    )

    output_df.groupBy('imsi', 'start_of_month').agg(
        F.max(F.when(F.col('rank_near_home_weekday') == 1, F.col('landmark_name_th'))).alias('landmark_name_th_near_home')
    )

    return output_df


def _geo_top_visit_exclude_homework(sum_duration, homework):
    win = Window().partitionBy('imsi').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    sum_duration_3mo = sum_duration.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn(
        "Sum", F.sum("sum_duration").over(win))

    result = sum_duration_3mo.join(homework, [sum_duration_3mo.imsi == homework.imsi,
                                              sum_duration_3mo.location_id == homework.home_location_id_weekday,
                                              sum_duration_3mo.start_of_month == homework.start_of_month],
                                   'left_anti').select(sum_duration_3mo.imsi, 'location_id', 'sum_duration',
                                                       sum_duration_3mo.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.home_weekend_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left_anti').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.work_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left_anti').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
    win = Window.partitionBy("start_of_month", "imsi").orderBy(F.col("sum_duration").desc(), F.col("location_id"))
    result = result.withColumn("rank", F.row_number().over(win))

    rank1 = result.where('rank=1').withColumn('top_location_1st', F.col('location_id')).drop('location_id', 'rank',
                                                                                             'sum_duration')
    rank2 = result.where('rank=2').withColumn('top_location_2nd', F.col('location_id')).drop('location_id', 'rank',
                                                                                             'sum_duration')
    rank3 = result.where('rank=3').withColumn('top_location_3rd', F.col('location_id')).drop('location_id', 'rank',
                                                                                             'sum_duration')
    df = rank1.join(rank2, ['imsi', 'start_of_month'], 'full').join(rank3, ['imsi', 'start_of_month'], 'full')

    return df
