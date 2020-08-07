from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
from pyspark.sql import Window
import logging
import os

from customer360.utilities.re_usable_functions import add_start_of_week_and_month, union_dataframes_with_missing_cols, \
    execute_sql, add_event_week_and_month_from_yyyymmdd, __divide_chunks, check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def l3_geo_top3_visit_exclude_hw_monthly(input_df: DataFrame, homework_df: DataFrame) -> DataFrame:
    """
    Args:
        input_df: geo_cust_location_monthly_hr
        +-----+-----+-------------+----------+-----------+----------+------+-----------------+--------------------+
        | imsi| hour| location_id | latitude | longitude | duration | days | partition_month | partition_weektype |
        +-----+-----+-------------+----------+-----------+----------+------+-----------------+--------------------+
        homework_df:

    Returns:
        result_df:
        +-----+---------------+------------------+------------------+------------------+  --------------------------+
        | imsi| start_of_month| top_location_1st | top_location_2st | top_location_3st | top_same_on_weekday_weekend|
        +-----+---------------+------------------+------------------+------------------+  --------------------------+

    """
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

    win = Window().partitionBy('imsi', 'location_id') \
        .orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)

    win_weektype = Window().partitionBy('imsi', 'location_id', 'partition_weektype') \
        .orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)

    # input_3m_df = input_df.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn(
    #     "Sum", F.sum("sum_duration").over(win))
    input_3m_df = input_df.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")) \
        .withColumn("duration_3m", F.sum("duration").over(win)) \
        .withColumn("days_3m", F.sum('days').over(win))\
        .withColumn("duration_weektype_3m", F.sum("duration").over(win_weektype))\
        .withColumn("days_weektype_3m", F.sum("days").over(win_weektype))

    win2 = Window().partitionBy('imsi', 'start_of_month') \
        .orderBy(F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc())

    win2_weektype = Window().partitionBy('imsi', 'start_of_month', 'partition_weektype') \
        .orderBy(F.col('location_id').asc(), F.col('duration_weektype_3m').desc(), F.col('days_weektype_3m').desc())

    input_3m_df = input_3m_df.withColumn('row_num', F.row_number().over(win2))\
        .withColumn('row_num_weektype', F.row_number().over(win2_weektype))
    input_3m_df = input_3m_df.where('row_num <= 6 and row_num_weektype = 1').drop('row_num')

    result_df = input_3m_df.join(homework_df,
                                 (input_3m_df.imsi == homework_df.imsi) &
                                 (input_3m_df.start_of_month == homework_df.start_of_month) &
                                 (
                                         (input_3m_df.location_id != homework_df.work_location_id) |
                                         (input_3m_df.location_id != homework_df.home_location_id_weekday) |
                                         (input_3m_df.location_id != homework_df.home_location_id_weekend)
                                 ), 'inner').select(
        input_df.imsi, input_df.start_of_month, input_df.location_id, 'duration_3m', 'days_3m')

    result_df = result_df.withColumn('row_num', F.row_number().over(win2)).where('row_num <= 3')

    result_df = result_df.groupBy('imsi', 'start_of_month') \
        .agg(F.max(F.when((F.col('row_num') == 1), F.col('location'))).alias('top_location_1st'),
             F.max(F.when((F.col('row_num') == 2), F.col('location'))).alias('top_location_2nd'),
             F.max(F.when((F.col('row_num') == 3), F.col('location'))).alias('top_location_3rd')
             )

    return result_df


def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    # Get max date of partition column
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]

    # Set the latest master DataFrame
    input_df = input_df.where('{0}={1}'.format(par_col, str(max_date)))

    return input_df


def massive_processing_for_int_home_work_monthly(input_df: DataFrame, config_home: str, config_work: str
                                                 ) -> DataFrame:
    """
    Args:
        input_df: geo_cust_location_monthly_hr
            +-----+-----+-------------+----------+-----------+----------+------+-----------------+--------------------+
            | imsi| hour| location_id | latitude | longitude | duration | days | partition_month | partition_weektype |
            +-----+-----+-------------+----------+-----------+----------+------+-----------------+--------------------+
        config_home:
        config_work:
    Returns:
    """
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

    return [output_df_work, output_df_home]


def int_geo_work_location_id_monthly(work_monthly: DataFrame) -> DataFrame:
    w_work = Window().partitionBy('imsi', 'location_id') \
        .orderBy(F.col("Month").cast("long")) \
        .rangeBetween(-(86400 * 89), 0)

    work_last_3m = work_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")) \
        .withColumn("duration_3m", F.sum("duration").over(w_work)) \
        .withColumn("days_3m", F.sum('days').over(w_work))

    work_last_3m = work_last_3m.dropDuplicates(['imsi', 'start_of_month', 'location_id', 'duration_3m', 'days_3m']) \
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m')

    w_work_num_row = Window().partitionBy('imsi', 'start_of_month') \
        .orderBy(F.col('location_id').asc(), F.col('duration_3m').desc(), F.col('days_3m').desc())

    work_last_3m = work_last_3m.withColumn('row_num', F.row_number().over(w_work_num_row))
    work_last_3m = work_last_3m.where('row_num = 1').drop('row_num')
    work_last_3m = work_last_3m.select('imsi', 'start_of_month',
                                       (F.col('location_id').alias('work_location_id')),
                                       (F.col('latitude').alias('work_latitude')),
                                       (F.col('longitude').alias('work_longitude')))

    return work_last_3m


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

    home_last_3m = home_last_3m.groupBy('imsi', 'start_of_month') \
        .agg(F.max(F.when((F.col('week_type') == 'weekday'), F.col('location'))).alias('home_location_id_weekday'),
             F.max(F.when((F.col('week_type') == 'weekday'), F.col('latitude'))).alias('home_latitude_weekday'),
             F.max(F.when((F.col('week_type') == 'weekday'), F.col('longitude'))).alias('home_longitude_weekday'),
             F.max(F.when((F.col('week_type') == 'weekend'), F.col('location'))).alias('home_location_id_weekend'),
             F.max(F.when((F.col('week_type') == 'weekend'), F.col('latitude'))).alias('home_latitude_weekend'),
             F.max(F.when((F.col('week_type') == 'weekend'), F.col('longitude'))).alias('home_longitude_weekend')
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

    home_df = home_df.select('imsi', 'start_of_month').distinct()
    work_df = work_df.select('imsi', 'start_of_month').distinct()
    list_imsi = home_df.union(work_df).distinct()

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


def l3_geo_home_weekday_city_citizens_monthly(home_work_location_id, master, sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([home_work_location_id, master]):
        return get_spark_empty_df()

    home_work_location_id = data_non_availability_and_missing_check(df=home_work_location_id,
                                                                    grouping="monthly",
                                                                    par_col="start_of_month",
                                                                    target_table_name="l3_geo_home_weekday_city_citizens_monthly",
                                                                    missing_data_check_flg='N')
    if check_empty_dfs([home_work_location_id]):
        return get_spark_empty_df()

    # Get last master
    max_date = master.selectExpr('max(partition_date)').collect()[0][0]
    master = master.where('partition_date=' + str(max_date))

    # Add start_of_month in master
    master = master.withColumn("start_of_month",
                               F.to_date(F.date_trunc('month', F.to_date(F.col("partition_date").cast(StringType()),
                                                                         'yyyyMMdd'))))
    master.drop('partition_date')

    # Join Home and master
    home_location_id_master = home_work_location_id.join(master,
                                                         [
                                                             home_work_location_id.home_weekday_location_id == master.location_id],
                                                         'left').select(home_work_location_id.start_of_month, 'imsi',
                                                                        'home_weekday_location_id', 'region_name',
                                                                        'province_name', 'district_name',
                                                                        'sub_district_name')

    home_weekday_window = Window().partitionBy(F.col('start_of_month'), F.col('region_name'),
                                               F.col('province_name'), F.col('district_name'),
                                               F.col('sub_district_name'))

    home_location_id_master = home_location_id_master.withColumn('citizens',
                                                                 F.approx_count_distinct(F.col("imsi")).over(
                                                                     home_weekday_window)) \
        .dropDuplicates(
        ['start_of_month', 'region_name', 'province_name', 'district_name', 'sub_district_name', 'citizens']) \
        .select('start_of_month', 'region_name', 'province_name', 'district_name', 'sub_district_name', 'citizens')
    df = node_from_config(home_location_id_master, sql)
    return df


def l3_geo_work_area_center_average_monthly(visti_hr, home_work):
    visti_hr = visti_hr.filter('partition_date >= 20200401 and partition_date <= 20200627')

    # ----- Data Availability Checks -----
    if check_empty_dfs([visti_hr, home_work]):
        return get_spark_empty_df()

    visti_hr = data_non_availability_and_missing_check(df=visti_hr,
                                                       grouping="monthly",
                                                       par_col="partition_date",
                                                       target_table_name="l3_geo_work_area_center_average_monthly",
                                                       missing_data_check_flg='N')

    home_work = data_non_availability_and_missing_check(df=home_work,
                                                        grouping="monthly",
                                                        par_col="start_of_month",
                                                        target_table_name="l3_geo_work_area_center_average_monthly",
                                                        missing_data_check_flg='N')

    min_value = union_dataframes_with_missing_cols(
        [
            visti_hr.select(
                F.to_date(F.date_trunc('month',
                                       F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd'))).alias(
                    "max_date")),
            home_work.select(F.max(F.col("start_of_month")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    visti_hr = visti_hr.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    home_work = home_work.filter(F.col("start_of_month") <= min_value)

    if check_empty_dfs([visti_hr, home_work]):
        return get_spark_empty_df()
    # ----- Transformation -----

    # Clean data
    visit_hr_drop = visti_hr.drop('partition_hour')
    visit_hr_drop = visit_hr_drop.where("hour > 5 and hour < 19")
    work = home_work.drop('ome_weekday_location_id:string', 'home_weekday_latitude', 'home_weekday_longitude',
                          'home_weekend_location_id', 'home_weekend_latitude', 'home_weekend_longitude')

    # Add event_partition_date
    visit_hr_drop = visit_hr_drop.withColumn("event_partition_date",
                                             F.to_date(F.col('partition_date').cast(StringType()), 'yyyyMMdd'))
    # Group by daily
    visit_hr_agg = visit_hr_drop.groupBy('imsi', 'location_id', 'latitude', 'longitude', 'event_partition_date') \
        .agg(F.sum('duration').alias('duration'), F.count('hour').alias('incident'))

    # Add start_of_month
    visit_hr_agg = visit_hr_agg.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date")))

    # Group by monthly
    visit_hr_agg_monthly = visit_hr_agg.groupBy('imsi', 'location_id', 'latitude', 'longitude', 'start_of_month') \
        .agg(F.sum('duration').alias('duration'), F.sum('incident').alias('incident'),
             F.count('location_id').alias('days'))

    # Last 3 month aggregate
    w_3month = Window().partitionBy(F.col('imsi'), F.col('location_id'), F.col('latitude'), F.col('longitude')).orderBy(
        F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    visit_hr_agg_monthly_3month = visit_hr_agg_monthly.withColumn("Month",
                                                                  F.to_timestamp("start_of_month", "yyyy-MM-dd")) \
        .withColumn("3_duration", F.sum("duration").over(w_3month)) \
        .withColumn("3_incident", F.sum("incident").over(w_3month)) \
        .withColumn("3_days", F.sum("days").over(w_3month))

    # Drop duplicate
    visit_hr_agg_monthly_3month = visit_hr_agg_monthly_3month \
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', '3_duration', '3_incident', '3_days')\
        .drop_duplicates()

    visit_hr_agg_monthly_3month = visit_hr_agg_monthly_3month.withColumnRenamed('3_duration', 'duration') \
        .withColumnRenamed('3_incident', 'incident') \
        .withColumnRenamed('3_days', 'days')

    w = Window().partitionBy('imsi', 'start_of_month')
    _score = 0.7 * (F.col('duration') / F.sum('duration').over(w)) + 0.2 * ( \
                F.col('incident') / F.sum('incident').over(w)) + 0.1 * (F.col('days') / F.sum('days').over(w))

    # Calculate score
    visit_hr_agg_monthly_score = visit_hr_agg_monthly_3month.withColumn('score', _score)

    # Calculate average lat and long
    work_center_average = visit_hr_agg_monthly_score.groupBy('imsi', 'start_of_month') \
        .agg(F.avg(F.col('latitude') * F.col('score')).alias('avg_latitude'), \
             F.avg(F.col('longitude') * F.col('score')).alias('avg_longitude'))

    w_order = Window().partitionBy('imsi', 'start_of_month').orderBy('score')
    visit_hr_agg_monthly_score_normal_rank = visit_hr_agg_monthly_score.withColumn('rank', F.dense_rank().over(w_order))
    visit_hr_agg_monthly_score_normal_rank = visit_hr_agg_monthly_score_normal_rank.where('rank > 6')

    visit_hr_agg_monthly_join = work_center_average.join(visit_hr_agg_monthly_score_normal_rank, [
        work_center_average.imsi == visit_hr_agg_monthly_score_normal_rank.imsi, \
        work_center_average.start_of_month == visit_hr_agg_monthly_score_normal_rank.start_of_month], \
                                                         'left') \
        .select(work_center_average.imsi, work_center_average.start_of_month, 'avg_latitude',
                'avg_longitude', 'latitude', 'longitude')

    # Calculate radius
    work_radius = visit_hr_agg_monthly_join.groupBy('imsi', 'start_of_month') \
        .agg(
        F.max((F.acos(F.cos(F.radians(90 - F.col('avg_latitude'))) * F.cos(F.radians(90 - F.col('latitude'))) + F.sin( \
            F.radians(90 - F.col('avg_latitude'))) * F.sin(F.radians(90 - F.col('latitude'))) * F.cos(F.radians( \
            F.col('avg_longitude') - F.col('longitude')))) * 6371).cast('double')).alias('radius'))

    # Calculate difference from home_work_location_id
    work_center_average_diff = work_center_average.join(work, [work_center_average.imsi == work.imsi, \
                                                               work_center_average.start_of_month == work.start_of_month], \
                                                        'left') \
        .select(work_center_average.imsi, work_center_average.start_of_month, 'avg_latitude',
                'avg_longitude', 'work_latitude', 'work_longitude')

    work_center_average_diff = work_center_average_diff.withColumn('distance_difference', F.when(
        (work_center_average_diff.work_latitude.isNull()) | (work_center_average_diff.work_longitude.isNull()), 0.0) \
                                                                   .otherwise((F.acos(F.cos(
        F.radians(90 - F.col('avg_latitude'))) * F.cos(
        F.radians(90 - F.col('work_latitude'))) + F.sin( \
        F.radians(90 - F.col('avg_latitude'))) * F.sin(
        F.radians(90 - F.col('work_latitude'))) * F.cos(
        F.radians( \
            F.col('avg_longitude') - F.col(
                'work_longitude')))) * 6371).cast('double')))

    work_center_average_diff = work_center_average_diff.withColumnRenamed('avg_latitude', 'work_avg_latitude') \
        .withColumnRenamed('avg_longitude', 'work_avg_longitude')

    work_final = work_center_average_diff.join(work_radius,
                                               [work_center_average_diff.imsi == work_radius.imsi, \
                                                work_center_average_diff.start_of_month == work_radius.start_of_month],
                                               'inner') \
        .select(work_center_average_diff.imsi, work_center_average_diff.start_of_month, 'work_avg_latitude',
                'work_avg_longitude', 'distance_difference', 'radius')
    return work_final


def l3_geo_use_traffic_favorite_location_monthly(data_df: DataFrame,
                                                 homework_df: DataFrame,
                                                 top3visit_df: DataFrame,
                                                 param_config: str) -> DataFrame:
    # Use column: vol_all and call_traffic
    join_df = data_df.join(homework_df, [], 'left')\
        .select('mobile_no', 'start_of_month', 'location_id', 'latitude', 'longitude', 'total_minute', 'call_traffic')

    join_df = join_df.join(top3visit_df, [], 'left')\
        .select('mobile_no', 'start_of_month', 'location_id', 'latitude', 'longitude', 'total_minute', 'call_traffic')

    output_df = join_df
    """SELECT
            IMSI,
            start_of_month,
            sum(Home_traffic_KB) as Home_traffic_KB,
            sum(Work_traffic_KB) as Work_traffic_KB,
            sum(Top1_location_traffic_KB) as Top1_location_traffic_KB,
            sum(Top2_location_traffic_KB) as Top2_location_traffic_KB,
            sum(share_Home_traffic_KB) as share_Home_traffic_KB,
            sum(share_Work_traffic_KB) as share_Work_traffic_KB,
            sum(share_Top1_location_traffic_KB) as share_Top1_location_traffic_KB,
            sum(share_Top2_location_traffic_KB) as share_Top2_location_traffic_KB
        from ( select
                    IMSI,
                    event_partition_date,
                    start_of_month,
                    Home_traffic_KB,
                    Work_traffic_KB,
                    Top1_location_traffic_KB,
                    Top2_location_traffic_KB,
                    ((Home_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Home_traffic_KB,
                    ((Work_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Work_traffic_KB,
                    ((Top1_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top1_location_traffic_KB,
                    ((Top2_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top2_location_traffic_KB
                FROM  GEO_TEMP_04 )
        group by IMSI, start_of_month
    """
    f'''
              select
                a.start_of_month
                ,a.imsi
                ,sum(b.no_of_call+b.no_of_inc) as call_count_location_{df_temp_01}  
              from {df_temp_01} a
              left join usage_voice b
                on a.access_method_num = b.access_method_num
                and a.start_of_month = b.start_of_month
              where b.service_type in ('VOICE','VOLTE')
                and a.{df_temp_01}_lac = b.lac
                 and a.{df_temp_01}_ci = b.ci
              group by 1,2
            '''
    return output_df


def _geo_top_visit_exclude_homework(sum_duration, homework):
    win = Window().partitionBy('imsi').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    sum_duration_3mo = sum_duration.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn(
        "Sum", F.sum("sum_duration").over(win))

    result = sum_duration_3mo.join(homework, [sum_duration_3mo.imsi == homework.imsi,
                                              sum_duration_3mo.location_id == homework.home_weekday_location_id,
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