import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
from pyspark.sql import Window
import logging
import os
from pyspark.sql import types as T
import statistics

from customer360.utilities.re_usable_functions import add_start_of_week_and_month, union_dataframes_with_missing_cols, \
    execute_sql, add_event_week_and_month_from_yyyymmdd, __divide_chunks, check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", "base")
run_mode = os.getenv("DATA_AVAILABILITY_CHECKS", None)
log = logging.getLogger(__name__)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")

def l3_geo_top_visit_exclude_homework(sum_duration, homework):
    win = Window().partitionBy('imsi').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    sum_duration_3mo = sum_duration.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn(
        "Sum", F.sum("sum_duration").over(win))

    result = sum_duration_3mo.join(homework, [sum_duration_3mo.imsi == homework.imsi,
                                              sum_duration_3mo.location_id == homework.home_weekday_location_id,
                                              sum_duration_3mo.start_of_month == homework.start_of_month],
                                   'left').select(sum_duration_3mo.imsi, 'location_id', 'sum_duration',
                                                  sum_duration_3mo.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.home_weekend_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.work_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
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

def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    # Get max date of partition column
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]

    # Set the latest master DataFrame
    input_df = input_df.where('{0}='.format(par_col) + str(max_date))

    return input_df


def l3_geo_time_spent_by_location_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_time_spent_by_location_monthly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = massive_processing_monthly(df, sql, "l3_geo_time_spent_by_location_monthly", 'start_of_month')

    print('DEBUG : ------------------------------------------------> l3_geo_time_spent_by_location_monthly')
    df.show(10)

    return df


def l3_geo_area_from_ais_store_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_area_from_ais_store_monthly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l3_geo_area_from_ais_store_monthly')
    df.show(10)

    return df


def l3_geo_area_from_competitor_store_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_area_from_competitor_store_monthly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    df = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l3_geo_area_from_competitor_store_monthly')
    df.show(10)

    return df


###total_distance_km###
def l3_geo_total_distance_km_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_total_distance_km_monthly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    df = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l3_geo_total_distance_km_monthly')
    df.show(10)

    return df


###Top_3_cells_on_voice_usage###
def l3_geo_top3_cells_on_voice_usage(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_top3_cells_on_voice_usage",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


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

    print('DEBUG : ------------------------------------------------> l3_geo_top3_cells_on_voice_usage')
    df.show(10)

    return df


def l3_geo_distance_top_call(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_distance_top_call",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = df.groupBy("imsi", "start_of_month").agg(
        F.max("top_distance_km").alias("max_distance_top_call"),
        F.min("top_distance_km").alias("min_distance_top_call"),
        F.avg("top_distance_km").alias("avg_distance_top_call"),
        F.when(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2))).cast("string") == 'NaN', 0).otherwise(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2)))).alias("sd_distance_top_call"),
        F.sum("top_distance_km").alias("sum_distance_top_call"))

    print('DEBUG : ------------------------------------------------> l3_geo_distance_top_call')
    df.show(10)

    return df



# 47 The favourite location
def l3_the_favourite_locations_monthly(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_the_favourite_locations_monthly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    ### config
    spark = get_spark_session()
    df.createOrReplaceTempView('l1_df_the_favourite_location_daily')
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

    print('DEBUG : ------------------------------------------------> l3_the_favourite_locations_monthly')
    l3.show(10)

    return l3


def massive_processing_monthly(data_frame: DataFrame, sql, output_df_catalog, partition_col) -> DataFrame:
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
    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col(partition_col).isin(*[curr_item]))
        small_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, small_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col(partition_col).isin(*[first_item]))
    return_df = node_from_config(return_df, sql)
    return return_df


# Set function for massive process: Daily --> Monthly
def massive_processing_for_home_work(
        input_df,
        config_home,
        config_work,
        source_partition_col="partition_date"
):
    # filter
    input_df = input_df.filter('partition_date >= 20191101 and partition_date <= 20191130')

    CNTX = load_context(Path.cwd(), env=os.getenv("CONF", "base"))

    # ----- Data Availability Checks -----
    if check_empty_dfs(input_df):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_date",
                                                       target_table_name="l3_geo_home_work_location_id_monthly",
                                                       missing_data_check_flg='N')
    if check_empty_dfs(input_df):
        return get_spark_empty_df()
    # ----- Transformation -----

    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(__divide_chunks(mvv_array, 5))
    add_list = mvv_new

    #Set first dataframe to merge
    if len(add_list) != 1:
        last_item = add_list[0]
        logging.info("First date to run for {0}".format(str(last_item)))
        small_df_last = data_frame.filter(F.col(source_partition_col).isin(*[last_item]))

        # Add 2 columns: event_partition_date, start_of_month
        small_df_last = small_df_last.withColumn("event_partition_date", F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd'))
        small_df_last = small_df_last.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col("event_partition_date"))))

        # Work
        output_df_work = _int_l4_geo_work_location_id_monthly(small_df_last, config_work)
        CNTX.catalog.save(config_work["output_catalog"], output_df_work)

        # Home
        output_df_home = _int_l4_geo_home_location_id_monthly(small_df_last, config_home)
        CNTX.catalog.save(config_home["output_catalog"], output_df_home)
        add_list.remove(last_item)
    elif len(add_list) == 1:
        last_item = add_list[0]
        small_df_last = data_frame.filter(F.col(source_partition_col).isin(*[last_item]))

        # Add 2 columns: event_partition_date, start_of_month
        small_df_last = small_df_last.withColumn("event_partition_date",
                                                 F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
        small_df_last = small_df_last.withColumn("start_of_month",
                                                 F.to_date(F.date_trunc('month', F.col("event_partition_date"))))

        # Work
        output_df_work = _int_l4_geo_work_location_id_monthly(small_df_last, config_work)

        # Home
        output_df_home = _int_l4_geo_home_location_id_monthly(small_df_last, config_home)

        return [output_df_home, output_df_work]

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(source_partition_col).isin(*[curr_item]))

        # Add 2 columns: event_partition_date, start_of_month
        small_df = small_df.withColumn("event_partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
        small_df = small_df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col("event_partition_date"))))

        # Work
        after_output_df_work = CNTX.catalog.load(config_work["output_catalog"])
        output_df_work = _int_l4_geo_work_location_id_monthly(small_df, config_work)
        output_df_work = output_df_work.select(after_output_df_work.columns)
        output_df_work_union = after_output_df_work.union(output_df_work)
        output_df_work_union = output_df_work_union.groupBy("imsi", "location_id", "latitude", "longitude", "start_of_month")\
            .agg(F.sum("duration").alias("duration"), F.sum("days").alias("days"))
        CNTX.catalog.save(config_work["output_catalog"], output_df_work_union)

        # Home
        after_output_df_home = CNTX.catalog.load(config_home["output_catalog"])
        output_df_home = _int_l4_geo_home_location_id_monthly(small_df, config_home)
        output_df_home = output_df_home.select(after_output_df_home.columns)
        output_df_home_union = after_output_df_home.union(output_df_home)
        output_df_home_union = output_df_home_union.groupBy("imsi", "location_id", "latitude", "longitude", "week_type", "start_of_month")\
            .agg(F.sum("duration").alias("duration"), F.sum("days").alias("days"))
        CNTX.catalog.save(config_home["output_catalog"], output_df_home_union)


    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(source_partition_col).isin(*[first_item]))
    # Add 2 columns: event_partition_date, start_of_month
    return_df = return_df.withColumn("event_partition_date",
                                             F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
    return_df = return_df.withColumn("start_of_month",
                                             F.to_date(F.date_trunc('month', F.col("event_partition_date"))))
    # Work
    after_output_df_work = CNTX.catalog.load(config_work["output_catalog"])
    output_df_work = _int_l4_geo_work_location_id_monthly(return_df, config_work)
    output_df_work = output_df_work.select(after_output_df_work.columns)
    output_df_work_union = after_output_df_work.union(output_df_work)
    output_df_work_union = output_df_work_union.groupBy("imsi", "location_id", "latitude", "longitude",
                                                        "start_of_month") \
        .agg(F.sum("duration").alias("duration"), F.sum("days").alias("days"))

    # Home
    after_output_df_home = CNTX.catalog.load(config_home["output_catalog"])
    output_df_home = _int_l4_geo_home_location_id_monthly(return_df, config_home)
    output_df_home = output_df_home.select(after_output_df_home.columns)
    output_df_home_union = after_output_df_home.union(output_df_home)
    output_df_home_union = output_df_home_union.groupBy("imsi", "location_id", "latitude", "longitude", "week_type",
                                                        "start_of_month") \
        .agg(F.sum("duration").alias("duration"), F.sum("days").alias("days"))

    return [output_df_home_union, output_df_work_union]

def _int_l4_geo_home_location_id_monthly(df, config):
    # Add column week_type
    df = df.withColumn('week_type', F.when(((F.dayofweek(F.col('event_partition_date')) == 1) | (F.dayofweek(F.col('event_partition_date')) == 7)), 'weekend')
                                                                             .otherwise('weekday').cast(StringType()))
    df = node_from_config(df, config)

    return df


def _int_l4_geo_work_location_id_monthly(df, config):
    df = node_from_config(df, config)
    return df


def int_geo_home_work_list_imsi_monthly(home_monthly, work_monthly):
    list_imsi_work = work_monthly.select('imsi', 'start_of_month').distinct()
    list_imsi_home = home_monthly.select('imsi', 'start_of_month').distinct()
    list_imsi = list_imsi_work.union(list_imsi_home).distinct()

    return list_imsi


def int_geo_work_location_id_monthly(work_monthly, list_imsi):
    # Work
    w_work = Window().partitionBy('imsi', 'location_id').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    work_last_3m = work_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd"))\
        .withColumn("duration_3m", F.sum("duration").over(w_work))\
        .withColumn("days_3m", F.sum('days').over(w_work))

    work_last_3m = work_last_3m.dropDuplicates(['imsi', 'start_of_month', 'location_id', 'duration_3m', 'days_3m'])\
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m')


    w_work_num_row = Window().partitionBy('imsi', 'start_of_month').orderBy(F.col('duration_3m').desc(), F.col('days_3m').desc())
    work_last_3m = work_last_3m.withColumn('row_num', F.row_number().over(w_work_num_row))
    work_last_3m = work_last_3m.where('row_num = 1').drop('row_num')

    work_last_3m = list_imsi.join(work_last_3m, ['imsi', 'start_of_month'], 'left').select(list_imsi.imsi, list_imsi.start_of_month, 'location_id', 'latitude', 'longitude')

    return work_last_3m


def int_geo_home_location_id_monthly(home_monthly):
    # Home weekday & weekend
    w_home = Window().partitionBy('imsi', 'location_id', 'week_type').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    home_last_3m = home_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd"))\
        .withColumn("duration_3m", F.sum("duration").over(w_home))\
        .withColumn("days_3m", F.sum('days').over(w_home))
    home_last_3m = home_last_3m.dropDuplicates(['imsi', 'week_type', 'start_of_month', 'location_id', 'duration_3m', 'days_3m'])\
        .select('imsi', 'start_of_month', 'week_type', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m')

    w_num_row = Window().partitionBy('imsi', 'week_type', 'start_of_month').orderBy(F.col('duration_3m').desc(), F.col('days_3m').desc())
    home_last_3m = home_last_3m.withColumn('row_num', F.row_number().over(w_num_row))

    home_last_3m = home_last_3m.where('row_num = 1').drop('row_num')
    home_last_3m_weekday = home_last_3m.where("week_type = 'weekday'").select('imsi', 'start_of_month', (F.col('location_id').alias('home_weekday_location_id')),
                                                                                               (F.col('latitude').alias('home_weekday_latitude')),
                                                                                               (F.col('longitude').alias('home_weekday_longitude')))
    home_last_3m_weekend = home_last_3m.where("week_type = 'weekend'").select('imsi', 'start_of_month',
                                                                                               (F.col('location_id').alias('home_weekend_location_id')),
                                                                                               (F.col('latitude').alias('home_weekend_latitude')),
                                                                                               (F.col('longitude').alias('home_weekend_longitude')))
    return [home_last_3m_weekday, home_last_3m_weekend]


def l3_geo_home_work_location_id_monthly(home_last_3m_weekday, home_last_3m_weekend, work_last_3m, sql):
    home_work = work_last_3m.join(home_last_3m_weekday, ['imsi', 'start_of_month'], 'left').select(
        work_last_3m.start_of_month, work_last_3m.imsi,
        'home_weekday_location_id', 'home_weekday_latitude',
        'home_weekday_longitude', 'location_id', 'latitude',
        'longitude')

    home_work_final = home_work.join(home_last_3m_weekend, ['imsi', 'start_of_month'], 'left').select(
        home_work.start_of_month, home_work.imsi,
        'home_weekday_location_id',
        'home_weekday_latitude', 'home_weekday_longitude',
        'home_weekend_location_id',
        'home_weekend_latitude', 'home_weekend_longitude',
        (F.col('location_id').alias('work_location_id')),
        (F.col('latitude').alias('work_latitude')),
        (F.col('longitude').alias('work_longitude')))

    df = node_from_config(home_work_final, sql)
    return df

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
                                                             home_work_location_id.home_weekday_location_id == master.location_id,
                                                             home_work_location_id.start_of_month == master.start_of_month],
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
    home_work = home_work.filter(F.col("partition_month") <= min_value)


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
        .drop_duplicates(subset=['imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', '3_duration', '3_incident', '3_days']) \
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', '3_duration', '3_incident', '3_days')

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
                'work_longitude')))) * 6371).cast('double'))
                                                                   )

    work_center_average_diff = work_center_average_diff.withColumnRenamed('avg_latitude', 'work_avg_latitude') \
        .withColumnRenamed('avg_longitude', 'work_avg_longitude')

    work_final = work_center_average_diff.join(work_radius,
                                               [work_center_average_diff.imsi == work_radius.imsi, \
                                                work_center_average_diff.start_of_month == work_radius.start_of_month],
                                               'inner') \
        .select(work_center_average_diff.imsi, work_center_average_diff.start_of_month, 'work_avg_latitude',
                'work_avg_longitude', 'distance_difference', 'radius')
    return work_final


###Traffic_fav_location###
def l3_data_traffic_home_work_fn(geo_mst_cell_masterplan,
                                 geo_home_work_data,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily,
                                 HOME_WORK_WEEKDAY_LOCATION_ID):

    ###TABLE###
    spark = get_spark_session()

    geo_mst_cell_masterplan.createOrReplaceTempView('GEO_MST_CELL_MASTERPLAN')
    geo_home_work_data.createOrReplaceTempView('LOCATION_HOMEWORK_NEW_1')
    profile_customer_profile_ma.createOrReplaceTempView('PROFILE_CUSTOMER_PROFILE_MA')
    usage_sum_data_location_daily.createOrReplaceTempView('USAGE_SUM_DATA_LOCATION_DAILY')
    #TEMP1#
    GEO_TEMP_00 = spark.sql("""
        SELECT
            B.IMSI,
            A.CI,
            A.LAC
        FROM GEO_MST_CELL_MASTERPLAN A,LOCATION_HOMEWORK_NEW_1 B
        WHERE A.LOCATION_ID=B."""+str(HOME_WORK_WEEKDAY_LOCATION_ID)+"""
    """)

    GEO_TEMP_00.createOrReplaceTempView('GEO_TEMP_00')

    # TEMP2#
    GEO_TEMP_01 = spark.sql("""
        SELECT
            B.ACCESS_METHOD_NUM,
            A.IMSI,
            A.LAC,
            A.CI
        FROM GEO_TEMP_00 A ,PROFILE_CUSTOMER_PROFILE_MA B
        WHERE A.IMSI = B.IMSI
        GROUP BY A.IMSI,A.LAC,A.CI,B.ACCESS_METHOD_NUM
    """)

    GEO_TEMP_01.createOrReplaceTempView('GEO_TEMP_01')

    # TEMP3#
    GEO_TEMP_02 = spark.sql("""
        SELECT
            B.DATE_ID,
            A.IMSI,
            SUM(VOL_DOWNLINK_KB+VOL_UPLINK_KB) AS TOTAL_DATA_TRAFFIC_KB
        FROM GEO_TEMP_01 A,
            USAGE_SUM_DATA_LOCATION_DAILY B
        WHERE A.ACCESS_METHOD_NUM = B.MOBILE_NO
            AND B.LAC = A.LAC
            AND A.CI = B.CI
        GROUP BY 1,2
    """)

    return GEO_TEMP_02

def l3_data_traffic_top1_top2_fn(geo_mst_cell_masterplan,
                                 geo_home_work_data,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily,
                                 HOME_WORK_WEEKDAY_LOCATION_ID):
    ###TABLE###
    spark = get_spark_session()

    geo_mst_cell_masterplan.createOrReplaceTempView('GEO_MST_CELL_MASTERPLAN')
    geo_home_work_data.createOrReplaceTempView('LOCATION_HOMEWORK_NEW_1')
    profile_customer_profile_ma.createOrReplaceTempView('PROFILE_CUSTOMER_PROFILE_MA')
    usage_sum_data_location_daily.createOrReplaceTempView('USAGE_SUM_DATA_LOCATION_DAILY')
    #TEMP1#
    GEO_TEMP_00 = spark.sql("""
        SELECT
            B.IMSI,
            A.CI,
            A.LAC
        FROM GEO_MST_CELL_MASTERPLAN A,LOCATION_HOMEWORK_NEW_1 B
        WHERE A.LOCATION_ID=B."""+str(HOME_WORK_WEEKDAY_LOCATION_ID)+"""
    """)

    GEO_TEMP_00.createOrReplaceTempView('GEO_TEMP_00')

    # TEMP2#
    GEO_TEMP_01 = spark.sql("""
        SELECT
            B.ACCESS_METHOD_NUM,
            A.IMSI,
            A.LAC,
            A.CI
        FROM GEO_TEMP_00 A ,PROFILE_CUSTOMER_PROFILE_MA B
        WHERE A.IMSI = B.IMSI
        GROUP BY A.IMSI,A.LAC,A.CI,B.ACCESS_METHOD_NUM
    """)

    GEO_TEMP_01.createOrReplaceTempView('GEO_TEMP_01')

    # TEMP3#
    GEO_TEMP_02 = spark.sql("""
        SELECT
            B.DATE_ID,
            A.IMSI,
            SUM(VOL_DOWNLINK_KB+VOL_UPLINK_KB) AS TOTAL_DATA_TRAFFIC_KB
        FROM GEO_TEMP_01 A,
            USAGE_SUM_DATA_LOCATION_DAILY B
        WHERE A.ACCESS_METHOD_NUM = B.MOBILE_NO
            AND B.LAC = A.LAC
            AND A.CI = B.CI
        GROUP BY 1,2
    """)


    return GEO_TEMP_02

def _geo_top_visit_exclude_homework(sum_duration, homework):
    win = Window().partitionBy('imsi').orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    sum_duration_3mo = sum_duration.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn(
        "Sum", F.sum("sum_duration").over(win))

    result = sum_duration_3mo.join(homework, [sum_duration_3mo.imsi == homework.imsi,
                                              sum_duration_3mo.location_id == homework.home_weekday_location_id,
                                              sum_duration_3mo.start_of_month == homework.start_of_month],
                                   'left').select(sum_duration_3mo.imsi, 'location_id', 'sum_duration',
                                                  sum_duration_3mo.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.home_weekend_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
    result = result.join(homework,
                         [result.imsi == homework.imsi, result.location_id == homework.work_location_id,
                          result.start_of_month == homework.start_of_month],
                         'left').select(result.imsi, 'location_id', 'sum_duration', result.start_of_month)
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

def l3_data_traffic_home_work_top1_top2(geo_mst_cell_masterplan,
                                        geo_home_work_data,
                                        profile_customer_profile_ma,
                                        usage_sum_data_location_daily,
                                        geo_exclude_home_work):
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_sum_data_location_daily, profile_customer_profile_ma, geo_mst_cell_masterplan, geo_home_work_data, geo_exclude_home_work]):
        return get_spark_empty_df()

    usage_sum_data_location_daily = data_non_availability_and_missing_check(df=usage_sum_data_location_daily,
                                                                            grouping="monthly",
                                                                            par_col="partition_date",
                                                                            target_table_name="l3_use_non_homework_features")


    profile_customer_profile_ma = data_non_availability_and_missing_check(df=profile_customer_profile_ma,
                                                                          grouping="monthly",
                                                                          par_col="partition_month",
                                                                          target_table_name="l3_use_non_homework_features")

    min_value = union_dataframes_with_missing_cols(
        [
            usage_sum_data_location_daily.select(
                F.to_date(F.date_trunc('month',
                                       F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd'))).alias(
                    "max_date")),
            profile_customer_profile_ma.select(
                F.to_date(F.max(F.col("partition_month")).cast(StringType()), 'yyyyMM').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    usage_sum_data_location_daily = usage_sum_data_location_daily.filter(
        F.to_date(F.date_trunc("month", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)
    profile_customer_profile_ma = profile_customer_profile_ma.filter(F.to_date(F.col("partition_month").cast(StringType()), 'yyyyMM') <= min_value)

    geo_mst_cell_masterplan = get_max_date_from_master_data(geo_mst_cell_masterplan, 'partition_date')

    if check_empty_dfs([usage_sum_data_location_daily, profile_customer_profile_ma]):
        return get_spark_empty_df()
    # ----- Transformation -----

    ### where
    spark = get_spark_session()
    # profile_customer_profile_ma_A = profile_customer_profile_ma.agg(F.max("partition_date")).collect()[0][0]
    # profile_customer_profile_ma = profile_customer_profile_ma.where("partition_date = '" + str(profile_customer_profile_ma_A) + "'")

    # profile_last_date = profile_customer_profile_ma.agg(F.max("partition_month")).collect()[0][0]
    # profile_customer_profile_ma = profile_customer_profile_ma.where("partition_month = '"+str(profile_last_date)+"'")
    # geo_home_work_last_date = geo_home_work_data.agg(F.max("start_of_month")).collect()[0][0]
    # geo_home_work_data = geo_home_work_data.where("start_of_month = '" + str(geo_home_work_last_date)+"'")
    # geo_exclude_home_work_last_date = geo_exclude_home_work.agg(F.max("start_of_month")).collect()[0][0]
    # geo_exclude_home_work = geo_exclude_home_work.where("start_of_month = '" + str(geo_exclude_home_work_last_date)+"'")

    geo_exclude_home_work = _geo_top_visit_exclude_homework(geo_exclude_home_work, geo_home_work_data)

    l3_data_traffic_home_work_fn(geo_mst_cell_masterplan,
                                 geo_home_work_data,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily,"HOME_WEEKDAY_LOCATION_ID")\
        .createOrReplaceTempView('Home')
    l3_data_traffic_home_work_fn(geo_mst_cell_masterplan,
                                 geo_home_work_data,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily,"WORK_LOCATION_ID")\
        .createOrReplaceTempView('Work')
    l3_data_traffic_top1_top2_fn(geo_mst_cell_masterplan,
                                 geo_exclude_home_work,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily, "TOP_LOCATION_1ST")\
        .createOrReplaceTempView('Top1')
    l3_data_traffic_top1_top2_fn(geo_mst_cell_masterplan,
                                 geo_exclude_home_work,
                                 profile_customer_profile_ma,
                                 usage_sum_data_location_daily, "TOP_LOCATION_2ND")\
        .createOrReplaceTempView('Top2')

    Home_Work = spark.sql("""
        SELECT 
            A.DATE_ID,
            A.IMSI,
            A.TOTAL_DATA_TRAFFIC_KB AS Home_traffic_KB,
            B.TOTAL_DATA_TRAFFIC_KB AS Work_traffic_KB,
            C.TOTAL_DATA_TRAFFIC_KB AS Top1_location_traffic_KB,
            D.TOTAL_DATA_TRAFFIC_KB AS Top2_location_traffic_KB
        FROM Home A 
        JOIN Work B
            ON B.DATE_ID=A.DATE_ID AND A.IMSI = B.IMSI
        JOIN Top1 C
            ON C.DATE_ID=A.DATE_ID AND A.IMSI = C.IMSI
        JOIN Top2 D
            ON D.DATE_ID=A.DATE_ID AND D.IMSI = B.IMSI
    """)

    Home_Work = Home_Work.withColumn("event_partition_date", F.to_date(Home_Work.DATE_ID.cast(DateType()), "yyyyMMdd"))
    # Home_Work = Home_Work.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date")))
    Home_Work = Home_Work.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date")))
    Home_Work.createTempView('GEO_TEMP_04')

    # print('DEBUG : ------------------------------------------------> (1)')
    # Home_Work.show(10)

    data_traffic_location = spark.sql("""
        SELECT
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
    """)

    # print('DEBUG : ------------------------------------------------> l3_data_traffic_home_work_top1_top2')
    # log.info('test debug :---------')
    log.info(data_traffic_location.show(10))
    return data_traffic_location



###Traffic_fav_location###
def l3_geo_use_Share_traffic_monthly(df, sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_use_non_homework_features",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    l3_df_2 = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l3_geo_use_Share_traffic_monthly')
    l3_df_2.show(10)

    return l3_df_2


###feature_sum_voice_location###
def _homework_join_master_profile(cell_masterplan,geo_homework,profile_ma,Column_Name):
    geo_homework.createOrReplaceTempView('geo_homework')
    cell_masterplan.createOrReplaceTempView('cell_masterplan')
    profile_ma.createOrReplaceTempView('profile_ma')

    spark = get_spark_session()
    df_temp_00 = spark.sql(f'''
      select 
        a.imsi,
        a.{Column_Name}_location_id,
        b.lac as {Column_Name}_lac,
        b.ci as {Column_Name}_ci,
        a.start_of_month
      from geo_homework a
      left join cell_masterplan b
        on a.{Column_Name}_location_id = b.location_id
      group by 1,2,3,4,5
      ''')

    # print('DEBUG : ------------------------------------------------> _homework_join_master_profile (0)')
    # df_temp_00.show(10)

    df_temp_00.createOrReplaceTempView('temp_00')

    df_temp_01 = spark.sql(f'''
      select
        a.imsi,
        a.start_of_month,
        b.access_method_num,
        a.{Column_Name}_location_id,
        a.{Column_Name}_lac,
        a.{Column_Name}_ci
      from temp_00 a
      left join profile_ma b
        on a.imsi = b.imsi and a.start_of_month = b.start_of_month
      group by 1,2,3,4,5,6
      ''')

    # print('DEBUG : ------------------------------------------------> _homework_join_master_profile (1)')
    # df_temp_01.show(10)

    return df_temp_01


def _geo_top_visit_join_master_profile(cell_masterplan,geo_top_visit,profile_ma,Column_Name):
    geo_top_visit.createOrReplaceTempView('geo_top_visit')
    cell_masterplan.createOrReplaceTempView('cell_masterplan')
    profile_ma.createOrReplaceTempView('profile_ma')

    spark = get_spark_session()
    df_temp_00 = spark.sql(f'''
      select
        a.imsi,
        a.{Column_Name},
        b.lac as {Column_Name}_lac,
        b.ci as {Column_Name}_ci,
        a.start_of_month
      from geo_top_visit a
      left join cell_masterplan b
        on a.{Column_Name} = b.location_id
      group by 1,2,3,4,5
      ''')

    # print('DEBUG : ------------------------------------------------> _geo_top_visit_join_master_profile (0)')
    # df_temp_00.show(10)

    df_temp_00.createOrReplaceTempView('temp_00')

    df_temp_01 = spark.sql(f'''
      select
        a.imsi,
        a.start_of_month,
        b.access_method_num,
        a.{Column_Name},
        a.{Column_Name}_lac,
        a.{Column_Name}_ci
      from temp_00 a
      left join profile_ma b
        on a.imsi = b.imsi and a.start_of_month = b.start_of_month
      group by 1,2,3,4,5,6
      ''')

    # print('DEBUG : ------------------------------------------------> _geo_top_visit_join_master_profile (1)')
    # df_temp_01.show(10)

    return df_temp_01


def l3_call_location_home_work_monthly(cell_masterplan,geo_homework,profile_ma,usage_sum_voice,geo_top_visit_exc_homework):

    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_sum_voice, cell_masterplan, geo_homework, profile_ma, geo_top_visit_exc_homework]):
        return get_spark_empty_df()

    usage_sum_voice = data_non_availability_and_missing_check(df=usage_sum_voice,
                                                                            grouping="monthly",
                                                                            par_col="partition_date",
                                                                            target_table_name="l3_call_location_home_work_monthly")

    profile_ma = data_non_availability_and_missing_check(df=profile_ma,
                                                         grouping="monthly",
                                                         par_col="partition_month",
                                                         target_table_name="l3_call_location_home_work_monthly")

    # geo_homework = data_non_availability_and_missing_check
    # geo_top_visit_exc_homework = data_non_availability_and_missing_check

    cell_masterplan = get_max_date_from_master_data(cell_masterplan, 'partition_date')

    min_value = union_dataframes_with_missing_cols(
        [
            usage_sum_voice.select(
                F.to_date(F.date_trunc('month', F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd'))).alias("max_date")),
            profile_ma.select(
                F.to_date(F.max(F.col("partition_month")).cast(StringType()), 'yyyyMM').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    usage_sum_voice = usage_sum_voice.filter(
        F.to_date(F.date_trunc("month", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    profile_ma = profile_ma.filter(F.to_date(F.col("partition_month").cast(StringType()), 'yyyyMM') <= min_value)

    if check_empty_dfs([usage_sum_voice, profile_ma]):
        return get_spark_empty_df()

    # ----- Transformation -----
    geo_homework = geo_homework.join(geo_top_visit_exc_homework,['imsi', 'start_of_month'],'full')

    # Pass
    # print('DEBUG : ------------------------------------------------> (1)')
    # geo_homework.printSchema()

    usage_sum_voice = usage_sum_voice.withColumn('start_of_month', F.to_date(F.date_trunc('month', F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))))
    usage_sum_voice.createOrReplaceTempView('usage_voice')

    profile_ma = profile_ma.withColumn('start_of_month', F.to_date(F.date_trunc('month', F.to_date(F.col("partition_month").cast(StringType()), 'yyyyMM'))))

    _homework_join_master_profile(cell_masterplan,geo_homework,profile_ma,
                                 "home_weekday").createOrReplaceTempView('home_weekday')
    # print('DEBUG : ------------------------------------------------> (a)')
    _homework_join_master_profile(cell_masterplan, geo_homework,profile_ma,
                                 "work").createOrReplaceTempView('work')
    # print('DEBUG : ------------------------------------------------> (b)')
    _geo_top_visit_join_master_profile(cell_masterplan,geo_homework,profile_ma,
                                 "top_location_1st").createOrReplaceTempView('top_location_1st')
    # print('DEBUG : ------------------------------------------------> (c)')
    _geo_top_visit_join_master_profile(cell_masterplan, geo_homework,profile_ma,
                                 "top_location_2nd").createOrReplaceTempView('top_location_2nd')
    print('DEBUG : ------------------------------------------------> (d)')

    spark = get_spark_session()
    print('DEBUG : ------------------------------------------------> (2)')
    spark.sql('''select * from home_weekday limit 10''').show()
    print('DEBUG : ------------------------------------------------> (3)')
    spark.sql('''select * from work limit 10''').show()
    print('DEBUG : ------------------------------------------------> (4)')
    spark.sql('''select * from top_location_1st limit 10''').show()
    print('DEBUG : ------------------------------------------------> (5)')
    spark.sql('''select * from top_location_2nd limit 10''').show()

    def sum_voice_daily(df_temp_01):
        spark = get_spark_session()
        df_sum_voice = spark.sql(f'''
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
        ''')
        return df_sum_voice

    sum_voice_daily('home_weekday').createOrReplaceTempView('df_call_home_weekday')
    sum_voice_daily('work').createOrReplaceTempView('df_call_work')
    sum_voice_daily('top_location_1st').createOrReplaceTempView('df_call_top_1st')
    sum_voice_daily('top_location_2nd').createOrReplaceTempView('df_call_top_2nd')

    spark = get_spark_session()
    print('DEBUG : ------------------------------------------------> (6)')
    spark.sql('''select * from df_call_home_weekday limit 10''').show()
    print('DEBUG : ------------------------------------------------> (7)')
    spark.sql('''select * from df_call_work limit 10''').show()
    print('DEBUG : ------------------------------------------------> (8)')
    spark.sql('''select * from df_call_top_1st limit 10''').show()
    print('DEBUG : ------------------------------------------------> (9)')
    spark.sql('''select * from df_call_top_2nd limit 10''').show()

    spark = get_spark_session()
    df_sum_voice_daily = spark.sql('''
        select
            a.start_of_month as start_of_month
            ,a.imsi
            ,a.call_count_location_home_weekday  
            ,b.call_count_location_work
            ,c.call_count_location_top_location_1st as call_count_location_top_1st
            ,d.call_count_location_top_location_2nd as call_count_location_top_2nd
        from df_call_home_weekday a
        full outer join df_call_work b
            on a.start_of_month = b.start_of_month and a.imsi = b.imsi
        full outer join df_call_top_1st c
            on a.start_of_month = c.start_of_month and a.imsi = c.imsi
        full outer join df_call_top_2nd d
            on a.start_of_month = d.start_of_month and a.imsi = d.imsi
    ''')  # Change join(inner) --> full outer join 20200721

    print('DEBUG : ------------------------------------------------> (10)')
    df_sum_voice_daily.show(10)

    return df_sum_voice_daily
