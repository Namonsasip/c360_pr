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


def l3_geo_time_spent_by_location_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_time_spent_by_location_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = massive_processing_monthly(df, sql, "l3_geo_time_spent_by_location_monthly", 'start_of_month')
    return df


def l3_geo_area_from_ais_store_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_area_from_ais_store_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = node_from_config(df, sql)
    return df


def l3_geo_area_from_competitor_store_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_area_from_competitor_store_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    df = node_from_config(df, sql)
    return df


###total_distance_km###
def l3_geo_total_distance_km_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_total_distance_km_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    df = node_from_config(df, sql)
    return df


###Traffic_fav_location###
def l3_geo_use_Share_traffic_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_use_Share_traffic_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date"))).drop( 'event_partition_date')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2


###feature_sum_voice_location###
def l3_geo_call_location_home_work_monthly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_call_location_home_work_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date"))).drop( 'event_partition_date')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2


###Top_3_cells_on_voice_usage###
def l3_geo_top3_cells_on_voice_usage(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_top3_cells_on_voice_usage",
                                                 missing_data_check_flg='Y')
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

    return df


def l3_geo_distance_top_call(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_geo_distance_top_call",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = df.groupBy("imsi", "start_of_month").agg(
        F.max("top_distance_km").alias("max_distance_top_call"),
        F.min("top_distance_km").alias("min_distance_top_call"),
        F.avg("top_distance_km").alias("avg_distance_top_call"),
        F.when(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2))).cast("string") == 'NaN', 0).otherwise(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2)))).alias("sd_distance_top_call"),
        F.sum("top_distance_km").alias("sum_distance_top_call"))
    return df



# 47 The favourite location
def l3_the_favourite_locations_monthly(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="monthly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l3_the_favourite_locations_monthly",
                                                 missing_data_check_flg='Y')
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
    # input_df = input_df.filter('partition_date >= 20190801 and partition_date <= 20191031')

    CNTX = load_context(Path.cwd(), env=os.getenv("CONF", "base"))

    # ----- Data Availability Checks -----
    if check_empty_dfs(input_df):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                 par_col="partition_date",
                                                 target_table_name="l3_geo_home_work_location_id_monthly",
                                                 missing_data_check_flg='Y')
    if check_empty_dfs(input_df):
        return get_spark_empty_df()
    # ----- Transformation -----

    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = config_home.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
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
    df = df.withColumn('week_type', F.when(((F.dayofweek(F.col('event_partition_date')) == 1) & (F.dayofweek(F.col('event_partition_date')) == 7)), 'weekend')
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


    w_work_num_row = Window().partitionBy('imsi', 'location_id', 'start_of_month').orderBy(F.col('duration_3m').desc(), F.col('days_3m').desc())
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

    w_num_row = Window().partitionBy('imsi', 'location_id', 'week_type', 'start_of_month').orderBy(F.col('duration_3m').desc(), F.col('days_3m').desc())
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
                                                                    missing_data_check_flg='Y')
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