import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from pyspark.sql import functions as F

from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics
from pyspark.sql import Window
from pyspark.sql.types import *

from customer360.utilities.re_usable_functions import add_start_of_week_and_month, union_dataframes_with_missing_cols, \
    execute_sql, add_event_week_and_month_from_yyyymmdd, __divide_chunks, check_empty_dfs, \
    data_non_availability_and_missing_check, l1_massive_processing
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", "base")
run_mode = os.getenv("DATA_AVAILABILITY_CHECKS", None)
log = logging.getLogger(__name__)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    # Get max date of partition column
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]

    # Set the latest master DataFrame
    input_df = input_df.where('{0}='.format(par_col) + str(max_date))

    return input_df


def distance_callculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) + \
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) * \
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast('double')


def massive_processing_with_l1_geo_area_from_ais_store_daily(shape, masterplan, geo_cust_cell_visit_time, sql):
    geo_cust_cell_visit_time=geo_cust_cell_visit_time.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([geo_cust_cell_visit_time, shape, masterplan]):
        return get_spark_empty_df()

    geo_cust_cell_visit_time = data_non_availability_and_missing_check(df=geo_cust_cell_visit_time,
                                                                       grouping="daily",
                                                                       par_col="partition_date",
                                                                       target_table_name="l1_geo_area_from_ais_store_daily")

    masterplan = get_max_date_from_master_data(masterplan, 'partition_date')
    shape = get_max_date_from_master_data(shape, 'partition_month')
    # ----- Transformation -----
    masterplan.createOrReplaceTempView("mst_cell_masterplan")
    shape.createOrReplaceTempView("mst_poi_shape")

    ss = get_spark_session()
    df = ss.sql("""
        select 
            a.landmark_sub_name_en,
            b.location_id,
            b.location_name,
            cast((acos(cos(radians(90-a.landmark_latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.landmark_latitude))*sin(radians(90-b.latitude))*cos(radians(a.landmark_longitude - b.longitude)))*6371) as decimal(13,2)) as distance_km
        from mst_poi_shape a,
            mst_cell_masterplan b
        where   a.landmark_cat_name_en in ('AIS')
        and cast((acos(cos(radians(90-a.landmark_latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.landmark_latitude))*sin(radians(90-b.latitude))*cos(radians(a.landmark_longitude - b.longitude)))*6371) as decimal(13,2)) <= (0.5)
        group by 1,2,3,4
    """)  # Fix this cossJoin --> left outer join --> Sould use this statement by Kun Good

    # join_df = master_df.join(shape_df, [master_df.location_id != shape_df.geo_shape_id], 'inner') \
    #     .select('landmark_sub_name_en', 'location_id', 'location_name',
    #             distance_callculate_statement('landmark_latitude', 'landmark_longitude', 'latitude', 'longitude').alias(
    #                 'distance_km')).filter('distance_km <= 0.5')

    if check_empty_dfs([geo_cust_cell_visit_time]):
        return get_spark_empty_df()

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]


    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = geo_cust_cell_visit_time
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))  # Changed 2 --> 5 20200721
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_geo_area_from_ais_store_daily(df, small_df, sql)
        CNTX.catalog.save(sql["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_geo_area_from_ais_store_daily(df, return_df, sql)
    return return_df


def massive_processing_with_l1_geo_area_from_competitor_store_daily(shape,masterplan,geo_cust_cell_visit_time,sql):
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.filter(
        'partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([geo_cust_cell_visit_time, shape, masterplan]):
        return get_spark_empty_df()

    geo_cust_cell_visit_time = data_non_availability_and_missing_check(df=geo_cust_cell_visit_time,
                                                                       grouping="daily",
                                                                       par_col="partition_date",
                                                                       target_table_name="l1_geo_area_from_competitor_store_daily")

    masterplan = get_max_date_from_master_data(masterplan, 'partition_date')
    shape = get_max_date_from_master_data(shape, 'partition_month')

    if check_empty_dfs([geo_cust_cell_visit_time]):
        return get_spark_empty_df()

    # ----- Transformation -----
    masterplan.createOrReplaceTempView("mst_cell_masterplan")
    shape.createOrReplaceTempView("mst_poi_shape")



    ss = get_spark_session()
    df = ss.sql("""
        select 
            a.landmark_sub_name_en,
            b.location_id,
            b.location_name,
            cast((acos(cos(radians(90-a.landmark_latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.landmark_latitude))*sin(radians(90-b.latitude))*cos(radians(a.landmark_longitude - b.longitude)))*6371) as decimal(13,2)) as distance_km
        from mst_poi_shape a,
            mst_cell_masterplan b
        where   a.landmark_cat_name_en in ('TRUE','DTAC')
        and cast((acos(cos(radians(90-a.landmark_latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.landmark_latitude))*sin(radians(90-b.latitude))*cos(radians(a.landmark_longitude - b.longitude)))*6371) as decimal(13,2)) <= (0.5)
        group by 1,2,3,4
    """)  # Fix this cossJoin --> left outer join --> Sould use this statement by Kun Good

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]


    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = geo_cust_cell_visit_time
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))  # Changed 2 --> 5 20200721
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_geo_area_from_competitor_store_daily(df, small_df, sql)
        CNTX.catalog.save(sql["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_geo_area_from_competitor_store_daily(df, return_df, sql)
    return return_df


def l1_geo_time_spent_by_location_daily(df,sql):
    df = df.filter('partition_date >= 20191101 and partition_date <= 20191130')

    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df,
                                                 grouping="daily",
                                                 par_col="partition_date",
                                                 target_table_name="l1_geo_time_spent_by_location_daily")

    if check_empty_dfs([df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    sql = """
        select 
            imsi,
            location_id,
            sum(duration) as sum_duration,
            event_partition_date,
            start_of_week,
            start_of_month
    from geo_cust_cell_visit_time
    group by imsi, location_id, event_partition_date, start_of_week, start_of_month
    """
    return_df = massive_processing_time_spent_daily(df, sql, "l1_geo_time_spent_by_location_daily", 'partition_date')

    return return_df


def l1_geo_area_from_ais_store_daily(df, geo_cust_cell_visit_time, sql):
    geo_cust_cell_visit_time  = add_start_of_week_and_month(geo_cust_cell_visit_time, "time_in")
    ss = get_spark_session()
    df.createOrReplaceTempView('temp_geo_ais_shop')

    geo_cust_cell_visit_time.createOrReplaceTempView('geo_cust_cell_visit_time')

    df2 = ss.sql("""
        select 
            a.imsi,
            a.event_partition_date,
            a.start_of_week,
            a.start_of_month,
            sum(duration) as duration,
            count(1) as num_of_times_per_day 
        from geo_cust_cell_visit_time a, temp_geo_ais_shop b 
        where a.location_id = b.location_id 
        group by 1,2,3,4
    """)  # It will be inner join

    df2 = node_from_config(df2,sql)

    return df2


def l1_geo_area_from_competitor_store_daily(df,geo_cust_cell_visit_time,sql):
    geo_cust_cell_visit_time = add_start_of_week_and_month(geo_cust_cell_visit_time, "time_in")
    ss = get_spark_session()

    df.createOrReplaceTempView('temp_geo_ais_shop')

    geo_cust_cell_visit_time.createOrReplaceTempView('geo_cust_cell_visit_time')

    df2 = ss.sql("""
        select 
            a.imsi,
            a.event_partition_date,
            a.start_of_week,
            a.start_of_month,
            sum(duration) as duration,
            count(1) as num_of_times_per_day 
        from geo_cust_cell_visit_time a, temp_geo_ais_shop b
        where a.location_id = b.location_id 
        group by 1,2,3,4
    """)  # It will be inner join

    df2 = node_from_config(df2, sql)

    return df2


def massive_processing_with_l1_geo_total_distance_km_daily(l0_df, sql):
    l0_df = l0_df.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([l0_df]):
        return get_spark_empty_df()

    l0_df = data_non_availability_and_missing_check(df=l0_df,
                                                    grouping="daily",
                                                    par_col="partition_date",
                                                    target_table_name="l1_geo_total_distance_km_daily")

    if check_empty_dfs([l0_df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = l0_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_geo_total_distance_km_daily(small_df, sql)
        CNTX.catalog.save(sql["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_geo_total_distance_km_daily(return_df, sql)
    return return_df


###total_distance_km###
def l1_geo_total_distance_km_daily(l0_df, sql):
    l0_df=l0_df.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([l0_df]):
        return get_spark_empty_df()

    l0_df = data_non_availability_and_missing_check(df=l0_df,
                                                    grouping="daily",
                                                    par_col="partition_date",
                                                    target_table_name="l1_geo_total_distance_km_daily")

    if check_empty_dfs([l0_df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    # Get spark session
    spark = get_spark_session()

    # Source Table
    l0_df1 = l0_df.withColumn("event_partition_date", F.to_date(l0_df.time_in.cast(DateType()), "yyyyMMdd")).drop("partition_date")

    l0_df1.createOrReplaceTempView('geo_cust_cell_visit_time')
    sql_query = """
        select
            imsi
            ,time_in
            ,latitude
            ,longitude
            ,event_partition_date
            ,row_number() over (partition by imsi,event_partition_date order by time_in) as row_num
        from geo_cust_cell_visit_time
        order by imsi,time_in,event_partition_date
    """
    l1_df = spark.sql(sql_query)
    l1_df.createOrReplaceTempView('geo_cust_cell_visit_time_row_num')
    sql_query1 = """
        select
            a.imsi
            ,a.time_in
            ,a.latitude as latitude_start
            ,a.longitude as longitude_start
            ,b.latitude as latitude_end
            ,b.longitude as longitude_end
            ,case 
                when b.latitude is null and b.longitude is null then 0 
                else cast((acos(cos(radians(90-a.latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.latitude))*sin(radians(90-b.latitude))*cos(radians(a.longitude - b.longitude)))*6371) as decimal(13,2)) 
            end as total_distance_km
            ,a.event_partition_date
        from geo_cust_cell_visit_time_row_num a
        left join geo_cust_cell_visit_time_row_num b
        on a.imsi = b.imsi
        and a.event_partition_date = b.event_partition_date
        and a.row_num = b.row_num-1
        order by a.imsi,a.time_in,a.event_partition_date
    """
    l1_df1 = spark.sql(sql_query1)
    l1_df1 = l1_df1.withColumn("start_of_week", F.to_date(F.date_trunc('week', l1_df1.event_partition_date)))
    l1_df1 = l1_df1.withColumn("start_of_month", F.to_date(F.date_trunc('month', l1_df1.event_partition_date)))

    l1_df2 = node_from_config(l1_df1, sql)

    return l1_df2


def massive_processing_with_l1_geo_cust_subseqently_distance(cell_visit, sql):
    cell_visit = cell_visit.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([cell_visit]):
        return get_spark_empty_df()

    cell_visit = data_non_availability_and_missing_check(df=cell_visit,
                                                         grouping="daily",
                                                         par_col="partition_date",
                                                         target_table_name="l1_geo_cust_subseqently_distance")

    if check_empty_dfs([cell_visit]):
        return get_spark_empty_df()

    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = cell_visit
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_geo_cust_subseqently_distance(small_df, sql)
        CNTX.catalog.save(sql["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_geo_cust_subseqently_distance(return_df, sql)
    return return_df


def l1_geo_cust_subseqently_distance(cell_visit, sql):
    # ----- Transformation -----
    # Drop unneeded column
    cell_visit = cell_visit.drop('cell_id', 'time_out', 'hour_in', 'hour_out')
    cell_visit = cell_visit.where(F.col("imsi").isNotNull())

    # Add event_partition_date and start_of_week
    cell_visit = cell_visit.withColumn("event_partition_date", F.to_date(F.col('partition_date').cast(StringType()), 'yyyyMMdd'))
    cell_visit = cell_visit.drop('partition_date')
    # cell_visit = cell_visit.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date')))).drop('partition_date')

    # Window for lead function
    w_lead = Window().partitionBy('imsi', 'event_partition_date').orderBy('time_in')

    # Merge cell_visit table
    cell_visit = cell_visit.withColumn('location_id_next', F.lead('location_id', 1).over(w_lead)).select('imsi', 'time_in', 'location_id_next', 'location_id', 'latitude', 'longitude', 'event_partition_date')
    cell_visit = cell_visit.filter('location_id_next != location_id')
    cell_visit = cell_visit.drop('location_id_next')

    # Add latitude and longitude
    cell_visit_lat = cell_visit.withColumn('latitude_next', F.lead('latitude', 1).over(w_lead))
    cell_visit_lat_long = cell_visit_lat.withColumn('longitude_next', F.lead('longitude', 1).over(w_lead))

    # Calculate distance
    cell_visit_distance = cell_visit_lat_long.withColumn('distance_km',
                                                         F.when(cell_visit_lat_long.latitude_next.isNull(),
                                                                0.00).otherwise(
                                                             (F.acos(F.cos(
                                                                 F.radians(90 - cell_visit_lat_long.latitude)) * F.cos(
                                                                 F.radians(
                                                                     90 - cell_visit_lat_long.latitude_next)) + F.sin(
                                                                 F.radians(90 - cell_visit_lat_long.latitude)) * F.sin(
                                                                 F.radians(
                                                                     90 - cell_visit_lat_long.latitude_next)) * F.cos(
                                                                 F.radians(
                                                                     cell_visit_lat_long.longitude_next - cell_visit_lat_long.longitude))) * 6371).cast(
                                                                 'double')))

    cell_visit_distance =cell_visit_distance.drop('latitude_next').drop('longitude_next')

    # Sum of distance group by imsi, start_of_month
    df = cell_visit_distance.groupBy('imsi', 'event_partition_date') \
        .agg({'distance_km':'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'event_partition_date', 'distance_km')

    # df = node_from_config(cell_visit_distance, sql)

    return df


def massive_processing_with_l1_location_of_visit_ais_store_daily(shape,cust_cell_visit,sql):
    cust_cell_visit = cust_cell_visit.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([cust_cell_visit, shape]):
        return get_spark_empty_df()

    cust_cell_visit = data_non_availability_and_missing_check(df=cust_cell_visit,
                                                              grouping="daily",
                                                              par_col="partition_date",
                                                              target_table_name="l1_location_of_visit_ais_store_daily")

    shape = get_max_date_from_master_data(shape, 'partition_month')

    if check_empty_dfs([cust_cell_visit]):
        return get_spark_empty_df()

    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = cust_cell_visit
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_location_of_visit_ais_store_daily(shape,small_df,sql)
        CNTX.catalog.save(sql["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_location_of_visit_ais_store_daily(shape,return_df,sql)
    return return_df


###feature_AIS_store###
def l1_location_of_visit_ais_store_daily(shape,cust_cell_visit,sql):
    # ----- Transformation -----
    store_shape = shape.where('landmark_cat_name_en like "%AIS%"')
    store_shape.createOrReplaceTempView('geo_mst_lm_poi_shape')

    max_date = cust_cell_visit.selectExpr('max(partition_date)').collect()[0][0]
    cust_cell_visit = cust_cell_visit.where('partition_date='+str(max_date))
    cust_cell_visit = cust_cell_visit.withColumn("event_partition_date",
                                       F.to_date(F.col('partition_date').cast(StringType()), 'yyyyMMdd'))
    cust_cell_visit.createOrReplaceTempView('geo_cust_cell_visit_time')
    # Get spark session
    spark = get_spark_session()
    df = spark.sql("""
        SELECT DISTINCT 
            imsi,
            p1.location_id,
            landmark_name_th,
            landmark_sub_name_en,
            landmark_latitude,
            landmark_longitude,
            time_in,
            substr(partition_date,0,6) as partition_month,
            event_partition_date
        FROM geo_cust_cell_visit_time p1 join geo_mst_lm_poi_shape  p2
        WHERE p1.location_id =  p2.geo_shape_id
        AND p2.PARTITION_NAME = 'INTERNAL'
        AND p2.LANDMARK_CAT_NAME_EN IN ('AIS')
     """)

    store_visit = node_from_config(df,sql)

    return store_visit


def massive_processing_with_l1_geo_top3_cells_on_voice_usage(usage_df,geo_df,profile_df):
    usage_df = usage_df.filter('partition_date >= 20191101 and partition_date <= 20191130')
    profile_df = profile_df.filter('partition_month >= 201911')
    # exception_partitions=['20191106']
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_df, geo_df, profile_df]):
        return get_spark_empty_df()

    usage_df = data_non_availability_and_missing_check(df=usage_df,
                                                       grouping="daily",
                                                       par_col="partition_date",
                                                       target_table_name="l1_geo_top3_cells_on_voice_usage")

    profile_df = data_non_availability_and_missing_check(df=profile_df,
                                                         grouping="monthly",
                                                         par_col="partition_month",
                                                         target_table_name="L1_usage_sum_data_location_daily_data_profile_customer_profile_ma")

    geo_df = get_max_date_from_master_data(geo_df, 'partition_date')

    min_value = union_dataframes_with_missing_cols(
        [
            usage_df.select(
                F.to_date(F.date_trunc('month',
                                       F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd'))).alias(
                    "max_date")),
            profile_df.select(
                F.to_date(F.max(F.col("partition_month")).cast(StringType()), 'yyyyMM').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    usage_df = usage_df.filter(
        F.to_date(
            F.date_trunc("month", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))) <= min_value)

    profile_df = profile_df.filter(F.to_date(F.col("partition_month").cast(StringType()), 'yyyyMM') <= min_value)

    if check_empty_dfs([usage_df, profile_df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = usage_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_geo_top3_cells_on_voice_usage(small_df, geo_df, profile_df)
        CNTX.catalog.save("l1_geo_top3_cells_on_voice_usage", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_geo_top3_cells_on_voice_usage(return_df, geo_df, profile_df)
    return return_df


###Top_3_cells_on_voice_usage###
def l1_geo_top3_cells_on_voice_usage(usage_df,geo_df,profile_df):
    # ----- Transformation -----
    ### config
    spark = get_spark_session()

    ### add partition_date
    l0_df_usage1 = usage_df.withColumn("event_partition_date",F.to_date(usage_df.date_id.cast(DateType()), "yyyyMMdd"))

    # create temp
    l0_df_usage1.createOrReplaceTempView('usage_sum_voice_location_daily')
    geo_df.createOrReplaceTempView('geo_mst_cell_masterplan')
    profile_df.createOrReplaceTempView('profile_customer_profile_ma')

    ### spark_sql
    sql_query = """
        select
            c.imsi
            ,b.latitude
            ,b.longitude
            ,sum(a.no_of_call+a.no_of_inc) as total_call
            ,a.event_partition_date
        from usage_sum_voice_location_daily a
        join profile_customer_profile_ma c
            on a.access_method_num = c.access_method_num
        left join geo_mst_cell_masterplan b
            on a.lac = b.lac
            and a.ci = b.ci
        where service_type in ('VOICE','VOLTE')
        group by 1,2,3,5
        order by 1,2,3,5
    """
    l1_df = spark.sql(sql_query)
    l1_df.createOrReplaceTempView('L4_temp_table')

    sql_query1 = """
        select
            imsi
            ,latitude
            ,longitude
            ,total_call
            ,row_number() over (partition by imsi,event_partition_date order by total_call desc) as rnk
            ,event_partition_date
        from L4_temp_table
        where imsi is not null
            and latitude is not null 
            and longitude is not null
    """
    l1_df1 = spark.sql(sql_query1)
    l1_df1 = l1_df1.withColumn("start_of_week", F.to_date(F.date_trunc('week', l1_df1.event_partition_date)))
    l1_df1 = l1_df1.withColumn("start_of_month", F.to_date(F.date_trunc('month', l1_df1.event_partition_date)))
    # l1_df2 = node_from_config(l1_df1, sql)


    return l1_df1


###distance_top_call###
def l1_geo_distance_top_call(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df,
                                                 grouping="daily",
                                                 par_col="event_partition_date",
                                                 target_table_name="l1_geo_distance_top_call")

    if check_empty_dfs([df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    ### config
    spark = get_spark_session()

    # create temp
    df.createOrReplaceTempView('geo_top3_cells_on_voice_usage')

    sql_query = """
        select
            a.imsi
            ,case
                when b.latitude is null and b.longitude is null then 0 
                else cast((acos(cos(radians(90-a.latitude))*cos(radians(90-b.latitude))+sin(radians(90-a.latitude))*sin(radians(90-b.latitude))*cos(radians(a.longitude - b.longitude)))*6371) as decimal(13,2)) 
                end as top_distance_km
            ,a.event_partition_date
            ,a.start_of_week
            ,a.start_of_month
        from geo_top3_cells_on_voice_usage a
        left join geo_top3_cells_on_voice_usage b
            on a.imsi = b.imsi
            and a.event_partition_date = b.event_partition_date
            and b.rnk >= 2
        where a.rnk = 1
        order by 1,3,4,5
    """
    l1_df = spark.sql(sql_query)

    return l1_df


def l1_geo_number_of_bs_used(geo_cust_cell, sql):
    geo_cust_cell=geo_cust_cell.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # .filter('partition_month >= 201911')
    # exception_partitions=['20191106']
    # ----- Data Availability Checks -----
    if check_empty_dfs([geo_cust_cell]):
        return get_spark_empty_df()

    geo_cust_cell = data_non_availability_and_missing_check(df=geo_cust_cell,
                                                 grouping="daily",
                                                 par_col="partition_date",
                                                 target_table_name="l1_geo_number_of_bs_used")

    if check_empty_dfs([geo_cust_cell]):
        return get_spark_empty_df()

    # ----- Transformation -----
    geo_cust_cell = geo_cust_cell.withColumn("event_partition_date", F.to_date(F.col('partition_date').cast(StringType()), 'yyyyMMdd'))
    # df = node_from_config(geo_cust_cell, sql)
    df = l1_massive_processing(geo_cust_cell, sql)
    return df


def massive_processing_with_l1_the_favourite_locations_daily(usage_df_location,geo_df_masterplan):
    usage_df_location = usage_df_location.filter('partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_df_location, geo_df_masterplan]):
        return get_spark_empty_df()

    usage_df_location = data_non_availability_and_missing_check(df=usage_df_location,
                                                                grouping="daily",
                                                                par_col="partition_date",
                                                                target_table_name="l1_the_favourite_locations_daily",
                                                                exception_partitions=['20191106'])

    geo_df_masterplan = get_max_date_from_master_data(geo_df_masterplan, 'partition_date')

    if check_empty_dfs([usage_df_location]):
        return get_spark_empty_df()

    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = usage_df_location
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_the_favourite_locations_daily(small_df,geo_df_masterplan)
        CNTX.catalog.save("l1_the_favourite_locations_daily", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_the_favourite_locations_daily(return_df,geo_df_masterplan)
    return return_df


# 47 l1_the_favourite_locations_daily ====================
def l1_the_favourite_locations_daily(usage_df_location,geo_df_masterplan):
    # ----- Transformation -----
    ### config
    spark = get_spark_session()

    ### table view path
    geo_df_masterplan.createOrReplaceTempView("geo_mst_cell_masterplan")

    ### add partition_date
    sum_data_location = usage_df_location \
        .withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col("date_id")))) \
        .withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col("date_id"))))

    sum_data_location.createOrReplaceTempView('sum_data_location')

    # CreateTempView
    geo_df_masterplan.createOrReplaceTempView('geo_df_masterplan')
    usage_df_location.createOrReplaceTempView('usage_df_location')

    sql_l1_1 = """
        select 
            b.mobile_no
            ,b.date_id
            ,mp.location_id
            ,b.lac as lac
            ,b.ci as ci
            ,b.gprs_type
            ,mp.latitude
            ,mp.longitude
            ,b.start_of_week
            ,b.start_of_month
            ,case 
                when dayofweek(b.date_id) = 2 or dayofweek(b.date_id) = 3 or dayofweek(b.date_id) = 4 or dayofweek(b.date_id) = 5 or dayofweek(b.date_id) = 6 then "weekday"
                else "weekend" end as weektype
            ,sum(b.no_of_call) as all_no_of_call
            ,sum(b.vol_downlink_kb+b.vol_uplink_kb) as all_usage_data_kb
            ,case 
                when lower(b.gprs_type) like "3g%" then sum(b.vol_uplink_kb+b.vol_downlink_kb)
                else 0 end as vol_3g
            ,case
                when lower(b.gprs_type) like "4g%" then sum(b.vol_uplink_kb+b.vol_downlink_kb)
                else 0 end as vol_4g
            ,case
                when lower(b.gprs_type) like "5g%" then sum(b.vol_uplink_kb+b.vol_downlink_kb) 
                else 0 end as vol_5g
        from sum_data_location b  
        left join geo_mst_cell_masterplan mp
            on b.lac = mp.lac
            and b.ci = mp.ci
        where mp.location_id is not NULL
        GROUP BY b.mobile_no,b.date_id,mp.location_id,b.lac,b.ci,b.gprs_type,weektype,
            mp.latitude,mp.longitude,b.start_of_week,b.start_of_month
        order by date_id
    """
    l1 = spark.sql(sql_l1_1)

    l1 = l1.withColumn("event_partition_date", F.to_date(l1.date_id.cast(DateType()), "yyyyMMdd"))

    return l1

def massive_processing_time_spent_daily(data_frame: DataFrame, sql, output_df_catalog, partition_col) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
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
        small_df = data_frame.filter(f.col(partition_col).isin(*[curr_item]))
        small_df = add_start_of_week_and_month(small_df, "time_in")
        small_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
        output_df = ss.sql(sql)
        CNTX.catalog.save(output_df_catalog, output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col(partition_col).isin(*[first_item]))
    return_df = add_start_of_week_and_month(return_df, "time_in")
    return_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
    return_df = ss.sql(sql)
    return return_df


def massive_processing_with_l1_number_of_unique_cell_daily(usage_sum_data_location):
    usage_sum_data_location = usage_sum_data_location.filter(
        'partition_date >= 20191101 and partition_date <= 20191130')
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_sum_data_location]):
        return get_spark_empty_df()

    usage_sum_data_location = data_non_availability_and_missing_check(df=usage_sum_data_location,
                                                                      grouping="daily",
                                                                      par_col="partition_date",
                                                                      target_table_name="l1_number_of_unique_cell_daily",
                                                                      exception_partitions=['20191106'])

    if check_empty_dfs([usage_sum_data_location]):
        return get_spark_empty_df()
    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = usage_sum_data_location
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l1_number_of_unique_cell_daily(small_df)
        CNTX.catalog.save("l1_number_of_unique_cell_daily", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l1_number_of_unique_cell_daily(return_df)
    return return_df


def l1_number_of_unique_cell_daily(usage_sum_data_location):
    # ----- Transformation -----
    spark = get_spark_session()
    usage_sum_data_location = usage_sum_data_location.withColumn("event_partition_date",F.to_date(usage_sum_data_location.date_id))
    usage_sum_data_location.createOrReplaceTempView('usage_sum_data_location_daily')

    l1_df_4 = spark.sql("""
        select
            count(distinct mobile_no) AS MOBILE_NO,
            LAC,
            CI,
            case
                when dayofweek(event_partition_date) = 2
                    or dayofweek(event_partition_date) = 3
                    or dayofweek(event_partition_date) = 4
                    or dayofweek(event_partition_date) = 5
                    or dayofweek(event_partition_date) = 6 then   "weekday"
                else "weekend" end as WEEKTYPE,
            event_partition_date
        from usage_sum_data_location_daily
        group by lac , ci, event_partition_date
    """)

    return l1_df_4