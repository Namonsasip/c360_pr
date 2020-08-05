from pyspark.sql import DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

from customer360.utilities.re_usable_functions import add_event_week_and_month_from_yyyymmdd, check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df

conf = os.getenv("CONF", None)


def get_max_date_from_master_data(input_df: DataFrame, par_col='partition_date'):
    max_date = input_df.selectExpr('max({0})'.format(par_col)).collect()[0][0]
    input_df = input_df.where('{0}='.format(par_col) + str(max_date))

    return input_df


def distance_callculate_statement(first_lat: str, first_long: str, second_lat: str, second_long: str) -> Column:
    return (
            F.acos(
                F.cos(F.radians(90 - F.col(first_lat))) * F.cos(F.radians(90 - F.col(second_lat))) + \
                F.sin(F.radians(90 - F.col(first_lat))) * F.sin(F.radians(90 - F.col(second_lat))) * \
                F.cos(F.radians(F.col(first_long) - F.col(second_long)))
            ) * 6371
    ).cast(DecimalType(13,2))


def l1_geo_mst_location_near_shop_master(master_df: DataFrame, shape_df: DataFrame) -> DataFrame:
    if check_empty_dfs([master_df, shape_df]):
        return get_spark_empty_df()

    master_df = get_max_date_from_master_data(master_df, 'partition_date')
    shape_df = get_max_date_from_master_data(shape_df, 'partition_month')

    # Pyspark version: Proved
    # https://adb-334552184297553.13.azuredatabricks.net/?o=334552184297553#notebook/2101000251613420/command/3846088851487352
    output_df = master_df.crossJoin(shape_df)\
        .select('landmark_sub_name_en', 'location_id', 'location_name',
                distance_callculate_statement('landmark_latitude', 'landmark_longitude', 'latitude', 'longitude')
                .alias('distance_km'))\
        .where("landmark_cat_name_en in ('AIS', 'DTAC', 'TRUE') and distance_km <= (0.5)")\
        .dropDuplicates()

    return output_df


def l1_geo_mst_location_ais_shop_master(shape_df: DataFrame) -> DataFrame:
    if check_empty_dfs([shape_df]):
        return get_spark_empty_df()

    shape_df = get_max_date_from_master_data(shape_df, 'partition_month')

    # output_df = shape_df.filter("landmark_cat_name_en in ('AIS', 'DTAC', 'TRUE')")
    output_df = shape_df.filter("landmark_cat_name_en = 'AIS'")

    return output_df


def l1_geo_time_spent_by_store_daily(timespent_df: DataFrame, master_df: DataFrame) -> DataFrame:
    output_df = timespent_df.join(master_df, ['location_id'], 'left')\
        .select('imsi', timespent_df.location_id, 'duration', 'num_visit', 'landmark_cat_name_en',
                'event_partition_date', 'start_of_week', 'start_of_month')

    return output_df


def _divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def _massive_processing_daily(data_frame: DataFrame,
                              config_params: str,
                              func_name: callable
                              ) -> DataFrame:
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(_divide_chunks(mvv_array, config_params.get("partition_num_per_job", 1)))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col('partition_date').isin(*[curr_item]))
        small_df = add_event_week_and_month_from_yyyymmdd(small_df, 'partition_date')
        output_df = func_name(small_df, config_params)
        CNTX.catalog.save(config_params["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('partition_date').isin(*[first_item]))
    return_df = add_event_week_and_month_from_yyyymmdd(return_df, 'partition_date')
    return_df = func_name(return_df, config_params)
    return return_df


def _massive_processing_with_join_daily(data_frame: DataFrame,
                                        join_frame: DataFrame,
                                        config_params: str,
                                        func_name: callable
                                        ) -> DataFrame:
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = data_frame
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(_divide_chunks(mvv_array, config_params.get("partition_num_per_job", 1)))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col('partition_date').isin(*[curr_item]))
        small_df = add_event_week_and_month_from_yyyymmdd(small_df, 'partition_date')
        output_df = func_name(small_df, join_frame, config_params)
        CNTX.catalog.save(config_params["output_catalog"], output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('partition_date').isin(*[first_item]))
    return_df = add_event_week_and_month_from_yyyymmdd(return_df, 'partition_date')
    return_df = func_name(return_df, join_frame, config_params)
    return return_df


def l1_geo_total_distance_km_daily(cell_visit: DataFrame, sql) -> DataFrame:
    # ----- Transformation -----
    # Drop unneeded column
    cell_visit = cell_visit.drop('cell_id', 'time_out', 'hour_in', 'hour_out')
    cell_visit = cell_visit.where(F.col("imsi").isNotNull())

    # Window for lead function
    w_lead = Window().partitionBy('imsi', 'event_partition_date').orderBy('time_in')

    # Merge cell_visit table
    cell_visit = cell_visit.withColumn('location_id_next', F.lead('location_id', 1).over(w_lead))\
        .select('imsi', 'time_in', 'location_id_next', 'location_id', 'latitude', 'longitude', 'event_partition_date')
    cell_visit = cell_visit.filter('location_id_next != location_id')
    cell_visit = cell_visit.drop('location_id_next')

    # Add latitude and longitude
    cell_visit_lat = cell_visit.withColumn('latitude_next', F.lead('latitude', 1).over(w_lead))
    cell_visit_lat_long = cell_visit_lat.withColumn('longitude_next', F.lead('longitude', 1).over(w_lead))

    # Calculate distance
    cell_visit_distance = cell_visit_lat_long.withColumn('distance_km',
                                                         F.when(cell_visit_lat_long.latitude_next.isNull(), 0.00)
                                                         .otherwise(
                                                             distance_callculate_statement('latitude',
                                                                                           'longitude',
                                                                                           'latitude_next',
                                                                                           'longitude_next')))

    cell_visit_distance =cell_visit_distance.drop('latitude_next').drop('longitude_next')

    # Sum of distance group by imsi, start_of_month
    output_df = cell_visit_distance.groupBy('imsi', 'event_partition_date') \
        .agg({'distance_km':'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'event_partition_date', 'distance_km')

    # df = node_from_config(cell_visit_distance, sql)
    return output_df


def l1_geo_visit_ais_store_location_daily(cust_visit: DataFrame, shape_df: DataFrame, config_param) -> DataFrame:
    output_df = cust_visit.join(shape_df, [cust_visit.location_id == shape_df.geo_shape_id], 'left')\
        .select('imsi', 'location_id', 'landmark_name_th', 'landmark_sub_name_en',
                'landmark_latitude', 'landmark_longitude', 'time_in',
                'event_partition_date', 'start_of_week', 'start_of_month').dropDuplicates()

    return output_df


def int_l1_geo_top3_voice_location_daily(usagevoice_df: DataFrame,
                                         master_df: DataFrame,
                                         config_param: str) -> DataFrame:
    join_df = usagevoice_df.join(master_df, ['lac', 'ci'], 'left').where("service_type in ('VOICE','VOLTE')") \
        .groupBy('access_method_num', 'location_id', 'latitude', 'longitude',
                 'event_partition_date', 'start_of_week', 'start_of_month') \
        .agg(F.sum(F.col('no_of_call') + F.col('no_of_inc')).alias('total_call'))

    win = Window().partitionBy('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month') \
        .orderBy(F.col('total_call').desc())

    output_df = join_df.withColumn('rank', F.row_number().over(win)).where('rank <= 3')
    output_df = output_df.groupBy('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month') \
        .agg(F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_location_1st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_latitude_1st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_longitude_1st'),
             F.max(F.when((F.col('rank') == 2), F.col('location'))).alias('top_voice_location_2st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_latitude_2st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_longitude_2st'),
             F.max(F.when((F.col('rank') == 3), F.col('location'))).alias('top_voice_location_3st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_latitude_3st'),
             F.max(F.when((F.col('rank') == 1), F.col('location'))).alias('top_voice_longitude_3st')
             )

    return output_df


def l1_geo_top3_voice_location_daily(input_df: DataFrame, config_param: str) -> DataFrame:
    output_df = input_df.select('access_method_num', 'event_partition_date', 'start_of_week', 'start_of_month',)

    # Calculate max distance sum(dis(1,2), dis(1,3))
    sql_query = """
            select
                a.imsi
                ,case
                    when b.latitude is null and b.longitude is null then 0 
                    else cast((acos(cos(radians(90-a.latitude))*
                        cos(radians(90-b.latitude))+sin(radians(90-a.latitude))*
                        sin(radians(90-b.latitude))*cos(radians(a.longitude - b.longitude)))*6371) as decimal(13,2)) 
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

    return output_df


def massive_processing_with_l1_geo_visit_ais_store_location_daily(cust_visit_df: DataFrame,
                                                                  shape_df: DataFrame,
                                                                  config_param: str
                                                                  ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_visit_ais_store_location_daily")

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(cust_visit_df,
                                                    shape_df,
                                                    config_param,
                                                    l1_geo_visit_ais_store_location_daily)
    return output_df


def massive_processing_with_l1_geo_time_spent_by_location_daily(cust_visit_df: DataFrame,
                                                                config_param: str
                                                                ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_time_spent_by_location_daily")

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          config_param,
                                          node_from_config)
    return output_df


def massive_processing_with_l1_geo_count_visit_by_location_daily(cust_visit_df: DataFrame,
                                                                 config_param: str
                                                                 ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_count_visit_by_location_daily")

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          config_param,
                                          node_from_config)
    return output_df


def massive_processing_with_l1_geo_total_distance_km_daily(cust_visit_df: DataFrame,
                                                           config_param: str
                                                           ) -> DataFrame:
    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    cust_visit_df = data_non_availability_and_missing_check(df=cust_visit_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_total_distance_km_daily")

    if check_empty_dfs([cust_visit_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_daily(cust_visit_df,
                                          config_param,
                                          l1_geo_total_distance_km_daily)
    return output_df


def massive_processing_with_int_l1_geo_top3_voice_location_daily(usagevoice_df: DataFrame,
                                                                 master_df: DataFrame,
                                                                 config_param: str
                                                                 ) -> DataFrame:
    if check_empty_dfs([usagevoice_df, master_df]):
        return get_spark_empty_df()

    usagevoice_df = data_non_availability_and_missing_check(df=usagevoice_df,
                                                            grouping="daily",
                                                            par_col="partition_date",
                                                            target_table_name="l1_geo_top3_voice_location_daily")

    if check_empty_dfs([usagevoice_df]):
        return get_spark_empty_df()

    output_df = _massive_processing_with_join_daily(usagevoice_df,
                                                    master_df,
                                                    config_param,
                                                    int_l1_geo_top3_voice_location_daily)
    return output_df



























def massive_processing_with_l1_the_favourite_locations_daily(usage_df_location,geo_df_masterplan):
    usage_df_location = usage_df_location.filter('partition_date >= 20200401 and partition_date <= 20200627')
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_df_location, geo_df_masterplan]):
        return get_spark_empty_df()

    usage_df_location = data_non_availability_and_missing_check(df=usage_df_location,
                                                                grouping="daily",
                                                                par_col="partition_date",
                                                                target_table_name="l1_the_favourite_locations_daily",
                                                                exception_partitions=['2019-11-06'])

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
        small_df = data_frame.filter(F.col('partition_date').isin(*[curr_item]))
        output_df = l1_the_favourite_locations_daily(small_df,geo_df_masterplan)
        CNTX.catalog.save("l1_the_favourite_locations_daily", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('partition_date').isin(*[first_item]))
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


def l1_the_favourite_locations_daily_rework(input_df: DataFrame, master_df: DataFrame) -> DataFrame:
    """
    input_df: usage_sum_data_location_daily
    output_df: usage_sum_data_location_daily
    +-------------+----------+-----+---------+---------+---------+----------+------------+-------+------+------+------+
    | mobile_no   | date_id  | lac |       ci|gprs_type|week_type|no_of_call|total_minute|vol_all|vol_3g|vol_4g|vol_5g|
    +-------------+----------+-----+---------+---------+---------+----------+------------+-------+------+------+------+
    |+++0VA.070...|2020-05-25|23088|350365113|    4GLTE|  weekday|       2.0|         2.0|   0.00|  0.00|  0.00|  0.00|
    |+++0VA.070...|2020-05-25| 5905|    52111|    3GGSN|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|
    |+++0VA.070...|2020-05-25| 5905|    40117|    3GGSN|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|
    |+++0VA.070...|2020-05-25| 5905|    40118|    3GGSN|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|
    output_df: with master_df
    +------------+---------+----------+
    | location_id| latitude| longitude|
    +------------+---------+----------+
    output_df: with groupBy
    +-------------+----------+-----+---------+---------+----------+------------+-------+------+------+------+---------------+
    | mobile_no   | date_id  | lac |       ci|week_type|no_of_call|total_minute|vol_all|vol_3g|vol_4g|vol_5g|number_customer|
    +-------------+----------+-----+---------+---------+----------+------------+-------+------+------+------+---------------+
    |+++0VA.070...|2020-05-25|23088|350365113|  weekday|       2.0|         2.0|   0.00|  0.00|  0.00|  0.00|              1|
    |+++0VA.070...|2020-05-25| 5905|    52111|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|            113|
    |+++0VA.070...|2020-05-25| 5905|    40117|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|             12|
    |+++0VA.070...|2020-05-25| 5905|    40118|  weekday|       1.0|         0.0|   0.00|  0.00|  0.00|  0.00|            144|
    """
    # Add event_partition_date column to DataFrame
    input_df = input_df.withColumn("event_partition_date", F.to_date(input_df.date_id.cast(DateType()), "yyyyMMdd"))
    input_df = input_df.withColumn('week_type', F.when(
        ((F.dayofweek(F.col('event_partition_date')) == 1) | (F.dayofweek(F.col('event_partition_date')) == 7)),
        'weekend').otherwise('weekday'))


    def sum_usage_date_statement(input_params: str) -> Column:
        return (
            F.when(
                F.lower(F.col('gprs_type')).like("{}%".format(input_params)),
                F.sum(F.col('vol_uplink_kb') + F.col('vol_downlink_kb'))
            ).otherwise(0)
        ).alias('vol_{}'.format(input_params))

    output_df = input_df.groupBy('mobile_no', 'event_partition_date', 'lac', 'ci', 'gprs_type', 'week_type').agg(
        F.sum('no_of_call').alias('no_of_call'),
        F.sum('total_minute').alias('total_minute'),
        F.sum(F.col('vol_downlink_kb') + F.col('vol_uplink_kb')).alias('vol_all'),
        sum_usage_date_statement('3g'),
        sum_usage_date_statement('4g'),
        sum_usage_date_statement('5g')
    )

    # Clear master table
    master_df = get_max_date_from_master_data(master_df, 'partition_date')
    output_df = output_df.join(master_df, [output_df.lac == master_df.lac, output_df.ci == master_df.ci], 'left')\
        .select('mobile_no', 'date_id', master_df.location_id, output_df.lac, output_df.ci, master_df.latitude,
                master_df.longitude, 'gprs_type', 'week_type', 'no_of_call', 'total_minute', 'vol_all', 'vol_3g',
                'vol_4g', 'vol_5g').dropDuplicates()

    w_unique_location = Window.partitionBy('lac', 'ci', 'date_id', 'week_type')

    # GroupBy
    output_df = output_df.groupBy('mobile_no', 'event_partition_date', 'lac', 'ci', 'gprs_type', 'week_type'
                                  ,'location_id', 'latitude', 'longitude'
                                  ).agg(
        F.sum('no_of_call').alias('no_of_call'),
        F.sum('total_minute').alias('total_minute'),
        F.sum('vol_all').alias('vol_all'),
        F.sum('vol_3g').alias('vol_3g'),
        F.sum('vol_4g').alias('vol_4g'),
        F.sum('vol_5g').alias('vol_5g')
    ).withColumn('number_customer', F.approx_count_distinct('mobile_no').over(w_unique_location))

    return output_df


def massive_processing_with_l1_number_of_unique_cell_daily(usage_sum_data_location):
    usage_sum_data_location = usage_sum_data_location.filter('partition_date >= 20200401 and partition_date <= 20200627')
    # ----- Data Availability Checks -----
    if check_empty_dfs([usage_sum_data_location]):
        return get_spark_empty_df()

    usage_sum_data_location = data_non_availability_and_missing_check(df=usage_sum_data_location,
                                                                      grouping="daily",
                                                                      par_col="partition_date",
                                                                      target_table_name="l1_number_of_unique_cell_daily",
                                                                      exception_partitions=['2019-11-06'])

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
        small_df = data_frame.filter(F.col('partition_date').isin(*[curr_item]))
        output_df = l1_number_of_unique_cell_daily(small_df)
        CNTX.catalog.save("l1_number_of_unique_cell_daily", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col('partition_date').isin(*[first_item]))
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