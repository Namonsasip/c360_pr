import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config, expansion
from kedro.context.context import load_context
from pathlib import Path
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


def l2_geo_time_spent_by_location_weekly(df,sql):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df,grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_time_spent_by_location_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    # ----- Transformation -----
    df = massive_processing_time_spent_weekly(df,sql,"l2_geo_time_spent_by_location_weekly", 'start_of_week')
    print('DEBUG : ------------------------------------------------> l2_geo_time_spent_by_location_weekly')
    df.show(10)

    return df


def l2_geo_area_from_ais_store_weekly(df, sql):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_area_from_ais_store_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l2_geo_area_from_ais_store_weekly')
    df.show(10)

    return df

def l2_geo_area_from_competitor_store_weekly(df,sql):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_area_from_competitor_store_weekly",
                                                 missing_data_check_flg='N')

    if check_empty_dfs([df]):
        return get_spark_empty_df()


    df = node_from_config(df,sql)

    print('DEBUG : ------------------------------------------------> l2_geo_area_from_competitor_store_weekly')
    df.show(10)

    return df


def l2_geo_cust_subseqently_distance_weekly(df, sql):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_cust_subseqently_distance_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    # Add start_of_week
    df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))

    # Add week_type
    df = df.withColumn("week_type", F.when(
        (F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
                                                                  .otherwise('weekday').cast(StringType())
                                                                  )

    print(df.printSchema())

    # start_of_week, weekday= , weekend=
    df_week_type = df.groupBy('imsi', 'start_of_week', 'week_type') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'week_type', 'distance_km')

    print("Group by pass with week_type")

    df_week = df.groupBy('imsi', 'start_of_week') \
        .agg({'distance_km': 'sum'}).withColumnRenamed('sum(distance_km)', 'distance_km') \
        .select('imsi', 'start_of_week', 'distance_km')

    print("Group by pass")

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
    # | imsi | start_of_week | distance_km | weekday_distance_km | weekend_distance_km |

    print('Start use node from config')

    df = df_finish_week_2.groupBy('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km') \
        .agg({'imsi': 'count'}).withColumnRenamed('count(imsi)', 'count_row') \
        .select('imsi', 'start_of_week', 'distance_km', 'weekday_distance_km', 'weekend_distance_km')

    # df = node_from_config(df_finish_week_2, sql)

    return df


###total_distance_km###
def l2_geo_total_distance_km_weekly(df: DataFrame, sql: dict):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_total_distance_km_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()


    # return_df = expansion(return_df, sql)
    df = node_from_config(df, sql)

    print('DEBUG : ------------------------------------------------> l2_geo_total_distance_km_weekly')
    df.show(10)

    return df

###Traffic_fav_location###
# def   l2_geo_use_traffic_home_work_weekly(df, sql):
#     # ----- Data Availability Checks -----
#     if check_empty_dfs([df]):
#         return get_spark_empty_df()
#
#     df = data_non_availability_and_missing_check(df=df, grouping="weekly",
#                                                  par_col="event_partition_date",
#                                                  target_table_name="l2_geo_use_traffic_home_work_weekly",
#                                                  missing_data_check_flg='N')
#     if check_empty_dfs([df]):
#         return get_spark_empty_df()
#
#
#     l2_df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date"))).drop('event_partition_date')
#     l2_df_2 = node_from_config(l2_df, sql)
#     return l2_df_2

###Number_of_base_station###
def l2_geo_data_count_location_weekly(df,sql):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_data_count_location_weekly",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    L2_DF = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "event_partition_date")))\
        .drop('event_partition_date')
    df = node_from_config(L2_DF, sql)

    return df


###Top_3_cells_on_voice_usage###
def l2_geo_top3_cells_on_voice_usage(df,sql):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_top3_cells_on_voice_usage",
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
    ,row_number() over (partition by imsi,start_of_week order by total_call desc) as rnk
    ,start_of_week
    from top3_cells_on_voice_usage
    """
    df = spark.sql(sql_query)
    df.cache()
    df = df.where("rnk <= 3")

    print('DEBUG : ------------------------------------------------> l2_geo_top3_cells_on_voice_usage')
    df.show(10)

    return df


###distance_top_call###
def l2_geo_distance_top_call(df):
    # .filter('partition_date >= 20200601 and partition_date <= 20200630')
    # .filter('partition_month >= 201911')
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_geo_top3_cells_on_voice_usage",
                                                 missing_data_check_flg='N')
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    l1_df1 = df.groupBy("imsi", "start_of_week").agg(
        F.max("top_distance_km").alias("max_distance_top_call"), F.min("top_distance_km").alias("min_distance_top_call"),
        F.avg("top_distance_km").alias("avg_distance_top_call"), F.when(
            F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km), F.lit(2))).cast(
                "string") == 'NaN', 0).otherwise(
            F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km), F.lit(2)))).alias(
            "sd_distance_top_call"),F.sum("top_distance_km").alias("sum_distance_top_call"))

    print('DEBUG : ------------------------------------------------> l2_geo_distance_top_call')
    l1_df1.show(10)

    return l1_df1


### 47 l2_the_favourite_locations_daily ====================\
def l2_the_favourite_locations_weekly(df):
    # ----- Data Availability Checks -----
    if check_empty_dfs([df]):
        return get_spark_empty_df()

    df = data_non_availability_and_missing_check(df=df, grouping="weekly",
                                                 par_col="event_partition_date",
                                                 target_table_name="l2_the_favourite_locations_weekly",
                                                 missing_data_check_flg='N')


    if check_empty_dfs([df]):
        return get_spark_empty_df()


    ### config
    spark = get_spark_session()
    df.createOrReplaceTempView('l1_df_the_favourite_location_daily')
    sql_query = """
    select
    mobile_no
    ,start_of_week
    ,lac	
    ,ci
    ,sum(vol_3g) as vol_3g
    ,sum(vol_4g) as vol_4g
    ,sum(vol_5g) as vol_5g
    from l1_df_the_favourite_location_daily
    group by 1,2,3,4
    order by 2,1,3,4
    """
    l2 = spark.sql(sql_query)

    return l2


def massive_processing_with_l2_same_favourite_location_weekend_weekday_weekly(l0_geo_cust_cell_visit_time_df):
    l0_geo_cust_cell_visit_time_df = l0_geo_cust_cell_visit_time_df.filter('partition_date >= 20200601 and partition_date <= 20200630')

    if check_empty_dfs([l0_geo_cust_cell_visit_time_df]):
        return get_spark_empty_df()

    l0_geo_cust_cell_visit_time_df = data_non_availability_and_missing_check(df=l0_geo_cust_cell_visit_time_df,
                                                                             grouping="weekly",
                                                                             par_col="partition_date",
                                                                             target_table_name="l2_same_favourite_location_weekend_weekday",
                                                                             missing_data_check_flg='N')

    if check_empty_dfs([l0_geo_cust_cell_visit_time_df]):
        return get_spark_empty_df()
    # ----- Transformation -----
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    ss = get_spark_session()
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = l0_geo_cust_cell_visit_time_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 10))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col('partition_date').isin(*[curr_item]))
        output_df = l2_same_favourite_location_weekend_weekday_weekly(small_df)
        CNTX.catalog.save("l2_same_favourite_location_weekend_weekday_weekly", output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col('partition_date').isin(*[first_item]))
    return_df = l2_same_favourite_location_weekend_weekday_weekly(return_df)
    return return_df


# 27 Same favourite location for weekend and weekday
def l2_same_favourite_location_weekend_weekday_weekly(l0_geo_cust_cell_visit_time_df: DataFrame):
    ### config
    spark = get_spark_session()

    # Assign day_of_week to weekday or weekend
    geo_df = l0_geo_cust_cell_visit_time_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', "time_in"))) \
        .withColumn("start_of_month", F.to_date(F.date_trunc('month', "time_in")))
    #print(" ========================= geo_df ======================")

    geo_df.createOrReplaceTempView('geo_df')

    sql_query = """
    select 
    imsi,
    start_of_week, 
    case when dayofweek(time_in) = 2 or dayofweek(time_in) = 3 or dayofweek(time_in) = 4 or dayofweek(time_in) = 5 \
    or dayofweek(time_in) = 6 then "weekday" else "weekend" end as weektype,location_id,sum(duration) as duration_sumtime \
    from geo_df where imsi is not NULL 
    group by 1,2,3,4 
    order by 1,2,3,4,5 desc"""
    l2_geo = spark.sql(sql_query)
    l2_geo.createOrReplaceTempView('l2_geo')
    #print(" ========================= l2_geo ======================")

    # where weekday seelcted
    sql_query = """select * from l2_geo where weektype = "weekday" """
    l2_geo_1 = spark.sql(sql_query)
    l2_geo_1.createOrReplaceTempView('l2_geo_1')
    #print(" ========================= l2_geo_1 ======================")

    # where weekend seelcted
    sql_query = """select * from l2_geo where weektype = "weekend" """
    #l2_geo_2.createOrReplaceTempView('l2_geo_2')
    l2_geo_2 = spark.sql(sql_query)
    l2_geo_2.createOrReplaceTempView('l2_geo_2')
    #print(" ========================= l2_geo_2 ======================")

    # Join weekday & weeekend & added Row_number desc
    sql_query = """ select 
    a.imsi
    ,a.start_of_week
    ,a.location_id
    ,sum(a.duration_sumtime) as duration_sum
    ,ROW_NUMBER() OVER(PARTITION BY a.imsi,a.start_of_week ORDER BY sum(a.duration_sumtime) desc) as ROW
    from l2_geo_1 as a
    left join l2_geo_2 as b
    on a.imsi = b.imsi and
    a.location_id = b.location_id
    group by 1,2,3
	order by 1,2,3,4,5 desc
	"""
    l2 = spark.sql(sql_query)
    #l2.createOrReplaceTempView('l2')
    # print(" ========================= l2 ======================")

    print('DEBUG : ------------------------------------------------> l2_same_favourite_location_weekend_weekday_weekly')

    return l2


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
        small_df = data_frame.filter(f.col(partition_col).isin(*[curr_item]))
        small_df = node_from_config(small_df, sql)
        # small_df = add_start_of_week_and_month(small_df, "time_in")
        # small_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
        # output_df = ss.sql(sql)
        CNTX.catalog.save(output_df_catalog, small_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col(partition_col).isin(*[first_item]))
    return_df = node_from_config(return_df, sql)
    # return_df = add_start_of_week_and_month(return_df, "time_in")
    # return_df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
    # return_df = ss.sql(sql)
    return return_df