import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config, l4_rolling_window
from customer360.utilities.re_usable_functions import l3_massive_processing, l1_massive_processing
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics
from pyspark.sql import Window
from customer360.utilities.spark_util import get_spark_session


def l4_geo_top_visit_exclude_homework(sum_duration, homework):
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
    print('test')
    df.show()
    return df


def int_l4_geo_home_work_location_id(geo_cust_cell_visit_time, home_config, work_config, list_imsi_config):
    # Filter 2,3,4,5
    # geo_cust_cell_visit_time = geo_cust_cell_visit_time.filter('partition_date >= 20200201 and partition_date <= 20200531')

    # Add 2 columns: event_partition_date, start_of_month
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("event_partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col("event_partition_date"))))

    geo_cust_cell_visit_time_week_type = geo_cust_cell_visit_time.withColumn('week_type', F.when(F.dayofweek(F.col('event_partition_date')) == 1) & (F.dayofweek(F.col('event_partition_date')) == 7), 'weekend').otherwise('weekday').cast(StringType())

    # massive for list imsi for each month
    list_imsi = l3_massive_processing(geo_cust_cell_visit_time, list_imsi_config)

    # filter time of home
    home_monthly = l1_massive_processing(geo_cust_cell_visit_time_week_type, home_config)

    # filter time of work
    work_monthly = l1_massive_processing(geo_cust_cell_visit_time, work_config)

    return [home_monthly, work_monthly, list_imsi]

def l4_geo_home_work_location_id(home_monthly, work_monthly, list_imsi, sql):

    w_home = Window().partitionBy('imsi', 'location_id', 'week_type').orderBy(
        F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    # home_last_3m = geo_cust_time_of_home.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn("duration_3m", F.sum("duration").over(w_home)).withColumn("days_3m", F.sum('days').over(w_home))
    home_last_3m = home_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn("duration_3m", F.sum("duration").over(w_home)).withColumn("days_3m", F.sum('days').over(w_home))
    home_last_3m = home_last_3m.dropDuplicates(['imsi', 'start_of_month', 'location_id', 'duration_3m', 'days_3m']).select('imsi', 'start_of_month', 'week_type', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m')

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

    w_work = Window().partitionBy('imsi', 'location_id').orderBy(
        F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    work_last_3m = work_monthly.withColumn("Month", F.to_timestamp(
        "start_of_month", "yyyy-MM-dd")).withColumn("duration_3m", F.sum("duration").over(w_work)).withColumn("days_3m", F.sum('days').over(w_work))
    work_last_3m = work_last_3m.dropDuplicates(['imsi', 'start_of_month', 'location_id', 'duration_3m', 'days_3m'])\
        .select('imsi', 'start_of_month', 'location_id', 'latitude', 'longitude', 'duration_3m', 'days_3m')

    w_work_num_row = Window().partitionBy('imsi', 'location_id', 'start_of_month').orderBy(F.col('duration_3m').desc(), F.col('days_3m').desc())
    work_last_3m = work_last_3m.withColumn('row_num', F.row_number().over(w_work_num_row))
    work_last_3m = work_last_3m.where('row_num = 1').drop('row_num')

    work_last_3m = list_imsi.join(work_last_3m, ['imsi', 'start_of_month'], 'left').select(work_last_3m.imsi, work_last_3m.start_of_month, 'location_id', 'latitude', 'longitude')

    home_work = work_last_3m.join(home_last_3m_weekday, ['imsi', 'start_of_month'], 'left').select(work_last_3m.start_of_month, work_last_3m.imsi,
                                                 'home_weekday_location_id', 'home_weekday_latitude',
                                                 'home_weekday_longitude', 'location_id', 'latitude',
                                                 'longitude')

    home_work_final = home_work.join(home_last_3m_weekend, ['imsi', 'start_of_month'], 'left').select(home_work.imsi, home_work.start_of_month,
                                              'home_weekday_location_id',
                                              'home_weekday_latitude', 'home_weekday_longitude',
                                              'home_weekend_location_id',
                                              'home_weekend_latitude', 'home_weekend_longitude',
                                              (F.col('location_id').alias('work_location_id')),
                                              (F.col('latitude').alias('work_latitude')),
                                              (F.col('longitude').alias('work_longitude')))

    df = node_from_config(home_work_final, sql)
    return df


def l4_geo_home_weekday_city_citizens(home_work_location_id, master, sql):
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

    # Check DataFrame from SQL query statement
    print("Start for check the result from sql query statement JOIN")

    home_weekday_window = Window().partitionBy(F.col('start_of_month'), F.col('region_name'),
                                               F.col('province_name'), F.col('district_name'),
                                               F.col('sub_district_name'))

    home_location_id_master = home_location_id_master.withColumn('citizens',
                                                                 F.approx_count_distinct(F.col("imsi")).over(
                                                                     home_weekday_window)) \
        .dropDuplicates(
        ['start_of_month', 'region_name', 'province_name', 'district_name', 'sub_district_name', 'citizens']) \
        .select('start_of_month', 'region_name', 'province_name', 'district_name', 'sub_district_name', 'citizens')

    # Check DataFrame from SQL query statement
    print("Start for check the result from sql query statement WINDOW")

    df_01 = node_from_config(home_location_id_master, sql)
    return df_01


def l4_geo_population_aroud_home(geo_home_work_loation_id, cell_masterplan, sql):
    geo_home_work_loation_id.cache()
    cell_masterplan.cache()
    return None


##==============================Update 2020-06-12 by Thatt529==========================================##

###Traffic_fav_location###
def l4_Share_traffic(df):
    df.createOrReplaceTempView('GEO_TEMP_00')
    spark = get_spark_session()
    sql_query = """
    SELECT *
    ,(sum_Home_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week
    ,(sum_Work_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Work_traffic_KB_weekly_last_week
    ,(sum_Top1_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Top1_traffic_KB_weekly_last_week
    ,(sum_Top2_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Top2_traffic_KB_weekly_last_week
    ,(sum_Home_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_Home_weekly_last_two_week
    ,(sum_Work_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_Work_weekly_last_two_week
    ,(sum_Top1_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_Top1_weekly_last_two_week
    ,(sum_Top2_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_Top2_weekly_last_two_week
    ,(sum_Home_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_Home_weekly_last_four_week
    ,(sum_Work_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_Work_weekly_last_four_week
    ,(sum_Top1_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_Top1_weekly_last_four_week
    ,(sum_Top2_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_Top2_weekly_last_four_week
    ,(sum_Home_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_Home_weekly_last_twelve_week
    ,(sum_Work_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_Work_weekly_last_twelve_week
    ,(sum_Top1_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_Top1_weekly_last_twelve_week
    ,(sum_Top2_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_Top2_weekly_last_twelve_week
    FROM GEO_TEMP_00
    """
    df_sum = spark.sql(sql_query)
    return df_sum


###feature_AIS_store###
def l4_geo_last_AIS_store_visit(raw, sql):
    max_date = raw.selectExpr('max(partition_month)').collect()[0][0]
    raw.cache()
    raw = raw.where('partition_month=' + str(max_date))
    raw.createOrReplaceTempView('GEO_AIS_VISITED_SHOP')
    # Get spark session
    spark = get_spark_session()
    df = spark.sql("""
            SELECT 
                imsi,
                store_location_id as location_id,
                store_name as landmark_name_th,
                store_category as landmark_sub_name_en,
                MAX(last_visit) as last_visited,
                partition_month
            FROM GEO_AIS_VISITED_SHOP
            GROUP BY 1,2,3,4,6
         """)

    df.cache()
    print("Start for check result from sql query statement")
    # df.count()
    # df.show()

    out = node_from_config(df, sql)
    return out


def l4_geo_most_AIS_store_visit(raw, sql):
    max_date = raw.selectExpr('max(partition_month)').collect()[0][0]
    raw.cache()
    raw = raw.where('partition_month=' + str(max_date))
    raw.createOrReplaceTempView('GEO_AIS_VISITED_SHOP')
    # Get spark session
    spark = get_spark_session()
    df = spark.sql("""
            SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,most_visited,partition_month
            FROM (SELECT imsi,
                    store_location_id as location_id,
                    store_name as landmark_name_th,
                    store_category as landmark_sub_name_en,
                    COUNT(last_visit) as most_visited,
                    row_number() over(partition by store_location_id order by COUNT(last_visit)) as row_number,
                    store_latitude as landmark_latitude,
                    store_longitude as landmark_longitude,
                    partition_month
                FROM GEO_AIS_VISITED_SHOP
                GROUP BY 1,2,3,4,7,8,9
                ) A
                where A.row_number = 1
         """)
    df.cache()
    print("Start for check result from sql query statement")
    # df.count()
    # df.show()

    out = node_from_config(df, sql)
    return out


def l4_geo_store_close_to_home(home_work, locations, sql):

    month_id = home_work.selectExpr('max(start_of_month)').collect()[0][0]
    home_work = home_work.where(F.col('start_of_month') == str(month_id))
    home_work.createOrReplaceTempView('home_work_location')

    # print("DEBUG--------------------------(1)")
    spark = get_spark_session()
    locations.createOrReplaceTempView('mst_lm_poi_shape')
    df = spark.sql("""
            select A.*,
                B.landmark_name_th,
                B.landmark_latitude,
                B.landmark_longitude,
                B.geo_shape_id
            from home_work_location A cross 
            join mst_lm_poi_shape B
            where B.landmark_cat_name_en = 'AIS'
        """)
    df.createOrReplaceTempView('home_work_ais_store')
    # print("DEBUG--------------------------(2)")

    home_weekday = spark.sql("""
            select imsi,
                home_weekday_location_id,
                MIN(CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKDAY_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKDAY_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKDAY_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_weekday_home,
                first(landmark_name_th) as branch,
                first(geo_shape_id) as branch_location_id
            from home_work_ais_store
            where CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKDAY_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKDAY_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKDAY_LONGITUDE)))*6371) AS DECIMAL(13,2)) <= 100
            group by 1,2
        """)

    home_weekend = spark.sql("""
            select imsi,home_weekend_location_id,MIN(CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKEND_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKEND_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKEND_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_weekend_home, first(landmark_name_th) as branch, first(geo_shape_id) as branch_location_id
            from home_work_ais_store
            where CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKEND_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKEND_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKEND_LONGITUDE)))*6371) AS DECIMAL(13,2)) <= 100
            group by 1,2
        """)
    home_weekday.createOrReplaceTempView('home_weekday')
    home_weekend.createOrReplaceTempView('home_weekend')
    # print("DEBUG--------------------------(3)")

    df2 = spark.sql("""
            select a.imsi,
                a.home_weekday_location_id,
                a.range_from_weekday_home,
                a.branch as wd_location,
                a.branch_location_id as wd_location_id,
                b.home_weekend_location_id,
                b.range_from_weekend_home,
                b.branch as we_location,
                b.branch_location_id as we_location_id
            from home_weekday a left join home_weekend b
            on a.imsi = b.imsi
        """)
    df2.cache()
    out = node_from_config(df2, sql)
    return out


def l4_geo_store_close_to_work(home_work, locations, sql):
    # home_work.cache()
    month_id = home_work.selectExpr('max(start_of_month)').collect()[0][0]
    home_work = home_work.where(F.col('start_of_month') == str(month_id))
    home_work.createOrReplaceTempView('home_work_location')
    spark = get_spark_session()
    # locations = spark.read.parquet("dbfs:/mnt/customer360-blob-data/C360/GEO/geo_mst_lm_poi_shape")
    locations.createOrReplaceTempView('MST_LM_POI_SHAPE')
    df = spark.sql("""
                select A.*,B.landmark_name_th,B.landmark_latitude,B.landmark_longitude,B.geo_shape_id
                from home_work_location A cross join mst_lm_poi_shape B
                where B.landmark_cat_name_en = 'AIS'
            """)
    df.createOrReplaceTempView('home_work_ais_store')

    df2 = spark.sql("""
            select imsi,
                work_location_id,
                MIN(CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-WORK_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-WORK_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - WORK_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_work,
                first(landmark_name_th) as branch,
                first(geo_shape_id) as branch_location_id
            from home_work_ais_store
            where CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-WORK_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-WORK_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - WORK_LONGITUDE)))*6371) AS DECIMAL(13,2)) <= 100
            group by 1,2
        """)
    df2.cache()
    out = node_from_config(df2, sql)
    return out


##==============================Update 2020-06-17 by Thatt529==========================================##

###Distance between nearest store and most visited store###
def l4_geo_range_from_most_visited(most,close,sql):
    most.cache()
    month_id = most.selectExpr('max(partition_month)').collect()[0][0]
    most = most.where('partition_month=' + str(month_id))
    most.createOrReplaceTempView('GEO_AIS_VISITED_SHOP')
    month_id = close.selectExpr('max(start_of_month)').collect()[0][0]
    close = close.where('start_of_month=' + str(month_id))
    close.createOrReplaceTempView('closest_store')
    spark = get_spark_session()
    most_visit = spark.sql("""
                SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,most_visited,landmark_latitude,landmark_longitude,partition_month
                FROM (SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,row_number() over(partition by LOCATION_ID order by COUNT(TIME_IN)) as row_number,COUNT(TIME_IN) as most_visited,landmark_latitude,landmark_longitude,partition_month
                FROM GEO_AIS_VISITED_SHOP
                GROUP BY 1,2,3,4,7,8,9
                ) A
                where A.row_number = 1;
             """)

    most_visit.cache()
    most_visit.createOrReplaceTempView('most_visit_store')
    locations = spark.read.parquet("dbfs:/mnt/customer360-blob-data/C360/GEO/geo_mst_lm_poi_shape")
    locations.createOrReplaceTempView('POI_SHAPE')
    closest = spark.sql("""
                select A.imsi,A.home_weekday_location_id,A.weekday_branch_name,A.weekday_branch_location_id,B.landmark_latitude as store_latitude,B.landmark_longitude as store_longitude
                from closest_store A, poi_shape B
                where A.weekday_branch_location_id = B.geo_shape_id
            """)
    closest.cache()
    closest.createOrReplaceTempView('closest_store_with_co')

def l4_geo_work_area_center_average(visti_hr, home_work, sql):
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




# Form ==============

# 48 The most frequently used Location for data sessions on weekdays (Mon to Fri)
def l4_the_most_frequently_location_weekdays(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query = """ 
    select * from 
    (
    select 
    mobile_no
    ,start_of_week
    ,latitude 
    ,longitude
    ,location_id as v_most_data_used_cell_weekday_0
    ,sum(all_no_of_call) as sum_all_no_of_call
    ,SUM(all_usage_data_kb) as  most_data_used_kb_weekday_0
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as ROW
    from (
    select * from l1_df_the_favourite_location_daily 
    where weektype = "weekday" 
    )
    group by mobile_no,start_of_week,location_id,latitude,longitude
    order by mobile_no,start_of_week,row asc
    )
    where row <= 5
    """
    l4_48 = spark.sql(sql_query)

    return l4_48

#49 The most frequently used Location for data sessions on weekdays (Mon to Fri) is 4G flag
def l4_the_most_frequently_location_weekdays_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')
    sql_query =""" 
    select 
    * from (
    select 
    mobile_no
    ,start_of_week
    ,location_id
    ,SUM(all_usage_data_kb) as  most_data_used_kb_weekday_4g
    ,sum(all_no_of_call) as sum_all_no_of_call_weekday_4g
    ,sum(vol_4g) as vol_4g_weekday
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as ROW
    from
    (
    select * from l1_df_the_favourite_location_daily 
    where weektype = "weekday" 
    and lower(gprs_type) like "4g%"
    )
    group by mobile_no,start_of_week,location_id
    order by mobile_no,start_of_week,row asc
    )
    where row <= 5
    """

    l4_49 = spark.sql(sql_query)

    return l4_49


#50 The most frequently used Location for data sessions on weekends
def l4_the_most_frequently_location_weekends(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query = """ 
    select * from 
    (
    select 
    mobile_no
    ,start_of_week
    ,latitude
    ,longitude
    ,location_id as v_most_data_used_cell_weekend
    ,sum(all_no_of_call) as sum_all_no_of_call_weekend
    ,SUM(all_usage_data_kb) as   most_data_used_kb_weekend
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as the_most
    from (
    select * from l1_df_the_favourite_location_daily 
    where weektype = "weekend" 
    )
    group by mobile_no,start_of_week,v_most_data_used_cell_weekend,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most <= 5
    """
    l4_50 = spark.sql(sql_query)

    return l4_50

#51 The most frequently used Location for data sessions on weekends is 4G flag
def l4_the_most_frequently_location_weekends_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query = """ 
    select 
    * from (
    select 
    mobile_no
    ,start_of_week
    ,location_id
    ,sum(all_no_of_call) as sum_all_no_of_call_weekday_4g
    ,SUM(all_usage_data_kb)   as  most_data_used_kb_weekday_4g
    ,sum(vol_4g) as vol_4g_weekday
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as ROW
    from
    (
    select * from l1_df_the_favourite_location_daily 
    where weektype = "weekend" and lower(gprs_type) like "4g%"
    )
    group by mobile_no,start_of_week,location_id
    order by mobile_no,start_of_week,row asc
    )
    where row <= 2
    """
    l4_51 = spark.sql(sql_query)

    return l4_51


#52 The most frequently used Location for data sessions
def l4_the_most_frequently_location(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    * from (
    select 
    mobile_no
    ,start_of_week
    ,location_id
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call
    ,SUM(all_usage_data_kb) as  most_data_used_kb
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as the_most
    from l1_df_the_favourite_location_daily 
    group by mobile_no,start_of_week,location_id,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most <= 5
    """
    l4_52 = spark.sql(sql_query)

    return l4_52


#53 The most frequently used Location for data sessions is 4G flag
def l4_the_most_frequently_location_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    * from (
    select 
    mobile_no
    ,start_of_week
    ,location_id
    ,latitude
    ,longitude
    ,gprs_type
    ,sum(all_no_of_call) as sum_all_no_of_call_4g
    ,sum(vol_4g) as vol_4g_weekday
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY sum(vol_4g)  desc) as the_most
    from l1_df_the_favourite_location_daily
    group by mobile_no,start_of_week,location_id,gprs_type,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most <= 2
    """

    l4_53 = spark.sql(sql_query)

    return l4_53


#54 The second most frequently used cell for data sessions on weekdays (Mon to Fri)
def l4_the_second_frequently_location_weekdays(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    * from (
    select 
    mobile_no
    ,start_of_week
    ,location_id
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_weekday
    ,SUM(all_usage_data_kb) as most_data_used_kb_weekday
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(all_usage_data_kb)  desc) as the_most
    from l1_df_the_favourite_location_daily
    group by mobile_no,start_of_week,location_id,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """

    l4_54 = spark.sql(sql_query)

    return l4_54

#55 The second most frequently used cell for data sessions on weekdays (Mon to Fri) is 4G flag
def l4_the_second_frequently_location_weekdays_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    mobile_no
    ,start_of_week
    ,v_most_data_used_cell_weekday_4G_1
    ,latitude,longitude
    ,v_most_data_used_kb_weekday_4G_1
    ,sum_all_no_of_call_weekday_4g
    from
    (
    select 
    mobile_no
    ,start_of_week
    ,location_id as v_most_data_used_cell_weekday_4G_1
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_weekday_4g
    ,SUM(vol_4g) as v_most_data_used_kb_weekday_4G_1
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(vol_4g) desc) as the_most
    from l1_df_the_favourite_location_daily
    where lower(gprs_type) like '%4g%'
    group by mobile_no,start_of_week,v_most_data_used_cell_weekday_4G_1,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """

    l4_55 = spark.sql(sql_query)

    return l4_55


#56 The second most frequently used cell for data sessions on weekends
def l4_the_second_frequently_location_weekends(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    mobile_no
    ,start_of_week
    ,v_most_data_used_cell_weekend
    ,latitude,longitude
    ,v_most_data_used_kb_weekend_1
    ,sum_all_no_of_call_weekend
    from 
    (
    select 
    mobile_no
    ,start_of_week
    ,location_id as v_most_data_used_cell_weekend
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_weekend
    ,SUM(all_usage_data_kb)   as  v_most_data_used_kb_weekend_1
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as the_most
    from (
    select * from l1_df_the_favourite_location_daily
    where weektype = "weekend" 
    )
    group by mobile_no,start_of_week,v_most_data_used_cell_weekend,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most = 2

    """

    l4_56 = spark.sql(sql_query)

    return l4_56


#57 The second most frequently used cell for data sessions on weekends is 4G flag
def l4_the_second_frequently_location_weekends_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    mobile_no
    ,start_of_week
    ,v_most_data_used_cell_weekend_4G_1
    ,latitude,longitude
    ,v_most_data_used_kb_weekend_4G_1
    ,sum_all_no_of_call_weekend_4g
    from
    (
    select 
    mobile_no
    ,start_of_week
    ,location_id as v_most_data_used_cell_weekend_4G_1
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_weekend_4g
    ,SUM(vol_4g) as v_most_data_used_kb_weekend_4G_1
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(vol_4g) desc) as the_most
    from l1_df_the_favourite_location_daily

    where lower(gprs_type) like '%4g%'
    and weektype = "weekend"
    group by mobile_no,start_of_week,v_most_data_used_cell_weekend_4G_1,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """

    l4_57 = spark.sql(sql_query)

    return l4_57


#58 The second most frequently used cell for data sessions
def l4_the_second_frequently_location(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    mobile_no
    ,start_of_week
    ,v_most_data_used_cell_1
    ,latitude,longitude
    ,v_most_data_used_kb_1
    ,sum_all_no_of_call

    from
    (
    select 
    mobile_no
    ,start_of_week
    ,location_id as v_most_data_used_cell_1
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call
    ,SUM(All_usage_data_kb) as v_most_data_used_kb_1
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(All_usage_data_kb) desc) as the_most
    from l1_df_the_favourite_location_daily
    group by mobile_no,start_of_week,v_most_data_used_cell_1,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """

    l4_58= spark.sql(sql_query)

    return l4_58



#59 The second most frequently used cell for data sessions is 4G flag
def l4_the_second_frequently_location_4g(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')

    sql_query =""" 
    select 
    mobile_no
    ,start_of_week
    ,v_most_data_used_cell_4G_1
    ,latitude
    ,longitude
    ,v_most_data_used_kb_4G_1
    ,sum_all_no_of_call_4g

    from
    (
    select 
    mobile_no
    ,start_of_week
    ,location_id as v_most_data_used_cell_4G_1
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_4g
    ,SUM(vol_4g) as v_most_data_used_kb_4G_1
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(vol_4g) desc) as the_most
    from l1_df_the_favourite_location_daily
    where lower(gprs_type) like '%4g%'

    group by mobile_no,start_of_week,v_most_data_used_cell_4G_1,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """
    l4_59 = spark.sql(sql_query)

    return l4_59


# =========================== Number most frequent weekday============================================
def l4_geo_number_most_frequent_weekday(geo_l1_favourite_location_date,geo_l4_most_frequency, sql):
        geo_l1_favourite_location_date.createOrReplaceTempView('geo_l1_favourite_location')
        geo_l4_most_frequency.createOrReplaceTempView('geo_l4_most_frequency')
        spark = get_spark_session()
        geo_location_data_used = spark.sql("""
        select
        b.mobile_no
        , b.weektype
        , a.start_of_week
        ,case when a.latitude is null and a.longitude is null then 0 
          else cast((acos(cos(radians(90-b.latitude))*cos(radians(90-a.latitude))+sin(radians(90-b.latitude))*sin(radians(90-a.latitude))*cos(radians(b.longitude - a.longitude)))*6371) as decimal(13,2)) 
          end as distance_km
        , sum(b.all_no_of_call) as NUMBER_OF_DATA_SESSION
        FROM geo_l4_most_frequency a
        join geo_l1_favourite_location b
        where b.WEEKTYPE = "weekday"
        AND a.mobile_no = b.mobile_no
        group by 1,2,3,4
        """)
        geo_location_data_cal = geo_location_data_used.groupBy("mobile_no", "start_of_week").agg(
                                                                    F.avg("distance_km").alias("avg_distance_km"),
                                                                    F.max("distance_km").alias("max_distance_km"),
                                                                    F.min("distance_km").alias("min_distance_km"),
                                                                    F.sum("distance_km").alias("sum_distance_km"))
        geo_location_data_cal.cache()
        out = node_from_config(geo_location_data_cal, sql)
        return out
# =========================== Number most frequent weekend ============================================
def l4_geo_number_most_frequent_weekend(geo_l1_favourite_location_date, geo_l4_most_frequency, sql):
    geo_l1_favourite_location_date.createOrReplaceTempView('geo_l1_favourite_location')
    geo_l4_most_frequency.createOrReplaceTempView('geo_l4_most_frequency')
    spark = get_spark_session()
    geo_location_data_used = spark.sql("""
        select
        b.mobile_no
        , b.weektype
        , a.start_of_week
        ,case when a.latitude is null and a.longitude is null then 0 
          else cast((acos(cos(radians(90-b.latitude))*cos(radians(90-a.latitude))+sin(radians(90-b.latitude))*sin(radians(90-a.latitude))*cos(radians(b.longitude - a.longitude)))*6371) as decimal(13,2)) 
          end as distance_km
        , sum(b.all_no_of_call) as NUMBER_OF_DATA_SESSION
        FROM geo_l4_most_frequency a
        join geo_l1_favourite_location b
        where b.WEEKTYPE = "weekend"
        AND a.mobile_no = b.mobile_no
        group by 1,2,3,4
        """)
    geo_location_data_cal = geo_location_data_used.groupBy("mobile_no", "start_of_week").agg(
        F.avg("distance_km").alias("avg_distance_km"),
        F.max("distance_km").alias("max_distance_km"),
        F.min("distance_km").alias("min_distance_km"),
        F.sum("distance_km").alias("sum_distance_km"))
    geo_location_data_cal.cache()
    out = node_from_config(geo_location_data_cal, sql)
    return out

# =========================== Number most frequent top five ============================================
def l4_geo_number_most_frequent_top_five_weekday(l1_favourite_location, l4_most_frequency, sql):
    l1_favourite_location.createOrReplaceTempView('geo_location_data')
    l4_most_frequency.createOrReplaceTempView('l4_most_frequency')

    l0_df1 = l1_favourite_location.withColumn("event_partition_date",
                                              F.to_date(l1_favourite_location.date_id.cast(DateType()),
                                                        "yyyy-MM-dd")).drop("date_id")
    spark = get_spark_session()
    l0_df1.createOrReplaceTempView('geo_location_data_1')
    geo_location_data_weekday = spark.sql("""
        select 
            b.event_partition_date,
            b.mobile_no, 
            b.weektype,
            a.sum_all_no_of_call,
            a.ROW
        from l4_most_frequency a
        left join geo_location_data_1 b
        on a.mobile_no = b.mobile_no
        where a.ROW = 1
        AND b.weektype = 'weekday'
        group by 1,2,3,4,5
        """)

    # =================================== Number most frequent weekday ====================================================
    # geo_location_data_calcu_weekday = geo_location_data_weekday.groupBy("mobile_no", "event_partition_date").agg(
    #     F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekday"))

    geo_location_data_avg_weekday = geo_location_data_weekday.groupBy("mobile_no","event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekday"),
        F.avg("sum_all_no_of_call").alias("avg_all_no_of_call_weekday"),
        F.max("sum_all_no_of_call").alias("max_all_no_of_call_weekday"),
        F.min("sum_all_no_of_call").alias("min_all_no_of_call_weekday"),
        F.count("sum_all_no_of_call").alias("count_sum_all_no_of_call_weekday"))

    out2 = node_from_config(geo_location_data_avg_weekday, sql)

    return out2

def l4_geo_number_most_frequent_top_five_weekend(l1_favourite_location, l4_most_frequency, sql):
    l1_favourite_location.createOrReplaceTempView('geo_location_data')
    l4_most_frequency.createOrReplaceTempView('l4_most_frequency')

    l0_df1 = l1_favourite_location.withColumn("event_partition_date",
                                              F.to_date(l1_favourite_location.date_id.cast(DateType()),
                                                        "yyyy-MM-dd")).drop("date_id")
    spark = get_spark_session()
    l0_df1.createOrReplaceTempView('geo_location_data_1')
    geo_location_data_weekend = spark.sql("""
        select 
            b.event_partition_date,
            b.mobile_no,
            b.weektype ,
            a.sum_all_no_of_call,
            a.ROW
        from l4_most_frequency a
        left join geo_location_data_1 b
        on a.mobile_no = b.mobile_no
        where a.ROW = '1'
        AND b.weektype = 'weekend'
        group by 1,2,3,4,5
        """)

    # =================================== Number most frequent weekend ====================================================
    # geo_location_data_calcu_weekend = geo_location_data_weekend.groupBy("mobile_no", "event_partition_date").agg(
    #     F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekend"))

    geo_location_data_avg_weekend = geo_location_data_weekend.groupBy("mobile_no", "event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekend"),
        F.avg("sum_all_no_of_call").alias("avg_all_no_of_call_weekend"),
        F.max("sum_all_no_of_call").alias("max_all_no_of_call_weekend"),
        F.min("sum_all_no_of_call").alias("min_all_no_of_call_weekend"),
        F.count("sum_all_no_of_call"))

    out3 = node_from_config(geo_location_data_avg_weekend, sql)

    return out3

def l4_geo_number_most_frequent_top_five(l1_favourite_location, l4_most_frequency, sql):
    l1_favourite_location.createOrReplaceTempView('geo_location_data')
    l4_most_frequency.createOrReplaceTempView('l4_most_frequency')

    l0_df1 = l1_favourite_location.withColumn("event_partition_date",
                                                F.to_date(l1_favourite_location.date_id.cast(DateType()),
                                                          "yyyy-MM-dd")).drop("date_id")
    l0_df1.createOrReplaceTempView('geo_location_data_1')
    spark = get_spark_session()

    geo_location_data_all = spark.sql("""
    select 
        b.event_partition_date,
        b.mobile_no,
        b.weektype,
        a.sum_all_no_of_call,
        a.ROW
    from l4_most_frequency a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where a.ROW = '1'
    group by 1,2,3,4,5
    """)

    # =================================== Number most frequent All ====================================================
    # geo_location_data_calcu_all = geo_location_data_all.groupBy("mobile_no", "event_partition_date").agg(
    #     F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_all"))

    geo_location_data_avg_all = geo_location_data_all.groupBy("event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_all"),
        F.avg("sum_all_no_of_call").alias("avg_all_no_of_call_all"),
        F.max("sum_all_no_of_call").alias("max_all_no_of_call_all"),
        F.min("sum_all_no_of_call").alias("min_all_no_of_call_all"),
        F.count("sum_all_no_of_call").alias("count_sum_all_no_of_call_all"))

    out1 = node_from_config(geo_location_data_avg_all, sql)

    return out1


###Number of Unique Cells Used###
def l4_geo_number_unique_cell_used(l1_df_1, sql):
    # get result and split weektype
    l1_df_2 = l1_df_1.withColumn("event_partition_date",
                                 F.to_date(l1_df_1.partition_date.cast(StringType()), "yyyyMMdd")).drop("partition_date")

    spark = get_spark_session()
    l1_df_2.createOrReplaceTempView('usage_sum_data_location_daily')

    l1_df_4 = spark.sql("""select count(mobile_no) AS mobile_no , lac , ci
        ,case when
            dayofweek(event_partition_date) = 2
            or dayofweek(event_partition_date) = 3
            or dayofweek(event_partition_date) = 4
            or dayofweek(event_partition_date) = 5
            or dayofweek(event_partition_date) = 6
        then     "weekday"
        else    "weekend"
        end as weektype,
        event_partition_date
        from usage_sum_data_location_daily
        group by lac , ci, event_partition_date
    """)
    l1_df_4.createOrReplaceTempView('usage_sum_data_count')

    # calculate weektype
    l4_df_1 = l1_df_4.groupBy("event_partition_date", "weektype") \
        .agg(F.sum("mobile_no").alias("durations"))

    l4_df_2 = l4_df_1.groupBy("event_partition_date", "weektype").agg(F.avg("durations").alias("avg_duration"),
                                                          F.max("durations").alias("max_duration"),
                                                          F.min("durations").alias("min_duration"),
                                                          F.sum("durations").alias("sum_duration"))

    l4_df_2.cache()

    out = node_from_config(l4_df_2, sql)
    return out

