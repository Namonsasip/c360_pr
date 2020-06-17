import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config, l4_rolling_window
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


def l4_geo_home_work_location_id(geo_cust_cell_visit_time, sql):
    # Filter 3 4 5
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.filter('partition_date >= 20200301')

    # Add 2 columns: event_partition_date, start_of_month
    geo_cust_cell_visit_time.cache()
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("event_partition_date",
                                                                   F.to_date(F.col("partition_date").cast(StringType()),
                                                                             'yyyyMMdd'))
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("start_of_month", F.to_date(
        F.date_trunc('month', F.col("event_partition_date"))))

    # Get spark session
    spark = get_spark_session()

    # Fix Time on Home Location
    geo_cust_cell_visit_time.createOrReplaceTempView('geo_cust_cell_visit_time')
    geo_cust_cell_visit_time_home = spark.sql("""
        select imsi, time_in, time_out, location_id, latitude, longitude,
            case
                when (hour_in < 18 and hour_out > 18) then ( to_unix_timestamp(time_out) - (to_unix_timestamp(event_partition_date) + 64800) )
                when (hour_in < 6 and hour_out > 6) then ( (to_unix_timestamp(event_partition_date) + 21600) - to_unix_timestamp(time_out) )
                else duration 
            end as duration,
            event_partition_date, start_of_month
        from geo_cust_cell_visit_time
        where duration <> 0
        and ((hour_in >= 18)
        or (hour_in < 18 and hour_out > 18)
        or (hour_in < 6 and hour_out > 6)
        or (hour_out <=6))
    """)
    geo_cust_cell_visit_time_home.cache()

    # Aggregate table daily: geo_cust_cell_visit_time_home
    geo_cust_cell_visit_time_home.createOrReplaceTempView('geo_cust_cell_visit_time_home')
    df_home_daily = spark.sql("""
        select 
            a.imsi
            ,a.location_id, a.latitude, a.longitude
            ,a.event_partition_date, a.start_of_month
            ,sum(a.duration) as duration
        from (
            geo_cust_cell_visit_time_home
        ) a
        group by a.imsi, a.location_id, a.latitude, a.longitude, a.event_partition_date, a.start_of_month
    """)
    df_home_daily.cache()
    spark.catalog.dropTempView("geo_cust_cell_visit_time_home")

    # Check DataFrame from SQL query statement
    print("Start for check the result from sql query statement of HOME")

    geo_cust_cell_visit_time_work = spark.sql("""
            select imsi, time_in, time_out, location_id, latitude, longitude,
                case
                    when ((hour_in >= 8 and hour_in < 18) and hour_out > 18) then ( (to_unix_timestamp(event_partition_date) + 64800) - (to_unix_timestamp(time_in)) )
                    else duration
                end as duration
               ,event_partition_date, start_of_month
            from geo_cust_cell_visit_time
            where duration <> 0
            and
            (((hour_in >= 8 and hour_in < 18) and hour_out <= 18)
             or ((hour_in >= 8 and hour_in < 18) and hour_out > 18))
        """)
    geo_cust_cell_visit_time_work.cache()
    spark.catalog.dropTempView("geo_cust_cell_visit_time")

    # Aggregate table daily: geo_cust_cell_visit_time_work
    geo_cust_cell_visit_time_work.createOrReplaceTempView('geo_cust_cell_visit_time_work')
    df_work_daily = spark.sql("""
        select a.imsi
            ,a.location_id, a.latitude, a.longitude
            ,a.event_partition_date, a.start_of_month
            ,sum(a.duration) as duration
        from (
        geo_cust_cell_visit_time_work
        ) a
        group by a.imsi, a.location_id, a.latitude, a.longitude, a.event_partition_date, a.start_of_month
    """)
    df_work_daily.cache()
    df_work_daily.createOrReplaceTempView("df_work_daily")
    spark.catalog.dropTempView("geo_cust_cell_visit_time_work")

    # Check DataFrame from SQL query statement
    print("Start for check result from sql query statement of WORK")

    # Add column Weekend and Weekday
    home_duration_dayily_with_weektype = df_home_daily.withColumn("week_type", F.when(
        (F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
                                                                  .otherwise('weekday').cast(StringType())
                                                                  )
    home_duration_dayily_with_weektype.cache()
    home_duration_dayily_with_weektype.createOrReplaceTempView('home_duration_dayily_with_weektype')

    df_home_combine_week_monthly = spark.sql("""
        select imsi,
            location_id, latitude, longitude,
            start_of_month, week_type,
            sum(duration) as duration
        from home_duration_dayily_with_weektype
        group by imsi, location_id, latitude, longitude, start_of_month, week_type
    """)
    df_home_combine_week_monthly.cache()
    spark.catalog.dropTempView("home_duration_dayily_with_weektype")

    df_work_monthly = spark.sql("""
        select
            imsi,
            location_id, latitude, longitude,
            start_of_month
            ,sum(duration) as duration
        from df_work_daily
        group by imsi, location_id, latitude, longitude, start_of_month
    """)
    df_work_monthly.cache()
    spark.catalog.dropTempView("df_work_daily")

    w_home = Window().partitionBy(F.col('imsi'), F.col('location_id'), F.col('week_type')).orderBy(
        F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    df_home_combine_week_monthly_sum_last_3_day = df_home_combine_week_monthly.withColumn("Month", F.to_timestamp(
        "start_of_month", "yyyy-MM-dd")).withColumn("Sum", F.sum("duration").over(w_home))

    df_home_combine_week_monthly_sum_last_3_day.createOrReplaceTempView('df_home_combine_week_monthly_sum_last_3_day')
    df_home_location = spark.sql("""
        select
            a.imsi,
            a.start_of_month,
            a.week_type,
            b.location_id,
            b.latitude,
            b.longitude
        from (
          select imsi,
            start_of_month,
            week_type,
            max(Sum) as Sum
          from df_home_combine_week_monthly_sum_last_3_day
          group by 1,2,3
        ) a
        left join df_home_combine_week_monthly_sum_last_3_day b
        on a.imsi = b.imsi and a.start_of_month = b.start_of_month and a.Sum = b.Sum and a.week_type = b.week_type
        group by 1,2,3,4,5,6
    """)
    df_home_location.cache()
    spark.catalog.dropTempView("df_home_combine_week_monthly_sum_last_3_day")

    # Check DataFrame from SQL query statement
    print("Start for check result from sql query statement of HOME")

    w_work = Window().partitionBy(F.col('imsi'), F.col('location_id')).orderBy(
        F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    df_home_combine_week_monthly_sum_last_3_day = df_work_monthly.withColumn("Month", F.to_timestamp("start_of_month",
                                                                                                     "yyyy-MM-dd")).withColumn(
        "Sum", F.sum("duration").over(w_work))

    df_home_combine_week_monthly_sum_last_3_day.createOrReplaceTempView('df_home_combine_week_monthly_sum_last_3_day')
    df_work_location = spark.sql("""
            select
                a.imsi,
                a.start_of_month,
                b.location_id,
                b.latitude,
                b.longitude
            from (
              select imsi,
                start_of_month,
                max(Sum) as Sum
              from df_home_combine_week_monthly_sum_last_3_day
              group by 1,2
            ) a
            left join df_home_combine_week_monthly_sum_last_3_day b
            on a.imsi = b.imsi and a.start_of_month = b.start_of_month and a.Sum = b.Sum
            group by 1,2,3,4,5
        """)
    df_work_location.cache()
    spark.catalog.dropTempView("df_work_monthly")

    # Check DataFrame from SQL query statement
    print("Start for check result from sql query statement of WORK")

    df_home_location.createOrReplaceTempView('df_home_location')
    df_work_location.createOrReplaceTempView('df_work_location')

    df_combine_home_work = spark.sql("""
        select
            a.imsi,
            a.start_of_month,
            b.location_id as home_weekday_location_id,
            b.latitude as home_weekday_latitude,
            b.longitude as home_weekday_longitude,
            c.location_id as home_weekend_location_id,
            c.latitude as home_weekend_latitude,
            c.longitude as home_weekend_longitude,
            a.location_id as work_location_id,
            a.latitude as work_latitude,
            a.longitude as work_longitude
        from df_work_location a
        left join df_home_location b
        on b.week_type = 'weekday' and a.imsi = b.imsi and a.start_of_month = b.start_of_month
        left join df_home_location c
        on c.week_type = 'weekend' and a.imsi = c.imsi and a.start_of_month = c.start_of_month
        group by 1,2,3,4,5,6,7,8,9,10,11
    """)
    df_combine_home_work.cache()
    spark.catalog.dropTempView("df_home_location")
    spark.catalog.dropTempView("df_work_location")

    # Check DataFrame from SQL query statement
    print("Start for check the result from sql query statement FINAL")

    df = node_from_config(df_combine_home_work, sql)
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
def l4_Share_traffic(df, sql):
    df2 = l4_rolling_window(df, sql)
    df2.createOrReplaceTempView('GEO_TEMP_00')
    spark = get_spark_session()
    df_sum = spark.sql(""" SELECT *,
    (sum_Home_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week,
    (sum_Work_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week,
    (sum_Top1_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week,
    (sum_Top2_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week,
    (sum_Home_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_weekly_last_two_week,
    (sum_Work_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_weekly_last_two_week,
    (sum_Top1_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_weekly_last_two_week,
    (sum_Top2_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_traffic_KB_weekly_last_two_week) AS SUM_TRAFFIC_weekly_last_two_week,
    (sum_Home_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_weekly_last_four_week,
    (sum_Work_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_weekly_last_four_week,
    (sum_Top1_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_weekly_last_four_week,
    (sum_Top2_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_traffic_KB_weekly_last_four_week) AS SUM_TRAFFIC_weekly_last_four_week,
    (sum_Home_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_weekly_last_twelve_week,
    (sum_Work_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_weekly_last_twelve_week,
    (sum_Top1_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_weekly_last_twelve_week,
    (sum_Top2_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_traffic_KB_weekly_last_twelve_week) AS SUM_TRAFFIC_weekly_last_twelve_week,
    FROM GEO_TEMP_00 
    """)
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
            SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,MAX(TIME_IN) as last_visited,partition_month
            FROM GEO_AIS_VISITED_SHOP
            GROUP BY 1,2,3,4,6;
         """)

    df.cache()
    print("Start for check result from sql query statement")
    df.count()
    df.show()

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
            FROM (SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,row_number() over(partition by LOCATION_ID order by COUNT(TIME_IN)) as row_number,COUNT(TIME_IN) as most_visited,landmark_latitude,landmark_longitude,partition_month
                FROM GEO_AIS_VISITED_SHOP
                GROUP BY 1,2,3,4,7,8,9
                ) A
                where A.row_number = 1;
         """)
    df.cache()
    print("Start for check result from sql query statement")
    df.count()
    df.show()

    out = node_from_config(df, sql)
    return out


def l4_geo_store_close_to_home(home_work, sql):
    home_work.cache()
    month_id = home_work.selectExpr('max(start_of_month)').collect()[0][0]
    home_work = home_work.where('start_of_month=' + str(month_id))
    home_work.createOrReplaceTempView('home_work_location')
    spark = get_spark_session()
    locations = spark.read.parquet("dbfs:/mnt/customer360-blob-data/C360/GEO/geo_mst_lm_poi_shape")
    locations.createOrReplaceTempView('MST_LM_POI_SHAPE')
    df = spark.sql("""
            select A.*,B.landmark_name_th,B.landmark_latitude,B.landmark_longitude,B.geo_shape_id
            from home_work_sample A cross join mst_lm_poi_shape B
            where B.landmark_cat_name_en = 'AIS'
        """)
    df.createOrReplaceTempView('home_work_ais_store')

    home_weekday = spark.sql("""
            select imsi,home_weekday_location_id,MIN(CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKDAY_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKDAY_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKDAY_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_weekday_home, first(landmark_name_th) as branch, first(geo_shape_id) as branch_location_id
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

    df2 = spark.sql("""
            select a.imsi,a.home_weekday_location_id,a.range_from_weekday_home,a.branch as wd_location,a.branch_location_id as wd_location_id,b.home_weekend_location_id,b.range_from_weekend_home,b.branch as we_location,b.branch_location_id as we_location_id
            from home_weekday a left join home_weekend b
            on a.imsi = b.imsi
        """)
    df2.cache()
    out = node_from_config(df2, sql)
    return out


def l4_geo_store_close_to_work(home_work, sql):
    home_work.cache()
    month_id = home_work.selectExpr('max(start_of_month)').collect()[0][0]
    home_work = home_work.where('start_of_month=' + str(month_id))
    home_work.createOrReplaceTempView('home_work_location')
    spark = get_spark_session()
    locations = spark.read.parquet("dbfs:/mnt/customer360-blob-data/C360/GEO/geo_mst_lm_poi_shape")
    locations.createOrReplaceTempView('MST_LM_POI_SHAPE')
    df = spark.sql("""
                select A.*,B.landmark_name_th,B.landmark_latitude,B.landmark_longitude,B.geo_shape_id
                from home_work_sample A cross join mst_lm_poi_shape B
                where B.landmark_cat_name_en = 'AIS'
            """)
    df.createOrReplaceTempView('home_work_ais_store')

    df2 = spark.sql("""
            select imsi,work_location_id, MIN(CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-WORK_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-WORK_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - WORK_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_work, first(landmark_name_th) as branch, first(geo_shape_id) as branch_location_id
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
    visit_hr_drop = visit_hr_drop.where("hour > 5 and hour < 19").drop('partition_date')
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
        F.radians(90 - F.col('latitude'))) + F.sin( \
        F.radians(90 - F.col('avg_latitude'))) * F.sin(
        F.radians(90 - F.col('latitude'))) * F.cos(
        F.radians( \
            F.col('avg_longitude') - F.col(
                'longitude')))) * 6371).cast('double'))
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

    # range_diff = spark.sql("""
    #             select A.imsi,B.home_weekday_location_id,B.weekday_branch_name,B.weekday_branch_location_id,A.landmark_name_th,A.location_id,CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-STORE_LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-STORE_LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - STORE_LONGITUDE)))*6371) AS DECIMAL(13,2)) as range_diff,A.partition_month
    #             from most_visit_store A join closest_store_with_co B
    #             on A.imsi = B.imsi
    #         """)
    # range_diff.cache()
    # out = node_from_config(range_diff, sql)
    # return out

