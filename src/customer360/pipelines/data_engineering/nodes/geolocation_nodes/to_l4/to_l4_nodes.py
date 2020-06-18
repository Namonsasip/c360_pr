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
    select * from l1_feature47_2 
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
    ,location_id
    ,latitude
    ,longitude
    ,sum(all_no_of_call) as sum_all_no_of_call_weekday
    ,SUM(all_usage_data_kb) as most_data_used_kb_weekday
    ,ROW_NUMBER() OVER(partition by mobile_no,start_of_week ORDER BY SUM(all_usage_data_kb)  desc) as the_most
    from l1_feature47_2
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
    from l1_feature47_2
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
    from l1_feature47_2
    where lower(gprs_type) like '%4g%'

    group by mobile_no,start_of_week,v_most_data_used_cell_4G_1,latitude,longitude
    order by mobile_no,start_of_week,the_most asc
    )
    where the_most =2
    """
    l4_59 = spark.sql(sql_query)

    return l4_59


# =========================== Number most frequent ============================================
def l4_geo_number_most_frequent(l1_favourite_location, geo_l4_most_frequency, sql):
    l1_favourite_location.createOrReplaceTempView('geo_location_data')
    geo_l4_most_frequency.createOrReplaceTempView('geo_l4_most_frequency')
    spark = get_spark_session()

    # map masterplan to get lat/long
    geo_location_id_master_plan = spark.sql("""
    select a.latitude as latitude_landmark, a.longitude as longitude_landmark, a.location_id
    from geo_master_plan a
    left join geo_location_id_frequent b
    on a.location_id = b.location_id
    """)
    geo_location_id_master_plan.createOrReplaceTempView('geo_location_id_master_plan')

    # query to get result distanc km
    geo_location_data_used_l4 = spark.sql("""
    select
    a.mobile_no
    , a.start_of_week
    ,case when a.latitude is null and a.longitude is null then 0 
      else cast((acos(cos(radians(90-a.latitude))*cos(radians(90-b.latitude_landmark))+sin(radians(90-a.latitude))*sin(radians(90-b.latitude_landmark))*cos(radians(a.longitude - b.longitude_landmark)))*6371) as decimal(13,2)) 
      end as distance_km
    , sum(a.sum_all_no_of_call) as NUMBER_OF_DATA_SESSION
    FROM geo_l4_most_frequency a
    join geo_location_id_master_plan b
    where a.location_id = b.location_id
    group by 1,2,3
    order by 1,2,3
    """)
    geo_location_data_used_l4.createOrReplaceTempView('geo_location_data_used_l4')

    # get average, max, min, sum from distanc km
    geo_location_data_cal = geo_location_data_used_l4.groupBy("mobile_no", "start_of_week").agg(
        F.avg("distance_km").alias("avg_distance_km"), F.max("distance_km").alias("max_distance_km"),
        F.min("distance_km").alias("min_distance_km"), F.sum("distance_km").alias("sum_distance_km"))
    geo_location_data_cal.orderBy("mobile_no", "start_of_week")
    geo_location_data_cal.cache()

    out = node_from_config(geo_location_data_cal, sql)
    return out


# =========================== Number most frequent count ============================================
def l4_geo_number_most_frequent_count(l1_favourite_location, l4_most_frequency, sql):
    l1_favourite_location.createOrReplaceTempView('geo_location_data')
    l4_most_frequency.createOrReplaceTempView('l4_most_frequency')

    l0_df1 = l1_favourite_location_1.withColumn("event_partition_date",
                                                F.to_date(l1_favourite_location_1.date_id.cast(DateType()),
                                                          "yyyy-MM-dd")).drop("date_id")
    l0_df1.createOrReplaceTempView('geo_location_data_1')
    spark = get_spark_session()

    geo_location_count_weekday = spark.sql("""
    select 
    b.event_partition_date, b.mobile_no, b.weektype , a.sum_all_no_of_call, a.the_most
    from geo_l4_most_frequency_1 a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where b.weektype = 'weekday'
    group by 1,2,3,4,5
    """)

    geo_location_count_weekend = spark.sql("""
    select 
    b.event_partition_date, b.mobile_no, b.weektype , a.sum_all_no_of_call, a.the_most
    from geo_l4_most_frequency_1 a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where b.weektype = 'weekend'
    group by 1,2,3,4,5
    """)

    geo_location_count_weekday_1 = geo_location_count_weekday.groupBy("event_partition_date").agg(
        F.count("sum_all_no_of_call").alias("count_all_no_of_call_weekday"))
    geo_location_count_weekday_1.orderBy("event_partition_date")

    geo_location_count_weekend_1 = geo_location_count_weekend.groupBy("event_partition_date").agg(
        F.count("sum_all_no_of_call").alias("count_all_no_of_call_weekend"))
    geo_location_count_weekend_1.orderBy("event_partition_date")

    out1 = node_from_config(geo_location_count_weekday_1, sql)
    out2 = node_from_config(geo_location_count_weekend_1, sql)

    return out1, out2


# =========================== Number most frequent top five ============================================
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
    b.event_partition_date, b.mobile_no, b.weektype , a.sum_all_no_of_call, a.the_most
    from geo_l4_most_frequency_1 a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where a.the_most = '1'
    group by 1,2,3,4,5
    """)

    geo_location_data_weekday = spark.sql("""
    select 
    b.event_partition_date, b.mobile_no, b.weektype , a.sum_all_no_of_call, a.the_most
    from geo_l4_most_frequency_1 a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where a.the_most = '1'
    AND b.weektype = 'weekday'
    group by 1,2,3,4,5
    """)

    geo_location_data_weekend = spark.sql("""
    select 
    b.event_partition_date, b.mobile_no, b.weektype , a.sum_all_no_of_call, a.the_most
    from geo_l4_most_frequency_1 a
    left join geo_location_data_1 b
    on a.mobile_no = b.mobile_no
    where a.the_most = '1'
    AND b.weektype = 'weekend'
    group by 1,2,3,4,5
    """)

    # =================================== Number most frequent All ====================================================
    geo_location_data_calcu_all = geo_location_data_all.groupBy("mobile_no", "event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_all"))
    geo_location_data_avg_all = geo_location_data_calcu_all.groupBy("event_partition_date").agg(
        F.avg("sum_all_no_of_call_all").alias("avg_all_no_of_call_all"),
        F.max("sum_all_no_of_call_all").alias("max_all_no_of_call_all"),
        F.min("sum_all_no_of_call_all").alias("min_all_no_of_call_all"),
        F.count("sum_all_no_of_call_all").alias("count_sum_all_no_of_call_all"))

    out1 = node_from_config(geo_location_data_avg_all, sql)

    # =================================== Number most frequent weekday ====================================================
    geo_location_data_calcu_weekday = geo_location_data_weekday.groupBy("mobile_no", "event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekday"))

    geo_location_data_avg_weekday = geo_location_data_calcu_weekday.groupBy("event_partition_date").agg(
        F.avg("sum_all_no_of_call_weekday").alias("avg_all_no_of_call_weekday"),
        F.max("sum_all_no_of_call_weekday").alias("max_all_no_of_call_weekday"),
        F.min("sum_all_no_of_call_weekday").alias("min_all_no_of_call_weekday"),
        F.count("sum_all_no_of_call_weekday").alias("count_sum_all_no_of_call_weekday"))

    out2 = node_from_config(geo_location_data_avg_weekday, sql)

    # =================================== Number most frequent weekend ====================================================
    geo_location_data_calcu_weekend = geo_location_data_weekend.groupBy("mobile_no", "event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekend"))

    geo_location_data_avg_weekend = geo_location_data_calcu_weekend.groupBy("event_partition_date").agg(
        F.avg("sum_all_no_of_call_weekend").alias("avg_all_no_of_call_weekend"),
        F.max("sum_all_no_of_call_weekend").alias("max_all_no_of_call_weekend"),
        F.min("sum_all_no_of_call_weekend").alias("min_all_no_of_call_weekend"), F.count("sum_all_no_of_call_weekend"))

    out3 = node_from_config(geo_location_data_avg_weekend, sql)

    return out1, out2, out3

