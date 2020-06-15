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

def l4_geo_top_visit_exclude_homework(sum_duration,homework):
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
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("event_partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
    geo_cust_cell_visit_time = geo_cust_cell_visit_time.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col("event_partition_date"))))

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
    home_duration_dayily_with_weektype = df_home_daily.withColumn("week_type", F.when((F.dayofweek('event_partition_date') == 1) | (F.dayofweek('event_partition_date') == 7), 'weekend') \
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

    w_home = Window().partitionBy(F.col('imsi'), F.col('location_id'), F.col('week_type')).orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    df_home_combine_week_monthly_sum_last_3_day = df_home_combine_week_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn("Sum", F.sum("duration").over(w_home))

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



    w_work = Window().partitionBy(F.col('imsi'), F.col('location_id')).orderBy(F.col("Month").cast("long")).rangeBetween(-(86400 * 89), 0)
    df_home_combine_week_monthly_sum_last_3_day = df_work_monthly.withColumn("Month", F.to_timestamp("start_of_month", "yyyy-MM-dd")).withColumn("Sum", F.sum("duration").over(w_work))

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

def l4_geo_population_aroud_home(geo_home_work_loation_id, cell_masterplan,sql):
    geo_home_work_loation_id.cache()
    cell_masterplan.cache()
    return None


##==============================Update 2020-06-12 by Thatt529==========================================##

###Traffic_fav_location###
def l4_Share_traffic(df,sql):
    df2=l4_rolling_window(df,sql)
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
def l4_geo_last_AIS_store_visit(raw,sql):
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

def l4_geo_most_AIS_store_visit(raw,sql):
    max_date = raw.selectExpr('max(partition_month)').collect()[0][0]
    raw.cache()
    raw = raw.where('partition_month=' + str(max_date))
    raw.createOrReplaceTempView('GEO_AIS_VISITED_SHOP')
    # Get spark session
    spark = get_spark_session()
    df = spark.sql("""
            SELECT imsi,location_id,landmark_name_th,landmark_sub_name_en,COUNT(TIME_IN) as last_visited,partition_month
            FROM GEO_AIS_VISITED_SHOP
            GROUP BY 1,2,3,4,6;
         """)

    df.cache()
    print("Start for check result from sql query statement")
    df.count()
    df.show()

    out = node_from_config(df, sql)
    return out

def l4_geo_store_close_to_home(home_work,sql):
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

def l4_geo_store_close_to_work(home_work,sql):
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
    out = node_from_config(df2,sql)
    return out

##==============================Update 2020-06-15 by Thatt529==========================================##

###Top_3_cells_on_voice_usage###
def l4_geo_top3_cells_on_voice_usage(usage_df,geo_df,profile_df):
    ### config
    spark = get_spark_session()

    ### add partition_date
    l0_df_usage1 = usage_df.withColumn("event_partition_date",F.to_date(usage_df.date_id.cast(DateType()), "yyyyMMdd"))

    ### last_date
    geo_last_date = geo_df.agg(F.max("partition_date")).collect()[0][0]
    profile_last_date = profile_df.agg(F.max("partition_month")).collect()[0][0]

    ### where
    l0_df_geo1 = spark.read.parquet("dbfs:/mnt/customer360-blob-data/C360/GEO/geo_mst_cell_masterplan/partition_date=" + str(geo_last_date) + "/")
    l0_df_profile1 = profile_df.where("partition_month = '" + str(profile_last_date) + "'")

    # create temp
    l0_df_usage1.createOrReplaceTempView('usage_sum_voice_location_daily')
    l0_df_geo1.createOrReplaceTempView('geo_mst_cell_masterplan')
    l0_df_profile1.createOrReplaceTempView('profile_customer_profile_ma')

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
    """
    l1_df1 = spark.sql(sql_query1)
    l1_df2 = l1_df1.filter("rnk <= 3 and latitude is not null and longitude is not null")
    l1_df.cache()
    l1_df1.cache()
    l1_df2.cache()

    return l1_df2