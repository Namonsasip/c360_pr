import pyspark.sql.functions as f
from pyspark.sql import DataFrame
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
    execute_sql, add_event_week_and_month_from_yyyymmdd
from customer360.utilities.spark_util import get_spark_session

def l1_geo_time_spent_by_location_daily(df,sql):
    df = add_start_of_week_and_month(df, "time_in")
    print('debug1')
    df.show()
    ss = get_spark_session()
    df.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')
    stmt = """
    SELECT IMSI,LOCATION_ID,SUM(DURATION) AS SUM_DURATION,event_partition_date,start_of_week,start_of_month
    FROM GEO_CUST_CELL_VISIT_TIME
    GROUP BY IMSI,LOCATION_ID,event_partition_date,start_of_week,start_of_month
    """
    df = ss.sql(stmt)
    print('debug1')
    df.show()

    # df = node_from_config(df,sql)

    return df

def l1_geo_area_from_ais_store_daily(shape,masterplan,geo_cust_cell_visit_time,sql):
    geo_cust_cell_visit_time  = add_start_of_week_and_month(geo_cust_cell_visit_time, "time_in")
    geo_cust_cell_visit_time.show()

    max_date = masterplan.selectExpr('max(partition_date)').collect()[0][0]
    masterplan=masterplan.where('partition_date='+str(max_date))
    max_date = shape.selectExpr('max(partition_month)').collect()[0][0]
    shape=shape.where('partition_month='+str(max_date))
    masterplan.createOrReplaceTempView("mst_cell_masterplan")
    shape.createOrReplaceTempView("mst_poi_shape")

    ss = get_spark_session()
    df=ss.sql("""
    SELECT A.LANDMARK_SUB_NAME_EN,
    B.LOCATION_ID,B.LOCATION_NAME,
    CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-B.LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-B.LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - B.LONGITUDE)))*6371) AS DECIMAL(13,2)) AS DISTANCE_KM
    FROM mst_poi_shape A,
    mst_cell_masterplan B
    WHERE   A.LANDMARK_CAT_NAME_EN IN ('AIS')
    AND CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-B.LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-B.LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - B.LONGITUDE)))*6371) AS DECIMAL(13,2)) <= (0.5)
    GROUP BY 1,2,3,4
    """)
    df.cache()
    df.createOrReplaceTempView('TEMP_GEO_AIS_SHOP')

    geo_cust_cell_visit_time.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')

    df2 = ss.sql("""SELECT A.IMSI,A.event_partition_date,A.start_of_week,A.start_of_month,SUM(DURATION) AS DURATION,count(1) AS NUM_OF_TIMES_PER_DAY 
    FROM GEO_CUST_CELL_VISIT_TIME A,
    TEMP_GEO_AIS_SHOP B 
    WHERE A.LOCATION_ID = B.LOCATION_ID 
    GROUP BY 1,2,3,4""")
    df2.cache()
    df2=node_from_config(df2,sql)
    df.unpersist()
    df2.unpersist()
    return df2

def l1_geo_area_from_competitor_store_daily(shape,masterplan,geo_cust_cell_visit_time,sql):
    geo_cust_cell_visit_time.cache()
    geo_cust_cell_visit_time = add_start_of_week_and_month(geo_cust_cell_visit_time, "time_in")

    max_date = masterplan.selectExpr('max(partition_date)').collect()[0][0]
    masterplan = masterplan.where('partition_date=' + str(max_date))
    max_date = shape.selectExpr('max(partition_month)').collect()[0][0]
    shape = shape.where('partition_month=' + str(max_date))

    masterplan.createOrReplaceTempView("mst_cell_masterplan")
    shape.createOrReplaceTempView("mst_poi_shape")


    ss = get_spark_session()
    df = ss.sql("""
        SELECT A.LANDMARK_SUB_NAME_EN,
        B.LOCATION_ID,B.LOCATION_NAME,
        CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-B.LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-B.LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - B.LONGITUDE)))*6371) AS DECIMAL(13,2)) AS DISTANCE_KM
        FROM mst_poi_shape A,
        mst_cell_masterplan B
        WHERE   A.LANDMARK_CAT_NAME_EN IN ('TRUE','DTAC')
        AND CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-B.LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-B.LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - B.LONGITUDE)))*6371) AS DECIMAL(13,2)) <= (0.5)
        GROUP BY 1,2,3,4
        """)
    df.cache()
    df.createOrReplaceTempView('TEMP_GEO_AIS_SHOP')

    geo_cust_cell_visit_time.createOrReplaceTempView('GEO_CUST_CELL_VISIT_TIME')

    df2 = ss.sql("""SELECT A.IMSI,A.event_partition_date,A.start_of_week,A.start_of_month,SUM(DURATION) AS DURATION,count(1) AS NUM_OF_TIMES_PER_DAY 
        FROM GEO_CUST_CELL_VISIT_TIME A,
        TEMP_GEO_AIS_SHOP B 
        WHERE A.LOCATION_ID = B.LOCATION_ID 
        GROUP BY 1,2,3,4""")
    df2.cache()

    df2 = node_from_config(df2, sql)
    df.unpersist()
    df2.unpersist()

    return df2

def l1_geo_top_visit_exclude_homework_daily(df,homework,sql):
    df.createOrReplaceTempView('geo_cust_visit_time')
    homework.createOrReplaceTempView('homework_master')

    ss = get_spark_session()
    df = ss.sql("""
        xxxxxxxxxxxxxx
        """)

    df = node_from_config(df,sql)
    return df

##==============================Update 2020-06-12 by Thatt529==========================================##

###total_distance_km###
def l1_geo_total_distance_km_daily(l0_df, sql):
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
        ,case when b.latitude is null and b.longitude is null then 0 
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
    l1_df1.cache()
    l1_df1 = l1_df1.withColumn("start_of_week", F.to_date(F.date_trunc('week', l1_df1.event_partition_date)))
    l1_df1 = l1_df1.withColumn("start_of_month", F.to_date(F.date_trunc('month', l1_df1.event_partition_date)))

    # Check DataFrame from SQL query statement
    print("Start for check result from sql query statement")
    l1_df1.count()
    l1_df1.show()

    l1_df2 = node_from_config(l1_df1, sql)

    return l1_df2

###Traffic_fav_location###
def use_home_work(geo_mst_cell_masterplan,geo_home_work_data,profile_customer_profile_ma):
    geo_mst_cell_masterplan.createOrReplaceTempView('GEO_MST_CELL_MASTERPLAN')
    geo_home_work_data.createOrReplaceTempView('GEO_HOME_WORK_DATA')
    profile_customer_profile_ma.createOrReplaceTempView('PROFILE_CUSTOMER_PROFILE_MA')

    spark = get_spark_session()

    TEMP_GEO_DATA_00 = spark.sql(""" 
    SELECT Z.IMSI,Z.LOCATION_ID,Z.CI,Z.LAC FROM(
    SELECT B.IMSI,A.LOCATION_ID,A.LATITUDE,A.LONGITUDE,A.CI,A.LAC,B.WORK_LOCATION_ID,B.HOME_WEEKDAY_LOCATION_ID
    FROM GEO_MST_CELL_MASTERPLAN A
    LEFT JOIN LOCATION_HOMEWORK_NEW B
    ON  B.WORK_LOCATION_ID = A.LOCATION_ID OR B.HOME_WEEKDAY_LOCATION_ID=A.LOCATION_ID) Z
    WHERE Z.WORK_LOCATION_ID IS NOT NULL OR Z.HOME_WEEKDAY_LOCATION_ID IS NOT NULL
    """)
    TEMP_GEO_DATA_00.createOrReplaceTempView('TEMP_GEO_DATA_00')

    TEMP_GEO_DATA_01 = spark.sql("""
        SELECT A.IMSI,A.LAC,A.CI,B.ACCESS_METHOD_NUM
        FROM TEMP_GEO_DATA_00 A ,PROFILE_CUSTOMER_PROFILE_MA B
        WHERE A.IMSI = B.IMSI
        GROUP BY A.IMSI,A.LAC,A.CI,B.ACCESS_METHOD_NUM
        """)
    spark.catalog.dropTempView("GEO_MST_CELL_MASTERPLAN")
    spark.catalog.dropTempView("GEO_HOME_WORK_DATA")
    spark.catalog.dropTempView("PROFILE_CUSTOMER_PROFILE_MA")
    return TEMP_GEO_DATA_01


def L1_data_traffic_home_work_FN(geo_mst_cell_masterplan,geo_home_work_data,profile_customer_profile_ma,usage_sum_data_location_daily,HOME_WORK_WEEKDAY_LOCATION_ID):
    ###TABLE###

    spark = get_spark_session()
    use_home_work(geo_mst_cell_masterplan, geo_home_work_data, profile_customer_profile_ma).createOrReplaceTempView('TEMP_GEO_DATA_01')
    geo_mst_cell_masterplan.createOrReplaceTempView('GEO_MST_CELL_MASTERPLAN')
    geo_home_work_data.createOrReplaceTempView('LOCATION_HOMEWORK_NEW')
    profile_customer_profile_ma.createOrReplaceTempView('PROFILE_CUSTOMER_PROFILE_MA')
    usage_sum_data_location_daily.createOrReplaceTempView('USAGE_SUM_DATA_LOCATION_DAILY')
    #TEMP1#
    GEO_TEMP_00 = spark.sql("""
    SELECT B.IMSI,A.CI,A.LAC
    FROM GEO_MST_CELL_MASTERPLAN A,LOCATION_HOMEWORK_NEW B
    WHERE A.LOCATION_ID=B."""+HOME_WORK_WEEKDAY_LOCATION_ID+"""
    """)

    GEO_TEMP_00.createOrReplaceTempView('GEO_TEMP_00')

    # TEMP2#
    GEO_TEMP_01 = spark.sql("""
    SELECT B.ACCESS_METHOD_NUM,A.IMSI,A.LAC,A.CI
    FROM GEO_TEMP_00 A ,PROFILE_CUSTOMER_PROFILE_MA B
    WHERE A.IMSI = B.IMSI
    GROUP BY A.IMSI,A.LAC,A.CI,B.ACCESS_METHOD_NUM
    """)

    GEO_TEMP_01.createOrReplaceTempView('GEO_TEMP_01')

    # TEMP3#
    GEO_TEMP_02 = spark.sql("""
    SELECT B.DATE_ID,A.IMSI,SUM(VOL_DOWNLINK_KB+VOL_UPLINK_KB) AS TOTAL_DATA_TRAFFIC_KB
    FROM GEO_TEMP_01 A,
    USAGE_SUM_DATA_LOCATION_DAILY B
    WHERE A.ACCESS_METHOD_NUM = B.MOBILE_NO
    AND B.LAC = A.LAC
    AND A.CI = B.CI
    GROUP BY 1,2
    """)
    return  GEO_TEMP_02

def L1_data_traffic_home_work_Top1_TOP2(geo_mst_cell_masterplan,geo_home_work_data,profile_customer_profile_ma,usage_sum_data_location_daily):
    L1_data_traffic_home_work_FN(geo_mst_cell_masterplan, geo_home_work_data, profile_customer_profile_ma,usage_sum_data_location_daily,"HOME_WEEKDAY_LOCATION_ID").createOrReplaceTempView('Home')
    L1_data_traffic_home_work_FN(geo_mst_cell_masterplan, geo_home_work_data, profile_customer_profile_ma,usage_sum_data_location_daily,"WORK_LOCATION_ID").createOrReplaceTempView('Work')
    L1_data_traffic_home_work_FN(geo_mst_cell_masterplan, geo_home_work_data, profile_customer_profile_ma, usage_sum_data_location_daily, "TOP_LOCATION_1ST ").createOrReplaceTempView('Top1')
    L1_data_traffic_home_work_FN(geo_mst_cell_masterplan, geo_home_work_data, profile_customer_profile_ma,usage_sum_data_location_daily, "TOP_LOCATION_2ND ").createOrReplaceTempView('Top2')
    spark = get_spark_session()
    Home_Work = spark.sql(""""
    SELECT A.DATE_ID AS event_partition_date ,
    A.IMSI,A.TOTAL_DATA_TRAFFIC_KB AS Home_traffic_KB,
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
    Home_Work.createTempView('GEO_TEMP_04')
    data_traffic_location = spark.sql("""
    SELECT event_partition_date,IMSI,
    Home_traffic_KB,
    Work_traffic_KB,
    Top1_location_traffic_KB,
    Top2_location_traffic_KB,
    ((Home_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Home_traffic_KB,
    ((Work_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Work_traffic_KB,
    ((Top1_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top1_location_traffic_KB,
    ((Top2_location_traffic_KB*100)/(Home_traffic_KB+Work_traffic_KB+Top1_location_traffic_KB+Top2_location_traffic_KB)) AS share_Top2_location_traffic_KB
    FROM  GEO_TEMP_04
    """)
    return data_traffic_location


###feature_sum_voice_location###
def homework_join_master_profile(cell_masterplan,geo_homework,profile_ma,usage_sum_voice,Column_Name):
    geo_homework.createOrReplaceTempView('geo_homework')
    cell_masterplan.createOrReplaceTempView('cell_masterplan')
    profile_ma.createOrReplaceTempView('profile_ma')

    spark = get_spark_session()
    df_temp_00 = spark.sql(f'''
      select a.imsi,
      a.{Column_Name}_location_id,
      b.lac as {Column_Name}_lac,
      b.ci as {Column_Name}_ci
      from geo_homework a
      left join cell_masterplan b
        on a.{Column_Name}_location_id = b.location_id
      group by 1,2,3,4
      ''')
    df_temp_00.createOrReplaceTempView('temp_00')

    df_temp_01 = spark.sql(f'''
      select a.imsi,
      b.access_method_num,
      a.{Column_Name}_location_id,
      a.{Column_Name}_lac,
      a.{Column_Name}_ci
      from temp_00 a
      left join profile_ma b
      on a.imsi = b.imsi
      group by 1,2,3,4,5
      ''')

    return df_temp_01

def geo_top_visit_join_master_profile(cell_masterplan,geo_top_visit,profile_ma,Column_Name):
    geo_top_visit.createOrReplaceTempView('geo_top_visit')
    cell_masterplan.createOrReplaceTempView('cell_masterplan')
    profile_ma.createOrReplaceTempView('profile_ma')

    spark = get_spark_session()
    df_temp_00 = spark.sql(f'''
      select a.imsi,
      a.{Column_Name},
      b.lac as {Column_Name}_lac,s
      b.ci as {Column_Name}_ci
      from geo_top_visit a
      left join cell_masterplan b
        on a.{Column_Name} = b.location_id
      group by 1,2,3,4
      ''')
    df_temp_00.createOrReplaceTempView('temp_00')

    df_temp_01 = spark.sql(f'''
      select a.imsi,
      b.access_method_num,
      a.{Column_Name},
      a.{Column_Name}_lac,
      a.{Column_Name}_ci
      from temp_00 a
      left join profile_ma b
      on a.imsi = b.imsi
      group by 1,2,3,4,5
      ''')

    return df_temp_01

def l1_call_location_home_work(cell_masterplan,geo_homework,profile_ma,usage_sum_voice,geo_top_visit_exc_homework):
    homework_join_master_profile(cell_masterplan,geo_homework,profile_ma,
                                 "home_weekday").createOrReplaceTempView('home_weekday')
    homework_join_master_profile(cell_masterplan, geo_homework,profile_ma,
                                 "work").createOrReplaceTempView('work')
    geo_top_visit_join_master_profile(cell_masterplan,geo_homework,profile_ma,
                                 "top_location_1st").createOrReplaceTempView('top_1st')
    geo_top_visit_join_master_profile(cell_masterplan, geo_homework,profile_ma,
                                 "top_location_2nd").createOrReplaceTempView('top_2nd')


    def sum_voice_daily(df_temp_01):
        spark = get_spark_session()
        df_sum_voice = spark.sql(f'''
          select cast(substr(date_id,1,10)as date) as event_partiiton_date
          ,a.imsi
          ,sum(b.no_of_call+b.no_of_inc) as call_count_location_{df_temp_01}  
          from {df_temp_01} a
          left join usage_voice b
          where b.service_type in ('VOICE','VOLTE')
          on a.access_method_num = b.access_method_num
          and a.home_weekday_lac = b.lac
          and a.home_weekday_ci = b.ci
          group by 1,2
          ''')
        return df_sum_voice

    sum_voice_daily('home_weekday').createOrReplaceTempView('df_call_home_weekday')
    sum_voice_daily('work').createOrReplaceTempView('df_call_work')
    sum_voice_daily('location_top_1st').createOrReplaceTempView('df_call_top_1st')
    sum_voice_daily('location_top_2nd').createOrReplaceTempView('df_call_top_2nd')

    spark = get_spark_session()
    df_sum_voice_daily = spark.sql('''
        select a.event_partiiton_date
        ,a.imsi
        ,a.call_count_location_home_weekday  
        ,b.call_count_location_work
        ,c.call_count_location_top_1st
        ,d.call_count_location_top_1st
        from df_call_home_weekday a
        join df_call_work b
            on a.event_partiiton_date = b.event_partiiton_date and a.imsi = b.imsi
        join df_call_top_1st c
            on a.event_partiiton_date = c.event_partiiton_date and a.imsi = c.imsi
        join df_call_top_2nd d
            on a.event_partiiton_date = d.event_partiiton_date and a.imsi = d.imsi
    ''')

    return df_sum_voice_daily

def l1_geo_cust_subseqently_distance(cell_visit, sql):

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

###feature_AIS_store###
def l1_location_of_visit_ais_store_daily(shape,cust_cell_visit,sql):
    max_date = cust_cell_visit.selectExpr('max(partition_date)').collect()[0][0]
    shape.cache()
    store_shape = shape.where('landmark_cat_name_en like "%AIS%"')
    store_shape.createOrReplaceTempView('geo_mst_lm_poi_shape')
    cust_cell_visit.cache()
    cust_cell_visit = cust_cell_visit.where('partition_date='+str(max_date))
    cust_cell_visit.createOrReplaceTempView('geo_cust_cell_visit_time')
    # Get spark session
    spark = get_spark_session()
    df = spark.sql("""
        SELECT DISTINCT imsi,p1.location_id,landmark_name_th,landmark_sub_name_en,landmark_latitude,landmark_longitude,time_in,substr(partition_date,0,6) as partition_month
        FROM GEO_CUST_CELL_VISIT_TIME p1 join geo_mst_lm_poi_shape  p2
        WHERE p1.location_id =  p2.geo_shape_id
        AND p2.PARTITION_NAME = 'INTERNAL'
        AND p2.LANDMARK_CAT_NAME_EN IN ('AIS') ;
     """)

    df.cache()
    print("Start for check result from sql query statement")
    df.count()
    df.show()

    store_visit = node_from_config(df,sql)
    return store_visit

## ==============================Update 2020-06-15 by Thatt529==========================================##

###Top_3_cells_on_voice_usage###
def l1_geo_top3_cells_on_voice_usage(usage_df,geo_df,profile_df):
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
    and latitude is not null 
    and longitude is not null
    """
    l1_df1 = spark.sql(sql_query1)
    l1_df.cache()
    l1_df1.cache()
    l1_df1 = l1_df1.withColumn("start_of_week", F.to_date(F.date_trunc('week', l1_df1.event_partition_date)))
    l1_df1 = l1_df1.withColumn("start_of_month", F.to_date(F.date_trunc('month', l1_df1.event_partition_date)))
    # l1_df2 = node_from_config(l1_df1, sql)
    return l1_df1


## ==============================Update 2020-06-17 by Thatt529==========================================##

###distance_top_call###
def l1_geo_distance_top_call(df):
    ### config
    spark = get_spark_session()

    # create temp
    df.createOrReplaceTempView('geo_top3_cells_on_voice_usage')

    sql_query = """
    select
    a.imsi
    ,case when b.latitude is null and b.longitude is null then 0 
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
    l1_df1 = l1_df.groupBy("imsi", "event_partition_date", "start_of_week", "start_of_month").agg(
        F.max("top_distance_km").alias("max_distance_top_call"), F.min("top_distance_km").alias("min_distance_top_call"),
        F.avg("top_distance_km").alias("avg_distance_top_call"), F.when(
            F.sqrt(F.avg(l1_df.top_distance_km * l1_df.top_distance_km) - F.pow(F.avg(l1_df.top_distance_km), F.lit(2))).cast(
                "string") == 'NaN', 0).otherwise(
            F.sqrt(F.avg(l1_df.top_distance_km * l1_df.top_distance_km) - F.pow(F.avg(l1_df.top_distance_km), F.lit(2)))).alias(
            "sd_distance_top_call"), F.sum("top_distance_km").alias("sum_distance_top_call"))
    l1_df.cache()
    l1_df1.cache()
    l1_df1.show(5)

    return l1_df1
