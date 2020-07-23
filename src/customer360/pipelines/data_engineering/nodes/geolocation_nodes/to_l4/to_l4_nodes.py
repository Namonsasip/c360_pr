import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import *
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config, l4_rolling_window, create_weekly_lookback_window, \
    create_daily_lookback_window, __get_l4_time_granularity_column, create_monthly_lookback_window, _get_full_data
from customer360.utilities.re_usable_functions import l3_massive_processing, l1_massive_processing, __divide_chunks, \
    check_empty_dfs, data_non_availability_and_missing_check
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics
from pyspark.sql import Window
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df



###Traffic_fav_location###
def l4_Share_traffic(df, ini_work_week,work_week):

    # df = node_from_config(df, ini_work_week)
    df = l4_rolling_window(df, work_week)

    df.createOrReplaceTempView('GEO_TEMP_00')
    spark = get_spark_session()
    total_last_month = '(sum_{0}_monthly_last_month + sum_{1}_monthly_last_month + ' \
                    'sum_{2}_monthly_last_month + sum_{3}_monthly_last_month)'.format('Home_traffic_KB',
                                                                                      'Work_traffic_KB',
                                                                                      'Top1_location_traffic_KB',
                                                                                      'Top2_location_traffic_KB')

    total_last_three_month = '(sum_{0}_monthly_last_three_month + sum_{1}_monthly_last_three_month + ' \
                    'sum_{2}_monthly_last_three_month + sum_{3}_monthly_last_three_month)'.format('Home_traffic_KB',
                                                                                      'Work_traffic_KB',
                                                                                      'Top1_location_traffic_KB',
                                                                                      'Top2_location_traffic_KB')
    sql_query = """
        select *,
            sum_{0}_monthly_last_month * 100 / {4} as share_{0}_last_month,
            sum_{1}_monthly_last_month * 100 / {4} as share_{1}_last_month,
            sum_{2}_monthly_last_month * 100 / {4} as share_{2}_last_month,
            sum_{3}_monthly_last_month * 100 / {4} as share_{3}_last_month,
            sum_{0}_monthly_last_three_month * 100 / {5} as share_{0}_last_three_month,
            sum_{1}_monthly_last_three_month * 100 / {5} as share_{1}_last_three_month,
            sum_{2}_monthly_last_three_month * 100 / {5} as share_{2}_last_three_month,
            sum_{3}_monthly_last_three_month * 100 / {5} as share_{3}_last_three_month
        from GEO_TEMP_00
    """.format('Home_traffic_KB', 'Work_traffic_KB', 'Top1_location_traffic_KB', 'Top2_location_traffic_KB',
               total_last_month, total_last_three_month)

    # sql_query = """
    # SELECT *
    #     ,(sum_Home_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Home_traffic_KB_weekly_last_week
    #     ,(sum_Work_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Work_traffic_KB_weekly_last_week
    #     ,(sum_Top1_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Top1_traffic_KB_weekly_last_week
    #     ,(sum_Top2_location_traffic_KB_weekly_last_week*100)/(sum_Home_traffic_KB_weekly_last_week+sum_Work_traffic_KB_weekly_last_week+sum_Top1_location_traffic_KB_weekly_last_week+sum_Top2_location_traffic_KB_weekly_last_week) AS share_Top2_traffic_KB_weekly_last_week
    #     ,(sum_Home_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS share_SUM_TRAFFIC_Home_weekly_last_two_week
    #     ,(sum_Work_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS share_SUM_TRAFFIC_Work_weekly_last_two_week
    #     ,(sum_Top1_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS share_SUM_TRAFFIC_Top1_weekly_last_two_week
    #     ,(sum_Top2_location_traffic_KB_weekly_last_two_week*100)/(sum_Home_traffic_KB_weekly_last_two_week+sum_Work_traffic_KB_weekly_last_two_week+sum_Top1_location_traffic_KB_weekly_last_two_week+sum_Top2_location_traffic_KB_weekly_last_two_week) AS share_SUM_TRAFFIC_Top2_weekly_last_two_week
    #     ,(sum_Home_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS share_SUM_TRAFFIC_Home_weekly_last_four_week
    #     ,(sum_Work_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS share_SUM_TRAFFIC_Work_weekly_last_four_week
    #     ,(sum_Top1_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS share_SUM_TRAFFIC_Top1_weekly_last_four_week
    #     ,(sum_Top2_location_traffic_KB_weekly_last_four_week*100)/(sum_Home_traffic_KB_weekly_last_four_week+sum_Work_traffic_KB_weekly_last_four_week+sum_Top1_location_traffic_KB_weekly_last_four_week+sum_Top2_location_traffic_KB_weekly_last_four_week) AS share_SUM_TRAFFIC_Top2_weekly_last_four_week
    #     ,(sum_Home_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS share_SUM_TRAFFIC_Home_weekly_last_twelve_week
    #     ,(sum_Work_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS share_SUM_TRAFFIC_Work_weekly_last_twelve_week
    #     ,(sum_Top1_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS share_SUM_TRAFFIC_Top1_weekly_last_twelve_week
    #     ,(sum_Top2_location_traffic_KB_weekly_last_twelve_week*100)/(sum_Home_traffic_KB_weekly_last_twelve_week+sum_Work_traffic_KB_weekly_last_twelve_week+sum_Top1_location_traffic_KB_weekly_last_twelve_week+sum_Top2_location_traffic_KB_weekly_last_twelve_week) AS share_SUM_TRAFFIC_Top2_weekly_last_twelve_week
    # FROM GEO_TEMP_00
    # """
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
    out = node_from_config(df, sql)
    return out


def l4_geo_store_close_to_home(home_work, locations, sql):
    month_id = home_work.selectExpr('max(start_of_month)').collect()[0][0]
    home_work = home_work.where(F.col('start_of_month') == str(month_id))
    home_work.createOrReplaceTempView('home_work_location')
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
    home_weekday = spark.sql("""
            select a.imsi,
                a.home_weekday_location_id,
                MIN(CAST((ACOS(COS(RADIANS(90-a.LANDMARK_LATITUDE))*COS(RADIANS(90-a.HOME_WEEKDAY_LATITUDE))+SIN(RADIANS(90-a.LANDMARK_LATITUDE))*SIN(RADIANS(90-a.HOME_WEEKDAY_LATITUDE))*COS(RADIANS(a.LANDMARK_LONGITUDE - a.HOME_WEEKDAY_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_weekday_home,
                first(a.landmark_name_th) as branch,
                first(a.geo_shape_id) as branch_location_id,
                a.start_of_month
            from (select *,CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKDAY_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKDAY_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKDAY_LONGITUDE)))*6371) AS DECIMAL(13,2)) as dist from home_work_ais_store ) a
            where  a.dist <= 100
            group by 1,2,6
        """)
    home_weekend = spark.sql("""
            select a.imsi,
                a.home_weekend_location_id,
                MIN(CAST((ACOS(COS(RADIANS(90-a.LANDMARK_LATITUDE))*COS(RADIANS(90-a.HOME_WEEKEND_LATITUDE))+SIN(RADIANS(90-a.LANDMARK_LATITUDE))*SIN(RADIANS(90-a.HOME_WEEKEND_LATITUDE))*COS(RADIANS(a.LANDMARK_LONGITUDE - a.HOME_WEEKEND_LONGITUDE)))*6371) AS DECIMAL(13,2))) AS range_from_weekend_home, 
                first(a.landmark_name_th) as branch, 
                first(a.geo_shape_id) as branch_location_id,
                a.start_of_month
            from (select *,CAST((ACOS(COS(RADIANS(90-LANDMARK_LATITUDE))*COS(RADIANS(90-HOME_WEEKEND_LATITUDE))+SIN(RADIANS(90-LANDMARK_LATITUDE))*SIN(RADIANS(90-HOME_WEEKEND_LATITUDE))*COS(RADIANS(LANDMARK_LONGITUDE - HOME_WEEKEND_LONGITUDE)))*6371) AS DECIMAL(13,2)) as dist from home_work_ais_store ) a
            where  a.dist <= 100
            group by 1,2,6
        """)
    home_weekday.createOrReplaceTempView('home_weekday')
    home_weekend.createOrReplaceTempView('home_weekend')
    df2 = spark.sql("""
            select a.imsi,
                a.home_weekday_location_id,
                a.range_from_weekday_home,
                a.branch as wd_location,
                a.branch_location_id as wd_location_id,
                b.home_weekend_location_id,
                b.range_from_weekend_home,
                b.branch as we_location,
                b.branch_location_id as we_location_id,
                a.start_of_month
            from home_weekday a left join home_weekend b
            on a.imsi = b.imsi
            and a.start_of_month = b.start_of_month
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
                select
                    A.imsi,
                    A.home_weekday_location_id,
                    A.weekday_branch_name,
                    A.weekday_branch_location_id,
                    B.landmark_latitude as store_latitude,
                    B.landmark_longitude as store_longitude
                from closest_store A, poi_shape B
                where A.weekday_branch_location_id = B.geo_shape_id
            """)
    closest.cache()
    closest.createOrReplaceTempView('closest_store_with_co')
    range_diff = spark.sql("""
    select 
        A.imsi,
        B.home_weekday_location_id,
        B.weekday_branch_name,
        B.weekday_branch_location_id,
        A.landmark_name_th,
        A.location_id,
        CAST((ACOS(COS(RADIANS(90-A.LANDMARK_LATITUDE))*COS(RADIANS(90-STORE_LATITUDE))+SIN(RADIANS(90-A.LANDMARK_LATITUDE))*SIN(RADIANS(90-STORE_LATITUDE))*COS(RADIANS(A.LANDMARK_LONGITUDE - STORE_LONGITUDE)))*6371) AS DECIMAL(13,2)) as range_diff,
        A.partition_month
    from most_visit_store A join closest_store_with_co B
    on A.imsi = B.imsi
    """)
    range_diff.cache()
    out = node_from_config(range_diff, sql)
    return out


#27 Same favourite location for weekend and weekday
def l4_same_favourite_location_weekend_weekday_weekly(l2_same_favourite_location_weekend_weekday_weekly):
    ### config
    spark = get_spark_session()
    l2_same_favourite_location_weekend_weekday_weekly.createOrReplaceTempView('l4_geo')

    # Top 5 selected
    sql_query = """
    select
        imsi
        ,start_of_week
        ,location_id
        ,duration_sum
        ,ROW
    from l4_geo
    where ROW <= 5
    order by 1,2,4 desc
    """
    l4 = spark.sql(sql_query)
    return l4


# 47 l4_the_favourite_locations_daily ====================
def l4_the_favourite_locations_daily(l1_the_favourite_locations_daily):

    ### config
    spark = get_spark_session()
    l1_the_favourite_locations_daily.createOrReplaceTempView('l1_geo')

    # Top 5 selected
    sql_query = """
    select 
        mobile_no
        ,date_id
        ,location_id
        ,gprs_type
        ,all_usage_data_kb
        ,the_most
    from(
            select 
                mobile_no
                ,date_id
                ,location_id
                ,gprs_type
                ,all_usage_data_kb
                ,ROW_NUMBER() OVER(partition by mobile_no,date_id,location_id,gprs_type ORDER BY all_usage_data_kb desc) as the_most
            from l1_geo)
    where the_most <= 5
    """
    l4 = spark.sql(sql_query)
    return l4


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
        * 
    from (
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
            select * 
            from l1_df_the_favourite_location_daily 
            where weektype = "weekday" and lower(gprs_type) like "4g%"
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
    geo_location_data_avg_weekend = geo_location_data_weekend.groupBy("mobile_no", "event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_weekend"),
        F.avg("sum_all_no_of_call").alias("avg_all_no_of_call_weekend"),
        F.max("sum_all_no_of_call").alias("max_all_no_of_call_weekend"),
        F.min("sum_all_no_of_call").alias("min_all_no_of_call_weekend"),
        F.count("sum_all_no_of_call"))

    out3 = node_from_config(geo_location_data_avg_weekend, sql)

    return out3

# =========================== Number most frequent top five All ============================================
def l4_geo_number_most_frequent_top_five(l1_favourite_location):
    # ----- Data Availability Checks -----
    if check_empty_dfs([l1_favourite_location]):
        return get_spark_empty_df()

    l1_favourite_location = data_non_availability_and_missing_check(df=l1_favourite_location, grouping="daily",
                                                                       par_col="event_partition_date",
                                                                       target_table_name="l4_geo_number_most_frequent_top_five")

    if check_empty_dfs([l1_favourite_location]):
        return get_spark_empty_df()

    # ----- Transformation -----
    l1_favourite_location.createOrReplaceTempView('geo_location_data')

    spark = get_spark_session()

    geo_location_data_all = spark.sql("""
    select 
    mobile_no, event_partition_date ,weektype ,the_most    
    from geo_location_data
    group by 1,2,3,4
    """)

    geo_location_data_count_all = geo_location_data_all.groupBy("mobile_no", "event_partition_date").agg(F.count("mobile_no").alias("count_mobile_no"))

    geo_location_data_sum_all = geo_location_data_count_all.groupBy("mobile_no", "event_partition_date").agg(F.sum("count_mobile_no").alias("sum_mobile_no"))

    geo_location_data_avg_all = geo_location_data_sum_all.groupBy("mobile_no", "event_partition_date").agg(
        F.avg("sum_mobile_no").alias("avg_sum_mobile_no"),
        F.max("sum_mobile_no").alias("max_sum_mobile_no"),
        F.min("sum_mobile_no").alias("min_sum_mobile_no"))

    geo_location_data_avg_all.cache()

    return geo_location_data_avg_all

    # =================================== Number most frequent All ====================================================
    geo_location_data_avg_all = geo_location_data_all.groupBy("event_partition_date").agg(
        F.sum("sum_all_no_of_call").alias("sum_all_no_of_call_all"),
        F.avg("sum_all_no_of_call").alias("avg_all_no_of_call_all"),
        F.max("sum_all_no_of_call").alias("max_all_no_of_call_all"),
        F.min("sum_all_no_of_call").alias("min_all_no_of_call_all"),
        F.count("sum_all_no_of_call").alias("count_sum_all_no_of_call_all"))

    out1 = node_from_config(geo_location_data_avg_all, sql)

    return out1


###Number of Unique Cells Used###
def l4_geo_number_unique_cell_used(l1_df_1):
    # ----- Data Availability Checks -----
    if check_empty_dfs([l1_df_1]):
        return get_spark_empty_df()

    l1_df_1 = data_non_availability_and_missing_check(df=l1_df_1, grouping="daily",
                                                                    par_col="event_partition_date",
                                                                    target_table_name="l4_geo_number_unique_cell_used")

    if check_empty_dfs([l1_df_1]):
        return get_spark_empty_df()

    # ----- Transformation -----
    spark = get_spark_session()
    l4_df_1 = l1_df_1.groupBy("event_partition_date", "weektype") \
    .agg(F.sum("mobile_no").alias("durations"))

    l4_df_2 = l4_df_1.groupBy("event_partition_date").agg(F.avg("durations").alias("avg_duration"),
                                                          F.max("durations").alias("max_duration"),
                                                          F.min("durations").alias("min_duration"),
                                                          F.sum("durations").alias("sum_duration"))

    l4_df_2.cache()

    return l4_df_2

def l4_rolling_window_de(input_df: DataFrame, config: dict):
    """
    :param input_df:
    :param config:
    :return:
    """
    if len(input_df.head(1)) == 0:
        logging.info("l4_rolling_window -> df == 0 records found in input dataset")
        return input_df
    logging.info("l4_rolling_window -> df > 0 records found in input dataset")
    ranked_lookup_enable_flag = config.get('ranked_lookup_enable_flag', "No")

    if ranked_lookup_enable_flag.lower() == 'yes':
        full_data = _get_full_data(input_df, config)
        input_df = full_data

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = """
        select 
            {}
        from input_table
        {}
    """

    features = []

    features.extend(config["partition_by"])

    read_from = config.get("read_from")
    features.append(__get_l4_time_granularity_column(read_from))
    features = list(set(features))  # Remove duplicates

    for agg_function, column_list in config["feature_list"].items():
        for each_feature_column in column_list:
            if read_from == 'l1':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(7, config["partition_by"]),
                    column_name="{}_daily_last_seven_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(14, config["partition_by"]),
                    column_name="{}_daily_last_fourteen_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(30, config["partition_by"]),
                    column_name="{}_daily_last_thirty_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(90, config["partition_by"]),
                    column_name="{}_daily_last_ninety_day".format(each_feature_column)
                ))

            elif read_from == 'l2':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(1, config["partition_by"]),
                    column_name="{}_weekly_last_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(2, config["partition_by"]),
                    column_name="{}_weekly_last_two_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(4, config["partition_by"]),
                    column_name="{}_weekly_last_four_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(12, config["partition_by"]),
                    column_name="{}_weekly_last_twelve_week".format(each_feature_column)
                ))
            else:
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(1, config["partition_by"]),
                    column_name="{}_monthly_last_month".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(3, config["partition_by"]),
                    column_name="{}_monthly_last_three_month".format(each_feature_column)
                ))

    sql_stmt = sql_stmt.format(',\n'.join(features),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()
    df = spark.sql(sql_stmt)

    return df