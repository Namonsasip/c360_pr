import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1.to_l1_nodes import gen_max_sql
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics
from pyspark.sql import Window

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