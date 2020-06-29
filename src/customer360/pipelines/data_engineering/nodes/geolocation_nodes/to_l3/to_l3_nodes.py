import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics
from customer360.utilities.spark_util import get_spark_session
conf = os.getenv("CONF", "base")
run_mode = os.getenv("DATA_AVAILABILITY_CHECKS", None)
log = logging.getLogger(__name__)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def l3_geo_time_spent_by_location_monthly(df,sql):
    df = massive_processing_monthly(df, sql, "l3_geo_time_spent_by_location_monthly", 'start_of_month')
    return df

def l3_geo_area_from_ais_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

def l3_geo_area_from_competitor_store_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

##==============================Update 2020-06-12 by Thatt529==========================================##

###total_distance_km###
def l3_geo_total_distance_km_monthly(df,sql):
    df = node_from_config(df, sql)
    return df

###Traffic_fav_location###
def l3_geo_use_Share_traffic_monthly(df,sql):
    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "start_of_week"))).drop( 'start_of_week')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2

###feature_sum_voice_location###
def l3_geo_call_location_home_work_monthly(df,sql):
    l3_df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', "event_partition_date"))).drop( 'event_partition_date')
    l3_df_2 = node_from_config(l3_df,sql)
    return l3_df_2

## ==============================Update 2020-06-15 by Thatt529==========================================##

###Top_3_cells_on_voice_usage###
def l3_geo_top3_cells_on_voice_usage(df,sql):
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
        ,row_number() over (partition by imsi,start_of_month order by total_call desc) as rnk
        ,start_of_month
        from top3_cells_on_voice_usage
        """
    df = spark.sql(sql_query)
    df.cache()
    df = df.where("rnk <= 3")

    return df


def l3_geo_distance_top_call(df):
    df = df.groupBy("imsi", "start_of_month").agg(
        F.max("top_distance_km").alias("max_distance_top_call"),
        F.min("top_distance_km").alias("min_distance_top_call"),
        F.avg("top_distance_km").alias("avg_distance_top_call"),
        F.when(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2))).cast("string") == 'NaN', 0).otherwise(F.sqrt(F.avg(df.top_distance_km * df.top_distance_km) - F.pow(F.avg(df.top_distance_km),F.lit(2)))).alias("sd_distance_top_call"),
        F.sun("top_distance_km").alias("sum_distance_top_call"))
    return df



# 47 The favourite location
def l3_the_favourite_locations_monthly(l1_df_the_favourite_location_daily):
    ### config
    spark = get_spark_session()
    l1_df_the_favourite_location_daily.createOrReplaceTempView('l1_df_the_favourite_location_daily')
    sql_query = """
    select
    mobile_no
    ,start_of_month
    ,lac	
    ,ci
    ,sum(vol_3g) as vol_3g
    ,sum(vol_4g) as vol_4g
    ,sum(vol_5g) as vol_5g
    from l1_df_the_favourite_location_daily
    group by 1,2,3,4
    order by 2,1,3,4
    """
    l3 = spark.sql(sql_query)
    return l3

def massive_processing_monthly(data_frame: DataFrame, sql, output_df_catalog, partition_col) -> DataFrame:
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
    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new
    first_item = add_list[-1]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(f.col(partition_col).isin(*[curr_item]))
        small_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, small_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(f.col(partition_col).isin(*[first_item]))
    return_df = node_from_config(return_df, sql)
    return return_df