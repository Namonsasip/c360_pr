from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, add_event_week_and_month_from_yyyymmdd

def l1_touchpoints_contact_call_center_features(input_df,input_cust):
    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')
    spark = get_spark_session()
    input_df.registerTempTable("touchpoints_acc_oa_log")

    stmt_full = """
    select partition_date
    ,calling_pty as access_method_num
    ,count(distinct call_id) as touchpoints_sum_contact_call_center
    from touchpoints_acc_oa_log
    where calling_pty <> 'mY.SkNmSWuIJngX33pOIv0QbWc+1Zy9FzT1niNnHeJmrCnDbALKd2gc6VHvv+T1y' 
    and segment = 1
    group by 1,2
        """


    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    df_output = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'inner')


    return df_output


def l1_touchpoints_contact_myais_features(input_df,input_cust):
    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')

    spark = get_spark_session()

    input_df.registerTempTable("touchpoints_myais_distinct_sub_daily")

    stmt_full = """
            select partition_date
            ,access_method_num
            ,count(*) as touchpoints_sum_contact_myais
            from touchpoints_myais_distinct_sub_daily
            where access_method_num is not null
            group by 1,2
           """
    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    df_output = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'left')

    return df_output

def l1_touchpoints_aunjai_chatbot_features(input_df,input_cust):
    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')

    spark = get_spark_session()

    input_df.registerTempTable("online_acc_ai_chatbot_summary")

    stmt_full = """
           select partition_date
           ,mobile_number as access_method_num
           ,count(distinct request_id) as touchpoints_sum_contact_chatbot
           from online_acc_ai_chatbot_summary
           where mobile_number <> 'mY.SkNmSWuIJngX33pOIv0QbWc+1Zy9FzT1niNnHeJmrCnDbALKd2gc6VHvv+T1y' 
           group by 1,2
           """
    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    df_output = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'left')

    return df_output

def l1_touchpoints_contact_shop_features(input_df,input_cust):
    if check_empty_dfs([input_df, input_cust]):
        return get_spark_empty_df()

    input_cust = input_cust.select('access_method_num', 'subscription_identifier', 'event_partition_date')
    spark = get_spark_session()
    input_df.registerTempTable("touchpoints_acc_qmt_transaction")

    stmt_full = """
    select partition_date
    ,access_method_num
    ,sum(case when shop_type = 'ais_shop' then total_contact else 0 end) as touchpoints_sum_contact_ais_shop
    ,sum(case when shop_type = 'serenade_club' then total_contact else 0 end) as touchpoints_sum_contact_serenade_club
    ,sum(case when shop_type = 'asp' then total_contact else 0 end) as touchpoints_sum_contact_asp
    from
    (
    select partition_date
    ,access_method_num
    ,case when location_shop_name_en like 'Serenade%' then 'serenade_club'
          when location_shop_name_en like 'ASP@%' then 'asp'
          when location_shop_name_en like 'TWUP@%' then 'asp'
          else 'ais_shop' end shop_type
    ,count(distinct location_shop_name_en) as total_contact
    from touchpoints_acc_qmt_transaction
    where access_method_num is not null
    group by 1,2,3
    ) as s
    group by 1,2
        """


    df = spark.sql(stmt_full)
    df = add_event_week_and_month_from_yyyymmdd(df, 'partition_date')
    df_output = df.join(input_cust, ['access_method_num', 'event_partition_date'], 'left')


    return df_output



def dac_for_touchpoints_to_l1_intermediate_pipeline(input_df: DataFrame, cust_df: DataFrame, target_table_name: str, exception_partition=None):

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name,
                                                       exception_partitions=exception_partition)

    cust_df = data_non_availability_and_missing_check(df=cust_df, grouping="daily", par_col="event_partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df, cust_df]):
        return [get_spark_empty_df(), get_spark_empty_df()]

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            cust_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    cust_df = cust_df.filter(F.col("event_partition_date") <= min_value)

    return [input_df, cust_df]

    ################################# End Implementing Data availability checks ###############################


def dac_for_touchpoints_to_l1_pipeline_from_l0(input_df: DataFrame, target_table_name: str):

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name=target_table_name)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df

