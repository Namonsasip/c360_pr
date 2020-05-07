from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols
from pyspark.sql import functions as F
from pyspark.sql.types  import *
from src.customer360.utilities.spark_util import get_spark_empty_df

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, execute_sql


def union_master_package_table(
        prepaid_main_master_df: DataFrame,
        prepaid_ontop_master_df: DataFrame,
        postpaid_main_master_df: DataFrame,
        postpaid_ontop_master_df: DataFrame
) -> None:
    spark = get_spark_session()
    prepaid_main_master_df.createOrReplaceTempView("prepaid_main_master_df")
    prepaid_ontop_master_df.createOrReplaceTempView("prepaid_ontop_master_df")
    postpaid_main_master_df.createOrReplaceTempView("postpaid_main_master_df")
    postpaid_ontop_master_df.createOrReplaceTempView("postpaid_ontop_master_df")

    unioned_main_master = spark.sql("""
        select package_id as promotion_code,
                main_package_4g_type_tariff as data_speed,
                null as data_mb_in_pack,
                package_service_type as service_group,
                null as tv_vdo_bundling,
                partition_date 
        from prepaid_main_master_df
        union all
        select promotion_code,
                data_speed,
                data_mb_in_pack,
                service_group,
                tv_vdo_bundling,
                partition_date 
        from postpaid_main_master_df
    """)
    unioned_main_master.createOrReplaceTempView("unioned_main_master")
    unioned_main_master.cache()

    unioned_ontop_master = spark.sql("""
        select 
            promotion_code,
            mm_data_speed,
            data_quota,
            null as cmm_sms_in_pack,
            null as cmm_voice_unit_quota_in_pack,
            null as cmm_voice_quota_in_pack,
            package_name,
            package_type,
            mm_types,
            partition_date
        from prepaid_ontop_master_df
        union all
        select
            promotion_code,
            mm_data_speed,
            data_quota,
            cmm_sms_in_pack,
            cmm_voice_unit_quota_in_pack,
            cmm_voice_quota_in_pack,
            null as package_name,
            package_type,
            mm_types,
            partition_date
        from postpaid_ontop_master_df
    """)
    unioned_ontop_master.createOrReplaceTempView("unioned_ontop_master")
    unioned_ontop_master.cache()


def join_with_master_package(
        grouped_cust_promo_df,
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
) -> DataFrame:
    spark = get_spark_session()

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
                        ,postpaid_ontop_master_df]):
        return get_spark_empty_df()


    prepaid_main_master_df = data_non_availability_and_missing_check(df=prepaid_main_master_df
         ,grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    prepaid_ontop_master_df = data_non_availability_and_missing_check(df=prepaid_ontop_master_df
         , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    postpaid_main_master_df = data_non_availability_and_missing_check(df=postpaid_main_master_df
        , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    postpaid_ontop_master_df = data_non_availability_and_missing_check(df=postpaid_ontop_master_df
        , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")

    if check_empty_dfs([prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
                           , postpaid_ontop_master_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################


    min_value = union_dataframes_with_missing_cols(
        [
            grouped_cust_promo_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            prepaid_main_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()),'yyyyMMdd').alias("max_date")),
            prepaid_ontop_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()),'yyyyMMdd').alias("max_date")),
            postpaid_main_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()),'yyyyMMdd').alias("max_date")),
            postpaid_ontop_master_df.select(F.to_date(F.max(F.col("partition_date")).cast(StringType()),'yyyyMMdd').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    grouped_cust_promo_df = grouped_cust_promo_df.filter(F.col("event_partition_date") <= min_value)
    grouped_cust_promo_df.createOrReplaceTempView("grouped_cust_promo_df")

    prepaid_main_master_df = prepaid_main_master_df.filter(F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd') <= min_value)
    prepaid_ontop_master_df = prepaid_ontop_master_df.filter(F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd') <= min_value)
    postpaid_main_master_df = postpaid_main_master_df.filter(F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd') <= min_value)
    postpaid_ontop_master_df = postpaid_ontop_master_df.filter(F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd') <= min_value)

    union_master_package_table(
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
    )

    result_df = spark.sql("""
        with flatten_cust_promo_df as (
            select 
                product_postpaid_promo_boolean,
                product_prepaid_promo_boolean,
                
                main_package_features.promo_name as product_main_package_name,
                main_package_features.promo_package_price as product_main_package_price,
                main_package_features.promo_cd as product_main_package_promo_cd,
                main_package_features.promo_end_dttm as product_main_package_validity,
                
                main_package_features.previous_main_promotion_id as product_prev_main_package_name,
                main_package_features.previous_promo_end_dttm as product_prev_main_package_end_dttm,
                
                ontop_package_features[0].promo_name as product_ontop_1_package_name,
                ontop_package_features[0].promo_package_price as product_ontop_1_package_price,
                ontop_package_features[0].promo_cd as product_ontop_1_package_promo_cd,
                ontop_package_features[0].promo_end_dttm as product_ontop_1_package_validity,
                
                ontop_package_features[1].promo_name as product_ontop_2_package_name,
                ontop_package_features[1].promo_package_price as product_ontop_2_package_price,
                ontop_package_features[1].promo_cd as product_ontop_2_package_promo_cd,
                ontop_package_features[1].promo_end_dttm as product_ontop_2_package_validity,
                
                product_main_package_count,
                product_total_main_package_price,
                
                product_ontop_package_count,
                product_total_ontop_package_price,
                
                product_fbb_flag,
                product_landline_flag,
                product_mobile_flag,
                
                crm_subscription_id as subscription_identifier, 
                partition_date,
                start_of_week,
                start_of_month,
                event_partition_date
            from grouped_cust_promo_df
        )
        select cp_df.*,
                main_df.data_speed as product_main_data_throughput_limit,
                ontop_df1.mm_data_speed as product_ontop_1_data_throughput_limit,
                ontop_df2.mm_data_speed as product_ontop_2_data_throughput_limit,
                
                main_df.data_mb_in_pack as product_main_data_traffic_limit,
                ontop_df1.data_quota as product_ontop_1_data_traffic_limit,
                ontop_df2.data_quota as product_ontop_2_data_traffic_limit,
                
                boolean(datediff(event_partition_date, product_prev_main_package_end_dttm) < 30) as product_main_package_changed_last_month,
                
                boolean(datediff(event_partition_date, product_prev_main_package_end_dttm) < 60 and
                        datediff(event_partition_date, product_prev_main_package_end_dttm) >= 30) as product_main_package_changed_last_two_month,
                        
                boolean(datediff(event_partition_date, product_prev_main_package_end_dttm) < 90 and
                        datediff(event_partition_date, product_prev_main_package_end_dttm) >= 60) as product_main_package_changed_last_three_month,
                        
                boolean(datediff(event_partition_date, product_prev_main_package_end_dttm) < 180 and
                        datediff(event_partition_date, product_prev_main_package_end_dttm) >= 90) as product_main_package_changed_last_six_month,
                
                boolean(main_df.tv_vdo_bundling is not null) as product_paytv_flag,
                boolean(lower(main_df.service_group) = 'data') as product_mobile_data_only_flag
                
        from flatten_cust_promo_df cp_df
        left join unioned_main_master main_df
            on main_df.promotion_code = cp_df.product_main_package_promo_cd
            and main_df.partition_date = cp_df.partition_date
            
        left join unioned_ontop_master ontop_df1
            on ontop_df1.promotion_code = cp_df.product_ontop_1_package_promo_cd
            and ontop_df1.partition_date = cp_df.partition_date
            
        left join unioned_ontop_master ontop_df2
            on ontop_df2.promotion_code = cp_df.product_ontop_2_package_promo_cd
            and ontop_df2.partition_date = cp_df.partition_date
    """)

    return result_df


def product_customer_promotion_for_daily(input_df) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="partition_date",
                                                       target_table_name="l1_product_active_customer_promotion_features_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df

def product_fbb_a_customer_promotion_current_for_daily(input_df) -> DataFrame:
    """
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                            target_table_name="l1_product_active_fbb_customer_features_daily")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df
