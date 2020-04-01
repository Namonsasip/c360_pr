from pyspark.sql import DataFrame

from src.customer360.utilities.spark_util import get_spark_session


def join_with_master_package(
        grouped_cust_promo_df,
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df,
) -> DataFrame:
    spark = get_spark_session()

    grouped_cust_promo_df.createOrReplaceTempView("grouped_cust_promo_df")
    prepaid_main_master_df.createOrReplaceTempView("prepaid_main_master_df")
    prepaid_ontop_master_df.createOrReplaceTempView("prepaid_ontop_master_df")
    postpaid_main_master_df.createOrReplaceTempView("postpaid_main_master_df")
    postpaid_ontop_master_df.createOrReplaceTempView("postpaid_ontop_master_df")

    result_df = spark.sql("""
        with flatten_cust_promo_df as (
            select 
                postpaid_promo_boolean,
                prepaid_promo_boolean,
                
                main_package_features.promo_name as main_package_name,
                main_package_features.promo_package_price as main_package_price,
                main_package_features.promo_cd as main_package_promo_cd,
                main_package_features.promo_end_dttm as main_package_validity,
                
                main_package_features.previous_main_promotion_id as prev_main_package_name,
                main_package_features.previous_promo_end_dttm as prev_main_package_end_dttm,
                
                ontop_package_features.promo_name as ontop_package_name,
                ontop_package_features.promo_package_price as ontop_package_price,
                ontop_package_features.promo_cd as ontop_package_promo_cd,
                ontop_package_features.promo_end_dttm as ontop_package_validity,
                
                landline_flag,
                mobile_flag,
                
                crm_subscription_id as subscription_identifier, 
                partition_date,
                start_of_week,
                start_of_month,
                event_partition_date
            from grouped_cust_promo_df
        )
        select cp_df.*,
                coalesce(pre_m_df.main_package_4g_type_tariff, post_m_df.data_speed) as main_data_throughput_limit,
                coalesce(pre_ontop_df.mm_data_speed, post_ontop_df.mm_data_speed) as ontop_data_throughput_limit,
                
                post_m_df.data_mb_in_pack as main_data_traffic_limit,
                coalesce(pre_ontop_df.data_quota, post_ontop_df.data_quota) as ontop_data_traffic_limit,
                
                coalesce(boolean(lower(post_m_df.fbb_yn) = 'y'), false) as fbb_flag,
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 30) as main_package_changed_last_month,
                
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 60 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 30) as main_package_changed_last_two_month,
                        
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 90 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 60) as main_package_changed_last_three_month,
                        
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 180 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 90) as main_package_changed_last_six_month,
                
                boolean(post_m_df.tv_vdo_bundling is not null) as paytv_flag,
                boolean(lower(pre_m_df.package_service_type) = 'data' or lower(post_m_df.service_group) = 'data') as mobile_data_only_flag
                
        from flatten_cust_promo_df cp_df
        left join prepaid_main_master_df pre_m_df
            on pre_m_df.package_id = cp_df.main_package_promo_cd
            and pre_m_df.partition_date = cp_df.partition_date
        left join prepaid_ontop_master_df pre_ontop_df
            on pre_ontop_df.promotion_code = cp_df.ontop_package_promo_cd
            and pre_ontop_df.partition_date = cp_df.partition_date
        left join postpaid_main_master_df post_m_df
            on post_m_df.promotion_code = cp_df.main_package_promo_cd
            and post_m_df.partition_date = cp_df.partition_date
        left join postpaid_ontop_master_df post_ontop_df
            on post_ontop_df.promotion_code = cp_df.ontop_package_promo_cd
            and post_ontop_df.partition_date = cp_df.partition_date
    """)

    return result_df

