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
        select promotion_code,
                mm_data_speed,
                data_quota,
               partition_date
        from prepaid_ontop_master_df
        union all
        select
            promotion_code,
            mm_data_speed,
            data_quota,
            partition_date
        from postpaid_ontop_master_df
    """)
    unioned_ontop_master.createOrReplaceTempView("unioned_ontop_master")
    unioned_ontop_master.cache()

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
                
                ontop_package_features[0].promo_name as ontop_1_package_name,
                ontop_package_features[0].promo_package_price as ontop_1_package_price,
                ontop_package_features[0].promo_cd as ontop_1_package_promo_cd,
                ontop_package_features[0].promo_end_dttm as ontop_1_package_validity,
                
                ontop_package_features[1].promo_name as ontop_2_package_name,
                ontop_package_features[1].promo_package_price as ontop_2_package_price,
                ontop_package_features[1].promo_cd as ontop_2_package_promo_cd,
                ontop_package_features[1].promo_end_dttm as ontop_2_package_validity,
                
                main_package_count,
                total_main_package_price,
                
                ontop_package_count,
                total_ontop_package_price,
                
                fbb_flag,
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
                main_df.data_speed as main_data_throughput_limit,
                ontop_df1.mm_data_speed as ontop_1_data_throughput_limit,
                ontop_df2.mm_data_speed as ontop_2_data_throughput_limit,
                
                main_df.data_mb_in_pack as main_data_traffic_limit,
                ontop_df1.data_quota as ontop_1_data_traffic_limit,
                ontop_df2.data_quota as ontop_2_data_traffic_limit,
                
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 30) as main_package_changed_last_month,
                
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 60 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 30) as main_package_changed_last_two_month,
                        
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 90 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 60) as main_package_changed_last_three_month,
                        
                boolean(datediff(event_partition_date, prev_main_package_end_dttm) < 180 and
                        datediff(event_partition_date, prev_main_package_end_dttm) >= 90) as main_package_changed_last_six_month,
                
                boolean(main_df.tv_vdo_bundling is not null) as paytv_flag,
                boolean(lower(main_df.service_group) = 'data') as mobile_data_only_flag
                
        from flatten_cust_promo_df cp_df
        left join unioned_main_master main_df
            on main_df.promotion_code = cp_df.main_package_promo_cd
            and main_df.partition_date = cp_df.partition_date
            
        left join unioned_ontop_master ontop_df1
            on ontop_df1.promotion_code = cp_df.ontop_1_package_promo_cd
            and ontop_df1.partition_date = cp_df.partition_date
            
        left join unioned_ontop_master ontop_df2
            on ontop_df2.promotion_code = cp_df.ontop_2_package_promo_cd
            and ontop_df2.partition_date = cp_df.partition_date
    """)

    return result_df

