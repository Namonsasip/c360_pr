from pyspark.sql import DataFrame

from src.customer360.utilities.spark_util import get_spark_session


def __union_master_package_table(
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

    grouped_cust_promo_df.createOrReplaceTempView("grouped_cust_promo_df")

    __union_master_package_table(
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
    )

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


def get_activated_deactivated_number(
        cust_promo_df,
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df,
) -> DataFrame:
    spark = get_spark_session()

    cust_promo_df.createOrReplaceTempView("cust_promo_df")

    __union_master_package_table(
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
    )

    result_df = spark.sql("""
        with enriched_cust_promo_df as (
            select
                cp_df.crm_subscription_id as subscription_identifier,
                cp_df.start_of_week as start_of_week,
                cp_df.promo_status_end_dttm as promo_status_end_dttm,
                cp_df.promo_start_dttm as promo_start_dttm
                cp_df.promo_class as promo_class,
                
                ontop_df.mm_types as mm_types,
                
                to_date(cast(partition_date as string), 'yyyyMMdd') as event_partition_date,
                date_trunc('week', to_date(cast(partition_date as string), 'yyyyMMdd')) as start_of_week,
                date_trunc('month', to_date(cast(partition_date as string), 'yyyyMMdd')) as start_of_month,
                
                lower(regexp_extract(data_quota, '[0-9]*([a-z]*)', 1)) as data_unit,
                cast(regexp_extract(data_quota, '([0-9]*)[a-z]*', 1) as double) as data_amount,
                
                coalesce(cast(cmm_sms_in_pack as int), 
                         cast(regexp_extract(lower(package_name), '([0-9]+)sms', 1) as int)) as sms_amount,
                
                coalesce(case when lower(cmm_voice_unit_quota_in_pack) = 'minutes' 
                         then cast(cmm_voice_quota_in_pack as int)
                         else 0 end, 
                         cast(regexp_extract(lower(package_name), '([0-9]+)min', 1) as int)) as voice_minutes,
                         
                boolean(lower(promo_price_type) != 'recurring') as one_off_promo_flag,
                boolean(lower(promo_price_type) = 'recurring') as rolling_promo_flag,
                boolean(lower(package_type) = 'combo') as bundle_flag
                    
            from cust_promo_df cp_df
            left join unioned_main_master main_df
                on main_df.promotion_code = cp_df.promo_cd
                and main_df.partition_date = cp_df.partition_date
                
            left join unioned_ontop_master ontop_df
                on ontop_df.promotion_code = cp_df.promo_cd
                and ontop_df.partition_date = cp_df.partition_date
        )
        int_act_deact_features as (
            select 
                subscription_identifier, 
                start_of_week,
                
                -- DEACTIVATED FEATURES
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_sms_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_voice_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then sms_amount else 0 end) as deactivated_sms_volume_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then case when data_unit = 'gb' or data_unit is null then data_amount
                              when data_unit = 'mb' then data_amount/1000
                              else 0 end
                    else 0 end) as deactivated_data_volume_gb_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then voice_minutes else 0 end) as deactivated_voice_minutes_ontop_packages,
                    
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and one_off_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_oneoff_bundle_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and rolling_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_rolling_bundle_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_rolling_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_rolling_voice_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_one_off_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as deactivated_one_off_voice_ontop_packages,
                
                -- ACTIVATED FEATURES
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_sms_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_voice_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then sms_amount else 0 end) as activated_sms_volume_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then case when data_unit = 'gb' or data_unit is null then data_amount
                              when data_unit = 'mb' then data_amount/1000
                              else 0 end
                    else 0 end) as activated_data_volume_gb_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then voice_minutes else 0 end) as activated_voice_minutes_ontop_packages,
                    
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and one_off_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_oneoff_bundle_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and rolling_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_rolling_bundle_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_rolling_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_rolling_voice_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_one_off_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as activated_one_off_voice_ontop_packages
                    
            from enriched_cust_promo_df
            group by crm_subscription_id, start_of_week
        )
        select
            *,
            deactivated_one_off_data_ontop_packages/deactivated_rolling_data_ontop_packages as ratio_of_deactivated_one_off_to_rolling_data_ontop,
            deactivated_one_off_voice_ontop_packages/deactivated_rolling_voice_ontop_packages as ratio_of_deactivated_one_off_to_rolling_voice_ontop,
            
            activated_one_off_data_ontop_packages/activated_rolling_data_ontop_packages as ratio_of_activated_one_off_to_rolling_data_ontop,
            activated_one_off_voice_ontop_packages/activated_rolling_voice_ontop_packages as ratio_of_activated_one_off_to_rolling_voice_ontop
        from int_act_deact_features
    """)

    return result_df
