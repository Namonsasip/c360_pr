from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, \
    check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.pipelines.data_engineering.nodes.product_nodes.to_l1.to_l1_nodes import union_master_package_table
from src.customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from src.customer360.utilities.re_usable_functions import add_start_of_week_and_month


def get_activated_deactivated_features(
        cust_promo_df,
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
) -> DataFrame:
    spark = get_spark_session()


    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([cust_promo_df, prepaid_main_master_df
                           , prepaid_ontop_master_df, postpaid_main_master_df
                        ,postpaid_ontop_master_df]):
        return get_spark_empty_df()

    cust_promo_df = data_non_availability_and_missing_check(df=cust_promo_df
         , grouping="daily", par_col="event_partition_date",target_table_name="l2_product_activated_deactivated_features_weekly")

    # if check_empty_dfs([cust_promo_df,prepaid_main_master_df,prepaid_ontop_master_df,postpaid_main_master_df
    #                     ,postpaid_ontop_master_df]):
    #     return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    # min_value = union_dataframes_with_missing_cols(
    #     [
    #         cust_promo_df.select(F.max(F.col("event_partition_date")).alias("max_date")),
    #         prepaid_main_master_df.select(
    #             F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
    #         postpaid_main_master_df.select(
    #             F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
    #         prepaid_ontop_master_df.select(
    #             F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
    #         postpaid_ontop_master_df.select(
    #             F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date"))
    #     ]
    # ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date
    #
    # cust_promo_df = cust_promo_df.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    #
    # prepaid_main_master_df = prepaid_main_master_df.filter(
    #     F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    # prepaid_ontop_master_df = prepaid_ontop_master_df.filter(
    #     F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    # postpaid_main_master_df = postpaid_main_master_df.filter(
    #     F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    # postpaid_ontop_master_df = postpaid_ontop_master_df.filter(
    #     F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    # Since all tables are snapshot tables, computing groupBy on start_of_week could possibly create duplicate values
    # in features so we must keep the start_of_week only (Monday)

    prepaid_main_master_df = add_start_of_week_and_month(
        prepaid_main_master_df.withColumn(
            "partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')),
        "partition_date")

    prepaid_ontop_master_df = add_start_of_week_and_month(
        prepaid_ontop_master_df.withColumn(
            "partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')),
        "partition_date")

    postpaid_main_master_df = add_start_of_week_and_month(
        postpaid_main_master_df.withColumn(
            "partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')),
        "partition_date")

    postpaid_ontop_master_df = add_start_of_week_and_month(
        postpaid_ontop_master_df.withColumn(
            "partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')),
        "partition_date")

    cust_promo_df = cust_promo_df.filter(F.col("event_partition_date") == F.col("start_of_week"))
    # prepaid_main_master_df = prepaid_main_master_df.filter(F.col("partition_date") == F.col("start_of_week"))
    # prepaid_ontop_master_df = prepaid_ontop_master_df.filter(F.col("partition_date") == F.col("start_of_week"))
    # postpaid_main_master_df = postpaid_main_master_df.filter(F.col("partition_date") == F.col("start_of_week"))
    # postpaid_ontop_master_df = postpaid_ontop_master_df.filter(F.col("partition_date") == F.col("start_of_week"))

    drop_cols = ["event_partition_date", "start_of_week", "start_of_month"]
    cust_promo_df = cust_promo_df.drop(*drop_cols)
    # prepaid_main_master_df = prepaid_main_master_df.drop(*drop_cols)
    # prepaid_ontop_master_df = prepaid_ontop_master_df.drop(*drop_cols)
    # postpaid_main_master_df = postpaid_main_master_df.drop(*drop_cols)
    # postpaid_ontop_master_df = postpaid_ontop_master_df.drop(*drop_cols)

    cust_promo_df.createOrReplaceTempView("cust_promo_df")

    union_master_package_table(
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
    )

    result_df = spark.sql("""
        with enriched_cust_promo_df as (
            select
                cp_df.subscription_identifier,
                cp_df.start_of_week,
                cp_df.promo_end_dttm as promo_end_dttm,
                cp_df.promo_status_end_dttm as promo_status_end_dttm,
                cp_df.promo_start_dttm as promo_start_dttm,
                cp_df.promo_class as promo_class,
                cp_df.promo_package_price as promo_package_price,
                cp_df.promo_name as promo_name,
                cp_df.promo_type as promo_type,
                cp_df.promo_status as promo_status,
                
                ontop_df.mm_types as mm_types,
                
                regexp_extract(lower(ontop_df.data_quota), '[0-9]*([a-z]*)', 1) as data_unit,
                cast(regexp_extract(lower(ontop_df.data_quota), '([0-9]*)[a-z]*', 1) as double) as data_amount,
        
                coalesce(cast(ontop_df.cmm_sms_in_pack as int), 
                         cast(regexp_extract(lower(ontop_df.package_name), '([0-9]+)sms', 1) as int)) as sms_amount,
        
                coalesce(case when lower(ontop_df.cmm_voice_unit_quota_in_pack) = 'minutes' 
                        then cast(ontop_df.cmm_voice_quota_in_pack as int)
                        else 0 end, 
                        cast(regexp_extract(lower(ontop_df.package_name), '([0-9]+)min', 1) as int)) as voice_minutes,
                         
                boolean(lower(cp_df.promo_price_type) != 'recurring') as one_off_promo_flag,
                boolean(lower(cp_df.promo_price_type) = 'recurring') as rolling_promo_flag,
                boolean(lower(ontop_df.package_type) = 'combo') as bundle_flag,
                
                case when lower(promo_class) = 'value added services'
                then row_number() over (partition by cp_df.old_subscription_identifier, cp_df.promo_class, cp_df.partition_date
                                    order by cp_df.promo_start_dttm desc) 
                else null end as vas_package_purchase_time_rank,
                
                case when lower(promo_class) = 'main' and lower(main_df.service_group) like '%data%' 
                then row_number() over (partition by cp_df.old_subscription_identifier, cp_df.promo_class, cp_df.partition_date
                                    order by cp_df.promo_start_dttm desc) 
                else null end as data_package_purchase_time_rank,
                
                case when lower(promo_class) = 'main' and lower(main_df.service_group) like '%voice%' 
                then row_number() over (partition by cp_df.old_subscription_identifier, cp_df.promo_class, cp_df.partition_date
                                    order by cp_df.promo_start_dttm desc) 
                else null end as voice_package_purchase_time_rank
                    
            from cust_promo_df cp_df
            left join unioned_main_master main_df
                on main_df.promotion_code = cp_df.promo_cd
                
            left join unioned_ontop_master ontop_df
                on ontop_df.promotion_code = cp_df.promo_cd
        ),
        -- Intermediate activate/deactivate type of features (see confluence for feature dictionary)
        int_act_deact_features as (
            select 
                subscription_identifier,
                start_of_week,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(promo_class) = 'on-top'
                    then promo_package_price else 0 end) as product_total_activated_ontop_package_price,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(promo_class) = 'main'
                    then promo_package_price else 0 end) as product_total_activated_main_package_price,
                    
                -- DEACTIVATED FEATURES
                sum(case when
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_sms_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_voice_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then sms_amount else 0 end) as product_deactivated_sms_volume_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then case when data_unit = 'gb' or data_unit is null then data_amount
                              when data_unit = 'mb' then data_amount/1000
                              else 0 end
                    else 0 end) as product_deactivated_data_volume_gb_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then voice_minutes else 0 end) as product_deactivated_voice_minutes_ontop_packages,
                    
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and one_off_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_oneoff_bundle_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and rolling_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_rolling_bundle_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_rolling_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_rolling_voice_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_one_off_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_deactivated_one_off_voice_ontop_packages,
                
                -- ACTIVATED FEATURES
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_sms_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_voice_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%message%' or lower(mm_types) like '%sms%') 
                    and lower(promo_class) = 'on-top'
                    then sms_amount else 0 end) as product_activated_sms_volume_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and lower(promo_class) = 'on-top'
                    then case when data_unit = 'gb' or data_unit is null then data_amount
                              when data_unit = 'mb' then data_amount/1000
                              else 0 end
                    else 0 end) as product_activated_data_volume_gb_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and lower(promo_class) = 'on-top'
                    then voice_minutes else 0 end) as product_activated_voice_minutes_ontop_packages,
                    
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and one_off_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_oneoff_bundle_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and rolling_promo_flag 
                    and bundle_flag 
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_rolling_bundle_ontop_packages,
                    
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_rolling_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and rolling_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_rolling_voice_ontop_packages,
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(mm_types) like '%data%' 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_one_off_data_ontop_packages,
                
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and (lower(mm_types) like '%minute%' or lower(mm_types) like '%voice%') 
                    and one_off_promo_flag
                    and lower(promo_class) = 'on-top'
                    then 1 else 0 end) as product_activated_one_off_voice_ontop_packages,
                    
                max(case when data_package_purchase_time_rank = 1
                    then promo_package_price
                    else null end) as product_last_activated_main_data_promo_price,
                    
                max(case when voice_package_purchase_time_rank = 1
                    then promo_package_price
                    else null end) as product_last_activated_main_voice_promo_price,
                
                -- VAS related features
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(promo_class) = 'value added services'
                    then 1 else 0 end) as product_activated_vas_packages,
                    
                sum(case when 
                    (to_date(promo_start_dttm) between start_of_week and date_add(start_of_week, 7))
                    and lower(promo_class) = 'value added services'
                    then promo_package_price else 0 end) as product_total_activated_vas_packages_price,
                    
                    
                max(case when vas_package_purchase_time_rank = 1
                    then promo_package_price
                    else null end) as product_last_activated_vas_price,
                
                max(case when vas_package_purchase_time_rank = 1
                    then promo_name
                    else null end) as product_last_activated_vas_promo_name,
                
                -- DEACTIVATION DUE TO EXPIRED REASONS
                sum (case when 
                    (to_date(promo_status_end_dttm) between start_of_week and date_add (start_of_week, 7) )
                    and lower(promo_type) = 'promotion'
                    and lower(promo_status) = 'inactive'
                    and to_date(promo_end_dttm) = to_date(promo_status_end_dttm + interval 1 second)
                    then 1 else 0 end) as product_deactivated_package_due_to_expired_reason
                        
            from enriched_cust_promo_df
            group by subscription_identifier, start_of_week
        )
        select
            *,
            product_deactivated_one_off_data_ontop_packages/product_deactivated_rolling_data_ontop_packages as product_ratio_of_deactivated_one_off_to_rolling_data_ontop,
            product_deactivated_one_off_voice_ontop_packages/product_deactivated_rolling_voice_ontop_packages as product_ratio_of_deactivated_one_off_to_rolling_voice_ontop,
            
            product_activated_one_off_data_ontop_packages/product_activated_rolling_data_ontop_packages as product_ratio_of_activated_one_off_to_rolling_data_ontop,
            product_activated_one_off_voice_ontop_packages/product_activated_rolling_voice_ontop_packages as product_ratio_of_activated_one_off_to_rolling_voice_ontop
        from int_act_deact_features
    """)

    return result_df


def get_product_package_promotion_group_tariff_weekly(source_df: DataFrame) -> DataFrame:
    """
    :param source_df:
    :return:
    """
    if check_empty_dfs([source_df]):
        return get_spark_empty_df()

    source_df = source_df.withColumn(
        "start_of_week",
        F.to_date(F.date_trunc("week", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')))
    )

    source_df = source_df.select("start_of_week", "promotion_group_tariff", "package_id").distinct()

    return source_df
