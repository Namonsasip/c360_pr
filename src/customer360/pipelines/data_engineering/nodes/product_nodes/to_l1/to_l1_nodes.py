from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types  import *
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check


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

    # print("unioned_main_master")
    # unioned_main_master.show(10, False)
    # print("unioned_ontop_master")
    # unioned_ontop_master.show(10, False)


def join_with_master_package(
        grouped_cust_promo_df,
        prepaid_main_master_df,
        prepaid_ontop_master_df,
        postpaid_main_master_df,
        postpaid_ontop_master_df
) -> DataFrame:
    spark = get_spark_session()

    # ################################# Start Implementing Data availability checks ###############################
    # if check_empty_dfs([prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
    #                     ,postpaid_ontop_master_df]):
    #     return get_spark_empty_df()
    #
    #
    # prepaid_main_master_df = data_non_availability_and_missing_check(df=prepaid_main_master_df
    #      ,grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    # prepaid_ontop_master_df = data_non_availability_and_missing_check(df=prepaid_ontop_master_df
    #      , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    # postpaid_main_master_df = data_non_availability_and_missing_check(df=postpaid_main_master_df
    #     , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    # postpaid_ontop_master_df = data_non_availability_and_missing_check(df=postpaid_ontop_master_df
    #     , grouping="weekly", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    #
    # if check_empty_dfs([prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
    #                        , postpaid_ontop_master_df]):
    #     return get_spark_empty_df()
    #
    # ################################# End Implementing Data availability checks ###############################

    grouped_cust_promo_df.filter(F.col("access_method_num").isin([
        "H+Qh3mvgVftwZLr26rzpc+Fo9Z1CsMCmbvI6cJWmBN4N8iOuC24pob9lE0xe01FG",
        "l+PoGikDG2NhkqHvpPEORmm1O4GZES8zcJ31dEAbyaROyi5cpiVMUBZUqJGc1f9U",
        "HehqZXiNVovT6M.r+BaCT7fhKfvfsrzCrPpxZDMN3Ee.hls2hZ2vv9K+gKu9yoHa",
        "HZBYTDGI+nZiRG0gJcr7rCZxzORAI1Ge9z4NucAcWQsbh7G.k6CMLkV5+NOtXDQX",
    ])).show()

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
                
                subscription_identifier,
                national_id_card,
                access_method_num, 
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
            and to_date(cast(main_df.partition_date as string), 'yyyyMMdd') = cp_df.event_partition_date
            
        left join unioned_ontop_master ontop_df1
            on ontop_df1.promotion_code = cp_df.product_ontop_1_package_promo_cd
            and to_date(cast(ontop_df1.partition_date as string), 'yyyyMMdd') = cp_df.event_partition_date
            
        left join unioned_ontop_master ontop_df2
            on ontop_df2.promotion_code = cp_df.product_ontop_2_package_promo_cd
            and to_date(cast(ontop_df2.partition_date as string), 'yyyyMMdd') = cp_df.event_partition_date
    """)

    return result_df


def dac_product_customer_promotion_for_daily(input_df) -> DataFrame:
    """
    :return:
    """

    # ################################# Start Implementing Data availability checks #############################
    # if check_empty_dfs([input_df]):
    #     return get_spark_empty_df()
    #
    # input_df = data_non_availability_and_missing_check(df=input_df, grouping="weekly", par_col="partition_date",
    #                                                    target_table_name="l1_product_active_customer_promotion_features_daily")
    #
    # if check_empty_dfs([input_df]):
    #     return get_spark_empty_df()
    #
    # ################################# End Implementing Data availability checks ###############################

    return input_df


def dac_product_fbb_a_customer_promotion_current_for_daily(input_df) -> DataFrame:
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


def union_with_prepaid(prepaid_main_df: DataFrame, postpaid_df: DataFrame) -> DataFrame:

    prepaid_main_new_df = prepaid_main_df.select(
        F.lit("pre-paid").alias("promo_charge_type"),
        F.lit("main").alias("promo_class"),
        F.lit(None).alias("previous_main_promotion_id"),
        F.when(
            (F.col("request_status") == "C") & (
                F.coalesce(F.col("trans_type").cast(StringType()), F.lit("N")).isin(["N", "F"])),
            F.col("effective_date")
        ).otherwise(F.lit(None)).alias("previous_promo_end_dttm"),
        F.when(
            (F.col("request_status") == "C") & (
                F.coalesce(F.col("trans_type").cast(StringType()), F.lit("N")).isin(["N", "F"])),
            F.col("package_id")
        ).otherwise(F.lit(None)).alias("promo_cd"),
        F.lit(None).alias("promo_user_cat_cd"),
        F.col("access_method_num").alias("mobile_num"),
        F.lit(None).alias("crm_subscription_id"),   # Temporary - will be replaced from customer profile table
        F.col("expire_date").alias("promo_end_dttm"),
        F.col("expire_date").alias("promo_status_end_dttm"),
        F.lit(None).alias("promo_package_price"),   # Temporary - will be replaced from product promotion table
        F.lit(None).alias("promo_name"),            # Temporary - will be replaced from product promotion table
        F.col("partition_date").cast(StringType()).alias("partition_date"),
        F.lit("Recurring").alias("promo_price_type"),
        F.col("effective_date").alias("promo_start_dttm"),
        # F.when(
        #     F.to_date(F.col("expire_date").cast(StringType()), 'yyyy-MM-dd') >
        #     F.to_date(F.col("expire_date").cast(StringType()), 'yyyy-MM-dd')
        # ),
        F.lit("active").alias("promo_status"),
    )

    # prepaid_main_df = prepaid_main_df.select(
    #     F.lit("pre-paid").alias("promo_charge_type"),
    #     F.lit("main").alias("promo_class"),
    #     F.when(
    #         (F.col("request_status") == "C") & (F.coalesce(F.col("trans_type"), "N").isin(["N", "F"])),
    #         F.col("old_package_id")
    #     ).otherwise(F.lit(None)).alias("old_package_id"),
    #     F.when(
    #         (F.col("request_status") == "C") & (F.coalesce(F.col("trans_type"), "N").isin(["N", "F"])),
    #         F.col("effective_date")
    #     ).otherwise(F.lit(None)).alias("effective_date"),
    #     F.when(
    #         (F.col("request_status") == "C") & (F.coalesce(F.col("trans_type"), "N").isin(["N", "F"])),
    #         F.col("package_id")
    #     ).otherwise(F.lit(None)).alias("package_id"),
    #     F.lit(None).alias("promo_user_cat_cd"),
    #     F.col("access_method_num").alias("mobile_num"),
    #     F.col("expire_date").alias("promo_end_dttm"),
    #     F.col("expire_date").alias("promo_status_end_dttm"),
    #     F.col("partition_date"),
    #     F.lit("recurring").alias("promo_price_type"),
    #     F.col("effective_date").alias("promo_start_dttm")
    # )

    postpaid_new_df = postpaid_df.select(
        "promo_charge_type",
        "promo_class",
        "previous_main_promotion_id",
        "previous_promo_end_dttm",
        "promo_cd",
        "promo_user_cat_cd",
        "mobile_num",
        "crm_subscription_id",
        "promo_end_dttm",
        "promo_status_end_dttm",
        "promo_package_price",
        "promo_name",
        F.col("partition_date").cast(StringType()).alias("partition_date"),
        "promo_price_type",
        "promo_start_dttm",
        "promo_status"
    )

    print(prepaid_main_new_df.columns)
    print(postpaid_new_df.columns)

    return prepaid_main_new_df.unionByName(postpaid_new_df)
