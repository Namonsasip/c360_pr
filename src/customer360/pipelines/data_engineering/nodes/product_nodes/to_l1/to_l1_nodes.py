from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types  import *
from pyspark.sql.window import Window
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


def l1_prepaid_processing(prepaid_main_df: DataFrame,
                          prepaid_ontop_df: DataFrame,
                          customer_profile_df: DataFrame,
                          main_master_promotion_df: DataFrame,
                          ontop_master_promotion_df: DataFrame) -> DataFrame:

    prepaid_main_df = prepaid_main_df.alias("prepaid_main_df")
    prepaid_ontop_df = prepaid_ontop_df.alias("prepaid_ontop_df")
    main_master_promotion_df = main_master_promotion_df.alias("main_master_promotion_df")
    ontop_master_promotion_df = ontop_master_promotion_df.alias("ontop_master_promotion_df")

    customer_profile_columns = []

    # prepaid_main_customer_df = (prepaid_main_df
    #                                .join(customer_profile_df,
    #                                      (F.col("prepaid_main_df.access_method_num") == F.col("customer_profile_df.access_method_num")) &
    #                                      (F.to_date(F.col("prepaid_main_df.partition_date").cast(StringType()), "yyyyMMdd") >=
    #                                       F.to_date(F.col("customer_profile_df.register_date").cast(StringType()), "yyyyMMdd")),
    #                                      "inner"
    #                                      )
    #                                .withColumn("max_register_date", F.max("register_date")
    #                                            .over(Window.partitionBy("prepaid_main_df.access_method_num", "prepaid_main_df.partition_date")))
    #                                .where(F.col("register_date") == F.col("max_register_date"))
    #                                .select([f"prepaid_main_df.{i}" for i in prepaid_main_df.columns] + ["register_date"])
    #                                )

    # prepaid_main_customer_df = prepaid_main_customer_df.alias("prepaid_main_customer_df")

    prepaid_main_master_promotion_df = (prepaid_main_df
                                   .join(main_master_promotion_df, "package_id", "inner")
                                   .withColumn("offering_end_dt", F.to_date(F.col("offering_end_dt").cast(StringType()), 'yyyyMMdd'))
                                   .select([f"prepaid_main_df.{i}" for i in prepaid_main_df.columns] + [
                                        F.col("service_fee_tariff").alias("promo_package_price"),
                                        F.col("offering_desc").alias("promo_name"),
                                        F.when(F.col("offering_end_dt") > F.current_date(), F.lit("active")).otherwise(F.lit("inactive")).alias("promo_status")
                                    ])
                                   )

    prepaid_main_new_df = (prepaid_main_master_promotion_df.select(
        F.lit("pre-paid").alias("promo_charge_type"),
        F.lit("main").alias("promo_class"),
        F.when(
            (F.col("request_status") == "C") & (
                F.coalesce(F.col("trans_type").cast(StringType()), F.lit("N")).isin(["N", "F"])),
            F.col("old_package_id")
        ).otherwise(F.lit(None)).alias("previous_main_promotion_id"),
        F.when(
            (F.col("request_status") == "C") & (
                F.coalesce(F.col("trans_type").cast(StringType()), F.lit("N")).isin(["N", "F"])),
            F.to_date(F.col("effective_date").cast(StringType()), 'yyyyMMdd')
        ).otherwise(F.lit(None).cast('timestamp')).alias("previous_promo_end_dttm"),

        F.when(
            (F.col("request_status") == "C") & (
                F.coalesce(F.col("trans_type").cast(StringType()), F.lit("N")).isin(["N", "F"])),
            F.col("package_id")
        ).otherwise(F.lit(None)).alias("promo_cd"),
        F.lit(None).cast('string').alias("promo_user_cat_cd"),
        F.col("access_method_num").alias("mobile_num"),
        F.col("access_method_num").alias("access_method_num"),
        F.to_date(F.col("expire_date").cast(StringType()), 'yyyy-MM-dd').alias("promo_end_dttm"),
        F.to_date(F.col("expire_date").cast(StringType()), 'yyyy-MM-dd').alias("promo_status_end_dttm"),
        F.col("promo_package_price").alias("promo_package_price"),
        F.col("promo_name").alias("promo_name"),
        F.col("partition_date").cast(StringType()).alias("partition_date"),
        F.lit("Recurring").alias("promo_price_type"),
        F.to_date(F.col("effective_date").cast(StringType()), 'yyyy-MM-dd').alias("promo_start_dttm"),
        F.col("promo_status").alias("promo_status"),
        # F.col("register_date").alias("register_date")
    ))

    prepaid_ontop_master_promotion_df = (prepaid_ontop_df
                                         .join(ontop_master_promotion_df, F.col("prepaid_ontop_df.offering_id") == F.col("ontop_master_promotion_df.promotion_code"), "inner")
                                         .filter(F.col("recurring") == "Y")
                                         )

    prepaid_ontop_new_df = (prepaid_ontop_master_promotion_df.select(
        F.lit("pre-paid").alias("promo_charge_type"),
        F.lit("on-top").alias("promo_class"),
        F.lit(None).alias("previous_main_promotion_id"),
        F.lit(None).cast('timestamp').alias("previous_promo_end_dttm"),  # TODO: Confirm
        F.col("offering_id").alias("promo_cd"),
        F.lit(None).cast('string').alias("promo_user_cat_cd"),
        F.col("access_method_num").alias("mobile_num"),
        F.col("access_method_num").alias("access_method_num"),
        F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_end_dttm"),
        F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_status_end_dttm"),
        F.col("price").alias("promo_package_price"),
        F.col("offering_title").alias("promo_name"),
        F.col("prepaid_ontop_df.partition_date").cast(StringType()).alias("partition_date"),
        F.col("recurring").alias("promo_price_type"),
        F.to_date(F.coalesce(F.col("event_start_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_start_dttm"),
        F.lit("active").alias("promo_status")
    ))

    return prepaid_main_new_df.unionByName(prepaid_ontop_new_df)


def union_prepaid_postpaid(postpaid_df: DataFrame, prepaid_df: DataFrame) -> DataFrame:

    prepaid_df.printSchema()
    postpaid_df.printSchema()

    columns_to_select = [
        'access_method_num',
        'mobile_num',
        'national_id_card',
        'partition_date',
        'event_partition_date',
        'previous_main_promotion_id',
        'previous_promo_end_dttm',
        'promo_cd',
        'promo_charge_type',
        'promo_class',
        'promo_end_dttm',
        'promo_name',
        'promo_package_price',
        'promo_price_type',
        'promo_start_dttm',
        'promo_status',
        'promo_status_end_dttm',
        'promo_user_cat_cd',
        'start_of_month',
        'start_of_week',
        'subscription_identifier'
    ]

    prepaid_df = prepaid_df.select(columns_to_select)
    postpaid_df = postpaid_df.select(columns_to_select)

    print("Null partition prepaid_df", prepaid_df.filter(F.col("partition_date").isNull()).count())
    print("Null partition postpaid_df", postpaid_df.filter(F.col("partition_date").isNull()).count())

    return prepaid_df.unionByName(postpaid_df)
