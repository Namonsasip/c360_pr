from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, l1_massive_processing


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
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            prepaid_ontop_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            postpaid_main_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            postpaid_ontop_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    grouped_cust_promo_df = grouped_cust_promo_df.filter(F.col("event_partition_date") <= min_value)
    grouped_cust_promo_df.createOrReplaceTempView("grouped_cust_promo_df")

    prepaid_main_master_df = prepaid_main_master_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_ontop_master_df = prepaid_ontop_master_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    postpaid_main_master_df = postpaid_main_master_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    postpaid_ontop_master_df = postpaid_ontop_master_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

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


def dac_product_customer_promotion_for_daily(postpaid_df: DataFrame,
                                             prepaid_main_df: DataFrame,
                                             prepaid_ontop_df: DataFrame,
                                             customer_profile_df: DataFrame,
                                             prepaid_product_master_df: DataFrame,
                                             prepaid_product_ontop_df: DataFrame
                                             ) -> list:

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df, prepaid_product_ontop_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    postpaid_df = data_non_availability_and_missing_check(
        df=postpaid_df,grouping="weekly", par_col="partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    prepaid_main_df = data_non_availability_and_missing_check(
        df=prepaid_main_df, grouping="weekly", par_col="partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    prepaid_ontop_df = data_non_availability_and_missing_check(
        df=prepaid_ontop_df, grouping="weekly", par_col="partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    customer_profile_df = data_non_availability_and_missing_check(
        df=customer_profile_df, grouping="weekly", par_col="event_partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    prepaid_product_master_df = data_non_availability_and_missing_check(
        df=prepaid_product_master_df, grouping="weekly", par_col="partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    prepaid_product_ontop_df = data_non_availability_and_missing_check(
        df=prepaid_product_ontop_df, grouping="weekly", par_col="partition_date", target_table_name="l1_product_active_customer_promotion_features_daily")

    if check_empty_dfs([postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df, prepaid_product_ontop_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            postpaid_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            prepaid_main_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            # Comment out because data only available from June 10 2020:
            # prepaid_ontop_df.select(
            #     F.to_date(F.max(F.col("partition_date")).cast(StringType()),'yyyyMMdd').alias("max_date")),
            customer_profile_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            prepaid_product_master_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
            prepaid_product_ontop_df.select(
                F.to_date(F.max(F.col("partition_date")).cast(StringType()), 'yyyyMMdd').alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    postpaid_df = postpaid_df.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_main_df = prepaid_main_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    # Comment out because data only available from June 10 2020:
    # prepaid_ontop_df = prepaid_ontop_df.filter(F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd') <= min_value)
    customer_profile_df = customer_profile_df.filter(F.col("event_partition_date") <= min_value)
    prepaid_product_master_df = prepaid_product_master_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_product_ontop_df = prepaid_product_ontop_df.filter(
        F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    return [postpaid_df, prepaid_ontop_df, prepaid_main_df, customer_profile_df, prepaid_product_master_df,
            prepaid_product_ontop_df]


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


def l1_prepaid_postpaid_processing(prepaid_main_df: DataFrame,
                                   prepaid_ontop_df: DataFrame,
                                   postpaid_df: DataFrame,
                                   customer_profile_df: DataFrame,
                                   main_master_promotion_df: DataFrame,
                                   ontop_master_promotion_df: DataFrame) -> DataFrame:
    ############################################## Start Define Aliases ############################################

    prepaid_main_df = prepaid_main_df.alias("prepaid_main_df")
    prepaid_ontop_df = prepaid_ontop_df.alias("prepaid_ontop_df")
    main_master_promotion_df = main_master_promotion_df.alias("main_master_promotion_df")
    ontop_master_promotion_df = ontop_master_promotion_df.alias("ontop_master_promotion_df")
    customer_profile_df = customer_profile_df.alias("customer_profile_df")
    postpaid_df = postpaid_df.alias("postpaid_df")

    customer_profile_columns = [
        "start_of_week", "start_of_month", "subscription_identifier", "national_id_card",
        "old_subscription_identifier", "register_date", "event_partition_date"
    ]

    ############################################### End Define Aliases #############################################

    ########################################### Start Pre-Paid Processing #########################################

    product_prepaid_df = _prepare_prepaid(prepaid_main_df, prepaid_ontop_df,
                                          main_master_promotion_df, ontop_master_promotion_df)

    max_register_date_window = Window.partitionBy("prepaid_union.access_method_num", "prepaid_union.partition_date")

    prepaid_union_cus_df = (customer_profile_df
        .join(product_prepaid_df,
              (F.col("prepaid_union.access_method_num") == F.col(
                  "customer_profile_df.access_method_num")) &
              (F.to_date(F.col("prepaid_union.partition_date").cast(StringType()),
                         "yyyyMMdd") >=
               F.to_date(F.col("customer_profile_df.register_date").cast(StringType()),
                         "yyyyMMdd")),
              "left_outer"
              )
        .withColumn("max_register_date", F.max("register_date").over(max_register_date_window))
        .where(F.col("register_date") == F.col("max_register_date"))
        .select(
        [f"prepaid_union.{i}" for i in product_prepaid_df.columns] + customer_profile_columns
    ))

    ############################################ End Pre-Paid Processing ##########################################

    ########################################### Start Post-Paid Processing #########################################

    postpaid_cus_df = customer_profile_df.join(
        postpaid_df,
        (F.col("customer_profile_df.old_subscription_identifier") == F.col("postpaid_df.crm_subscription_id")) &
        (F.col("customer_profile_df.event_partition_date") == F.to_date(
            F.col("postpaid_df.partition_date").cast(StringType()), 'yyyyMMdd')),
        "left_outer"
    ).select([f"postpaid_df.{i}" for i in postpaid_df.columns] + customer_profile_columns + ["access_method_num"])

    ############################################ End Post-Paid Processing ##########################################

    union_df = _union_prepaid_postpaid(prepaid_union_cus_df, postpaid_cus_df)

    return union_df


def _safe_join_dimension(dimension_df: DataFrame, unique_keys: list) -> DataFrame:
    unique_keys_str = ",".join(unique_keys)

    return (dimension_df
            .withColumn("partition_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
            .withColumn("rn", F.expr(f"row_number() over(partition by {unique_keys_str} order by partition_date desc)"))
            .where("rn = 1")
            .drop("rn")
            )


def _prepare_prepaid(
        prepaid_main_df: DataFrame,
        prepaid_ontop_df: DataFrame,
        main_master_promotion_df: DataFrame,
        ontop_master_promotion_df: DataFrame
) -> DataFrame:
    main_master_promotion_df = _safe_join_dimension(
        main_master_promotion_df,
        ["package_id", "partition_date"]
    ).alias("main_master_promotion_df")

    ontop_master_promotion_df = _safe_join_dimension(
        ontop_master_promotion_df,
        ["promotion_code", "partition_date"]
    ).alias("ontop_master_promotion_df")

    prepaid_main_df = prepaid_main_df.withColumn("partition_date",
                                                 F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd')
                                                 ).alias("prepaid_main_df")
    prepaid_ontop_df = prepaid_ontop_df.withColumn("partition_date",
                                                   F.to_date(F.col("partition_date").cast(StringType()),
                                                             'yyyyMMdd')).alias("prepaid_ontop_df")

    prepaid_main_master_promotion_df = (prepaid_main_df
                                        .join(main_master_promotion_df, ["package_id", "partition_date"], "inner")
                                        .withColumn("offering_end_dt",
                                                    F.to_date(F.col("offering_end_dt").cast(StringType()), 'yyyyMMdd'))
                                        .select([f"prepaid_main_df.{i}" for i in prepaid_main_df.columns] + [
        F.col("service_fee_tariff").alias("promo_package_price"),
        F.col("offering_desc").alias("promo_name"),
        F.when(F.col("offering_end_dt") > F.current_date(), F.lit("active")).otherwise(F.lit("inactive")).alias(
            "promo_status")
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
        F.lit("recurring").alias("promo_price_type"),
        F.to_date(F.col("effective_date").cast(StringType()), 'yyyy-MM-dd').alias("promo_start_dttm"),
        F.col("promo_status").alias("promo_status")
    ))

    prepaid_ontop_master_promotion_df = prepaid_ontop_df.join(
        ontop_master_promotion_df,
        (F.col("prepaid_ontop_df.offering_id") == F.col("ontop_master_promotion_df.promotion_code")) &
        (F.col("prepaid_ontop_df.partition_date") == F.col("ontop_master_promotion_df.partition_date")),
        "inner"
    )

    prepaid_ontop_new_df = (prepaid_ontop_master_promotion_df
        .filter(F.lower(F.col("is_main_pro")) == "n")
        .select(
        F.lit("pre-paid").alias("promo_charge_type"),
        F.lit("on-top").alias("promo_class"),
        F.lit(None).alias("previous_main_promotion_id"),
        F.lit(None).cast('timestamp').alias("previous_promo_end_dttm"),
        F.col("offering_id").alias("promo_cd"),
        F.lit(None).cast('string').alias("promo_user_cat_cd"),
        F.col("access_method_num").alias("mobile_num"),
        F.col("access_method_num").alias("access_method_num"),
        F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_end_dttm"),
        F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias(
            "promo_status_end_dttm"),
        F.col("price").alias("promo_package_price"),
        F.col("offering_title").alias("promo_name"),
        F.col("prepaid_ontop_df.partition_date").cast(StringType()).alias("partition_date"),
        F.when(F.lower(F.col("recurring")) == "y", F.lit("recurring")).otherwise("non-recurring").alias(
            "promo_price_type"),
        F.to_date(F.coalesce(F.col("event_start_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_start_dttm"),
        F.lit("active").alias("promo_status")
    ))

    prepaid_union = prepaid_main_new_df.unionByName(prepaid_ontop_new_df).alias("prepaid_union")

    return prepaid_union


def _union_prepaid_postpaid(postpaid_df: DataFrame, prepaid_df: DataFrame) -> DataFrame:
    # Handle crm_subscription_id
    prepaid_df = prepaid_df.withColumn("crm_subscription_id", F.col("old_subscription_identifier"))
    postpaid_df = postpaid_df.withColumn("crm_subscription_id", F.col("old_subscription_identifier"))

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
        'subscription_identifier',
        'crm_subscription_id'
    ]

    prepaid_df = prepaid_df.select(columns_to_select)
    postpaid_df = postpaid_df.select(columns_to_select)

    return prepaid_df.unionByName(postpaid_df)


def l1_build_product(
        product_df: DataFrame,
        int_l1_product_active_customer_promotion_features: dict
) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    product_df = data_non_availability_and_missing_check(
        df=product_df,
        grouping="daily", par_col="partition_date",
        target_table_name="int_l1_product_active_customer_promotion_features_temp",
        missing_data_check_flg='Y')
    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols([
        product_df.select(F.max(F.col("partition_date")).alias("max_date")),
    ]).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    product_df = product_df.filter(F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd') <= min_value)

    return_df = l1_massive_processing(product_df, int_l1_product_active_customer_promotion_features)

    return return_df
