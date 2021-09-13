from pyspark.sql.types import *
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
import logging
import os
from pathlib import Path
from kedro.context.context import load_context
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, check_empty_dfs, \
    data_non_availability_and_missing_check, node_from_config
from src.customer360.utilities.re_usable_functions import l1_massive_processing

conf = os.getenv("CONF", None)


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
    if check_empty_dfs([grouped_cust_promo_df, prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
                        ,postpaid_ontop_master_df]):
        return get_spark_empty_df()

    grouped_cust_promo_df = data_non_availability_and_missing_check(df=grouped_cust_promo_df
         ,grouping="daily", par_col="event_partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    prepaid_main_master_df = data_non_availability_and_missing_check(df=prepaid_main_master_df
         ,grouping="daily", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    prepaid_ontop_master_df = data_non_availability_and_missing_check(df=prepaid_ontop_master_df
         , grouping="daily", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    postpaid_main_master_df = data_non_availability_and_missing_check(df=postpaid_main_master_df
        , grouping="daily", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")
    postpaid_ontop_master_df = data_non_availability_and_missing_check(df=postpaid_ontop_master_df
        , grouping="daily", par_col="partition_date",target_table_name="l1_product_active_customer_promotion_features_daily")

    if check_empty_dfs([grouped_cust_promo_df, prepaid_main_master_df, prepaid_ontop_master_df, postpaid_main_master_df
                           , postpaid_ontop_master_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            grouped_cust_promo_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
            prepaid_main_master_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            prepaid_ontop_master_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            postpaid_main_master_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            postpaid_ontop_master_df.select(
                F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
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
                
                ontop_package_features_1.promo_name as product_ontop_1_package_name,
                ontop_package_features_1.promo_package_price as product_ontop_1_package_price,
                ontop_package_features_1.promo_cd as product_ontop_1_package_promo_cd,
                ontop_package_features_1.promo_end_dttm as product_ontop_1_package_validity,
                
                ontop_package_features_2.promo_name as product_ontop_2_package_name,
                ontop_package_features_2.promo_package_price as product_ontop_2_package_price,
                ontop_package_features_2.promo_cd as product_ontop_2_package_promo_cd,
                ontop_package_features_2.promo_end_dttm as product_ontop_2_package_validity,
                
                product_main_package_count,
                product_total_main_package_price,
                
                product_ontop_package_count,
                product_total_ontop_package_price,
                
                product_fbb_flag,
                product_landline_flag,
                
                subscription_identifier,
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

    # ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df,
                        prepaid_product_ontop_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
                get_spark_empty_df(), get_spark_empty_df()]

    postpaid_df = data_non_availability_and_missing_check(
        df=postpaid_df, grouping="daily", par_col="partition_date",
        target_table_name="l1_product_active_customer_promotion_features_prepaid_postpaid")

    prepaid_main_df = data_non_availability_and_missing_check(
        df=prepaid_main_df, grouping="daily", par_col="partition_date",
        target_table_name="l1_product_active_customer_promotion_features_prepaid_postpaid")

    customer_profile_df = data_non_availability_and_missing_check(
        df=customer_profile_df, grouping="daily", par_col="event_partition_date",
        target_table_name="l1_product_active_customer_promotion_features_prepaid_postpaid")

    prepaid_product_master_df = data_non_availability_and_missing_check(
        df=prepaid_product_master_df, grouping="daily", par_col="partition_date",
        target_table_name="l1_product_active_customer_promotion_features_prepaid_postpaid")

    prepaid_product_ontop_df = data_non_availability_and_missing_check(
        df=prepaid_product_ontop_df, grouping="daily", par_col="partition_date",
        target_table_name="l1_product_active_customer_promotion_features_prepaid_postpaid")

    if check_empty_dfs([postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df,
                        prepaid_product_ontop_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
                get_spark_empty_df(), get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################

    min_value = union_dataframes_with_missing_cols(
        [
            postpaid_df.select(F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            prepaid_main_df.select(F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            prepaid_ontop_df.select(F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            customer_profile_df.select(F.max(F.col("event_partition_date")).alias("max_date")),
            prepaid_product_master_df.select(F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
            prepaid_product_ontop_df.select(F.max(F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd')).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    postpaid_df = postpaid_df.filter(
        F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_main_df = prepaid_main_df.filter(
        F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_ontop_df = prepaid_ontop_df.filter(
        F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    customer_profile_df = customer_profile_df.filter(F.col("event_partition_date") <= min_value)
    prepaid_product_master_df = prepaid_product_master_df.filter(
        F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)
    prepaid_product_ontop_df = prepaid_product_ontop_df.filter(
        F.to_date((F.col("partition_date")).cast(StringType()), 'yyyyMMdd') <= min_value)

    if check_empty_dfs([postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df,
                        prepaid_product_ontop_df]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(),
                get_spark_empty_df(), get_spark_empty_df()]

    return [postpaid_df, prepaid_main_df, prepaid_ontop_df, customer_profile_df, prepaid_product_master_df,
            prepaid_product_ontop_df]


def dac_product_fbb_a_customer_promotion_current_for_daily(input_df: DataFrame,
                                                           exception_partition_list_for_l0_product_fbb_a_customer_promotion_current_for_daily: dict,
                                                           feature_dict: dict,
                                                           customer_df: DataFrame
                                                           ) -> DataFrame:
    """
    :param input_df:
    :param feature_dict:
    :param customer_df:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(
        df=input_df,
        grouping="daily",
        par_col="partition_date",
        target_table_name="l1_product_active_fbb_customer_features_daily",
        exception_partitions=exception_partition_list_for_l0_product_fbb_a_customer_promotion_current_for_daily)

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################
    return_df = l1_massive_processing(input_df, feature_dict, customer_df)
    return return_df


def l1_prepaid_postpaid_processing(prepaid_main_df: DataFrame,
                                   prepaid_ontop_df: DataFrame,
                                   postpaid_df: DataFrame,
                                   customer_profile_df: DataFrame,
                                   main_master_promotion_df: DataFrame,
                                   ontop_master_promotion_df: DataFrame) -> DataFrame:

    if check_empty_dfs([prepaid_main_df, prepaid_ontop_df, postpaid_df, customer_profile_df, main_master_promotion_df,
                        ontop_master_promotion_df]):
        return get_spark_empty_df()

    prepaid_main_df = prepaid_main_df.alias("prepaid_main_df")
    prepaid_ontop_df = prepaid_ontop_df.alias("prepaid_ontop_df")
    main_master_promotion_df = main_master_promotion_df.alias("main_master_promotion_df")
    ontop_master_promotion_df = ontop_master_promotion_df.alias("ontop_master_promotion_df")
    customer_profile_df = customer_profile_df.alias("customer_profile_df")
    postpaid_df = postpaid_df.alias("postpaid_df")

    product_columns = ["promo_charge_type", "promo_class", "previous_main_promotion_id", "previous_promo_end_dttm",
                       "promo_cd", "promo_user_cat_cd", "promo_end_dttm", "promo_status_end_dttm",
                       "promo_package_price", "promo_name", "promo_price_type", "promo_start_dttm", "promo_status",
                       "mobile_num", "promo_type", "promo_user_type"
                       ]

    customer_profile_columns = ["access_method_num", "partition_date", "start_of_week", "start_of_month",
                                "subscription_identifier", "old_subscription_identifier",
                                "register_date", "event_partition_date"
                                ]

    # Union pre-paid main and pre-paid ontop and ensure it has same columns as post-paid
    product_prepaid_df = _prepare_prepaid(prepaid_main_df, prepaid_ontop_df,
                                          main_master_promotion_df, ontop_master_promotion_df)

    # Union pre-paid (main + on-top) with post-paid (main + on-top)
    union_df = _union_prepaid_postpaid(postpaid_df, product_prepaid_df).alias("product_union")

    CNTX = load_context(Path.cwd(), env=conf)

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    data_frame = union_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_array = list(divide_chunks(mvv_array, 30))
    add_list = mvv_array
    first_item = add_list[-1]
    add_list.remove(first_item)

    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item])).alias("small_df")
        cust_df = customer_profile_df.filter(F.col("event_partition_date").isin(*[curr_item])).alias("cust_df")

        join_union_df = cust_df.join(small_df,
                                     (F.col("small_df.access_method_num") == F.col("cust_df.access_method_num")) &
                                     (F.col("small_df.partition_date") == F.col("cust_df.event_partition_date"))
                                     )

        df_output = join_union_df.select([f"small_df.{i}" for i in product_columns] +
                                         [f"cust_df.{i}" for i in customer_profile_columns])

        CNTX.catalog.save("l1_product_active_customer_promotion_features_prepaid_postpaid", df_output)

    logging.info("running for dates {0}".format(str(first_item)))

    small_df = data_frame.filter(F.col("partition_date").isin(*[first_item])).alias("small_df")
    cust_df = customer_profile_df.filter(F.col("event_partition_date").isin(*[first_item])).alias("cust_df")
    join_union_df = cust_df.join(small_df,
                                 (F.col("small_df.access_method_num") == F.col("cust_df.access_method_num")) &
                                 (F.col("small_df.partition_date") == F.col("cust_df.event_partition_date"))
                                 )

    df_output = join_union_df.select([f"small_df.{i}" for i in product_columns] +
                                     [f"cust_df.{i}" for i in customer_profile_columns])

    return df_output


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
                                        .join(main_master_promotion_df, ["package_id", "partition_date"], "left_outer")
                                        .withColumn("offering_end_dt",
                                                    F.to_date(F.col("offering_end_dt").cast(StringType()), 'yyyyMMdd'))
                                        .select([f"prepaid_main_df.{i}" for i in prepaid_main_df.columns] +
                                                [
                                                    F.col("service_fee_tariff").alias("promo_package_price"),
                                                    F.col("offering_desc").alias("promo_name"),
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
        F.when(
            (F.col("partition_date") >= F.to_date(F.coalesce(F.col("effective_date"), F.lit("9999-12-31")), 'yyyy-MM-dd')) &
            (F.col("partition_date") < F.to_date(F.coalesce(F.col("expire_date"), F.lit("9999-12-31")), 'yyyy-MM-dd')),
            F.lit("active")
        ).otherwise(F.lit("inactive")).alias("promo_status"),
        F.lit(None).alias("promo_type"),
        F.lit("existing").alias("promo_user_type")  # used for filter condition for main package features post-paid
    ))

    prepaid_ontop_master_promotion_df = prepaid_ontop_df.join(
        ontop_master_promotion_df,
        (F.col("prepaid_ontop_df.offering_id") == F.col("ontop_master_promotion_df.promotion_code")) &
        (F.col("prepaid_ontop_df.partition_date") == F.col("ontop_master_promotion_df.partition_date")),
        "left_outer"
    )

    prepaid_ontop_new_df = (prepaid_ontop_master_promotion_df
        .filter(F.lower(F.col("is_main_pro")) == "n")
        .select(
            F.lit("pre-paid").alias("promo_charge_type"),
            F.lit("on-top").alias("promo_class"),
            F.lit(None).alias("previous_main_promotion_id"),
            F.lit(None).cast('timestamp').alias("previous_promo_end_dttm"),
            F.col("prepaid_ontop_df.offering_id").alias("promo_cd"),
            F.lit(None).cast('string').alias("promo_user_cat_cd"),
            F.col("access_method_num").alias("mobile_num"),
            F.col("access_method_num").alias("access_method_num"),
            F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_end_dttm"),
            F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias(
                "promo_status_end_dttm"),
            F.col("price_exc_vat").alias("promo_package_price"),
            F.col("offering_title").alias("promo_name"),
            F.col("prepaid_ontop_df.partition_date").cast(StringType()).alias("partition_date"),
            F.when(F.lower(F.col("recurring")) == "y", F.lit("recurring")).otherwise("non-recurring").alias(
                "promo_price_type"),
            F.to_date(F.coalesce(F.col("event_start_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd').alias("promo_start_dttm"),
            F.when(
                (F.col("prepaid_ontop_df.partition_date") >= F.to_date(F.coalesce(F.col("event_start_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd')) &
                (F.col("prepaid_ontop_df.partition_date") < F.to_date(F.coalesce(F.col("event_end_dttm"), F.lit("9999-12-31")), 'yyyy-MM-dd')),
                F.lit("active")
            ).otherwise(F.lit("inactive")).alias("promo_status"),
            F.lit(None).alias("promo_type"),
            F.lit("existing").alias("promo_user_type")  # used for filter condition for main package features post-paid
        )
    )

    prepaid_union = prepaid_main_new_df.unionByName(prepaid_ontop_new_df).alias("prepaid_union")

    return prepaid_union


def _union_prepaid_postpaid(postpaid_df: DataFrame, prepaid_df: DataFrame) -> DataFrame:
    postpaid_df = postpaid_df.withColumn("access_method_num", F.col("mobile_num"))

    # Post-paid convert partition_date column
    postpaid_df = postpaid_df.withColumn("partition_date", F.to_date(F.col("partition_date").cast(StringType()),'yyyyMMdd'))

    columns_to_select = [
        'access_method_num',
        'mobile_num',
        'partition_date',
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
        'promo_type',
        'promo_user_type'
    ]

    prepaid_df = prepaid_df.select(columns_to_select).withColumn("prepaid_postpaid_flag", F.lit("prepaid"))
    postpaid_df = (postpaid_df.select(columns_to_select)
                   .withColumn("promo_class",
                               F.when(F.lower(F.col("promo_class")).isin(["on-top", "on-top extra"]),
                                      F.lit("on-top")).otherwise(F.col("promo_class")))
                   .withColumn("prepaid_postpaid_flag", F.lit("postpaid"))
                   )

    return prepaid_df.unionByName(postpaid_df)


def l1_build_product(
        product_df: DataFrame,
        int_l1_product_active_customer_promotion_features: dict
) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([product_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################


    partition_cols = ["access_method_num",
                      "event_partition_date",
                      "subscription_identifier",
                      "partition_date",
                      "start_of_week",
                      "start_of_month",
                      "promo_class",
                      "promo_status"
                      ]

    product_df = (product_df
                  .withColumn("promo_class", F.lower(F.col("promo_class")))
                  .withColumn("promo_status", F.lower(F.col("promo_status")))
                  .withColumn("ontop_rn", F.when(F.lower(F.col("promo_class")) == "on-top", F.expr(
                    f"row_number() over(partition by {','.join(partition_cols)} order by promo_package_price desc)")).otherwise(
                    F.lit(None)))
                  )

    return_df = node_from_config(product_df, int_l1_product_active_customer_promotion_features)

    return return_df
