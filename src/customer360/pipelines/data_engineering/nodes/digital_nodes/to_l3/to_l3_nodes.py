import pyspark.sql.functions as f ,logging
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
import pyspark as pyspark
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from typing import Dict, Any
from functools import reduce
def build_digital_l3_monthly_features(cxense_user_profile: DataFrame,
                                      cust_df: DataFrame,
                                      node_config_dict: dict,
                                      except_partition) -> DataFrame:
    """
    :param cxense_user_profile:
    :param cust_df:
    :param node_config_dict:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cxense_user_profile, cust_df]):
        return get_spark_empty_df()

    cxense_user_profile = data_non_availability_and_missing_check(
        df=cxense_user_profile, grouping="monthly",
        par_col="partition_month",
        target_table_name="l3_digital_cxenxse_user_profile_monthly",
        exception_partitions=except_partition)

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_digital_cxenxse_user_profile_monthly")

    # new section to handle data latency
    cxense_user_profile = cxense_user_profile \
        .withColumn("partition_month", f.col("partition_month").cast(StringType())) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', f.to_date(f.col("partition_month"), 'yyyyMM'))))

    min_value = union_dataframes_with_missing_cols(
        [
            cxense_user_profile.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            cust_df.select(
                f.max(f.col("start_of_month")).alias("max_date"))
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    cxense_user_profile = cxense_user_profile.filter(f.col("start_of_month") <= min_value)
    cust_df = cust_df.filter(f.col("start_of_month") <= min_value)

    if check_empty_dfs([cxense_user_profile, cust_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    cxense_user_profile = cxense_user_profile.withColumnRenamed("mobile_no", "access_method_num") \
        .withColumn("device_type", f.when(f.col("groups") == "device-type", f.col("item")).otherwise(f.lit(None))) \
        .withColumn("device_brand", f.when(f.col("groups") == "device-brand", f.col("item")).otherwise(f.lit(None)))

    # This code will populate a subscriber id to the data set.
    join_key = ['access_method_num', 'start_of_month']
    cust_df = cust_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_month,access_method_num order by "
        "start_of_month desc, mobile_status_date desc)")) \
        .where("rn = 1")\
        .select("subscription_identifier", "access_method_num", "start_of_month")

    final_df = cust_df.join(cxense_user_profile, join_key)

    return_df = node_from_config(final_df, node_config_dict)

    return_df = return_df.where("subscription_identifier is not null and start_of_month is not null")

    return return_df

def l3_digital_mobile_web_category_agg_monthly (mobile_web_daily_agg: DataFrame) -> DataFrame :

    if check_empty_dfs([mobile_web_daily_agg]):
        return get_spark_empty_df()
    df_mobile_web_monthly = mobile_web_daily_agg.withColumn("start_of_month", f.to_date(f.date_trunc('month', "event_partition_date")))
    df_mobile_web_monthly_category_agg = df_mobile_web_monthly.groupBy("subscription_identifier","mobile_no","category_name","priority"
                                                                       ,"start_of_month").agg(
        f.sum("total_visit_count").alias("total_visit_count"),
        f.sum("total_visit_duration").alias("total_visit_duration"),
        f.sum("total_volume_byte").alias("total_volume_byte"),
        f.sum("total_download_byte").alias("total_download_byte"),
        f.sum("total_upload_byte").alias("total_upload_byte")
        )

    return df_mobile_web_monthly_category_agg

def relay_drop_nulls(df_relay: pyspark.sql.DataFrame):
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    )
    return df_relay_cleaned

def join_all(dfs, on, how="inner"):
    """
    Merge all the dataframes
    """
    return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

def digital_customer_relay_pageview_agg_monthly(
    df_pageview: pyspark.sql.DataFrame, pageview_count_visit_by_cid: Dict[str, Any],
):
    if check_empty_dfs([df_pageview]):
        return get_spark_empty_df()

    df_engagement_pageview_clean = relay_drop_nulls(df_pageview)
    df_engagement_pageview = df_engagement_pageview_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != ""))
    df_engagement_pageview = df_engagement_pageview.withColumnRenamed("cid", "campaign_id")
    df_engagement_pageview = df_engagement_pageview.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_pageview_visits = node_from_config(df_engagement_pageview, pageview_count_visit_by_cid)
    return df_engagement_pageview_visits

def digital_customer_relay_conversion_agg_monthly(
    df_conversion: pyspark.sql.DataFrame,df_conversion_package: pyspark.sql.DataFrame,conversion_count_visit_by_cid: Dict[str, Any],conversion_package_count_visit_by_cid: Dict[str, Any],
):
    if check_empty_dfs([df_conversion]):
        return get_spark_empty_df()
    if check_empty_dfs([df_conversion_package]):
        return get_spark_empty_df()

    df_engagement_conversion_clean = relay_drop_nulls(df_conversion)
    df_engagement_conversion = df_engagement_conversion_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42paymentStatus") == "successful"))
    df_engagement_conversion = df_engagement_conversion.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion = df_engagement_conversion.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package = df_engagement_conversion_package_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42Product_status") == "successful"))
    df_engagement_conversion_package = df_engagement_conversion_package.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_visits = node_from_config(df_engagement_conversion, conversion_count_visit_by_cid)
    df_engagement_conversion_package_visits = node_from_config(df_engagement_conversion_package, conversion_package_count_visit_by_cid)

    # df_engagement_conversion_visits.createOrReplaceTempView("df_engagement_conversion_visits")
    # df_engagement_conversion_package_visits.createOrReplaceTempView("df_engagement_conversion_package_visits")
    #
    # spark = get_spark_session()
    # df_conversion_and_package_visits = spark.sql("""
    # select
    # COALESCE(a.subscription_identifier,b.subscription_identifier) as subscription_identifier,
    # COALESCE(a.mobile_no,b.mobile_no) as mobile_no,
    # COALESCE(a.campaign_id,b.campaign_id) as campaign_id,
    # a.total_conversion_product_count as total_conversion_product_count,
    # b.total_conversion_package_count as total_conversion_package_count,
    # COALESCE(a.start_of_month,b.start_of_month) as start_of_month
    # from df_engagement_conversion_visits a
    # FULL JOIN df_engagement_conversion_package_visits b
    # ON a.subscription_identifier = b.subscription_identifier
    # and a.mobile_no = b.mobile_no
    # and a.campaign_id = b.campaign_id
    # and a.start_of_month = b.start_of_month
    # """)
    df_conversion_and_package_visits = join_all(
    [
        df_engagement_conversion_visits,
        df_engagement_conversion_package_visits
    ],
    on=["subscription_identifier", "start_of_month", "mobile_no","campaign_id"],
    how="outer",
    )
    return df_conversion_and_package_visits

def digital_customer_relay_pageview_fav_monthly(
    df_pageviews: pyspark.sql.DataFrame,
    df_productinfo: pyspark.sql.DataFrame,
    count_visit: Dict[str, Any],
    popular_url: Dict[str, Any],
    popular_subcategory1: Dict[str, Any],
    popular_subcategory2: Dict[str, Any],
    popular_cid: Dict[str, Any],
    popular_productname: Dict[str, Any],
    most_popular_url: Dict[str, Any],
    most_popular_subcategory1: Dict[str, Any],
    most_popular_subcategory2: Dict[str, Any],
    most_popular_cid: Dict[str, Any],
    most_popular_productname: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_pageviews]):
        return get_spark_empty_df()
    if check_empty_dfs([df_productinfo]):
        return get_spark_empty_df()
    df_engagement_pageview_clean = relay_drop_nulls(df_pageviews)
    df_engagement_pageview = df_engagement_pageview_clean.withColumnRenamed("cid", "campaign_id")
    df_engagement_pageview = df_engagement_pageview.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    df_engagement_pageview_visits = node_from_config(df_engagement_pageview, count_visit)

    df_engagement_productinfo_clean = relay_drop_nulls(df_productinfo)
    df_engagement_productinfo = df_engagement_productinfo_clean.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # most_popular_subcategory1
    df_pageviews_subcat1 = df_engagement_pageview.filter((f.col("subCategory1").isNotNull()) & (f.col("subCategory1") != ""))
    popular_subcategory1_df = node_from_config(df_pageviews_subcat1, popular_subcategory1)

    df_most_popular_subcategory1 = node_from_config(popular_subcategory1_df, most_popular_subcategory1)

    # most_popular_subcategory2
    df_pageviews_subcat2 = df_engagement_pageview.filter((f.col("subCategory2").isNotNull()) & (f.col("subCategory2") != ""))
    popular_subcategory2_df = node_from_config(df_pageviews_subcat2, popular_subcategory2)

    df_most_popular_subcategory2 = node_from_config(popular_subcategory2_df, most_popular_subcategory2)

    # most_popular_url
    df_pageviews_url = df_engagement_pageview.filter((f.col("url").isNotNull()) & (f.col("url") != ""))
    popular_url_df = node_from_config(df_pageviews_url, popular_url)

    df_most_popular_url = node_from_config(popular_url_df, most_popular_url)

    # most_popular_cid
    df_pageviews_cid = df_engagement_pageview.filter((f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_pageviews_cid, popular_cid)

    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    # most_popular_productname
    df_engagement_productinfo = df_engagement_productinfo.filter((f.col("R42productName").isNotNull()) & (f.col("R42productName") != ""))
    popular_productname_df = node_from_config(df_engagement_productinfo, popular_productname)

    df_most_popular_productname = node_from_config(popular_productname_df, most_popular_productname)

    pageviews_monthly_features = join_all(
        [
            df_engagement_pageview_visits,
            df_most_popular_subcategory1,
            df_most_popular_subcategory2,
            df_most_popular_url,
            df_most_popular_productname,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )
    return pageviews_monthly_features

def digital_customer_relay_conversion_fav_monthly(
        df_conversion: pyspark.sql.DataFrame,
        popular_product: Dict[str, Any],
        popular_cid: Dict[str, Any],
        most_popular_product: Dict[str, Any],
        most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_conversion]):
        return get_spark_empty_df()
    df_engagement_conversion_clean = relay_drop_nulls(df_conversion)
    df_engagement_conversion_clean = df_engagement_conversion_clean.filter(
        f.col("R42paymentStatus") == "successful")
    df_engagement_conversion = df_engagement_conversion_clean.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion = df_engagement_conversion.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # favourite product
    df_engagement_conversion_product = df_engagement_conversion.withColumn(
        "R42productLists", f.split("R42productLists", ",")
    ).withColumn("product", f.explode("R42productLists"))
    df_engagement_conversion_product_clean = df_engagement_conversion_product.filter(
        (f.col("product").isNotNull()) & (f.col("product") != ""))
    df_popular_product = node_from_config(df_engagement_conversion_product_clean, popular_product)
    df_most_popular_product = node_from_config(df_popular_product, most_popular_product)

    # favourite cid
    df_conversion_cid = df_engagement_conversion.filter(
        (f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_conversion_cid, popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    engagement_conversion_monthly_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_monthly_features

def digital_customer_relay_conversion_package_fav_monthly(
        df_conversion_package: pyspark.sql.DataFrame,
        popular_product: Dict[str, Any],
        popular_cid: Dict[str, Any],
        most_popular_product: Dict[str, Any],
        most_popular_cid: Dict[str, Any],
) -> pyspark.sql.DataFrame:
    if check_empty_dfs([df_conversion_package]):
        return get_spark_empty_df()
    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package_clean = df_engagement_conversion_package_clean.filter(
        f.col("R42Product_status") == "successful")
    df_engagement_conversion_package = df_engagement_conversion_package_clean.withColumnRenamed("cid",
                                                                                                "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("partition_date").cast("string"), 1, 4), f.lit("-"),
                 f.substring(f.col("partition_date").cast("string"), 5, 2), f.lit("-01")
                 ),
    ).drop(*["partition_date"])

    # favourite product
    df_engagement_conversion_package_product = df_engagement_conversion_package.withColumnRenamed("R42Product_name",
                                                                                                  "product")

    df_engagement_conversion_package_product_clean = df_engagement_conversion_package_product.filter(
        (f.col("product").isNotNull()) & (f.col("product") != ""))
    df_popular_product = node_from_config(
        df_engagement_conversion_package_product_clean, popular_product
    )
    df_most_popular_product = node_from_config(
        df_popular_product, most_popular_product
    )

    # favourite cid
    df_engagement_cid = df_engagement_conversion_package.filter(
        (f.col("campaign_id").isNotNull()) & (f.col("campaign_id") != ""))
    df_popular_cid = node_from_config(df_engagement_cid, popular_cid)
    df_most_popular_cid = node_from_config(df_popular_cid, most_popular_cid)

    engagement_conversion_package_monthly_features = join_all(
        [
            df_most_popular_product,
            df_most_popular_cid,
        ],
        on=["subscription_identifier", "start_of_month", "mobile_no"],
        how="outer",
    )

    return engagement_conversion_package_monthly_features


    ################################# mobile_app_monthly ###############################
def digital_mobile_app_category_agg_monthly(app_category_agg_daily: pyspark.sql.DataFrame,sql: Dict[str, Any]):
    app_category_agg_daily = app_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("event_partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["event_partition_date"])
    app_category_agg_daily = node_from_config(app_category_agg_daily,sql)
    return app_category_agg_daily

    ############################## favorite_app_monthly #############################
def digital_mobile_app_category_favorite_monthly(app_category_agg_daily: pyspark.sql.DataFrame,sql_total: Dict[str, Any],sql_transection: Dict[str, Any],sql_duration: Dict[str, Any],sql_volume: Dict[str, Any]):
    #---------------  sum traffic ------------------
    logging.info("favorite ------- > sum traffic")
    app_category_agg_daily_sql_total = node_from_config(app_category_agg_daily,sql_total)
    app_category_agg_daily = app_category_agg_daily.join(app_category_agg_daily_sql_total,
        on=[
            app_category_agg_daily["subscription_identifier"] == app_category_agg_daily_sql_total["subscription_identifier"],
            app_category_agg_daily["mobile_no"] == app_category_agg_daily_sql_total["mobile_no"],
            app_category_agg_daily["start_of_month"] == app_category_agg_daily_sql_total["start_of_month"],
        ],
        how="inner",
    )
    # app_category_agg_daily = app_category_agg_daily.select("subscription_identifier","mobile_no","priority","start_of_month","total_visit_count","total_visit_duration","total_volume_byte","sum_total_visit_count","sum_total_visit_duration", "sum_total_volume_byte")
    #---------------  sum cal fav ------------------
    logging.info("favorite ------- > cal")
    app_category_agg_daily_transection = node_from_config(app_category_agg_daily,sql_transection)
    logging.info("favorite ------- > transection complete")
    app_category_agg_daily_duration = node_from_config(app_category_agg_daily,sql_duration)
    logging.info("favorite ------- > duration complete")
    app_category_agg_daily_volume = node_from_config(app_category_agg_daily,sql_volume)
    logging.info("favorite ------- > volume complete")
    #---------------  union ------------------
    logging.info("favorite ------- > union")
    df_return = app_category_agg_daily_transection.union(app_category_agg_daily_duration)
    df_return = df_return.union(app_category_agg_daily_volume)
    return df_return

    ################################# combine_monthly ###############################

def digital_to_l3_digital_combine_agg_monthly(combine_category_agg_daily: pyspark.sql.DataFrame,sql: Dict[str, Any]):
    combine_category_agg_daily = combine_category_agg_daily.withColumn(
        "start_of_month",
        f.concat(f.substring(f.col("event_partition_date").cast("string"), 1, 7), f.lit("-01")
        ),
    ).drop(*["event_partition_date"])
    combine_category_agg_daily = node_from_config(combine_category_agg_daily,sql)
    return combine_category_agg_daily

    