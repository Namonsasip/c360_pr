import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
import pyspark as pyspark
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from typing import Dict, Any

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


def relay_drop_nulls(df_relay: pyspark.sql.DataFrame):
    df_relay_cleaned = df_relay.filter(
        (f.col("mobile_no").isNotNull())
        & (f.col("mobile_no") != "")
        & (f.col("subscription_identifier") != "")
        & (f.col("subscription_identifier").isNotNull())
    ).dropDuplicates()
    return df_relay_cleaned

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
        f.concat(
            f.substring(f.col("partition_date").cast("string"), 1, 6), f.lit("-01")
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
        f.concat(
            f.substring(f.col("partition_date").cast("string"), 1, 6), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_package_clean = relay_drop_nulls(df_conversion_package)
    df_engagement_conversion_package = df_engagement_conversion_package_clean.filter((f.col("cid").isNotNull()) & (f.col("cid") != "") & (f.col("R42Product_status") == "successful"))
    df_engagement_conversion_package = df_engagement_conversion_package.withColumnRenamed("cid", "campaign_id")
    df_engagement_conversion_package = df_engagement_conversion_package.withColumn(
        "start_of_month",
        f.concat(
            f.substring(f.col("partition_date").cast("string"), 1, 6), f.lit("-01")
        ),
    ).drop(*["partition_date"])

    df_engagement_conversion_visits = node_from_config(df_engagement_conversion, conversion_count_visit_by_cid)
    df_engagement_conversion_package_visits = node_from_config(df_engagement_conversion_package, conversion_package_count_visit_by_cid)

    df_engagement_conversion_visits.createOrReplaceTempView("df_engagement_conversion_visits")
    df_engagement_conversion_package_visits.createOrReplaceTempView("df_engagement_conversion_package_visits")

    spark = get_spark_session()
    df_conversion_and_package_visits = spark.sql("""
    select
    COALESCE(a.subscription_identifier,b.subscription_identifier) as subscription_identifier,
    COALESCE(a.mobile_no,b.mobile_no) as mobile_no,
    COALESCE(a.campaign_id,b.campaign_id) as campaign_id,
    a.total_conversion_product_count as total_conversion_product_count,
    b.total_conversion_package_count as total_conversion_package_count,
    COALESCE(a.event_partition_date,b.event_partition_date) as start_of_month
    from df_engagement_conversion_visits a
    FULL JOIN df_engagement_conversion_package_visits b
    ON a.subscription_identifier = b.subscription_identifier
    and a.mobile_no = b.mobile_no
    and a.campaign_id = b.campaign_id
    and a.start_of_month = b.start_of_month       
    """)
    return df_conversion_and_package_visits
