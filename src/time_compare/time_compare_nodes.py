from pyspark.sql import DataFrame
import pyspark
import pyspark.sql.functions as F
from customer360.utilities.spark_util import get_spark_session


def direct_load_node(l3_customer_profile_union_monthly_feature: DataFrame,
                     l4_usage_prepaid_postpaid_daily_features: DataFrame):
    l3_customer_profile_union_monthly_feature = l3_customer_profile_union_monthly_feature.where("start_of_month = '2020-06-01'")
    l4_usage_prepaid_postpaid_daily_features = l4_usage_prepaid_postpaid_daily_features.where("event_partition_date >= '2020-06-01'"
                                                                                              "and event_partition_date <= '2020-06-30'")
    joined_df = l3_customer_profile_union_monthly_feature.join(l4_usage_prepaid_postpaid_daily_features,
                                                               'subscription_identifier',
                                                               'left')
    print(joined_df.count())
    return None


def hive_load_node():
    spark = get_spark_session()
    # l3_customer_profile_union_monthly_feature = l3_customer_profile_union_monthly_feature.where(
    #     "start_of_month = '2020-06-01'")
    # l4_usage_prepaid_postpaid_daily_features = l4_usage_prepaid_postpaid_daily_features.where(
    #     "event_partition_date >= '2020-06-01'"
    #     "and event_partition_date <= '2020-06-30'")
    l3_customer_profile_union_monthly_feature = spark.sql("select * from c360_external.l3_customer_profile_union_monthly_feature"
                                                          "where start_of_month = '2020-06-01'")
    l4_usage_prepaid_postpaid_daily_features = spark.sql(
        "select * from c360_external.l4_usage_prepaid_postpaid_daily_features"
        " where event_partition_date >= '2020-06-01' and event_partition_date <= '2020-06-30'")
    joined_df = l3_customer_profile_union_monthly_feature.join(l4_usage_prepaid_postpaid_daily_features,
                                                               'subscription_identifier',
                                                               'left')
    print(joined_df.count())
    return None
