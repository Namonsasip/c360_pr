import pyspark.sql.functions as f
from pyspark.sql.functions import expr
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    union_dataframes_with_missing_cols
from src.customer360.utilities.spark_util import get_spark_empty_df


def build_digital_l3_monthly_features(cxense_user_profile: DataFrame,
                                      cust_df: DataFrame,
                                      node_config_dict: dict) -> DataFrame:
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
        target_table_name="l3_digital_cxenxse_user_profile_monthly")

    cust_df = data_non_availability_and_missing_check(
        df=cust_df, grouping="monthly",
        par_col="start_of_month",
        target_table_name="l3_digital_cxenxse_user_profile_monthly")

    # new section to handle data latency
    cxense_user_profile = cxense_user_profile\
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

    return_df = node_from_config(cxense_user_profile, node_config_dict)

    # This code will populate a subscriber id to the data set.
    cust_df_cols = ["subscription_identifier", "access_method_num", "national_id_card", "start_of_month"]
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols)
    cust_df = cust_df.withColumn("rn", expr(
        "row_number() over(partition by start_of_month,access_method_num order by start_of_month desc)"))
    cust_df = cust_df.where("rn = 1")
    cust_df = cust_df.drop("rn")

    final_df = return_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null and start_of_month is not null")

    final_df = final_df.drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df
