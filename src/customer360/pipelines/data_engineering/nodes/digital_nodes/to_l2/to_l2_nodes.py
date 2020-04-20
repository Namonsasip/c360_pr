import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import node_from_config


def build_digital_l2_weekly_features(cxense_site_traffic: DataFrame,
                                      cust_df: DataFrame,
                                      node_config_dict: dict) -> DataFrame:
    """
    :param cxense_site_traffics:
    :param cust_df:
    :param node_config_dict:
    :return:
    """
    cxense_site_traffic = cxense_site_traffic.withColumnRenamed("hash_id", "access_method_num") \
        .withColumn("partition_date", f.col("partition_date").cast(StringType())) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))\
        .withColumn("start_of_week", f.to_date(f.date_trunc('month', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))

    return_df = node_from_config(cxense_site_traffic, node_config_dict)

    # merging with customer dimension table
    cust_df_cols = ['access_method_num', 'partition_month', 'subscription_identifier']
    join_key = ['access_method_num', 'start_of_month']

    cust_df = cust_df.select(cust_df_cols).withColumnRenamed("partition_month", "start_of_month")

    final_df = return_df.join(cust_df, join_key)

    final_df = final_df.where("subscription_identifier is not null AND start_of_month is not null") \
        .drop_duplicates(subset=["subscription_identifier", "start_of_month"])

    return final_df
