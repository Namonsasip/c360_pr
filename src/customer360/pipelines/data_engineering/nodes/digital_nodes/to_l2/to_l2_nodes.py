import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType

from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df


def build_digital_l2_weekly_features(cxense_site_traffic: DataFrame,
                                     cust_df: DataFrame,
                                     weekly_dict: dict,
                                     popular_url_dict: dict,
                                     popular_postal_code_dict: dict,
                                     popular_referrer_query_dict: dict,
                                     popular_referrer_host_dict: dict,
                                     ) -> DataFrame:
    """
    :param cxense_site_traffic:
    :param cust_df:
    :param weekly_dict:
    :param popular_url_dict:
    :param popular_postal_code_dict:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cxense_site_traffic]):
        return get_spark_empty_df()

    cxense_site_traffic = data_non_availability_and_missing_check(df=cxense_site_traffic, grouping="weekly",
                                                                  par_col="partition_date",
                                                                  target_table_name="l2_digital_cxenxse_site_traffic_weekly")

    if check_empty_dfs([cxense_site_traffic]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    cust_df_cols = ['access_method_num', 'start_of_week', 'subscription_identifier']
    cxense_site_traffic = cxense_site_traffic.withColumnRenamed("hash_id", "access_method_num") \
        .withColumn("partition_date", f.col("partition_date").cast(StringType())) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))

    cxense_site_traffic = cxense_site_traffic.join(cust_df.select(cust_df_cols), cust_df_cols)

    weekly_features = node_from_config(cxense_site_traffic, weekly_dict)
    popular_url = node_from_config(cxense_site_traffic, popular_url_dict)
    popular_postal_code = node_from_config(cxense_site_traffic, popular_postal_code_dict)
    popular_referrer_query = node_from_config(cxense_site_traffic, popular_referrer_query_dict)
    popular_referrer_host = node_from_config(cxense_site_traffic, popular_referrer_host_dict)

    return [weekly_features, popular_url, popular_postal_code, popular_referrer_query, popular_referrer_host]
