from pyspark.sql import DataFrame
from customer360.utilities.config_parser import node_from_config
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check
from src.customer360.utilities.spark_util import get_spark_empty_df


def build_digital_l2_weekly_features(cxense_site_traffic: DataFrame,
                                     l1_digital_cxenxse_site_traffic_popular_host_daily: DataFrame,
                                     l1_digital_cxenxse_site_traffic_popular_postalcode_daily: DataFrame,
                                     l1_digital_cxenxse_site_traffic_popular_referrerquery_daily: DataFrame,
                                     l1_digital_cxenxse_site_traffic_popular_referrerhost_daily: DataFrame,
                                     exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily: dict,
                                     weekly_dict: dict,
                                     popular_url_dict: dict,
                                     popular_postal_code_dict: dict,
                                     popular_referrer_query_dict: dict,
                                     popular_referrer_host_dict: dict,
                                     ) -> [DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
    :param cxense_site_traffic:
    :param l1_digital_cxenxse_site_traffic_popular_host_daily:
    :param l1_digital_cxenxse_site_traffic_popular_postalcode_daily:
    :param l1_digital_cxenxse_site_traffic_popular_referrerquery_daily:
    :param l1_digital_cxenxse_site_traffic_popular_referrerhost_daily:
    :param exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily:
    :param weekly_dict:
    :param popular_url_dict:
    :param popular_postal_code_dict:
    :param popular_referrer_query_dict:
    :param popular_referrer_host_dict:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([cxense_site_traffic, l1_digital_cxenxse_site_traffic_popular_host_daily]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()
            , get_spark_empty_df()]

    cxense_site_traffic = data_non_availability_and_missing_check(
         df=cxense_site_traffic, grouping="weekly",
        par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l2_digital_cxenxse_site_traffic_weekly",
        exception_partitions=exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily
        )

    l1_digital_cxenxse_site_traffic_popular_host_daily = data_non_availability_and_missing_check(
        df=l1_digital_cxenxse_site_traffic_popular_host_daily, grouping="weekly",
        par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l2_digital_cxenxse_site_traffic_popular_host_weekly",
        exception_partitions=exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily
    )

    l1_digital_cxenxse_site_traffic_popular_postalcode_daily =  data_non_availability_and_missing_check(
        df=l1_digital_cxenxse_site_traffic_popular_postalcode_daily, grouping="weekly",
        par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l1_digital_cxenxse_site_traffic_popular_postalcode_daily",
        exception_partitions=exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily
    )

    l1_digital_cxenxse_site_traffic_popular_referrerquery_daily =  data_non_availability_and_missing_check(
        df=l1_digital_cxenxse_site_traffic_popular_referrerquery_daily, grouping="weekly",
        par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l2_digital_cxenxse_site_traffic_popular_referrerquery_weekly",
        exception_partitions=exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily
    )

    l1_digital_cxenxse_site_traffic_popular_referrerhost_daily =  data_non_availability_and_missing_check(
        df=l1_digital_cxenxse_site_traffic_popular_referrerhost_daily, grouping="weekly",
        par_col="event_partition_date",
        missing_data_check_flg='Y',
        target_table_name="l2_digital_cxenxse_site_traffic_popular_referrerhost_weekly",
        exception_partitions=exception_partition_list_for_l1_digital_cxenxse_site_traffic_daily
    )

    if check_empty_dfs([cxense_site_traffic]):
        return [get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df(), get_spark_empty_df()
            , get_spark_empty_df()]

    ################################# End Implementing Data availability checks ###############################
    weekly_features = node_from_config(cxense_site_traffic, weekly_dict)
    popular_url = node_from_config(l1_digital_cxenxse_site_traffic_popular_host_daily, popular_url_dict)
    popular_postal_code = node_from_config(l1_digital_cxenxse_site_traffic_popular_postalcode_daily,
                                           popular_postal_code_dict)
    popular_referrer_query = node_from_config(l1_digital_cxenxse_site_traffic_popular_referrerquery_daily
                                              , popular_referrer_query_dict)
    popular_referrer_host = node_from_config(l1_digital_cxenxse_site_traffic_popular_referrerhost_daily,
                                             popular_referrer_host_dict)

    return [weekly_features, popular_url, popular_postal_code, popular_referrer_query, popular_referrer_host]
