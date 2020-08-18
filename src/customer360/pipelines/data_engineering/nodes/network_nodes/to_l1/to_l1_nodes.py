import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check
from customer360.utilities.re_usable_functions import l1_massive_processing, union_dataframes_with_missing_cols
from customer360.utilities.spark_util import get_spark_empty_df, get_spark_session
from typing import List


def build_network_voice_features(int_l1_network_voice_features: DataFrame,
                                 l1_network_voice_features: dict,
                                 l1_customer_profile_union_daily_feature_for_l1_network_voice_features: DataFrame) -> DataFrame:
    """
    :param int_l1_network_voice_features:
    :param l1_network_voice_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_voice_features:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [int_l1_network_voice_features, l1_customer_profile_union_daily_feature_for_l1_network_voice_features]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=int_l1_network_voice_features, grouping="daily",
                                                       par_col="event_partition_date",
                                                       target_table_name="l1_network_voice_features")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_voice_features, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_voice_features")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([input_df, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(int_l1_network_voice_features,
                                      l1_network_voice_features,
                                      cust_df)
    return return_df


def build_network_good_and_bad_cells_features(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features: DataFrame,

        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features: DataFrame,
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features: DataFrame,

        l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features: DataFrame,
        l1_network_good_and_bad_cells_features: dict
) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features:
    :param l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features:
    :param l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features:
    :param l1_network_good_and_bad_cells_features:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,

             l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
             l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features,

             l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features,
             ]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,
            grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features = \
        data_non_availability_and_missing_check(
            df=l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features, grouping="daily",
            par_col="even_partition_date",
            target_table_name="l1_network_good_and_bad_cells_features")

    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
             l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features,

             l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
             l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features,

             l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features]):
        return get_spark_empty_df()

    # For min custoner check is not required as it will be a left join to customer from driving table
    min_value = union_dataframes_with_missing_cols(
        [
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
            l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features.select(
                f.max(f.col("partition_date")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features = \
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features = \
        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)
    l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features = \
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features \
            .filter(f.col("partition_date") <= min_value)

    ################################# End Implementing Data availability checks ###############################

    get_good_and_bad_cells_for_each_customer_df = get_good_and_bad_cells_for_each_customer(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_good_and_bad_cells_features,
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voice_1day_for_l1_network_good_and_bad_cells_features
    )

    get_transaction_on_good_and_bad_cells_df = get_transaction_on_good_and_bad_cells(
        get_good_and_bad_cells_for_each_customer_df,
        l0_geo_mst_cell_masterplan_current_for_l1_network_good_and_bad_cells_features,
        l0_usage_sum_voice_location_daily_for_l1_network_good_and_bad_cells_features)

    return_df = l1_massive_processing(get_transaction_on_good_and_bad_cells_df,
                                      l1_network_good_and_bad_cells_features,
                                      l1_customer_profile_union_daily_feature_for_l1_network_good_and_bad_cells_features
                                      )

    return return_df


def get_good_and_bad_cells_for_each_customer(
        im_df,
        streaming_df,
        web_df,
        voip_df,
        volte_df,
        voice_df
) -> DataFrame:
    spark = get_spark_session()

    voice_df.createOrReplaceTempView("voice_df")
    im_df.createOrReplaceTempView("im_df")
    streaming_df.createOrReplaceTempView("streaming_df")
    web_df.createOrReplaceTempView("web_df")
    voip_df.createOrReplaceTempView("voip_df")
    volte_df.createOrReplaceTempView("volte_df")

    result_df = spark.sql("""
        with unioned_df as (
            select msisdn, cs_cgi as cell_id, cei_voice_qoe as qoe, partition_date
            from voice_df
            union all
            
            select msisdn, cgisai as cell_id, cei_im_qoe as qoe, partition_date
            from im_df
            union all
            
            select msisdn, cgisai as cell_id, cei_stream_qoe as qoe, partition_date
            from streaming_df
            union all
            
            select msisdn, cgisai as cell_id, cei_web_qoe as qoe, partition_date
            from web_df
            union all
            
            select msisdn, cgisai as cell_id, cei_voip_qoe as qoe, partition_date
            from voip_df
            union all
            
            select msisdn, cgisai as cell_id, cei_volte_qoe as qoe, partition_date
            from volte_df
        ),
        grouped_cells as (
            select msisdn, cell_id, avg(qoe) as avg_qoe, partition_date
            from unioned_df
            group by msisdn, cell_id, partition_date
        )
        select *,
                boolean(avg_qoe >= 0.75*max(avg_qoe) over (partition by msisdn, partition_date)) as good_cells,
                boolean(avg_qoe <= 0.25*max(avg_qoe) over (partition by msisdn, partition_date)) as bad_cells,
                count(*) over (partition by msisdn, partition_date) as cell_id_count                                        
        from grouped_cells  
    """)

    return result_df


def get_transaction_on_good_and_bad_cells(
        ranked_cells_df,
        cell_master_plan_df,
        sum_voice_daily_location_df
) -> DataFrame:
    spark = get_spark_session()

    ranked_cells_df.createOrReplaceTempView("ranked_cells_df")
    cell_master_plan_df.createOrReplaceTempView("cell_master_plan_df")
    sum_voice_daily_location_df.createOrReplaceTempView("sum_voice_daily_location_df")

    result = spark.sql("""
        with geo as (
            select *, 
                   cast(substr(cgi,6) as integer) as trunc_cgi
            from cell_master_plan_df
        ),
        voice_tx_location as (
            select *,
                cast(case when char_length(ci) > 5 then ci else concat(lac, ci) end as integer) as trunc_cgi
            from sum_voice_daily_location_df
        ),
        enriched_voice_tx_location as (
            select /*+ BROADCAST(geo) */
                t1.access_method_num as access_method_num,
                t1.partition_date as partition_date,
                t1.service_type as service_type,
                t1.no_of_call + t1.no_of_inc as total_transaction,
                
                t2.cgi as cgi,
                t2.soc_cgi_hex as soc_cgi_hex                
            from voice_tx_location t1
            inner join geo t2
            on t1.trunc_cgi = t2.trunc_cgi
        ),
        joined_ranked_cells_df as (
            select 
                t1.msisdn as access_method_num,
                t1.partition_date as partition_date,
                sum(case when t1.good_cells == true then 1 else 0 end) as sum_of_good_cells,
                sum(case when t1.bad_cells == true then 1 else 0 end) as sum_of_bad_cells,
                max(cell_id_count) as cell_id_count,
                sum(case when t1.good_cells == true then t2.total_transaction else 0 end) as total_transaction_good_cells,
                sum(case when t1.bad_cells == true then t2.total_transaction else 0 end) as total_transaction_bad_cells
            from ranked_cells_df t1
            inner join enriched_voice_tx_location t2
            on t1.msisdn = t2.access_method_num
                and t1.cell_id = t2.soc_cgi_hex
                and t1.partition_date = t2.partition_date
            group by t1.msisdn, t1.partition_date
        )
        select *,
            (sum_of_good_cells/cell_id_count) as share_of_good_cells,
            (sum_of_bad_cells/cell_id_count) as share_of_bad_cells
        from joined_ranked_cells_df
    """)

    return result


def build_network_share_of_3g_time_in_total_time(
        l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time: DataFrame,
        l1_network_share_of_3g_time_in_total_time: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time: DataFrame) -> DataFrame:
    """
    :param l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time:
    :param l1_network_share_of_3g_time_in_total_time:
    :param l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time,
             l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time]):
        return get_spark_empty_df()

    l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time = \
        data_non_availability_and_missing_check(
            df=l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_share_of_3g_time_in_total_time")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_share_of_3g_time_in_total_time, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_share_of_3g_time_in_total_time")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_usage_sum_voice_location_daily_for_l1_network_share_of_3g_time_in_total_time,
                                      l1_network_share_of_3g_time_in_total_time,
                                      cust_df)
    return return_df


def build_network_data_traffic_features(
        int_l1_network_data_traffic_features: DataFrame,
        l1_network_data_traffic_features: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features: DataFrame) -> DataFrame:
    """

    :param int_l1_network_data_traffic_features:
    :param l1_network_data_traffic_features:
    :param l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [int_l1_network_data_traffic_features,
             l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features]):
        return get_spark_empty_df()

    int_l1_network_data_traffic_features = int_l1_network_data_traffic_features.\
        drop("event_partition_date", "start_of_month", "start_of_week")

    int_l1_network_data_traffic_features = \
        data_non_availability_and_missing_check(
            df=int_l1_network_data_traffic_features, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_data_traffic_features")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_data_traffic_features, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_data_traffic_features")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([int_l1_network_data_traffic_features, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(int_l1_network_data_traffic_features,
                                      l1_network_data_traffic_features,
                                      cust_df)
    return return_df


def build_network_data_cqi(
        l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi: DataFrame,
        l1_network_data_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_data_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi:
    :param l1_network_data_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_data_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_data_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_data_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_data_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_data_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_dataqoe_usr_1day_for_l1_network_data_cqi,
                                      l1_network_data_cqi,
                                      cust_df)
    return return_df


def build_network_im_cqi(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi: DataFrame,
                         l1_network_im_cqi: dict,
                         l1_customer_profile_union_daily_feature_for_l1_network_im_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi:
    :param l1_network_im_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_im_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_im_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_im_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_im_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_im_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_im_1day_for_l1_network_im_cqi,
                                      l1_network_im_cqi,
                                      cust_df)
    return return_df


def build_network_streaming_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi: DataFrame,
        l1_network_streaming_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi:
    :param l1_network_streaming_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_streaming_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_streaming_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_streaming_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_stream_1day_for_l1_network_streaming_cqi,
                                      l1_network_streaming_cqi,
                                      cust_df)
    return return_df


def build_network_web_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi: DataFrame,
        l1_network_web_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_web_cqi: DataFrame) -> DataFrame:
    """

    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi:
    :param l1_network_web_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_web_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_web_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_web_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_web_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_web_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_web_1day_for_l1_network_web_cqi,
                                      l1_network_web_cqi,
                                      cust_df)
    return return_df


def build_network_voip_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi: DataFrame,
        l1_network_voip_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi:
    :param l1_network_voip_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_voip_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_voip_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi,
                                      l1_network_voip_cqi,
                                      cust_df)
    return return_df


def build_network_volte_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi: DataFrame,
        l1_network_volte_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_cell_usr_voip_1day_for_l1_network_voip_cqi:
    :param l1_network_voip_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_voip_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_volte_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_volte_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_volte_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_cell_usr_volte_1day_for_l1_network_volte_cqi,
                                      l1_network_volte_cqi,
                                      cust_df)
    return return_df


def build_network_user_cqi(
        l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi: DataFrame,
        l1_network_user_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_user_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi:
    :param l1_network_user_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_user_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_user_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_user_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_user_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_user_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_cei_usr_1day_for_l1_network_user_cqi,
                                      l1_network_user_cqi,
                                      cust_df)
    return return_df


def build_network_file_transfer_cqi(
        l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi: DataFrame,
        l1_network_file_transfer_cqi: dict,
        l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi: DataFrame) -> DataFrame:
    """
    :param l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi:
    :param l1_network_file_transfer_cqi:
    :param l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi:
    :return:
    """
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs(
            [l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi,
             l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi]):
        return get_spark_empty_df()

    l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi = \
        data_non_availability_and_missing_check(
            df=l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi, grouping="daily",
            par_col="partition_date",
            target_table_name="l1_network_user_cqi")

    cust_df = data_non_availability_and_missing_check(
        df=l1_customer_profile_union_daily_feature_for_l1_network_file_transfer_cqi, grouping="daily",
        par_col="event_partition_date",
        target_table_name="l1_network_user_cqi")

    # Min function is not required as driving table is network and join is based on that

    if check_empty_dfs([l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi, cust_df]):
        return get_spark_empty_df()
    ################################# End Implementing Data availability checks ###############################

    return_df = l1_massive_processing(l0_network_sdr_dyn_cea_cei_qoe_usr_fileaccess_1day_for_l1_network_file_transfer_cqi,
                                      l1_network_file_transfer_cqi,
                                      cust_df)
    return return_df
