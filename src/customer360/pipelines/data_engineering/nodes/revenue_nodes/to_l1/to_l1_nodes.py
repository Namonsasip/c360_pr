import logging
import os
from pathlib import Path

from kedro.context.context import load_context
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.config_parser import node_from_config
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check, union_dataframes_with_missing_cols
from pyspark.sql.types import StringType

conf = os.getenv("CONF", None)


def massive_processing_with_customer(input_df: DataFrame
                                     , customer_df: DataFrame
                                     , sql: dict) -> DataFrame:
    """
    :param input_df:
    :param customer_df:
    :param sql:
    :return:
    """

    ################################# Start Implementing Data availability checks ###############################
    if check_empty_dfs([input_df, customer_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_revenue_prepaid_pru_f_usage_multi_daily")

    input_df = input_df.withColumn("overlap_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))

    customer_df = data_non_availability_and_missing_check(df=customer_df, grouping="daily",
                                                          par_col="event_partition_date",
                                                          target_table_name="l1_revenue_prepaid_pru_f_usage_multi_daily")

    min_value = union_dataframes_with_missing_cols(
        [
            input_df.select(
                F.max(F.col("overlap_date")).alias("max_date")),
            customer_df.select(
                F.max(F.col("event_partition_date")).alias("max_date")),
        ]
    ).select(F.min(F.col("max_date")).alias("min_date")).collect()[0].min_date

    input_df = input_df.filter(F.col("overlap_date") <= min_value).drop("overlap_date")
    customer_df = customer_df.filter(F.col("event_partition_date") <= min_value)

    if check_empty_dfs([input_df, customer_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    data_frame = data_frame.withColumn("total_vol_gprs_2g_3g", F.col("total_vol_gprs") - F.col("total_vol_gprs_4g")) \
                            .withColumn("filter_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyyMMdd'))
    dates_list = data_frame.select('filter_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "start_of_week",
                "start_of_month"
                ]
    join_cols = ['access_method_num', 'event_partition_date', "start_of_week", "start_of_month"]

    customer_df = customer_df.where("charge_type = 'Pre-paid'").select(sel_cols)

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("filter_date").isin(*[curr_item])) \
            .drop_duplicates(subset=["access_method_num", "partition_date"])
        small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        output_df = small_cus_df.join(output_df, join_cols, "left")
        CNTX.catalog.save("l1_revenue_prepaid_pru_f_usage_multi_daily", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("filter_date").isin(*[first_item])) \
        .drop_duplicates(subset=["access_method_num", "partition_date"])
    return_df = node_from_config(return_df, sql)
    small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[first_item]))
    return_df = small_cus_df.join(return_df, join_cols, "left")
    return return_df

def revenue_prepaid_pru_f_usage(input_df, sql):
    input_df = input_df.withColumn("total_vol_gprs_2g_3g", F.col("total_vol_gprs") - F.col("total_vol_gprs_4g"))
    input_df = node_from_config(input_df, sql)
    # input_df = input_df.withColumnRenamed("c360_subscription_identifier", "subscription_identifier")
    output_df = input_df.select("access_method_num"
                                , "start_of_week"
                                , "start_of_month"
                                , "subscription_identifier"
                                , "rev_arpu_total_net_rev"
                                , "rev_arpu_net_tariff_rev_reward"
                                , "rev_arpu_net_tariff_rev_exc_reward"
                                , "rev_arpu_share_of_exc_reward_over_total_rev"
                                , "rev_arpu_share_of_revenue_reward_over_total_rev"
                                , "rev_arpu_diff_in_exc_reward_rev_reward"
                                , "rev_arpu_data_number_of_on_top_pkg"
                                , "rev_arpu_data_rev"
                                , "rev_arpu_data_rev_by_on_top_pkg"
                                , "rev_arpu_data_rev_by_ppu"
                                , "rev_arpu_data_rev_4g"
                                , "rev_arpu_share_data_rev_4g"
                                , "rev_arpu_data_rev_by_on_top_4g"
                                , "rev_arpu_share_data_rev_by_pkg_4g"
                                , "rev_arpu_data_rev_by_ppu_4g"
                                , "rev_arpu_share_data_rev_by_ppu_4g"
                                , "rev_arpu_data_rev_2g_3g"
                                , "rev_arpu_share_data_rev_2g_3g"
                                , "rev_arpu_data_rev_by_on_top_pkg_2g_3g"
                                , "rev_arpu_share_data_rev_by_on_top_pkg_2g_3g"
                                , "rev_arpu_data_rev_by_ppu_2g_3g"
                                , "rev_arpu_share_data_rev_by_ppu_2g_3g"
                                , "rev_arpu_data_rev_by_per_unit"
                                , "rev_arpu_data_rev_per_unit_2g_3g"
                                , "rev_arpu_data_rev_per_unit_4g"
                                , "rev_arpu_diff_rev_by_on_top_pkg_ppu"
                                , "rev_arpu_diff_rev_by_on_top_pkg_ppu_4g"
                                , "rev_arpu_diff_rev_by_on_top_pkg_ppu_2g_3g"
                                , "rev_arpu_diff_rev_2g_3g_vs_4g"
                                , "rev_arpu_diff_rev_per_unit_2g_3g_vs_4g"
                                , "rev_arpu_voice"
                                , "rev_arpu_voice_intra_ppu"
                                , "rev_arpu_share_voice_intra"
                                , "rev_arpu_voice_non_intra_ppu"
                                , "rev_arpu_share_voice_non_intra"
                                , "rev_arpu_voice_per_call"
                                , "rev_arpu_voice_intra_per_call"
                                , "rev_arpu_voice_non_intra_per_call"
                                , "rev_arpu_voice_per_minute"
                                , "rev_arpu_voice_intra_per_minute"
                                , "rev_arpu_voice_non_intra_per_minute"
                                , "rev_arpu_diff_voice_intra_non_intra"
                                , "rev_arpu_diff_voice_intra_non_intra_per_min"
                                , "rev_arpu_diff_voice_intra_non_intra_per_call"
                                , "rev_arpu_days_0_rev"
                                , "rev_arpu_days_data_0_rev"
                                , "rev_arpu_days_data_on_top_pkg_0_rev"
                                , "rev_arpu_days_data_ppu_0_rev"
                                , "rev_arpu_days_4g_data_0_rev"
                                , "rev_arpu_days_2g_3g_data_0_rev"
                                , "rev_arpu_days_4g_data_on_top_pkg_0_rev"
                                , "rev_arpu_days_2g_3g_data_on_top_pkg_0_rev"
                                , "rev_arpu_days_4g_data_ppu_0_rev"
                                , "rev_arpu_days_2g_3g_data_ppu_0_rev"
                                , "rev_arpu_days_voice_0_rev"
                                , "rev_arpu_days_voice_intra_0_rev"
                                , "rev_arpu_days_voice_non_intra_0_rev"
                                , "rev_arpu_days_voice_per_call_0_rev"
                                , "rev_arpu_days_voice_intra_per_call_0_rev"
                                , "rev_arpu_days_voice_non_intra_per_call_0_rev"
                                , "rev_arpu_days_voice_per_min_0_rev"
                                , "rev_arpu_days_voice_intra_per_min_0_rev"
                                , "rev_arpu_days_voice_non_intra_per_min_0_rev"
                                , "rev_arpu_data_last_date_on_top_pkg"
                                , "event_partition_date"
                                )
    return output_df