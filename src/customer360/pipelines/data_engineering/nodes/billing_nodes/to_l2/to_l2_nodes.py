import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window, DataFrame
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging

def massive_processing_weekly(data_frame: DataFrame, dict_obj: dict, output_df_catalog) -> DataFrame:
    """
    :param data_frame:
    :param dict_obj:
    :return:
    """
    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]
    CNTX = load_context(Path.cwd(), env='base')
    data_frame = data_frame
    dates_list = data_frame.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new
    first_item = add_list[0]
    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_week").isin(*[curr_item]))
        output_df = node_from_config(small_df, dict_obj)
        CNTX.catalog.save(output_df_catalog, output_df)
    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_week").isin(*[first_item]))
    return_df = node_from_config(return_df, dict_obj)
    return return_df

def billing_topup_count_and_volume_node_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_topup_and_volume")
    return return_df

def billing_arpu_roaming_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_weekly_rpu_roaming")
    return return_df

def billing_before_topup_balance_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_before_top_up_balance")
    return return_df

def billing_top_up_channels_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_top_up_channels")
    return return_df

def billing_most_popular_top_up_channel_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_most_popular_top_up_channel")
    return return_df

def billing_last_top_up_channel_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_last_top_up_channel")
    return return_df

def billing_popular_topup_day_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_popular_topup_day")
    return return_df

def billing_popular_topup_hour_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_popular_topup_hour")
    return return_df

def billing_time_since_last_topup_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_time_since_last_top_up")
    return return_df

def billing_last_three_topup_volume_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_last_three_topup_volume")
    return return_df

def billing_time_diff_between_topups_weekly(input_df, sql) -> DataFrame:
    """
    :return:
    """
    return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_topup_time_diff")
    return return_df


def top_up_channel_joined_data(input_df,topup_type_ref):

    output_df = input_df.join(topup_type_ref,input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,'left')

    return output_df




