import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window, DataFrame
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *

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

def recharge_data_with_customer_profile_joined(customer_prof,recharge_data):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date",
                                         "start_of_week",
                                         "start_of_month")

    output_df = customer_prof.join(recharge_data,(customer_prof.access_method_num == recharge_data.access_method_num) &
                                   (customer_prof.register_date.eqNullSafe(f.to_date(recharge_data.register_date))) &
                                   (customer_prof.event_partition_date == f.to_date(recharge_data.recharge_date)),'left')

    output_df = output_df.drop(recharge_data.access_method_num)\
        .drop(recharge_data.register_date)\
        .drop(recharge_data.start_of_week)

    return output_df

def top_up_channel_joined_data(input_df,topup_type_ref):

    output_df = input_df.join(topup_type_ref,input_df.recharge_type == topup_type_ref.recharge_topup_event_type_cd,'left')

    return output_df

def customized_processing(data_frame: DataFrame, cust_prof: DataFrame, recharge_type_df: DataFrame, sql: dict, output_df_catalog) -> DataFrame:
    """
    :return:
    """
    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env='base')
    cust_data_frame = cust_prof
    dates_list = cust_prof.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.withColumn("start_of_week",F.to_date(F.date_trunc('week',data_frame.recharge_date)))
        small_df = small_df.filter(f.to_date('start_of_week').isin(*[curr_item]))
        customer_prof_df = cust_data_frame.filter(F.col('start_of_week').isin(*[curr_item]))
        joined_df = recharge_data_with_customer_profile_joined(customer_prof_df,small_df)
        joined_df_with_recharge_type = top_up_channel_joined_data(joined_df,recharge_type_df)
        output_df = node_from_config(joined_df_with_recharge_type, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.withColumn("start_of_week", F.to_date(F.date_trunc('week', data_frame.recharge_date)))
    small_df = small_df.filter(f.to_date('start_of_week').isin(*[first_item]))
    customer_prof_df = cust_data_frame.filter(F.col('start_of_week').isin(*[first_item]))
    joined_df = recharge_data_with_customer_profile_joined(customer_prof_df, small_df)
    joined_df_with_recharge_type = top_up_channel_joined_data(joined_df, recharge_type_df)
    output_df = node_from_config(joined_df_with_recharge_type, sql)

    return output_df

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

def billing_last_top_up_channel_weekly(input_df, customer_profile_df, recharge_type_df,sql) -> DataFrame:
    """
    :return:
    """
    customer_prof = customer_profile_df.select("access_method_num",
                                         "subscription_identifier",
                                         F.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",F.to_date(F.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",F.to_date(F.date_trunc('week',customer_prof.event_partition_date)))

    return_df = customized_processing(input_df, customer_prof, recharge_type_df, sql, "l2_billing_and_payments_weekly_last_top_up_channel")
    return return_df

# def billing_time_since_last_topup_weekly(input_df, sql) -> DataFrame:
#     """
#     :return:
#     """
#     return_df = massive_processing_weekly(input_df, sql, "l2_billing_and_payments_weekly_time_since_last_top_up")
#     return return_df

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





