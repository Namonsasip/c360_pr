import pyspark.sql.functions as f
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os

conf = os.environ["CONF"]
def massive_processing(input_df, sql, partition, output_df_catalog):
    """
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select(partition).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(partition).isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(partition).isin(*[first_item]))
    return_df = node_from_config(return_df, sql)

    return return_df

def device_most_used_weekly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_week", "l2_device_most_used_weekly")
    return return_df

def device_number_of_phone_updates_weekly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_week", "l2_device_number_of_phone_updates_weekly")
    return return_df

def device_previous_configurations_weekly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing(input_df, sql, "start_of_week", "l2_previous_device_handset_summary_with_configuration_weekly")
    return return_df

def device_summary_with_customer_profile(customer_prof,hs_summary):

    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",f.to_date(f.date_trunc('month',customer_prof.event_partition_date)))\
        .withColumn("start_of_week",f.to_date(f.date_trunc('week',customer_prof.event_partition_date)))

    hs_summary_with_customer_profile = customer_prof.join(hs_summary,
                                  (customer_prof.access_method_num == hs_summary.mobile_no) &
                                  (customer_prof.register_date.eqNullSafe(f.to_date(hs_summary.register_date))) &
                                  (customer_prof.event_partition_date == f.to_date(hs_summary.date_id)), "left")

    hs_summary_with_customer_profile = hs_summary_with_customer_profile.drop(hs_summary.register_date)

    return hs_summary_with_customer_profile


def device_summary_with_config(hs_summary,hs_configs):

    hs_configs = hs_configs.withColumn("start_of_month",f.to_date(f.date_trunc('month',"month_id")))

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_month == hs_configs.start_of_month), "left")\
        .drop(hs_configs.start_of_month)\
        .drop(hs_summary.handset_type)\
        .drop(hs_configs.dual_sim)\
        .drop(hs_configs.hs_support_lte_1800)

    return joined_data

def filter(input_df):

    output_df = input_df.where("rank = 1").drop("rank")

    return output_df