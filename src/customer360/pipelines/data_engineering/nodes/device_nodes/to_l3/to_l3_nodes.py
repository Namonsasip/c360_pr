from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
from customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import massive_processing
import os

conf = os.getenv("CONF", None)


def massive_processing_monthly(input_df, sql, output_df_catalog):
    """
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select("start_of_month").distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("start_of_month").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("start_of_month").isin(*[first_item]))
    return_df = node_from_config(return_df, sql)

    return return_df


def customize_massive_processing(customer_prof, input_df, sql, is_rank, partition, output_df_catalog):
    """
    :return:
    """

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    cust_data_frame = customer_prof
    dates_list = cust_data_frame.select(partition).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(partition).isin(*[curr_item]))
        cust_df = cust_data_frame.filter(F.col(partition).isin(*[curr_item]))
        result_df = node_from_config(small_df, sql)
        if (is_rank):
            result_df = result_df.where("rank = 1")
        output_df = join_with_customer_profile_monthly(cust_df, result_df)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col(partition).isin(*[first_item]))
    cust_df = cust_data_frame.filter(F.col(partition).isin(*[first_item]))
    result_df = node_from_config(small_df, sql)
    if (is_rank):
        result_df = result_df.where("rank = 1")
    output_df = join_with_customer_profile_monthly(cust_df, result_df)

    return output_df


def device_most_used_monthly(customer_prof, input_df, sql):
    """
    :return:
    """
    customer_prof = customer_prof.where("cust_active_this_month = 'Y'") \
        .select(F.col("partition_month").alias("start_of_month"),
                "access_method_num",
                F.to_date("register_date").alias("register_date"),
                "subscription_identifier")

    return_df = customize_massive_processing(customer_prof, input_df, sql, True, "start_of_month",
                                             "l3_device_most_used_monthly")
    return return_df


def device_number_of_phone_updates_monthly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing_monthly(input_df, sql, "l3_device_number_of_phone_updates_monthly")
    return return_df


def device_current_configurations_monthly(input_df, sql):
    """
    :return:
    """
    return_df = massive_processing_monthly(input_df, sql, "l3_device_handset_summary_with_configuration_monthly")
    return return_df


def device_previous_configurations_monthly(customer_prof, input_df, sql):
    """
    :return:
    """
    customer_prof = customer_prof.where("cust_active_this_month = 'Y'") \
        .select(F.col("partition_month").alias("start_of_month"),
                "access_method_num",
                F.to_date("register_date").alias("register_date"),
                "subscription_identifier")

    return_df = customize_massive_processing(customer_prof, input_df, sql, False, "start_of_month",
                                             "l3_previous_device_handset_summary_with_configuration_monthly")
    return return_df


def join_with_customer_profile_monthly(customer_prof, hs_summary):
    hs_summary_with_customer_profile = customer_prof.join(hs_summary,
                                                          (customer_prof.access_method_num == hs_summary.mobile_no) &
                                                          (customer_prof.register_date.eqNullSafe(
                                                              F.to_date(hs_summary.register_date))) &
                                                          (customer_prof.start_of_month == hs_summary.start_of_month),
                                                          "left")

    hs_summary_with_customer_profile = hs_summary_with_customer_profile.drop(hs_summary.register_date) \
        .drop(hs_summary.start_of_month)

    return hs_summary_with_customer_profile
