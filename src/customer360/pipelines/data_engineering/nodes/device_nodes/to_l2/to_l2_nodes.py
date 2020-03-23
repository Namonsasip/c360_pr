import pyspark.sql.functions as f
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import Window
from pyspark.sql.types import StringType

conf = os.getenv("CONF", None)


def massive_processing(customer_prof, input_df, sql, is_rank, partition, output_df_catalog):
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
        output_df = join_with_customer_profile(cust_df, result_df)
        CNTX.catalog.save(output_df_catalog, output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    small_df = data_frame.filter(F.col(partition).isin(*[first_item]))
    cust_df = cust_data_frame.filter(F.col(partition).isin(*[first_item]))
    result_df = node_from_config(small_df, sql)
    if (is_rank):
        result_df = result_df.where("rank = 1")
    output_df = join_with_customer_profile(cust_df, result_df)

    return output_df


def device_most_used_weekly(customer_prof, input_df, sql):
    """
    :return:
    """
    customer_prof = derive_customer_profile(customer_prof)

    return_df = massive_processing(customer_prof, input_df, sql, True, "start_of_week", "l2_device_most_used_weekly")
    return return_df


def device_number_of_phone_updates_weekly(input_df, sql):
    """
    :return:
    """
    customer_prof = derive_customer_profile(customer_prof)

    return_df = massive_processing(customer_prof, input_df, sql, False, "start_of_week",
                                   "l2_device_number_of_phone_updates_weekly")
    return return_df


def device_previous_configurations_weekly(customer_prof, input_df, sql):
    """
    :return:
    """
    customer_prof = derive_customer_profile(customer_prof)

    return_df = massive_processing(customer_prof, input_df, sql, False, "start_of_week",
                                   "l2_previous_device_handset_summary_with_configuration_weekly")
    return return_df


def device_current_configuration_weekly(customer_prof, input_df, sql):
    """
    :return:
    """
    customer_prof = derive_customer_profile(customer_prof)

    return_df = massive_processing(customer_prof, input_df, sql, True, "start_of_week",
                                   "l2_device_handset_summary_with_configuration_weekly")
    return return_df


def derive_customer_profile(customer_prof):
    customer_prof = customer_prof.select("access_method_num",
                                         "subscription_identifier",
                                         f.to_date("register_date").alias("register_date"),
                                         "event_partition_date")

    customer_prof = customer_prof.withColumn("start_of_month",
                                             f.to_date(f.date_trunc('month', customer_prof.event_partition_date))) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', customer_prof.event_partition_date)))

    customer_prof = customer_prof.dropDuplicates(
        ["start_of_month", "start_of_week", "access_method_num", "register_date", "subscription_identifier"])

    return customer_prof


def join_with_customer_profile(customer_prof, hs_summary):
    hs_summary_with_customer_profile = customer_prof.join(hs_summary,
                                                          (customer_prof.access_method_num == hs_summary.mobile_no) &
                                                          (customer_prof.register_date.eqNullSafe(
                                                              f.to_date(hs_summary.register_date))) &
                                                          (customer_prof.start_of_week == hs_summary.start_of_week),
                                                          "left")

    hs_summary_with_customer_profile = hs_summary_with_customer_profile.drop(hs_summary.register_date) \
        .drop(hs_summary.start_of_week) \
        .drop(hs_summary.start_of_month)

    return hs_summary_with_customer_profile


def device_summary_with_configuration(hs_summary, hs_configs):
    hs_configs = hs_configs.withColumn("partition_date", hs_configs["partition_date"].cast(StringType()))
    hs_configs = hs_configs.withColumn("start_of_week",
                                       f.to_date(f.date_trunc('week', f.to_date(f.col("partition_date"), 'yyyyMMdd'))))

    hs_config_sel = ["start_of_week", "hs_brand_code", "hs_model_code", "month_id", "os",
                     "launchprice", "saleprice", "gprs_handset_support", "hsdpa", "google_map", "video_call"]

    hs_configs = hs_configs.select(hs_config_sel)

    partition = Window.partitionBy(["start_of_week", "hs_brand_code", "hs_model_code"]).orderBy(F.col("month_id").desc())

    # removing duplicates within a week
    hs_configs = hs_configs.withColumn("rnk", F.row_number().over(partition))
    hs_configs = hs_configs.filter(f.col("rnk") == 1)

    joined_data = hs_summary.join(hs_configs,
                                  (hs_summary.handset_brand_code == hs_configs.hs_brand_code) &
                                  (hs_summary.handset_model_code == hs_configs.hs_model_code) &
                                  (hs_summary.start_of_week == hs_configs.start_of_week), "left") \
                                  .drop(hs_configs.start_of_week) \

    return joined_data
