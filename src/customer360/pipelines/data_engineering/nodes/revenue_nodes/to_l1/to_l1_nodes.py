import logging
import os
from pathlib import Path

from kedro.context.context import load_context
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from customer360.utilities.config_parser import node_from_config
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, \
    data_non_availability_and_missing_check
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

    customer_df = data_non_availability_and_missing_check(df=customer_df, grouping="daily",
                                                          par_col="event_partition_date",
                                                          target_table_name="l1_revenue_prepaid_pru_f_usage_multi_daily")

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
                            .withColumn("filter_date", F.to_date(F.col("partition_date").cast(StringType()), 'yyyy-MM-dd'))
    dates_list = data_frame.select('filter_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    customer_df = customer_df.where("charge_type = 'Pre-paid'") \
        .select("access_method_num", "subscription_identifier", "event_partition_date", "start_of_week")
    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("filter_date").isin(*[curr_item])) \
            .drop_duplicates(subset=["access_method_num", "partition_date"])
        small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        output_df = small_cus_df.join(output_df, ["access_method_num", "event_partition_date", "start_of_week"], "left")
        CNTX.catalog.save("l1_revenue_prepaid_pru_f_usage_multi_daily", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("filter_date").isin(*[first_item])) \
        .drop_duplicates(subset=["access_method_num", "partition_date"])
    return_df = node_from_config(return_df, sql)
    small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[first_item]))
    return_df = small_cus_df.join(return_df, ["access_method_num", "event_partition_date", "start_of_week"], "left")
    return return_df
