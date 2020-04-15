from pyspark.sql import DataFrame
from customer360.utilities.re_usable_functions import union_dataframes_with_missing_cols, execute_sql
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from src.customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)

def massive_processing_with_customer(input_df, customer_df, sql):
    """
    :return:
    """

    if len(input_df.head(1)) == 0 | len(customer_df.head(1)) == 0:
        return get_spark_empty_df()

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new
    customer_df = customer_df.where("charge_type = 'Pre-paid'")\
        .select("access_method_num", "subscription_identifier", "event_partition_date", "start_of_week")
    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item]))
        small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[curr_item]))
        output_df = node_from_config(small_df, sql)
        output_df = small_cus_df.join(output_df, ["access_method_num", "event_partition_date"], "left")
        CNTX.catalog.save("l1_revenue_prepaid_pru_f_usage_multi_daily", output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("partition_date").isin(*[first_item]))
    return_df = node_from_config(return_df, sql)
    small_cus_df = customer_df.filter(F.col("event_partition_date").isin(*[first_item]))
    return_df =  small_cus_df.join(return_df, ["access_method_num", "event_partition_date"], "left")
    return return_df
