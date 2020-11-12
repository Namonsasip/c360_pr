import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from src.customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check

conf = os.getenv("CONF", None)


def generate_l1_layer(device_df: DataFrame
                      , customer_df: DataFrame) -> DataFrame:
    """
    :param device_df:
    :param customer_df:
    :return:
    """

    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([device_df, customer_df]):
        return get_spark_empty_df()

    device_df = data_non_availability_and_missing_check(df=device_df, grouping="daily", par_col="partition_date",
                                                       target_table_name="l1_devices_summary_customer_handset_daily")

    customer_df = data_non_availability_and_missing_check(df=customer_df, grouping="daily", par_col="event_partition_date",
                                                        target_table_name="l1_devices_summary_customer_handset_daily")

    if check_empty_dfs([device_df, customer_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################


    device_df = device_df.filter(f.col("mobile_no").isNotNull())
    device_df = device_df.withColumn("event_partition_date", f.to_date('date_id')) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', "date_id"))) \
        .withColumnRenamed("mobile_no", "access_method_num")

    join_cols = ['access_method_num',
                 'event_partition_date',
                 "start_of_week"]

    sel_cols = ['access_method_num',
                'event_partition_date',
                "subscription_identifier",
                "start_of_week"]

    final_df = device_df.join(customer_df.select(sel_cols), join_cols)
    final_df = final_df.filter(f.col("subscription_identifier").isNotNull()).distinct()

    return final_df
