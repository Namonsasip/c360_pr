import os

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

conf = os.getenv("CONF", None)


def generate_l1_layer(device_df: DataFrame
                      , customer_df: DataFrame) -> DataFrame:
    """
    :param device_df:
    :param customer_df:
    :return:
    """

    if len(device_df.head(1)) == 0:
        return device_df

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
