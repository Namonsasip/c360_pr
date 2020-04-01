import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os


def l2_number_of_bs_used(input_df):
    df = input_df.withColumn("start_of_week", f.to_date(f.date_trunc('week', "event_partition_date"))) \
        .drop('event_partition_date')
    df = (df.groupby('imsi', 'start_of_week').agg(F.collect_set("cell_id_list")))
    df = df.select("imsi", "start_of_week",
                   f.array_distinct(f.flatten("collect_set(cell_id_list)")).alias('distinct_list'))
    # df = df.select('*', F.size('distinct_list').alias('count_bs'))

    return df


def l2_number_of_location_with_transactions(input_df, sql):
    df = input_df.withColumn("start_of_week", f.to_date(f.date_trunc('week', "event_partition_date"))) \
        .drop('event_partition_date')
    df = (df.groupby('imsi', 'start_of_week').agg(F.collect_set("location_list")))
    df = df.select("imsi", "start_of_week",
                   f.array_distinct(f.flatten("collect_set(location_list)")).alias('distinct_list'))

    df = node_from_config(df, sql)

    return df
