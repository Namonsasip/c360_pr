import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os


def l0_int_number_of_bs_used(input_df):
    df = input_df.select('imsi', 'cell_id', 'time_in')
    df = df.withColumn("event_partition_date", f.to_date('time_in')) \
        .withColumn("start_of_week", f.to_date(f.date_trunc('week', "time_in"))) \
        .withColumn("start_of_month", f.to_date(f.date_trunc('month', "time_in"))) \
        .drop(df.time_in)

    return df
