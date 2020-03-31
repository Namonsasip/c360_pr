import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os



def l1_int_number_of_bs_used(input_df):
    # df = input_df.select('imsi', 'cell_id', 'time_in')
    df = input_df.select('imsi', 'cell_id', 'partition_date')
    # df = df.withColumn("event_partition_date", f.to_date('time_in')) \
    df = df.withColumn('event_partition_date',f.to_date(df.partition_date.cast("string"), 'yyyyMMdd'))\
        .groupby("imsi","event_partition_date")\
        .agg(F.collect_set("cell_id").alias('cell_id_list'))
    df = df.select('*', F.size('cell_id_list'))

    return df
