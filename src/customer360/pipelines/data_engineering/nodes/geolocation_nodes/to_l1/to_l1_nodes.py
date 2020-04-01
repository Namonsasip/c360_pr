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
    df = df.withColumn('event_partition_date', f.to_date(df.partition_date.cast("string"), 'yyyyMMdd')) \
        .groupby("imsi", "event_partition_date") \
        .agg(F.collect_set("cell_id").alias('cell_id_list'))
    # df = df.select('*', F.size('cell_id_list'))

    return df


def l1_number_of_location_with_transactions(footfall_df, mst_cell_df, sql):
    footfall_df = footfall_df.select('imsi', 'cell_id', 'partition_date')
    footfall_df = footfall_df.withColumn('event_partition_date',
                                         f.to_date(footfall_df.partition_date.cast("string"), 'yyyyMMdd'))

    # for test (use time_in instead of partition_date)
    # footfall_df = footfall_df.select('imsi', 'cell_id', 'time_in')
    # footfall_df = footfall_df.withColumn('event_partition_date',
    #                                      f.to_date('time_in'))

    mst_cell_df = mst_cell_df.select('soc_cgi_hex', 'location_id')

    joined_data = footfall_df.join(mst_cell_df, footfall_df.cell_id == mst_cell_df.soc_cgi_hex, 'left')

    joined_data = joined_data.groupBy('imsi', 'event_partition_date').agg(
        F.collect_set("location_id").alias('location_list'))
    # joined_data = joined_data.select('*', F.size('location_list').alias('count_location'))

    df = node_from_config(joined_data, sql)

    return df
