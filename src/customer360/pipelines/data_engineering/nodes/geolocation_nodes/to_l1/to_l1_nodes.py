import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
import statistics

from customer360.utilities.re_usable_functions import add_start_of_week_and_month


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

    mst_cell_df = mst_cell_df.select('soc_cgi_hex', 'location_id')

    joined_data = footfall_df.join(mst_cell_df, footfall_df.cell_id == mst_cell_df.soc_cgi_hex, 'left')

    joined_data = joined_data.groupBy('imsi', 'event_partition_date').agg(
        F.collect_set("location_id").alias('location_list'))
    # joined_data = joined_data.select('*', F.size('location_list').alias('count_location'))

    df = node_from_config(joined_data, sql)

    return df


def l1_geo_voice_distance_daily_intermediate(df):
    df = df.where('service_type!="SMS"')
    df = add_start_of_week_and_month(df, "date_id")

    df = df.select('*', F.concat(df.lac, df.ci).alias('lac_ci'),
                   (df.no_of_call + df.no_of_inc).alias('sum_call'))
    df = df.groupBy('access_method_num', 'lac_ci', 'event_partition_date', 'start_of_week', 'start_of_month').agg(
        F.sum('sum_call').alias('sum_call'))
    return df


def l1_geo_voice_distance_daily(df, sql):
    df = (df.groupBy('access_method_num', 'event_partition_date').agg(F.collect_list('sum_call').alias('sum_list')
                                                                      , F.max('sum_call').alias('max')))
    df = df.withColumn('sorted_sum', F.array_sort("sum_list"))
    df = df.withColumn('length_without_max', F.size(df.sorted_sum) - 1)
    df = df.withColumn('list_without_max', F.expr("slice(sorted_sum, 1, length_without_max)"))
    distance = F.udf(lambda x, y: [y - i for i in x], T.ArrayType(T.FloatType()))
    df = df \
        .withColumn('distance_list', distance('list_without_max', 'max'))

    df = df.withColumn('max_distance', F.array_max('distance_list')) \
        .withColumn('min_distance', F.array_min('distance_list'))

    stdev_udf = F.udf(lambda x: None if len(x) < 2 or type(x) is not list else statistics.stdev(x))

    query = """aggregate(
        `{col}`,
        CAST(0.0 AS double),
        (acc, x) -> acc + x,
        acc -> acc / size(`{col}`)
    ) AS  `avg_distance`""".format(col="distance_list")

    df = df.selectExpr("*", query)
    df = df.withColumn('stdev_distance', stdev_udf('distance_list'))
    df = df.drop('max', 'sorted_sum', 'length_without_max', 'list_without_max', 'distance_list')

    df = node_from_config(df, sql)

    return df
