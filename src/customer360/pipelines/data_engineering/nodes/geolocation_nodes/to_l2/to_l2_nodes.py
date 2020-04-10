import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from customer360.utilities.config_parser import node_from_config
from kedro.context.context import load_context
from pathlib import Path
import logging
import os
from pyspark.sql import types as T
import statistics


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


def l2_geo_voice_distance_daily(df, sql):
    df = (df.groupBy('access_method_num', 'start_of_week').agg(F.collect_list('sum_call').alias('sum_list')
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

def l2_first_data_session_cell_identifier_weekly(df,sql):
    df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))
    df = df.selectExpr('*', 'row_number() over(partition by mobile_no,start_of_week order by event_partition_date ASC) as rank_weekly')

    return df