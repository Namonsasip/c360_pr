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
    df = input_df.select('imsi', 'cell_id', 'partition_date')
    df = df.withColumn('event_partition_date',
                       f.to_date(df.partition_date.cast("string"), 'yyyyMMdd')) \
        .groupby("imsi", "event_partition_date") \
        .agg(F.collect_set("cell_id").alias('cell_id_list'))

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


def l1_geo_voice_distance_daily_intermediate(df):  # added cgi_partial
    df = df.where('service_type!="SMS"')
    df = df.selectExpr("*",
                       "CASE WHEN service_type in ('VOICE') THEN  CONCAT(LPAD(lac, 5, '0'),"
                       "LPAD(ci, 5, '0')) WHEN service_type in ('VOLTE') THEN  CONCAT(LPAD("
                       "ci, 9, '0'))  END AS cgi_partial")
    df = add_start_of_week_and_month(df, "date_id")

    df = df.select('*',
                   (df.no_of_call + df.no_of_inc).alias('sum_call'))
    df = df.groupBy('access_method_num', 'cgi_partial', 'event_partition_date', 'start_of_week', 'start_of_month').agg(
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


def l1_geo_data_distance_daily_intermediate(df):
    # df = df.where('service_type!="SMS"')
    df = add_start_of_week_and_month(df, "date_id")
    df = df.withColumn("day_of_week", F.date_format(F.col("event_partition_date"), "u"))
    # df = df.select('*', F.concat(df.lac, df.ci).alias('lac_ci'),
    #                (df.no_of_call + df.no_of_inc).alias('sum_call'))
    df = df.groupBy('access_method_num', 'lac_ci', 'event_partition_date', 'start_of_week', 'start_of_month').agg(
        F.sum('sum_call').alias('sum_call'))
    return df


def l1_first_data_session_cell_identifier_daily(df, sql):  # partial cgi added
    df = df.selectExpr('*', 'row_number() over(partition by mobile_no,date_id order by hour_id ASC) as rank')
    df = df.selectExpr("*",
                       "CASE WHEN gprs_type in ('GGSN','3GGSN') THEN  CONCAT(LPAD(lac, 5, '0'),LPAD(ci, 5, '0')) WHEN "
                       "gprs_type in ('4GLTE') THEN  CONCAT(LPAD(ci, 9, '0')) END AS cgi_partial")
    df = node_from_config(df, sql)
    return df


def l1_usage_sum_data_location_dow_intermediate(df):
    # partition for pipeline testing
    # df=df.where('partition_date="20191216"')

    df = df.selectExpr("*",
                       "CASE WHEN gprs_type in ('GGSN','3GGSN') THEN  CONCAT(LPAD(lac, 5, "
                       "'0'),LPAD(ci, 5, '0')) WHEN gprs_type in ('4GLTE') THEN  CONCAT(LPAD("
                       "ci, 9, '0'))  END AS cgi_partial")
    df = df.groupBy('date_id', 'mobile_no', 'gprs_type', 'cgi_partial').agg(F.sum('no_of_call').alias('sum_call'))
    df = add_start_of_week_and_month(df, "date_id")

    df = df.withColumn("day_of_week", F.date_format(F.col("event_partition_date"), "u"))
    return df


def l1_geo_data_distance_daily(df, sql):
    df = (df.groupBy('mobile_no', 'event_partition_date').agg(F.collect_list('sum_call').alias('sum_list')
                                                              , F.max('sum_call').alias('max')))
    df = df.withColumn('sorted_sum', F.array_sort("sum_list"))
    df = df.withColumn('length_without_max', F.size(df.sorted_sum) - 1)
    df = df.withColumn('list_without_max', F.expr("slice(sorted_sum, 1, length_without_max)"))
    distance = F.udf(lambda x, y: [y - i for i in x], T.ArrayType(T.FloatType()))
    df = df \
        .withColumn('distance_list', distance('list_without_max', 'max'))

    df = df.withColumn('max_distance', F.array_max('distance_list')) \
        .withColumn('min_distance', F.array_min('distance_list'))

    # stdev_udf = F.udf(lambda x: None if len(x) < 2 or type(x) is not list else statistics.stdev(x))

    query = """aggregate(
        `{col}`,
        CAST(0.0 AS double),
        (acc, x) -> acc + x,
        acc -> acc / size(`{col}`)
    ) AS  `avg_distance`""".format(col="distance_list")

    df = df.selectExpr("*", query)
    # df = df.withColumn('stdev_distance', stdev_udf('distance_list'))
    df = df.drop('max', 'sorted_sum', 'length_without_max', 'list_without_max', 'distance_list')

    df = node_from_config(df, sql)

    return df


def l1_geo_data_distance_weekday_daily(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)')
    df = (df.groupBy('mobile_no', 'event_partition_date').agg(F.collect_list('sum_call').alias('sum_list')
                                                              , F.max('sum_call').alias('max')))
    df = df.withColumn('sorted_sum', F.array_sort("sum_list"))
    df = df.withColumn('length_without_max', F.size(df.sorted_sum) - 1)
    df = df.withColumn('list_without_max', F.expr("slice(sorted_sum, 1, length_without_max)"))
    distance = F.udf(lambda x, y: [y - i for i in x], T.ArrayType(T.FloatType()))
    df = df \
        .withColumn('distance_list', distance('list_without_max', 'max'))

    df = df.withColumn('max_distance', F.array_max('distance_list')) \
        .withColumn('min_distance', F.array_min('distance_list'))

    # stdev_udf = F.udf(lambda x: None if len(x) < 2 or type(x) is not list else statistics.stdev(x))

    query = """aggregate(
        `{col}`,
        CAST(0.0 AS double),
        (acc, x) -> acc + x,
        acc -> acc / size(`{col}`)
    ) AS  `avg_distance`""".format(col="distance_list")

    df = df.selectExpr("*", query)
    # df = df.withColumn('stdev_distance', stdev_udf('distance_list'))
    df = df.drop('max', 'sorted_sum', 'length_without_max', 'list_without_max', 'distance_list')

    df = node_from_config(df, sql)
    return df


def l1_geo_data_distance_weekend_daily(df, sql):
    df = df.where('day_of_week in (6,7)')
    df = (df.groupBy('mobile_no', 'event_partition_date').agg(F.collect_list('sum_call').alias('sum_list')
                                                              , F.max('sum_call').alias('max')))
    df = df.withColumn('sorted_sum', F.array_sort("sum_list"))
    df = df.withColumn('length_without_max', F.size(df.sorted_sum) - 1)
    df = df.withColumn('list_without_max', F.expr("slice(sorted_sum, 1, length_without_max)"))
    distance = F.udf(lambda x, y: [y - i for i in x], T.ArrayType(T.FloatType()))
    df = df \
        .withColumn('distance_list', distance('list_without_max', 'max'))

    df = df.withColumn('max_distance', F.array_max('distance_list')) \
        .withColumn('min_distance', F.array_min('distance_list'))

    # stdev_udf = F.udf(lambda x: None if len(x) < 2 or type(x) is not list else statistics.stdev(x))

    query = """aggregate(
        `{col}`,
        CAST(0.0 AS double),
        (acc, x) -> acc + x,
        acc -> acc / size(`{col}`)
    ) AS  `avg_distance`""".format(col="distance_list")

    df = df.selectExpr("*", query)
    # df = df.withColumn('stdev_distance', stdev_udf('distance_list'))
    df = df.drop('max', 'sorted_sum', 'length_without_max', 'list_without_max', 'distance_list')

    df.show(88, False)
    df = node_from_config(df, sql)
    df.show(88, False)

    return df

# def l1_geo_data_frequent_cell_weekday_daily(df, sql):  # in progress weekday 434,437,443,449,452,455,458,461,464
#     df = df.where('day_of_week in (1,2,3,4,5)')
#     ranked = df.selectExpr('*', 'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
#     # ranked = df.withColumn('rank_1', F.when(df.rank == 1, df.sum_call)).withColumn('rank_2',
#     #                                                                                            F.when(df.rank == 2,
#     #                                                                                                   df.sum_call)).withColumn(
#     #     'rank_3', F.when(df.rank == 3, df.sum_call))
#     # ranked = ranked.withColumn('rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('rank_2',
#     #                                                                                            F.when(ranked.rank == 2,
#     #                                                                                                   ranked.sum_call)).withColumn(
#     #     'rank_3', F.when(ranked.rank == 3, ranked.sum_call))
#     # agg = ranked.groupBy('mobile_no', 'start_of_week').agg(F.sum('rank_1').alias('sum_no_of_call_rank_1'),
#     #                                                        F.sum('rank_2').alias('sum_no_of_call_rank_2'),
#     #                                                        F.sum('rank_3').alias('sum_no_of_call_rank_3'),
#     #                                                        F.count(F.lit(1)).alias("Number_of_Unique_Cells"))
#
#     ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
#         ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
#                                                        F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
#         'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
#                                                                        F.when(ranked.rank == 1, ranked.ci)).withColumn(
#         'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
#     df = node_from_config(ranked, sql)
#     agg = ranked.groupBy('mobile_no', 'start_of_week').agg(F.sum('sum_rank_1').alias('sum_no_of_call_rank_1'),
#                                                            F.sum('sum_rank_2').alias('sum_no_of_call_rank_2'),
#                                                            F.sum('sum_rank_3').alias('sum_no_of_call_rank_3'),
#                                                            F.count(F.lit(1)).alias("Number_of_Unique_Cells"),
#                                                            F.concat_ws("", F.collect_list(ranked.lac_rank_1)).alias(
#                                                                'lac_rank_1'),
#                                                            F.concat_ws("", F.collect_list(ranked.ci_rank_1)).alias(
#                                                                'ci_rank_1'),
#                                                            F.concat_ws("", F.collect_list(ranked.lac_rank_2)).alias(
#                                                                'lac_rank_2'),
#                                                            F.concat_ws("", F.collect_list(ranked.ci_rank_2)).alias(
#                                                                'ci_rank_2'))
#
#     #434 = groupby lac+ci count
#     #437 lac + ci with most  (first most)
#     #443  second most
#     #449  count cell with first most
#     #452  count cell with second most
#     #455 count third
#     #458 count fourth
#     #461 count fifth
#     #464 count all
#
#     return df
#
#
# def l1_geo_data_frequent_cell_weekend_daily(df, sql):  # in progress weekend 435,439,445,450,453,456,459,462,465
#     df = df.where('day_of_week in (6,7)')
#     df = node_from_config(df, sql)
#     return df
#
#
# def l1_geo_data_frequent_cell_daily(df, sql):  # in progress all 436, 441,447,451,454,457,460,463
#
#     df = node_from_config(df, sql)
#     return df
#
#
# def l1_geo_data_frequent_cell_4g_weekday_daily(df, sql):  # in progress 4g_weekday 438,444
#     df = df.where('day_of_week in (1,2,3,4,5)').where('gprs_type="4GLTE"')
#     df = node_from_config(df, sql)
#     return df
#
#
# def l1_geo_data_frequent_cell_4g_weekend_daily(df, sql):  # in progress 4g_weekend 440,446
#     df = df.where('day_of_week in (6,7)').where('gprs_type="4GLTE"')
#     df = node_from_config(df, sql)
#     return df
#
#
# def l1_geo_data_frequent_cell_4g_daily(df, sql):  # in progress 4g_all 442,448
#
#     df = node_from_config(df, sql)
#     return df
