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


def l3_number_of_bs_used(input_df):
    df = input_df.select('imsi', 'cell_id', 'time_in')
    df = df.withColumn("start_of_month", f.to_date(f.date_trunc('month', "time_in"))) \
        .drop(df.time_in)
    return df


def l3_geo_voice_distance_daily(df, sql):
    df = (df.groupBy('access_method_num', 'start_of_month').agg(F.collect_list('sum_call').alias('sum_list')
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


def l3_first_data_session_cell_identifier_monthly(df, sql):
    df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col('event_partition_date'))))
    df = df.selectExpr('*',
                       'row_number() over(partition by mobile_no,start_of_month order by event_partition_date ASC) as rank_monthly')

    return df


def l3_geo_data_distance_monthly(df, sql):
    df = (df.groupBy('mobile_no', 'start_of_month').agg(F.collect_list('sum_call').alias('sum_list')
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


def l3_geo_data_distance_weekday_monthly(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)')
    df = (df.groupBy('mobile_no', 'start_of_month').agg(F.collect_list('sum_call').alias('sum_list')
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


def l3_geo_data_distance_weekend_monthly(df, sql):
    df = df.where('day_of_week in (6,7)')
    df = (df.groupBy('mobile_no', 'start_of_month').agg(F.collect_list('sum_call').alias('sum_list')
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


def l3_geo_home_work_location_id(df, sql):
    df = df.withColumn('start_of_month',
                       f.to_date(df.partition_month.cast("string"), 'yyyyMM'))

    df = node_from_config(df, sql)

    return df


def l3_geo_data_frequent_cell_weekday_monthly(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_data_frequent_cell_weekend_monthly(df, sql):
    df = df.where('day_of_week in (6,7)')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_data_frequent_cell_monthly(df, sql):
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_data_frequent_cell_4g_weekday_monthly(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)').where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_data_frequent_cell_4g_weekend_monthly(df, sql):
    df = df.where('day_of_week in (6,7)').where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_data_frequent_cell_4g_monthly(df, sql):
    df = df.where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_month order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'lac_rank_1', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_1',
                                                                       F.when(ranked.rank == 1, ranked.ci)).withColumn(
        'lac_rank_2', F.when(ranked.rank == 1, ranked.lac)).withColumn('ci_rank_2', F.when(ranked.rank == 1, ranked.ci))
    df = node_from_config(ranked, sql)
    return df


def l3_geo_call_count_location_monthly(df, master, sql):
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    data_usage = df.groupBy('access_method_num', 'cgi_partial', 'start_of_month').agg(
        F.sum('sum_call').alias('sum_call')).drop('start_of_month')
    df = df.select('access_method_num', 'cgi_partial', 'start_of_month')
    test = master.select('access_method_num', 'cgi_partial_home').join(data_usage, [
        master.access_method_num == data_usage.access_method_num, master.cgi_partial_home == data_usage.cgi_partial],
                                                                       'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_call', 'sum_call_home')
    df = df.join(test, [df.cgi_partial == test.cgi_partial_home, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_office').join(data_usage, [
        master.access_method_num == data_usage.access_method_num, master.cgi_partial_office == data_usage.cgi_partial],
                                                                         'left').drop(
        data_usage.access_method_num).drop(data_usage.cgi_partial).withColumnRenamed('sum_call',
                                                                                     'sum_call_office')
    df = df.join(test, [df.cgi_partial == test.cgi_partial_office, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_other_rank_1').join(data_usage, [
        master.access_method_num == data_usage.access_method_num,
        master.cgi_partial_other_rank_1 == data_usage.cgi_partial], 'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_call', 'sum_call_other_rank_1')
    df = df.join(test,
                 [df.cgi_partial == test.cgi_partial_other_rank_1, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_other_rank_2').join(data_usage, [
        master.access_method_num == data_usage.access_method_num,
        master.cgi_partial_other_rank_2 == data_usage.cgi_partial], 'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_call', 'sum_call_other_rank_2')
    df = df.join(test,
                 [df.cgi_partial == test.cgi_partial_other_rank_2, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)


    df = node_from_config(df, sql)
def l3_geo_data_traffic_location_monthly(df,master,sql):
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    data_usage = df.groupBy('access_method_num', 'cgi_partial', 'start_of_month').agg(
        F.sum('sum_total_vol_kb').alias('sum_total_vol_kb')).drop('start_of_month')
    df = df.select('access_method_num', 'cgi_partial', 'start_of_month')
    test = master.select('access_method_num', 'cgi_partial_home').join(data_usage, [
        master.access_method_num == data_usage.access_method_num, master.cgi_partial_home == data_usage.cgi_partial],
                                                                       'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_total_vol_kb', 'sum_total_vol_kb_home')
    df = df.join(test, [df.cgi_partial == test.cgi_partial_home, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_office').join(data_usage, [
        master.access_method_num == data_usage.access_method_num, master.cgi_partial_office == data_usage.cgi_partial],
                                                                         'left').drop(
        data_usage.access_method_num).drop(data_usage.cgi_partial).withColumnRenamed('sum_total_vol_kb',
                                                                                     'sum_total_vol_kb_office')
    df = df.join(test, [df.cgi_partial == test.cgi_partial_office, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_other_rank_1').join(data_usage, [
        master.access_method_num == data_usage.access_method_num,
        master.cgi_partial_other_rank_1 == data_usage.cgi_partial], 'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_total_vol_kb', 'sum_total_vol_kb_other_rank_1')
    df = df.join(test,
                 [df.cgi_partial == test.cgi_partial_other_rank_1, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)
    test = master.select('access_method_num', 'cgi_partial_other_rank_2').join(data_usage, [
        master.access_method_num == data_usage.access_method_num,
        master.cgi_partial_other_rank_2 == data_usage.cgi_partial], 'left').drop(data_usage.access_method_num).drop(
        data_usage.cgi_partial).withColumnRenamed('sum_total_vol_kb', 'sum_total_vol_kb_other_rank_2')
    df = df.join(test,
                 [df.cgi_partial == test.cgi_partial_other_rank_2, df.access_method_num == test.access_method_num],
                 'left').drop(test.access_method_num)

    df = node_from_config(df, sql)
    return df
