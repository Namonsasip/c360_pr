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


def l2_number_of_bs_used(input_df,sql):

    df = input_df.withColumn("start_of_week", f.to_date(f.date_trunc('week', "event_partition_date"))) \
        .drop('event_partition_date')
    # print('beforenodete111')
    # df.show()
    df = (df.groupby('imsi', 'start_of_week').agg(F.collect_set("cell_id_set"),F.collect_list("cell_id_list")))
    df = df.select("imsi", "start_of_week",
                   f.array_distinct(f.flatten("collect_set(cell_id_set)")).alias('cell_id_set'),
                   f.concat(f.flatten("collect_list(cell_id_list)")).alias('cell_id_list')
                   )
    # df = df.select('*', F.size('distinct_list').alias('count_bs'))
    # print('beforenodetest')
    # df.show()
    df = node_from_config(df, sql)
    # print('afternodetest')
    # df.show()
    return df


def l2_number_of_location_with_transactions(input_df, sql):
    df = input_df.withColumn("start_of_week", f.to_date(f.date_trunc('week', "event_partition_date"))) \
        .drop('event_partition_date')
    df = (df.groupby('access_method_num', 'start_of_week').agg(F.collect_set("location_list")))
    df = df.select("access_method_num", "start_of_week",
                   f.array_distinct(f.flatten("collect_set(location_list)")).alias('distinct_list'),
                   )
    print('beforenodetest')
    df.show()
    df = node_from_config(df, sql)
    print('afternodetest')
    df.show()
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


def l2_first_data_session_cell_identifier_weekly(df, sql):
    df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col('event_partition_date'))))
    df = df.selectExpr('*',
                       'row_number() over(partition by mobile_no,start_of_week order by event_partition_date ASC) as rank_weekly')
    df = df.withColumnRenamed('mobile_no','access_method_num')
    return df


def l2_geo_data_distance_weekly(df, sql):
    df = (df.groupBy('mobile_no', 'start_of_week').agg(F.collect_list('sum_call').alias('sum_list')
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
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(df, sql)

    return df


def l2_geo_data_distance_weekday_weekly(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)')
    df = (df.groupBy('mobile_no', 'start_of_week').agg(F.collect_list('sum_call').alias('sum_list')
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
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(df, sql)
    return df


def l2_geo_data_distance_weekend_weekly(df, sql):
    df = df.where('day_of_week in (6,7)')
    df = (df.groupBy('mobile_no', 'start_of_week').agg(F.collect_list('sum_call').alias('sum_list')
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
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(df, sql)
    return df


def l2_geo_data_frequent_cell_weekday_weekly(df, sql):
    # 434 = groupby lac+ci count
    # 437 lac + ci with most  (first most)
    # 443  second most
    # 449  count cell with first most
    # 452  count cell with second most
    # 455 count third
    # 458 count fourth
    # 461 count fifth
    # 464 count all
    df = df.where('day_of_week in (1,2,3,4,5)')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)
    # agg = ranked.groupBy('mobile_no', 'start_of_week').agg(F.sum('sum_rank_1').alias('sum_no_of_call_rank_1'),
    #                                                        F.sum('sum_rank_2').alias('sum_no_of_call_rank_2'),
    #                                                        F.sum('sum_rank_3').alias('sum_no_of_call_rank_3'),
    #                                                        F.count(F.lit(1)).alias("Number_of_Unique_Cells"),
    #                                                        F.concat_ws("", F.collect_list(ranked.lac_rank_1)).alias(
    #                                                            'lac_rank_1'),
    #                                                        F.concat_ws("", F.collect_list(ranked.ci_rank_1)).alias(
    #                                                            'ci_rank_1'),
    #                                                        F.concat_ws("", F.collect_list(ranked.lac_rank_2)).alias(
    #                                                            'lac_rank_2'),
    #                                                        F.concat_ws("", F.collect_list(ranked.ci_rank_2)).alias(
    #                                                            'ci_rank_2'))

    return df


def l2_geo_data_frequent_cell_weekend_weekly(df, sql):
    df = df.where('day_of_week in (6,7)')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)
    return df


def l2_geo_data_frequent_cell_weekly(df, sql):
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')

    ranked = ranked.withColumn('sum_rank_1', F.when(ranked.rank == 1, ranked.sum_call)).withColumn('sum_rank_2', F.when(
        ranked.rank == 2, ranked.sum_call)).withColumn('sum_rank_3',
                                                       F.when(ranked.rank == 3, ranked.sum_call)).withColumn(
        'sum_rank_4', F.when(ranked.rank == 4, ranked.sum_call)).withColumn('sum_rank_5', F.when(ranked.rank == 5,
                                                                                                 ranked.sum_call)).withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)
    return df


def l2_geo_data_frequent_cell_4g_weekday_weekly(df, sql):
    df = df.where('day_of_week in (1,2,3,4,5)').where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)

    return df


def l2_geo_data_frequent_cell_4g_weekend_weekly(df, sql):
    df = df.where('day_of_week in (6,7)').where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)

    return df


def l2_geo_data_frequent_cell_4g_weekly(df, sql):
    df = df.where('gprs_type="4GLTE"')
    ranked = df.selectExpr('*',
                           'row_number() over(partition by mobile_no,start_of_week order by sum_call DESC) as rank')
    ranked = ranked.withColumn(
        'cgi_partial_rank_1', F.when(ranked.rank == 1, ranked.cgi_partial)).withColumn('cgi_partial_rank_2',
                                                                       F.when(ranked.rank == 2, ranked.cgi_partial))
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    df = node_from_config(ranked, sql)

    return df

def l2_geo_call_count_location_weekly(df,master,sql):
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    data_usage = df.groupBy('access_method_num', 'cgi_partial', 'start_of_week').agg(
        F.sum('sum_call').alias('sum_call')).drop('start_of_week')
    df = df.select('access_method_num', 'cgi_partial', 'start_of_week')
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
    return df

def l2_geo_data_traffic_location_weekly(df,master,sql):
    df = df.withColumnRenamed('mobile_no', 'access_method_num')
    data_usage = df.groupBy('access_method_num', 'cgi_partial', 'start_of_week').agg(
        F.sum('sum_total_vol_kb').alias('sum_total_vol_kb')).drop('start_of_week')
    df = df.select('access_method_num', 'cgi_partial', 'start_of_week')
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

def l2_geo_data_share_location_weekly(data_loc_int,master,sql):
    master_home = master.select('access_method_num', 'cgi_partial_home', 'cgi_partial_office',
                                'cgi_partial_other_rank_1', 'cgi_partial_other_rank_2')
    # select event_partition_date / start_of_week / start_of_month   base on  daily/weekly/monthly
    data_for_join = data_loc_int.select('start_of_week', 'sum_total_vol_kb', 'mobile_no', 'cgi_partial')
    home_usage = master_home.join(data_for_join, [master_home.access_method_num == data_for_join.mobile_no,
                                                  master_home.cgi_partial_home == data_for_join.cgi_partial],
                                  'left').drop('mobile_no', 'cgi_partial').withColumnRenamed('sum_total_vol_kb',
                                                                                             'sum_total_vol_kb_home').drop(
        'cgi_partial_office', 'cgi_partial_other_rank_1', 'cgi_partial_other_rank_2')

    office_usage = master_home.join(data_for_join, [master_home.access_method_num == data_for_join.mobile_no,
                                                    master_home.cgi_partial_office == data_for_join.cgi_partial],
                                    'left').drop('mobile_no', 'cgi_partial').withColumnRenamed('sum_total_vol_kb',
                                                                                               'sum_total_vol_kb_office').drop(
        'cgi_partial_home', 'cgi_partial_other_rank_1', 'cgi_partial_other_rank_2')

    other1_usage = master_home.join(data_for_join, [master_home.access_method_num == data_for_join.mobile_no,
                                                    master_home.cgi_partial_other_rank_1 == data_for_join.cgi_partial],
                                    'left').drop('mobile_no', 'cgi_partial').withColumnRenamed('sum_total_vol_kb',
                                                                                               'sum_total_vol_kb_other_rank_1').drop(
        'cgi_partial_home', 'cgi_partial_office', 'cgi_partial_other_rank_2')

    other2_usage = master_home.join(data_for_join, [master_home.access_method_num == data_for_join.mobile_no,
                                                    master_home.cgi_partial_other_rank_2 == data_for_join.cgi_partial],
                                    'left').drop('mobile_no', 'cgi_partial').withColumnRenamed('sum_total_vol_kb',
                                                                                               'sum_total_vol_kb_other_rank_2').drop(
        'cgi_partial_home', 'cgi_partial_office', 'cgi_partial_other_rank_1')

    df = data_for_join.join(home_usage, [data_loc_int.mobile_no == home_usage.access_method_num,
                                         data_loc_int.cgi_partial == home_usage.cgi_partial_home], 'left').drop(home_usage.start_of_week).drop(
        'access_method_num')
    # .drop(home_usage.event_partition_date)
    temp = df
    print(123)
    df.show()
    temp.show()
    df = df.join(office_usage,
                 [df.mobile_no == office_usage.access_method_num, df.cgi_partial == office_usage.cgi_partial_office],
                 'left').drop(office_usage.start_of_week).drop('access_method_num')
    temp2 = df
    # df=df.drop(office_usage.event_partition_date)

    df = df.join(other1_usage, [df.mobile_no == other1_usage.access_method_num,
                                df.cgi_partial == other1_usage.cgi_partial_other_rank_1], 'left').drop(other1_usage.start_of_week).drop(
        'access_method_num')
    # .drop(other1_usage.event_partition_date)
    df = df.join(other2_usage, [df.mobile_no == other2_usage.access_method_num,
                                df.cgi_partial == other2_usage.cgi_partial_other_rank_2], 'left').drop(other2_usage.start_of_week).drop(
        'access_method_num').withColumnRenamed('mobile_no', 'access_method_num')

    # .drop(other2_usage.event_partition_date)
    # df = df.groupBy('event_partition_date', 'access_method_num', ).agg(
    #     F.sum('sum_total_vol_kb').alias('sum_total_vol_kb'),
    #     F.sum('sum_total_vol_kb_home').alias('sum_total_vol_kb_home'),
    #     F.sum('sum_total_vol_kb_office').alias('sum_total_vol_kb_office'),
    #     F.sum('sum_total_vol_kb_other_rank_1').alias('sum_total_vol_kb_other_rank_1'),
    #     F.sum('sum_total_vol_kb_other_rank_2').alias('sum_total_vol_kb_other_rank_2'))
    # df = df.selectExpr('*', 'sum_total_vol_kb_home/sum_total_vol_kb*100 as data_share_home_percent',
    #                    'sum_total_vol_kb_office/sum_total_vol_kb*100 as data_share_office_percent',
    #                    'sum_total_vol_kb_other_rank_1/sum_total_vol_kb*100 as data_share_other_rank_1_percent',
    #                    'sum_total_vol_kb_other_rank_2/sum_total_vol_kb*100 as data_share_other_rank_2_percent')
    print('testdebug')
    df.show(20,False)
    df = node_from_config(df, sql)
    print('after')
    df.show()
    return df