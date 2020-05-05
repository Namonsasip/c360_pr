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
from pyspark.sql import Window

from customer360.utilities.re_usable_functions import add_start_of_week_and_month, union_dataframes_with_missing_cols


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


def l1_geo_favorite_cell_master_table(usage_sum_voice_location_daily, usage_sum_data_location_daily):
    '''
    Ans
       - GGSN: 2G
       - 3GGSN: 3G
       - 4GLTE: 4G
       - 0,4: International outbound roaming (IR) . It can not map to geo-master.
       - 8: NB-IoT (currently can use 4G cell only)
    '''

    #test
    usage_sum_voice_location_daily = usage_sum_voice_location_daily.limit(100000)
    usage_sum_data_location_daily = usage_sum_data_location_daily.limit(100000)



    usage_sum_voice_location_daily = usage_sum_voice_location_daily.where('service_type in ("VOICE","VOLTE")')
    usage_sum_voice_location_daily = usage_sum_voice_location_daily.selectExpr("*",
                                                                               "CASE WHEN service_type in ('VOICE') "
                                                                               "THEN  CONCAT(LPAD(lac, 5, '0'),"
                                                                               "LPAD(ci, 5, '0')) WHEN service_type "
                                                                               "in ('VOLTE') THEN  CONCAT(LPAD(ci, 9, "
                                                                               "'0')) END AS cgi_partial")
    # voice_location contains column no_of_call and no_of_inc
    usage_sum_voice_location_daily = usage_sum_voice_location_daily.selectExpr('*',
                                                                               'no_of_call+no_of_inc as no_of_call_new')
    usage_sum_voice_location_daily = usage_sum_voice_location_daily.drop('no_of_call').withColumnRenamed(
        "no_of_call_new", "no_of_call")

    usage_sum_data_location_daily = usage_sum_data_location_daily.where('gprs_type in ("GGSN","3GGSN","4GLTE")')
    usage_sum_data_location_daily = usage_sum_data_location_daily.selectExpr("*",
                                                                             "CASE WHEN gprs_type in ('GGSN','3GGSN') "
                                                                             "THEN  CONCAT(LPAD(lac, 5, '0'),LPAD(ci, "
                                                                             "5, '0')) WHEN gprs_type in ('4GLTE') "
                                                                             "THEN  CONCAT(LPAD(ci, 9, '0')) END AS "
                                                                             "cgi_partial")

    merged = union_dataframes_with_missing_cols([usage_sum_voice_location_daily.select("hour_id",
                                                                                       "access_method_num",
                                                                                       "cgi_partial", "no_of_call",
                                                                                       "date_id"),
                                                 usage_sum_data_location_daily.select("hour_id",
                                                                                      F.col("mobile_no").alias(
                                                                                          "access_method_num"),
                                                                                      "cgi_partial", "no_of_call",
                                                                                      "date_id")])
    merged = merged.withColumn("day_type", F.when(
        (F.date_format(F.col('date_id'), 'EEEE') != 'Saturday') & (F.date_format(F.col('date_id'), 'EEEE') != 'Sunday'),
        F.lit('Weekday')).otherwise(F.lit("Weekend")))
    merged = merged.withColumn("day_night",
                               F.when(((F.col('hour_id') >= 20) & (F.col('hour_id') <= 8)), F.lit('Night')).otherwise(
                                   F.lit("Day")))
    win = Window.partitionBy("access_method_num").orderBy(F.col("no_of_call").desc())
    home = merged.where("day_night = 'Night' or day_type = 'Weekend'").groupBy("access_method_num", "cgi_partial").agg(
        F.sum(F.col("no_of_call")).alias("no_of_call"))
    office = merged.where("day_night = 'Day' and day_type = 'Weekday'").groupBy("access_method_num", "cgi_partial").agg(
        F.sum(F.col("no_of_call")).alias("no_of_call"))
    temp_home = home

    home = home.withColumn("rnk", F.row_number().over(win)).where("rnk = 1").select("access_method_num", "cgi_partial")
    office = office.withColumn("rnk", F.row_number().over(win)).where("rnk = 1").select("access_method_num",
                                                                                        "cgi_partial")

    ## use the above dataset to start building features at L1
    union_home_office = home.union(office)
    union_home_office = union_home_office.withColumnRenamed('cgi_partial', 'cgi_partial_2').withColumnRenamed(
        'access_method_num', 'access_method_num_2')
    home = home.withColumnRenamed('cgi_partial', 'cgi_partial_home')
    office = office.withColumnRenamed('cgi_partial', 'cgi_partial_office')
    home_office = home.join(office, ["access_method_num"], how='outer')

    all = merged.groupBy("access_method_num", "cgi_partial").agg(F.sum(F.col("no_of_call")).alias("no_of_call"))
    ## Now find the top cell_ds similar to the above but make sure we do not have above cell_ids from home and office
    top_other = all.join(union_home_office, (all.access_method_num == union_home_office.access_method_num_2) & (
            all.cgi_partial == union_home_office.cgi_partial_2), how='left')
    top_other = top_other.filter(F.col("cgi_partial_2").isNull())
    ranked = top_other.withColumn("rnk", F.row_number().over(win)).select("access_method_num", "cgi_partial")

    ## Top Rank 1 apart from home/office take source as merged
    other_rank1 = ranked.where("rnk = 1").selectExpr("access_method_num", "cgi_partial as cgi_partial_other_rank_1")

    ## Top Rank 2 apart from home/office take source as merged
    other_rank2 = ranked.where("rnk = 2").selectExpr("access_method_num", "cgi_partial as cgi_partial_other_rank_2")
    ## Now it is upto your implementation if you want to merge it or not
    master_fav_cell = home_office.join(other_rank1, ['access_method_num'], how='left').join(other_rank2,
                                                                                            ['access_method_num'],
                                                                                            how='left')

    return master_fav_cell
