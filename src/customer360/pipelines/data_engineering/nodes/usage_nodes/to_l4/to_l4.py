from functools import reduce
from typing import Dict, Any

from pyspark.sql import DataFrame
from kedro.context import load_context
import logging, os
from pathlib import Path
from pyspark.sql import functions as F

from customer360.utilities.re_usable_functions import check_empty_dfs
from customer360.utilities.spark_util import get_spark_empty_df, get_spark_session

conf = os.getenv("CONF", None)


def join_all(dfs, on, how="full"):
    """
    Merge all the dataframes
    """
    return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)


def create_sql_stmt(config: dict, group_cols: Dict[str, Any], table_name: str, sub_id_table: str, suffix: str):
    sql_str = "SELECT "

    group_str = ','.join(group_cols)
    sql_str = sql_str + group_str + ","

    for agg_function, column_list in config["feature_list"].items():
        for each_feature_column in column_list:
            sql_str = sql_str+"{}({}) as {}_{}_{},".format(agg_function, each_feature_column, agg_function, each_feature_column, suffix)

    sql_str = sql_str[:-1] + " from {} ".format(table_name)
    sql_str = sql_str + "where subscription_identifier in (select subscription_identifier from {})".format(sub_id_table)
    sql_str = sql_str + " group by {}".format(group_str)
    logging.info("SQL Statement => " + sql_str)

    return sql_str


def split_category_rolling_windows_by_metadata(df_input: DataFrame, config: dict, target_table: str):
    if check_empty_dfs([df_input]):
        return get_spark_empty_df()

    spark = get_spark_session()

    group_cols = config["partition_by"]
    logging.info("group_cols: " + str(','.join(group_cols)))
    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == target_table) \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd')))
    df_maxdate = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 7))
    m_date_str = str(df_maxdate.collect()[0].max_date)
    logging.info("max date to load data: " + m_date_str)

    current_df = df_input.filter(F.col("start_of_week") == m_date_str).select("subscription_identifier").distinct()
    current_df.createOrReplaceTempView("sub_id_current")
    logging.info("----------- Create Temp Table sub_id_current Completed -----------")

    # look back last week
    date_of_last_week = df_maxdate.select(F.date_trunc("week", df_maxdate.max_date).alias("max_date"))\
        .collect()[0].max_date
    df_last_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")) == date_of_last_week)
    sql_last_week = create_sql_stmt(config, group_cols, "input_last_week", "sub_id_current", "weekly_last_week")
    df_last_week.createOrReplaceTempView("input_last_week")
    output_last_week = spark.sql(sql_last_week)

    # look back last 2 week
    date_of_last_two_week = df_maxdate.select(F.date_trunc("week", F.date_sub(df_maxdate.max_date, 7)).alias("max_date")) \
        .collect()[0].max_date
    df_last_two_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")).between(date_of_last_two_week, date_of_last_week))
    sql_last_two_week = create_sql_stmt(config, group_cols, "input_last_two_week", "sub_id_current", "weekly_last_two_week")
    df_last_two_week.createOrReplaceTempView("input_last_two_week")
    output_last_two_week = spark.sql(sql_last_two_week)

    # look back last 4 week
    date_of_last_four_week = df_maxdate.select(F.date_trunc("week", F.date_sub(df_maxdate.max_date, 21)).alias("max_date")) \
        .collect()[0].max_date
    df_last_four_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")).between(date_of_last_four_week, date_of_last_week))
    sql_last_four_week = create_sql_stmt(config, group_cols, "input_last_four_week", "sub_id_current", "weekly_last_four_week")
    df_last_four_week.createOrReplaceTempView("input_last_four_week")
    output_last_four_week = spark.sql(sql_last_four_week)

    # look back last 12 week
    date_of_last_twelve_week = df_maxdate.select(F.date_trunc("week", F.date_sub(df_maxdate.max_date, 77)).alias("max_date")) \
        .collect()[0].max_date
    df_last_twelve_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")).between(date_of_last_twelve_week, date_of_last_week))
    sql_last_twelve_week = create_sql_stmt(config, group_cols, "input_last_twelve_week", "sub_id_current", "weekly_last_twelve_week")
    df_last_twelve_week.createOrReplaceTempView("input_last_twelve_week")
    output_last_twelve_week = spark.sql(sql_last_twelve_week)

    # join
    logging.info("windows ------- > run join column")
    df_return = join_all([output_last_week, output_last_two_week, output_last_four_week, output_last_twelve_week], on=group_cols, how="full", )
    df_return = df_return.withColumn("start_of_week", F.lit(m_date_str))

    return df_return


def merge_all_usage_outputs(df1: DataFrame, df2: DataFrame, df3: DataFrame, df4: DataFrame) -> DataFrame:
    """
    :param df1:
    :param df2:
    :param df3:
    :return:
    """

    def divide_chunks(l, n):

        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    dates_list = df1.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    join_key = ["subscription_identifier", "start_of_week"]

    mvv_new = list(divide_chunks(mvv_array, 5))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        first_df = df1.filter(F.col("start_of_week").isin(*[curr_item]))
        second_df = df2.filter(F.col("start_of_week").isin(*[curr_item]))
        third_df = df3.filter(F.col("start_of_week").isin(*[curr_item]))
        fourth_df = df4.filter(F.col("start_of_week").isin(*[curr_item]))

        final_df = first_df.join(second_df, join_key)
        final_df = final_df.join(third_df, join_key)
        final_df = final_df.join(fourth_df, join_key)

        CNTX.catalog.save("l4_usage_postpaid_prepaid_weekly_features", final_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    first_df = df1.filter(F.col("start_of_week").isin(*[first_item]))
    second_df = df2.filter(F.col("start_of_week").isin(*[first_item]))
    third_df = df3.filter(F.col("start_of_week").isin(*[first_item]))
    fourth_df = df4.filter(F.col("start_of_week").isin(*[first_item]))

    return_df = first_df.join(second_df, join_key)
    return_df = return_df.join(third_df, join_key)
    return_df = return_df.join(fourth_df, join_key)

    return return_df
