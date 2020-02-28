import logging
import os
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql import SparkSession
from kedro.context import load_context
from pathlib import Path

from customer360.utilities.config_parser import node_from_config, expansion

conf = os.getenv("CONF", "local")


def union_dataframes_with_missing_cols(df_input_or_list, *args):
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list] + list(args)

    col_list = set()
    for df in df_list:
        for column in df.columns:
            col_list.add(column)

    def add_missing_cols(dataframe, col_list):
        missing_cols = [column for column in col_list if column not in dataframe.columns]
        for column in missing_cols:
            dataframe = dataframe.withColumn(column, F.lit(None))
        return dataframe.select(*sorted(col_list))

    df_list_updated = [add_missing_cols(df, col_list) for df in df_list]
    return reduce(DataFrame.union, df_list_updated)


def execute_sql(data_frame, table_name, sql_str):
    """

    :param data_frame:
    :param table_name:
    :param sql_str:
    :return:
    """
    ss = SparkSession.builder.getOrCreate()
    data_frame.registerTempTable(table_name)
    return ss.sql(sql_str)


def add_start_of_week_and_month(input_df, date_column="day_id"):
    input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col(date_column))))
    input_df = input_df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col(date_column))))
    input_df = input_df.withColumn("event_partition_date", F.to_date(F.col(date_column)))

    return input_df


def __divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def _l1_join_with_customer_profile(
        input_df,
        cust_profile_df,
        config,
        current_item
) -> DataFrame:

    cust_profile_col_to_select = list(config["join_column_with_cust_profile"].keys()) + ["start_of_week"]
    cust_profile_col_to_select = list(set(cust_profile_col_to_select))  # remove duplicates

    if isinstance(current_item[0], int):
        current_item = list(map(lambda x: datetime.strptime(str(x), '%Y%m%d'), current_item))

    # push down the filter to customer profile to reduce the join rows
    filtered_cust_profile_df = (cust_profile_df
                                .filter(F.col("event_partition_date").isin(current_item))
                                .select(cust_profile_col_to_select))

    return _join_with_filtered_customer_profile(
        input_df=input_df,
        filtered_cust_profile_df=filtered_cust_profile_df,
        config=config
    )


def _l2_join_with_customer_profile(
        input_df,
        cust_profile_df,
        config,
        current_item
) -> DataFrame:

    # grouping all distinct customer per week
    filtered_cust_profile_df = (cust_profile_df
                                .filter(F.col("start_of_week").isin(current_item))
                                .select(*config["join_column_with_cust_profile"].keys())
                                .distinct())

    return _join_with_filtered_customer_profile(
        input_df=input_df,
        filtered_cust_profile_df=filtered_cust_profile_df,
        config=config
    )


def _join_with_filtered_customer_profile(
    input_df,
    filtered_cust_profile_df,
    config,
) -> DataFrame:

    joined_condition = None
    for left_col, right_col in config["join_column_with_cust_profile"].items():
        condition = F.col("left.{}".format(left_col)).eqNullSafe(F.col("right.{}".format(right_col)))
        if joined_condition is None:
            joined_condition = condition
            continue

        joined_condition &= condition

    result_df = (filtered_cust_profile_df.alias("left")
                 .join(other=input_df.alias("right"),
                       on=joined_condition,
                       how="left"))

    col_to_select = []

    # Select all columns for right table except those used for joins
    # and exist in filtered_cust_profile_df columns
    for col in input_df.columns:
        if col in filtered_cust_profile_df.columns or \
                col in config["join_column_with_cust_profile"].values():
            continue
        col_to_select.append(F.col("right.{}".format(col)).alias(col))

    # Select all customer profile column used for joining
    for col in filtered_cust_profile_df.columns:
        col_to_select.append(F.col("left.{}".format(col)).alias(col))

    result_df = result_df.select(col_to_select)

    return result_df


def _massive_processing(
        input_df,
        config,
        source_partition_col="partition_date",
        sql_generator_func=node_from_config,
        cust_profile_df=None,
        cust_profile_join_func=None
) -> DataFrame:

    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select(source_partition_col).distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    partition_num_per_job = config.get("partition_num_per_job", 1)
    mvv_new = list(__divide_chunks(mvv_array, partition_num_per_job))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col(source_partition_col).isin(*[curr_item]))

        output_df = sql_generator_func(small_df, config)

        if cust_profile_df is not None:
            output_df = cust_profile_join_func(input_df=output_df,
                                               cust_profile_df=cust_profile_df,
                                               config=config,
                                               current_item=curr_item)

        CNTX.catalog.save(config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col(source_partition_col).isin(*[first_item]))
    return_df = sql_generator_func(return_df, config)

    if cust_profile_df is not None:
        return_df = cust_profile_join_func(input_df=return_df,
                                           cust_profile_df=cust_profile_df,
                                           config=config,
                                           current_item=first_item)

    return return_df


def l1_massive_processing(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="partition_date",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l1_join_with_customer_profile)
    return return_df


def l2_massive_processing(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:

    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="start_of_week",
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l2_join_with_customer_profile)
    return return_df


def l2_massive_processing_with_expansion(
        input_df,
        config,
        cust_profile_df=None
) -> DataFrame:
    return_df = _massive_processing(input_df=input_df,
                                    config=config,
                                    source_partition_col="start_of_week",
                                    sql_generator_func=expansion,
                                    cust_profile_df=cust_profile_df,
                                    cust_profile_join_func=_l2_join_with_customer_profile)
    return return_df

