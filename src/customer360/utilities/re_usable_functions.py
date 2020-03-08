import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from kedro.context import load_context
from pathlib import Path
from src.customer360.utilities.spark_util import get_spark_session

from src.customer360.utilities.config_parser import node_from_config

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
    ss = get_spark_session()
    data_frame.registerTempTable(table_name)
    return ss.sql(sql_str)


def add_start_of_week_and_month(input_df, date_column="day_id"):
    input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.col(date_column))))
    input_df = input_df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.col(date_column))))

    return input_df


def __divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def join_with_customer_profile(
        input_df,
        config,
        cust_profile_df,
):
    joined_condition = None
    for left_col, right_col in config["join_column_with_cust_profile"].items():
        condition = F.col("left.{}".format(left_col)).eqNullSafe(F.col("right.{}".format(right_col)))
        if joined_condition is None:
            joined_condition = condition
            continue

        joined_condition &= condition

    result_df = (cust_profile_df.alias("left")
                 .join(other=input_df.alias("right"),
                       on=joined_condition,
                       how="left"))
    col_to_select = []
    for col in input_df.columns:
        if col == "event_partition_date":
            col_to_select.append(F.col("left.{}".format(col)).alias(col))
            continue
        col_to_select.append(F.col("right.{}".format(col)).alias(col))

    right_cols = []
    result_df = result_df.select(right_cols)

    return result_df


def l1_massive_processing(
        input_df,
        config,
        cust_profile_df=None
):
    CNTX = load_context(Path.cwd(), env=conf)
    data_frame = input_df
    dates_list = data_frame.select('partition_date').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    logging.info("Dates to run for {0}".format(str(mvv_array)))

    mvv_new = list(__divide_chunks(mvv_array, 3))
    add_list = mvv_new

    first_item = add_list[0]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        small_df = data_frame.filter(F.col("partition_date").isin(*[curr_item]))

        output_df = node_from_config(small_df, config)

        # if cust_profile_df is not None:
        #     filtered_cust_profile = cust_profile_df.filter(F.col("event_partition_date").isin(*[
        #         datetime.strptime(str(each_date), "%Y%m%d").strftime("%Y-%m-%d") for each_date in curr_item
        #     ]))
        #
        #     output_df = join_with_customer_profile(input_df=output_df,
        #                                            cust_profile_df=filtered_cust_profile,
        #                                            config=config)

        CNTX.catalog.save(config["output_catalog"], output_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    return_df = data_frame.filter(F.col("partition_date").isin(*[first_item]))
    return_df = node_from_config(return_df, config)

    # if cust_profile_df is not None:
    #     filtered_cust_profile = cust_profile_df.filter(F.col("event_partition_date").isin(*[
    #         datetime.strptime(str(each_date), "%Y%m%d").strftime("%Y-%m-%d") for each_date in first_item
    #     ]))
    #
    #     return_df = join_with_customer_profile(input_df=output_df,
    #                                            cust_profile_df=filtered_cust_profile,
    #                                            config=config)

    return return_df
