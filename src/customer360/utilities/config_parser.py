import logging, os
from pathlib import Path
from typing import *
from pyspark.sql import DataFrame, functions as F

# from customer360.utilities.re_usable_functions import check_empty_dfs
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df
from pyspark.sql import DataFrame
from kedro.context import load_context
from pyspark.sql.functions import concat_ws,explode
from functools import reduce
from dateutil.relativedelta import relativedelta

import datetime
# Query generator class


class QueryGenerator:

    """
    Purpose: This class is used to generate the queries from configurations.
             It accepts table_name as string, table_params as dict
    """
    @staticmethod
    def aggregate(table_name, table_params, column_function, **kwargs):
        try:

            feature_list = table_params["feature_list"]

            features = column_function(feature_list, **kwargs)

            event_date_column = table_params.get('event_date_column')

            if event_date_column is not None:
                QueryGenerator.__add_start_of_week(features, event_date_column)
                QueryGenerator.__add_start_of_month(features, event_date_column)
                QueryGenerator.__add_event_partition_date(features, event_date_column)

            # if don't want to use where clause then put empty string "" in query_parameters.yaml
            where_clause = table_params.get("where_clause", "")

            # if features are not listed we can assume it to be *
            # or can raise a exception
            projection = ','.join(features) if len(features) != 0 else "*"

            # if don't want to use group by then put empty string "" in query_parameters.yaml

            granularity = table_params["granularity"]

            if granularity != "":
                query = "Select {},{} from {} {} group by {}".format(granularity, projection, table_name, where_clause,
                                                                     granularity)
            else:
                query = "Select {} from {} {}".format(projection, table_name, where_clause)

            logging.info("SQL QUERY {}".format(query))

            return query

        except Exception as e:
            print(str(e))
            print("Table parameters are missing.")

    @staticmethod
    def __add_event_partition_date(feature_list, event_date_column):
        feature_list.append("date({}) as event_partition_date".format(event_date_column))

    @staticmethod
    def __add_start_of_week(feature_list, event_date_column):
        feature_list.append("date(date_trunc('week', {})) as start_of_week".format(event_date_column))

    @staticmethod
    def __add_start_of_month(feature_list, event_date_column):
        feature_list.append("date(date_trunc('month', {})) as start_of_month".format(event_date_column))

    @staticmethod
    def normal_feature_listing(feature_list, **kwargs):
        features = []

        for (key, val) in feature_list.items():
            features.append("{} as {}".format(val, key))

        return features

    @staticmethod
    def expansion_feature_listing(feature_list, **kwargs):
        features = []

        for (key, val) in feature_list.items():
            for col in val:
                features.append("{}({}) as {}".format(key, col, col + "_" + key))

        return features


conf = os.getenv("CONF", None)

def check_empty_dfs(df_input_or_list):
    """
    Purpose: Its purpose is to check whether the input datasets are empty or not.
    You can input a single or a list of datasets. The return value would be an empty dataset if any one of the
    input dataset is empty other-wise the function will return false.
    :param df_input_or_list:
    :return:
    """
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list]

    ret_obj = False
    for df in df_list:
        if df is None:
            return True
        elif len(df.head(1)) == 0:
            return True
        else:
            pass
    return ret_obj

def __get_l4_time_granularity_column(read_from):
    """
    Purpose: To get the time granularity column for each layer.
    :param read_from:
    :return:
    """
    if read_from is None:
        raise ValueError("read_from is mandatory. Please specify either 'l1', 'l2', or 'l3'")

    if read_from.lower() == 'l1':
        return 'event_partition_date'

    elif read_from.lower() == 'l2':
        return "start_of_week"

    elif read_from.lower() == 'l3':
        return "start_of_month"

    raise ValueError("Unknown value for read_from. Please specify either 'l1', 'l2', or 'l3'")


def union_dataframes_with_missing_cols(df_input_or_list, *args):
    """
    Purpose: This is used to perform union of dataframes( both homogeneous/ heterogeneous)
    :param df_input_or_list:
    :param args:
    :return:
    """
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


def _get_full_data(src_data, fea_dict):
    """
    Purpose: This is used to get the missing partition entries in data.
    :param src_data:
    :param fea_dict:
    :return:
    """

    if len(src_data.head(1)) == 0:
        return src_data

    spark = get_spark_session()

    read_from = fea_dict.get("read_from")
    src_data.createOrReplaceTempView("src_data")
    if read_from is None:
        raise ValueError("read_from is mandatory. Please specify either 'l1' ,'l2', or 'l3'")

    if read_from.lower() == 'l1':
        data_range = "90"
        grouping = fea_dict.get("grouping", "event_partition_date")
        fix_dimension_cols = fea_dict["fix_dimension_cols"]
        fix_dimension_where_cond = fea_dict.get("fix_dimension_where_cond")

        target_dimension_col = fea_dict["target_dimension_col"]

        logging.info("data_range : {}".format(data_range))

    elif read_from.lower() == 'l2':
        data_range = "12"
        grouping = fea_dict.get("grouping", "start_of_week")
        fix_dimension_cols = fea_dict["fix_dimension_cols"]
        fix_dimension_where_cond = fea_dict.get("fix_dimension_where_cond")

        target_dimension_col = fea_dict["target_dimension_col"]
        logging.info("data_range : {}".format(data_range))

    elif read_from.lower() == 'l3':
        data_range = "3"
        grouping = fea_dict.get("grouping", "start_of_month")
        fix_dimension_cols = fea_dict["fix_dimension_cols"]
        fix_dimension_where_cond = fea_dict.get("fix_dimension_where_cond")

        target_dimension_col = fea_dict["target_dimension_col"]
        logging.info("data_range : {}".format(data_range))

    else:
        raise ValueError("read_from is mandatory. Please specify either 'l1', l2', or 'l3'")

    if fix_dimension_cols is None or target_dimension_col is None:
        raise ValueError("fix_dimension_cols and target_dimension_cols is mandatory for creating full_data")
    else:
        full_set_1 = spark.sql(
            "select {0},collect_set({1}) as set_1 from src_data {2} group by {0}".format(fix_dimension_cols,
                                                                                         target_dimension_col,
                                                                                         fix_dimension_where_cond))

        full_set_1 = full_set_1.withColumn("set_2", concat_ws(",", "set_1"))
        full_set_1.createOrReplaceTempView("full_set_1")

        full_set_2 = spark.sql(
            "select *, collect_set(set_2) over(order by {0} RANGE BETWEEN {1} PRECEDING AND 1 PRECEDING) as set_3 from full_set_1".format(
                grouping, data_range))
        full_set_2 = full_set_2.withColumn("set_4", concat_ws(",", "set_3"))
        full_set_2 = full_set_2.withColumn("set_5", F.split("set_2", ",")).withColumn("set_6", F.split("set_4", ","))
        full_set_2.createOrReplaceTempView("full_set_2")

        full_set_3 = spark.sql(
            "select *,array_distinct(set_5) as set_7, array_distinct(set_6) as set_8 from full_set_2").drop("set_1",
                                                                                                            "set_2",
                                                                                                            "set_3",
                                                                                                            "set_4",
                                                                                                            "set_5",
                                                                                                            "set_6")
        full_set_3.createOrReplaceTempView("full_set_3")

        full_set_4 = spark.sql("select *,array_except(set_8,set_7) as missing_set from full_set_3")
        full_set_4 = full_set_4.drop("set_7", "set_8")
        full_set_4 = full_set_4.withColumn(target_dimension_col, explode(full_set_4.missing_set)).drop("missing_set")

        full_data = union_dataframes_with_missing_cols(src_data, full_set_4)

        return full_data


def l4_rolling_window(input_df: DataFrame, config: dict):
    """
    Purpose: This is used to generate trend features using rolling window analytics function.
    :param input_df:
    :param config:
    :return:
    """
    if len(input_df.head(1)) == 0:
        logging.info("l4_rolling_window -> df == 0 records found in input dataset")
        return input_df
    logging.info("l4_rolling_window -> df > 0 records found in input dataset")
    ranked_lookup_enable_flag = config.get('ranked_lookup_enable_flag', "No")

    if ranked_lookup_enable_flag.lower() == 'yes':
        full_data = _get_full_data(input_df, config)
        input_df = full_data

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = """
        select 
            {}
        from input_table
        {}
    """

    features = []

    features.extend(config["partition_by"])

    read_from = config.get("read_from")
    features.append(__get_l4_time_granularity_column(read_from))
    features = list(set(features))  # Remove duplicates

    for agg_function, column_list in config["feature_list"].items():
        for each_feature_column in column_list:
            if read_from == 'l1':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(7, config["partition_by"]),
                    column_name="{}_{}_daily_last_seven_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(14, config["partition_by"]),
                    column_name="{}_{}_daily_last_fourteen_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(30, config["partition_by"]),
                    column_name="{}_{}_daily_last_thirty_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(90, config["partition_by"]),
                    column_name="{}_{}_daily_last_ninety_day".format(agg_function, each_feature_column)
                ))

            elif read_from == 'l2':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(1, config["partition_by"]),
                    column_name="{}_{}_weekly_last_week".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(2, config["partition_by"]),
                    column_name="{}_{}_weekly_last_two_week".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(4, config["partition_by"]),
                    column_name="{}_{}_weekly_last_four_week".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(12, config["partition_by"]),
                    column_name="{}_{}_weekly_last_twelve_week".format(agg_function, each_feature_column)
                ))
            else:
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(1, config["partition_by"]),
                    column_name="{}_{}_monthly_last_month".format(agg_function,
                                                                  each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(3, config["partition_by"]),
                    column_name="{}_{}_monthly_last_three_month".format(agg_function,
                                                                        each_feature_column)
                ))

    sql_stmt = sql_stmt.format(',\n'.join(features),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()
    df = spark.sql(sql_stmt)

    return df


def gen_sql_l4_by_metadata(config: dict, group_cols: Dict[str, Any], table_name: str, sub_id_table: str, suffix: str):
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


def l4_rolling_window_by_metadata(df_input: DataFrame, config: dict, target_table: str):
    """
        Purpose: This is used to generate trend features using rolling window analytics function.
        :param df_input:
        :param config:
        :param target_table:
        :return:
    """


    if check_empty_dfs([df_input]):
        logging.info("l4_rolling_window_by_period -> df == empty input dataset")
        return get_spark_empty_df()
    logging.info("l4_rolling_window_by_period -> not empty input dataset")

    CNTX = load_context(Path.cwd(), env=conf)


    def join_all_column(dfs, on, how="full"):
        """
        Merge all the dataframes
        """
        return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

    def rolling_window(max_date, read_from, df_input):
        if read_from == 'l1':
            df_maxdate = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 1))
            current_partition = df_maxdate.select(F.date_trunc("day", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)

            current_df = df_input.filter(F.col("event_partition_date") == m_date_str).select(
                "subscription_identifier").distinct()
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last 7 day
            last_seven_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 6)).alias("max_date")) \
                .collect()[0].max_date
            df_last_seven_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_seven_day, current_partition))
            sql_last_seven_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_seven_day", "sub_id_current",
                                                        "daily_last_seven_day")
            df_last_seven_day.createOrReplaceTempView("input_last_seven_day")
            output_last_seven_day = spark.sql(sql_last_seven_day)

            # look back last 14 day
            last_fourteen_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 13)).alias("max_date")) \
                .collect()[0].max_date
            df_last_fourteen_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_fourteen_day, current_partition))
            sql_last_fourteen_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_fourteen_day", "sub_id_current",
                                                           "daily_last_fourteen_day")
            df_last_fourteen_day.createOrReplaceTempView("input_last_fourteen_day")
            output_last_fourteen_day = spark.sql(sql_last_fourteen_day)

            # look back last 30 day
            last_thirty_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 29)).alias("max_date")) \
                .collect()[0].max_date
            df_last_thirty_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_thirty_day, current_partition))
            sql_last_thirty_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_thirty_day", "sub_id_current",
                                                         "daily_last_thirty_day")
            df_last_thirty_day.createOrReplaceTempView("input_last_thirty_day")
            output_last_thirty_day = spark.sql(sql_last_thirty_day)

            # look back last 90 day
            last_ninety_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 89)).alias("max_date")) \
                .collect()[0].max_date
            df_last_ninety_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_ninety_day, current_partition))
            sql_last_ninety_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_ninety_day", "sub_id_current",
                                                         "daily_last_ninety_day")
            df_last_ninety_day.createOrReplaceTempView("input_last_ninety_day")
            output_last_ninety_day = spark.sql(sql_last_ninety_day)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column(
                [output_last_seven_day, output_last_fourteen_day, output_last_thirty_day, output_last_ninety_day],
                on=group_cols, how="full", )
            df_return = df_return.withColumn("event_partition_date", F.lit(partition_run_str))

        elif read_from == 'l2':
            df_maxdate = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 7))
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)

            current_df = df_input.filter(F.col("start_of_week") == m_date_str).select("subscription_identifier").distinct()
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last week
            date_of_last_week = df_maxdate.select(F.date_trunc("week", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            df_last_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")) == date_of_last_week)
            sql_last_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_week", "sub_id_current", "weekly_last_week")
            df_last_week.createOrReplaceTempView("input_last_week")
            output_last_week = spark.sql(sql_last_week)

            # look back last 2 week
            date_of_last_two_week = df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 7)).alias("max_date")) \
                .collect()[0].max_date
            df_last_two_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_two_week, date_of_last_week))
            sql_last_two_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_two_week", "sub_id_current",
                                                       "weekly_last_two_week")
            df_last_two_week.createOrReplaceTempView("input_last_two_week")
            output_last_two_week = spark.sql(sql_last_two_week)

            # look back last 4 week
            date_of_last_four_week = \
            df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 21)).alias("max_date")) \
                .collect()[0].max_date
            df_last_four_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_four_week, date_of_last_week))
            sql_last_four_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_four_week", "sub_id_current",
                                                        "weekly_last_four_week")
            df_last_four_week.createOrReplaceTempView("input_last_four_week")
            output_last_four_week = spark.sql(sql_last_four_week)

            # look back last 12 week
            date_of_last_twelve_week = \
            df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 77)).alias("max_date")) \
                .collect()[0].max_date
            df_last_twelve_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_twelve_week, date_of_last_week))
            sql_last_twelve_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_twelve_week", "sub_id_current",
                                                          "weekly_last_twelve_week")
            df_last_twelve_week.createOrReplaceTempView("input_last_twelve_week")
            output_last_twelve_week = spark.sql(sql_last_twelve_week)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column(
                [output_last_week, output_last_two_week, output_last_four_week, output_last_twelve_week],
                on=group_cols, how="full", )
            df_return = df_return.withColumn("start_of_week", F.lit(partition_run_str))

        else:
            df_maxdate = max_date.withColumn("max_date", F.add_months(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.add_months(F.col("max_date"), 1))
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)

            current_df = df_input.filter(F.col("start_of_month") == m_date_str).select("subscription_identifier").distinct()
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last month
            date_of_last_month = df_maxdate.select(F.date_trunc("month", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            df_last_month = df_input.filter(F.date_trunc("month", F.col("start_of_month")) == date_of_last_month)
            sql_last_month = gen_sql_l4_by_metadata(config, group_cols, "input_last_month", "sub_id_current",
                                                    "monthly_last_month")
            df_last_month.createOrReplaceTempView("input_last_month")
            output_last_month = spark.sql(sql_last_month)

            # look back last 3 month
            date_of_last_three_month = \
            df_maxdate.select(F.date_trunc("month", F.add_months(F.col("max_date"), -2)).alias("max_date")) \
                .collect()[0].max_date
            df_last_three_month = df_input.filter(
                F.date_trunc("month", F.col("start_of_month")).between(date_of_last_three_month, date_of_last_month))
            sql_last_three_month = gen_sql_l4_by_metadata(config, group_cols, "input_last_three_month", "sub_id_current",
                                                          "monthly_last_three_month")
            df_last_three_month.createOrReplaceTempView("input_last_three_month")
            output_last_three_month = spark.sql(sql_last_three_month)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column([output_last_month, output_last_three_month], on=group_cols, how="full", )
            df_return = df_return.withColumn("start_of_month", F.lit(partition_run_str))
        return df_return

    p_increment = str(os.getenv("RUN_INCREMENT", "Yes"))
    p_partition = str(os.getenv("RUN_PARTITION", "no_input"))

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    metadata_last_date = metadata.filter(F.col("table_name") == target_table) \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd')))

    spark = get_spark_session()
    group_cols = config["partition_by"]
    read_from = config.get("read_from")
    logging.info("read_from --> " + read_from)

    if p_increment.lower() != 'no':
        p_curent_date = str(metadata_last_date.collect()[0][0])
        max_date_data = str((df_input.select(df_input.columns[-1]).rdd.max())[df_input.columns[-1]])

        min_tgt_filter_date = datetime.datetime.strptime(p_curent_date, '%Y-%m-%d')
        max_tgt_filter_date = datetime.datetime.strptime(max_date_data, '%Y-%m-%d')

        curent_week = min_tgt_filter_date - datetime.timedelta(days=min_tgt_filter_date.weekday() % 7)
        p_curent_week = (curent_week - relativedelta(weeks=1)).strftime("%Y-%m-%d")

        p_curent_months = (min_tgt_filter_date - relativedelta(months=1)).strftime("%Y-%m-01")

        date_generated = [min_tgt_filter_date + datetime.timedelta(days=x) for x in
                              range(0, (max_tgt_filter_date - min_tgt_filter_date).days)]

        list_date_data = []
        for date in date_generated:
            if read_from == 'l1':
                list_date_data.append(str(date.strftime("%Y-%m-%d")))
            elif read_from == 'l2':
                p_run_date = date - relativedelta(weeks=1)
                if str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime(
                        "%Y-%m-%d")) not in list_date_data and \
                        str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime(
                            "%Y-%m-%d")) != p_curent_week:
                    list_date_data.append(
                        str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime("%Y-%m-%d")))
            else:
                p_run_date = (date - relativedelta(months=1)).strftime("%Y-%m-01")
                if p_run_date not in list_date_data and p_run_date != p_curent_months:
                    list_date_data.append(str(p_run_date))
    else:
        list_date_data = []

    if list_date_data != []:
        p_loop = 0
        for p_run_date in list_date_data:
            logging.info("Data Run Date : "+p_run_date)
            p_max_date = spark.sql(""" select  to_date('"""+p_run_date+"""','yyyy-MM-dd') as max_date""")
            df_return = rolling_window(p_max_date, read_from, df_input)
            if p_loop == 0:
                df1 = df_return
                p_loop = 1
            else:
                if p_loop == 1:
                    p_loop = 2
                dfUnion = df1.unionAll(df_return)
                df1 = dfUnion
        if p_loop == 1:
            df_result = df1
        else:
            df_result = dfUnion
    elif p_increment.lower() == 'no':
        if read_from == 'l1':
            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
            p_run_date = str((p_date - relativedelta(days=1)).strftime('%Y-%m-%d'))
        elif read_from == 'l2':
            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
            p_current_date = p_date - datetime.timedelta(days=p_date.weekday() % 7)
            p_run_date = str((p_current_date - relativedelta(weeks=1)).strftime('%Y-%m-%d'))
        else:
            p_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
            p_run_date = str((p_date - relativedelta(months=1)).strftime('%Y-%m-01'))

        p_max_date = spark.sql(""" select  to_date('"""+p_run_date+"""','yyyy-MM-dd') as max_date""")
        df_result = rolling_window(p_max_date, read_from, df_input)
    else:
        logging.info("=======> Run Date Empty Data <=======")
        df_result = get_spark_empty_df()

    return df_result



def l4_rolling_window_by_metadata_with_customer_profile(df_input: DataFrame, cust_df: DataFrame , config: dict, target_table: str):
    """
        Purpose: This is used to generate trend features using rolling window analytics function.
        :param df_input: dataFrame input
        :param config: structure for generate statment
        :param target_table: target table name for query metadata
        :param cust_df: customer profile for lookup
        :return:
    """


    if check_empty_dfs([df_input]):
        logging.info("l4_rolling_window_by_period -> df == empty input dataset")
        return get_spark_empty_df()
    logging.info("l4_rolling_window_by_period -> not empty input dataset")

    CNTX = load_context(Path.cwd(), env=conf)

    def join_all_column(dfs, on, how="full"):
        """
        Merge all the dataframes
        """
        return reduce(lambda x, y: x.join(y, on=on, how=how), dfs)

    def rolling_window(max_date, read_from, df_input):
        if read_from == 'l1':
            df_maxdate = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 1))
            current_partition = df_maxdate.select(F.date_trunc("day", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)
            logging.info("event_partition_date: " + partition_run_str)

            current_df = cust_df.filter(F.col("event_partition_date") == partition_run_str).select(
                "subscription_identifier").distinct()
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last 7 day
            last_seven_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 6)).alias("max_date")) \
                .collect()[0].max_date
            df_last_seven_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_seven_day, current_partition))
            sql_last_seven_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_seven_day", "sub_id_current",
                                                        "daily_last_seven_day")
            df_last_seven_day.createOrReplaceTempView("input_last_seven_day")
            output_last_seven_day = spark.sql(sql_last_seven_day)

            # look back last 14 day
            last_fourteen_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 13)).alias("max_date")) \
                .collect()[0].max_date
            df_last_fourteen_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_fourteen_day, current_partition))
            sql_last_fourteen_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_fourteen_day", "sub_id_current",
                                                           "daily_last_fourteen_day")
            df_last_fourteen_day.createOrReplaceTempView("input_last_fourteen_day")
            output_last_fourteen_day = spark.sql(sql_last_fourteen_day)

            # look back last 30 day
            last_thirty_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 29)).alias("max_date")) \
                .collect()[0].max_date
            df_last_thirty_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_thirty_day, current_partition))
            sql_last_thirty_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_thirty_day", "sub_id_current",
                                                         "daily_last_thirty_day")
            df_last_thirty_day.createOrReplaceTempView("input_last_thirty_day")
            output_last_thirty_day = spark.sql(sql_last_thirty_day)

            # look back last 90 day
            last_ninety_day = df_maxdate.select(F.date_trunc("day", F.date_sub(F.col("max_date"), 89)).alias("max_date")) \
                .collect()[0].max_date
            df_last_ninety_day = df_input.filter(
                F.date_trunc("day", F.col("event_partition_date")).between(last_ninety_day, current_partition))
            sql_last_ninety_day = gen_sql_l4_by_metadata(config, group_cols, "input_last_ninety_day", "sub_id_current",
                                                         "daily_last_ninety_day")
            df_last_ninety_day.createOrReplaceTempView("input_last_ninety_day")
            output_last_ninety_day = spark.sql(sql_last_ninety_day)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column(
                [output_last_seven_day, output_last_fourteen_day, output_last_thirty_day, output_last_ninety_day],
                on=group_cols, how="full", )
            df_return = df_return.withColumn("event_partition_date", F.lit(partition_run_str))

        elif read_from == 'l2':
            df_maxdate = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.date_add(F.col("max_date"), 7))
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)
            logging.info("start_of_week: " + partition_run_str)

            current_df = cust_df.filter(F.col("start_of_week") == partition_run_str).select("subscription_identifier").distinct()
            cust_df.groupBy("start_of_week").count().show() #################################################################
            cust_df.filter(F.col("start_of_week") == partition_run_str).groupBy("start_of_week").count().show() #################################################################
            df_input.groupBy("start_of_week").count().show()  #################################################################
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last week
            date_of_last_week = df_maxdate.select(F.date_trunc("week", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            df_last_week = df_input.filter(F.date_trunc("week", F.col("start_of_week")) == date_of_last_week)
            sql_last_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_week", "sub_id_current", "weekly_last_week")
            df_last_week.createOrReplaceTempView("input_last_week")
            output_last_week = spark.sql(sql_last_week)

            # look back last 2 week
            date_of_last_two_week = df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 7)).alias("max_date")) \
                .collect()[0].max_date
            df_last_two_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_two_week, date_of_last_week))
            sql_last_two_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_two_week", "sub_id_current",
                                                       "weekly_last_two_week")
            df_last_two_week.createOrReplaceTempView("input_last_two_week")
            output_last_two_week = spark.sql(sql_last_two_week)

            # look back last 4 week
            date_of_last_four_week = \
            df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 21)).alias("max_date")) \
                .collect()[0].max_date
            df_last_four_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_four_week, date_of_last_week))
            sql_last_four_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_four_week", "sub_id_current",
                                                        "weekly_last_four_week")
            df_last_four_week.createOrReplaceTempView("input_last_four_week")
            output_last_four_week = spark.sql(sql_last_four_week)

            # look back last 12 week
            date_of_last_twelve_week = \
            df_maxdate.select(F.date_trunc("week", F.date_sub(F.col("max_date"), 77)).alias("max_date")) \
                .collect()[0].max_date
            df_last_twelve_week = df_input.filter(
                F.date_trunc("week", F.col("start_of_week")).between(date_of_last_twelve_week, date_of_last_week))
            sql_last_twelve_week = gen_sql_l4_by_metadata(config, group_cols, "input_last_twelve_week", "sub_id_current",
                                                          "weekly_last_twelve_week")
            df_last_twelve_week.createOrReplaceTempView("input_last_twelve_week")
            output_last_twelve_week = spark.sql(sql_last_twelve_week)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column(
                [output_last_week, output_last_two_week, output_last_four_week, output_last_twelve_week],
                on=group_cols, how="full", )
            df_return = df_return.withColumn("start_of_week", F.lit(partition_run_str))

        else:
            df_maxdate = max_date.withColumn("max_date", F.add_months(F.col("max_date"), 0))
            df_partition_run = max_date.withColumn("max_date", F.add_months(F.col("max_date"), 1))
            m_date_str = str(df_maxdate.collect()[0].max_date)
            partition_run_str = str(df_partition_run.collect()[0].max_date)
            logging.info("max date to load data: " + m_date_str)
            logging.info("start_of_month: " + partition_run_str)

            current_df = cust_df.filter(F.col("start_of_month") == partition_run_str).select("subscription_identifier").distinct()
            current_df.createOrReplaceTempView("sub_id_current")
            logging.info("-------- Create sub_id_current from customer profile --------")

            # look back last month
            date_of_last_month = df_maxdate.select(F.date_trunc("month", F.col("max_date")).alias("max_date")) \
                .collect()[0].max_date
            df_last_month = df_input.filter(F.date_trunc("month", F.col("start_of_month")) == date_of_last_month)
            sql_last_month = gen_sql_l4_by_metadata(config, group_cols, "input_last_month", "sub_id_current",
                                                    "monthly_last_month")
            df_last_month.createOrReplaceTempView("input_last_month")
            output_last_month = spark.sql(sql_last_month)

            # look back last 3 month
            date_of_last_three_month = \
            df_maxdate.select(F.date_trunc("month", F.add_months(F.col("max_date"), -2)).alias("max_date")) \
                .collect()[0].max_date
            df_last_three_month = df_input.filter(
                F.date_trunc("month", F.col("start_of_month")).between(date_of_last_three_month, date_of_last_month))
            sql_last_three_month = gen_sql_l4_by_metadata(config, group_cols, "input_last_three_month", "sub_id_current",
                                                          "monthly_last_three_month")
            df_last_three_month.createOrReplaceTempView("input_last_three_month")
            output_last_three_month = spark.sql(sql_last_three_month)

            # join
            logging.info("windows ------- > run join column")
            df_return = join_all_column([output_last_month, output_last_three_month], on=group_cols, how="full", )
            df_return = df_return.withColumn("start_of_month", F.lit(partition_run_str))
        return df_return

    p_increment = str(os.getenv("RUN_INCREMENT", "Yes"))
    p_partition = str(os.getenv("RUN_PARTITION", "no_input"))

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    metadata_last_date = metadata.filter(F.col("table_name") == target_table) \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd')))

    spark = get_spark_session()
    group_cols = config["partition_by"]
    read_from = config.get("read_from")
    logging.info("read_from --> " + read_from)

    if p_increment.lower() != 'no':
        p_curent_date = str(metadata_last_date.collect()[0][0])
        max_date_data = str((df_input.select(df_input.columns[-1]).rdd.max())[df_input.columns[-1]])

        min_tgt_filter_date = datetime.datetime.strptime(p_curent_date, '%Y-%m-%d')
        max_tgt_filter_date = datetime.datetime.strptime(max_date_data, '%Y-%m-%d')

        curent_week = min_tgt_filter_date - datetime.timedelta(days=min_tgt_filter_date.weekday() % 7)
        p_curent_week = (curent_week - relativedelta(weeks=1)).strftime("%Y-%m-%d")

        p_curent_months = (min_tgt_filter_date - relativedelta(months=1)).strftime("%Y-%m-01")

        date_generated = [min_tgt_filter_date + datetime.timedelta(days=x) for x in
                              range(0, (max_tgt_filter_date - min_tgt_filter_date).days + 1 )]
        logging.info("date_gen")
        print(date_generated)
        list_date_data = []
        for date in date_generated:
            if read_from == 'l1':
                list_date_data.append(str(date.strftime("%Y-%m-%d")))
            elif read_from == 'l2':
                p_run_date = date - relativedelta(weeks=1)
                if str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime(
                        "%Y-%m-%d")) not in list_date_data and \
                        str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime(
                            "%Y-%m-%d")) != p_curent_week:
                    list_date_data.append(
                        str((p_run_date - datetime.timedelta(days=p_run_date.weekday() % 7)).strftime("%Y-%m-%d")))
            else:
                p_run_date = (date - relativedelta(months=1)).strftime("%Y-%m-01")
                if p_run_date not in list_date_data and p_run_date != p_curent_months:
                    list_date_data.append(str(p_run_date))
    else:
        list_date_data = []
    logging.info("list_date_data")
    print( list_date_data)
    if list_date_data != []:
        p_loop = 0
        for p_run_date in list_date_data:
            logging.info("Data Run Date : "+p_run_date)
            p_max_date = spark.sql(""" select  to_date('"""+p_run_date+"""','yyyy-MM-dd') as max_date""")
            df_return = rolling_window(p_max_date, read_from, df_input)
            df_return.groupBy("start_of_week").count().show()
            if p_loop == 0:
                # df1 = df_return
                df_result = df_return
            else:
                # if p_loop == 1:
                #     p_loop = 2
                # dfUnion = df1.unionAll(df_return)
                # df1 = dfUnion
                df_result = df_result.unionAll(df_return)
            p_loop += 1
        # if p_loop == 1:
        #     df_result = df1
        # else:
        #     df_result = dfUnion
    elif p_increment.lower() == 'no':
        if read_from == 'l1':
            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
            p_run_date = str((p_date - relativedelta(days=1)).strftime('%Y-%m-%d'))
        elif read_from == 'l2':
            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
            p_current_date = p_date - datetime.timedelta(days=p_date.weekday() % 7)
            p_run_date = str((p_current_date - relativedelta(weeks=1)).strftime('%Y-%m-%d'))
        else:
            p_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
            p_run_date = str((p_date - relativedelta(months=1)).strftime('%Y-%m-01'))

        p_max_date = spark.sql(""" select  to_date('"""+p_run_date+"""','yyyy-MM-dd') as max_date""")
        df_result = rolling_window(p_max_date, read_from, df_input)
    else:
        logging.info("=======> Run Date Empty Data <=======")
        df_result = get_spark_empty_df()

    return df_result

def create_daily_lookback_window(
        num_of_days,
        partition_column,
        order_by_column="event_partition_date"
):
    """
    Purpose: This is used to generate the daily lookback window queries.
    :param num_of_days:
    :param partition_column:
    :param order_by_column:
    :return:
    """
    max_seconds_in_day = 24 * 60 * 60

    window_statement = create_window_statement(
        partition_column=partition_column,
        order_by_column=order_by_column,
        start_interval="{} preceding".format(num_of_days * max_seconds_in_day),
        end_interval="1 preceding"
    )

    return window_statement


def create_monthly_lookback_window(
        num_of_month,
        partition_column,
        order_by_column="start_of_month"
):
    """
    Purpose: This is used to generate the monthly lookback window queries.
    :param num_of_month:
    :param partition_column:
    :param order_by_column:
    :return:
    """
    max_seconds_in_month = 31 * 24 * 60 * 60

    window_statement = create_window_statement(
        partition_column=partition_column,
        order_by_column=order_by_column,
        start_interval="{} preceding".format(num_of_month * max_seconds_in_month),
        end_interval="1 preceding"
    )

    return window_statement


def create_weekly_lookback_window(
        num_of_week,
        partition_column,
        order_by_column="start_of_week"
):
    """
    Purpose: This is used to generate the weekly lookback window queries.
    :param num_of_week:
    :param partition_column:
    :param order_by_column:
    :return:
    """
    seconds_in_week = 7 * 24 * 60 * 60

    window_statement = create_window_statement(
        partition_column=partition_column,
        order_by_column=order_by_column,
        start_interval="{} preceding".format(num_of_week * seconds_in_week),
        end_interval="1 preceding"
    )

    return window_statement


def create_window_statement(
        partition_column,
        order_by_column,
        start_interval,
        end_interval
):
    """
    Purpose: This is used to generate the sql window statements.
    :param partition_column:
    :param order_by_column:
    :param start_interval:
    :param end_interval:
    :return:
    """
    return """
            partition by {partition_column} 
            order by cast(cast({order_by_column} as timestamp) as long) asc
            range between {start_interval} and {end_interval}
            """.format(partition_column=','.join(partition_column),
                       order_by_column=order_by_column,
                       start_interval=start_interval,
                       end_interval=end_interval)


def node_from_config(input_df: DataFrame, config: dict) -> DataFrame:
    """
    Purpose: This is used to automatically generate features using configurations
    :param input_df:
    :param config:
    :return:
    """
    if len(input_df.head(1)) == 0:
        return input_df

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = QueryGenerator.aggregate(
        table_name=table_name,
        table_params=config,
        column_function=QueryGenerator.normal_feature_listing)

    spark = get_spark_session()

    df = spark.sql(sql_stmt)
    return df


def expansion(input_df, config) -> DataFrame:
    """
    Purpose: This is an extended version of node_from_config function
    This function will expand the base feature based on parameters
    :param input_df:
    :param config:
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = QueryGenerator.aggregate(
        table_name=table_name,
        table_params=config,
        column_function=QueryGenerator.expansion_feature_listing
    )

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()

    df = spark.sql(sql_stmt)
    return df


def customUnion(df1, df2):
    """
    Purpose: To perform union of 2 dataframes.
    :param df1:
    :param df2:
    :return:
    """
    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))
    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return F.lit(None).alias(colname)
        cols = map(processCols, allcols)
        return list(cols)
    appended = df1.select(expr(cols1, total_cols)).union(df2.select(expr(cols2, total_cols)))
    return appended


def __generate_l4_rolling_ranked_column(
        input_df,
        config
) -> DataFrame:
    """
    Purpose: This is used to generate the ranked rolling window features.
    :param input_df:
    :param config:
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)

    sql_stmt = """
        select 
            {}
        from input_table
        {}
    """

    features = []

    features.extend(config["partition_by"])

    read_from = config.get("read_from", "")
    features.append(__get_l4_time_granularity_column(read_from))

    for alias, col_name in config["feature_column"].items():
        features.append("{} as {}".format(col_name, alias))

    order = config.get("order", "desc")
    if read_from == 'l1':
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_seven_day,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_seven_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_fourteen_day,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_fourteen_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_thirty_day,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_thirty_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_ninety_day,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_ninety_day".format(config["order_by_column_prefix"]),
            order=order
        ))

    elif read_from == 'l2':
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_week,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_two_week,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_two_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_four_week,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_four_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_twelve_week,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_twelve_week".format(config["order_by_column_prefix"]),
            order=order
        ))
    else:
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as rank_last_month,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_monthly_last_month".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as rank_last_three_month,
            {order_column}
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_monthly_last_three_month".format(config["order_by_column_prefix"]),
            order=order
        ))

    sql_stmt = sql_stmt.format(',\n'.join(set(features)),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()

    df = spark.sql(sql_stmt)

    return df


def __construct_null_safe_join_condition(
        column_list,
        left_alias='left',
        right_alias='right'
):
    """
    Purpose: To perform the null safe joins
    :param column_list:
    :param left_alias:
    :param right_alias:
    :return:
    """
    join_condition = (F.col("{}.{}".format(left_alias, column_list[0]))
                      .eqNullSafe(F.col("{}.{}".format(right_alias, column_list[0]))))

    if len(column_list) == 1:
        return join_condition

    for each_col in column_list[1:]:
        join_condition &= (F.col("{}.{}".format(left_alias, each_col))
                           .eqNullSafe(F.col("{}.{}".format(right_alias, each_col))))

    return join_condition


def join_l4_rolling_ranked_table(result_df, config):
    """
    Purpose: To perform the joins for rolling ranked table.
    :param result_df:
    :param config:
    :return:
    """

    for _, df in result_df.items():
        if len(df.head(1)) == 0:
            return df

    feature_column = [F.col("left.{}".format(each_col)) for each_col in config["partition_by"]]

    final_df = None
    for window_range, df in result_df.items():

        for each_feature_column in config["feature_column"]:
            feature_column.append(F.col(each_feature_column)
                                  .alias("{}_{}".format(each_feature_column, window_range)))

        if final_df is None:
            final_df = df.alias("left").select(feature_column)
            feature_column = list(map(lambda x: 'left.{}'.format(x), final_df.columns))
            continue

        # Always join on partition_by because it defines the granularity
        join_condition = __construct_null_safe_join_condition(config["partition_by"])

        final_df = (final_df.alias("left").join(other=df.alias("right"),
                                                on=join_condition,
                                                how='inner')
                    .select(feature_column))

        feature_column = list(map(lambda x: 'left.{}'.format(x), final_df.columns))

    return final_df


def __generate_l4_filtered_ranked_table(
        ranked_df,
        config
):
    """
    Purpose: To generate the filtered ranked tables
    :param ranked_df:
    :param config:
    :return:
    """
    result_df = {}
    read_from = config["read_from"]

    to_join = config.get("to_join", True)
    rank = config.get("rank", 1)

    if read_from == 'l1':
        result_df["last_seven_day"] = ranked_df.where(F.col("daily_rank_last_seven_day") == rank)
        result_df["last_fourteen_day"] = ranked_df.where(F.col("daily_rank_last_fourteen_day") == rank)
        result_df["last_thirty_day"] = ranked_df.where(F.col("daily_rank_last_thirty_day") == rank)
        result_df["last_ninety_day"] = ranked_df.where(F.col("daily_rank_last_ninety_day") == rank)

    elif read_from == 'l2':
        result_df["last_week"] = ranked_df.where(F.col("weekly_rank_last_week") == rank)
        result_df["last_two_week"] = ranked_df.where(F.col("weekly_rank_last_two_week") == rank)
        result_df["last_four_week"] = ranked_df.where(F.col("weekly_rank_last_four_week") == rank)
        result_df["last_twelve_week"] = ranked_df.where(F.col("weekly_rank_last_twelve_week") == rank)
    else:
        result_df["last_month"] = ranked_df.where(F.col("monthly_rank_last_month") == rank)
        result_df["last_three_month"] = ranked_df.where(F.col("monthly_rank_last_three_month") == rank)

    if to_join:
        return join_l4_rolling_ranked_table(result_df, config)

    return result_df


def l4_rolling_ranked_window(
        input_df,
        config
) -> Dict[str, DataFrame]:
    """
    Purpose: To generate the rolling ranked window features.
    :param input_df:
    :param config:
    :return:
    """

    if len(input_df.head(1)) == 0:
        return input_df

    ranked_df = __generate_l4_rolling_ranked_column(input_df, config)
    result_df = __generate_l4_filtered_ranked_table(ranked_df, config)
    return result_df
