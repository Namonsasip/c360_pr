import logging
from typing import *
from pyspark.sql import DataFrame, functions as F
from src.customer360.utilities.spark_util import get_spark_session


# Query generator class
class QueryGenerator:

    # accept table_name as string, table_params as dict
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

            if granularity!="":
                query = "Select {},{} from {} {} group by {}".format(granularity, projection, table_name, where_clause, granularity)
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


def __get_l4_time_granularity_column(read_from):
    if read_from is None:
        raise ValueError("read_from is mandatory. Please specify either 'l1', 'l2', or 'l3'")

    if read_from.lower() == 'l1':
        return 'event_partition_date'

    elif read_from.lower() == 'l2':
        return "start_of_week"

    elif read_from.lower() == 'l3':
        return "start_of_month"

    raise ValueError("Unknown value for read_from. Please specify either 'l1', 'l2', or 'l3'")


def l4_rolling_window(input_df, config):
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


def create_daily_lookback_window(
        num_of_days,
        partition_column,
        order_by_column="event_partition_date"
):
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
    return """
            partition by {partition_column} 
            order by cast(cast({order_by_column} as timestamp) as long) asc
            range between {start_interval} and {end_interval}
            """.format(partition_column=','.join(partition_column),
                       order_by_column=order_by_column,
                       start_interval=start_interval,
                       end_interval=end_interval)


def node_from_config(input_df, config) -> DataFrame:
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
    This function will expand the base feature based on parameters
    :param input_df:
    :param config:
    :return:
    """
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


def __generate_l4_rolling_ranked_column(
        input_df,
        config
) -> DataFrame:

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
            order by {order_column} {order}) as daily_rank_last_seven_day
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_seven_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_fourteen_day
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_fourteen_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_thirty_day
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_thirty_day".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as daily_rank_last_ninety_day
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_daily_last_ninety_day".format(config["order_by_column_prefix"]),
            order=order
        ))

    elif read_from == 'l2':
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_two_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_two_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_four_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_four_week".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as weekly_rank_last_twelve_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_twelve_week".format(config["order_by_column_prefix"]),
            order=order
        ))
    else:
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as rank_last_month
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_monthly_last_month".format(config["order_by_column_prefix"]),
            order=order
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column} {order}) as rank_last_three_month
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
    join_condition = (F.col("{}.{}".format(left_alias, column_list[0]))
                      .eqNullSafe(F.col("{}.{}".format(right_alias, column_list[0]))))

    if len(column_list) == 1:
        return join_condition

    for each_col in column_list[1:]:
        join_condition &= (F.col("{}.{}".format(left_alias, each_col))
                           .eqNullSafe(F.col("{}.{}".format(right_alias, each_col))))

    return join_condition


def join_l4_rolling_ranked_table(result_df, config):
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

    ranked_df = __generate_l4_rolling_ranked_column(input_df, config)
    result_df = __generate_l4_filtered_ranked_table(ranked_df, config)
    return result_df
