import logging
from typing import *
from pyspark.sql import SparkSession, DataFrame, functions as F


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
            where_clause = table_params["where_clause"]

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
    features.append("start_of_month")

    read_from = config.get("read_from")

    if read_from == 'l2':
        features.append("start_of_week")

    for agg_function, column_list in config["feature_list"].items():
        for each_feature_column in column_list:
            if read_from == 'l2':
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
                window=create_monthly_lookback_window(1, config["partition_by"]),
                column_name="{}_{}_{}_last_month".format(agg_function,
                                                         each_feature_column,
                                                         "weekly" if read_from == "l2" else "monthly")
            ))

            features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                function=agg_function,
                feature_column=each_feature_column,
                window=create_monthly_lookback_window(3, config["partition_by"]),
                column_name="{}_{}_{}_last_three_month".format(agg_function,
                                                               each_feature_column,
                                                               "weekly" if read_from == "l2" else "monthly")
            ))

    sql_stmt = sql_stmt.format(',\n'.join(features),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = SparkSession.builder.getOrCreate()
    df = spark.sql(sql_stmt)

    return df


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
        end_interval="current row"
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

    spark = SparkSession.builder.getOrCreate()

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
        column_function=QueryGenerator.expansion_feature_listing,
        level=config['type']
    )

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = SparkSession.builder.getOrCreate()

    df = spark.sql(sql_stmt)
    return df


def l4_rolling_ranked_window(
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
    features.append("start_of_month")

    read_from = config.get("read_from")

    if read_from == 'l2':
        features.append("start_of_week")

    for alias, col_name in config["feature_column"].items():
        features.append("{} as {}".format(col_name, alias))

    if read_from == 'l2':
        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column}) as weekly_rank_last_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_week".format(config["order_by_column_prefix"])
        ))

        features.append("""
            row_number() over (partition by {partition_column} 
            order by {order_column}) as weekly_rank_last_two_week
            """.format(
            partition_column=','.join(config["partition_by"]),
            order_column="{}_weekly_last_two_week".format(config["order_by_column_prefix"])
        ))

    features.append("""
        row_number() over (partition by {partition_column} 
        order by {order_column}) as {grouping}_rank_last_month
        """.format(
        partition_column=','.join(config["partition_by"]),
        order_column="{}_{}_last_month".format(config["order_by_column_prefix"],
                                               "weekly" if read_from == 'l2' else "monthly"),
        grouping="weekly" if read_from == 'l2' else "monthly"
    ))

    features.append("""
        row_number() over (partition by {partition_column} 
        order by {order_column}) as {grouping}_rank_last_three_month
        """.format(
        partition_column=','.join(config["partition_by"]),
        order_column="{}_{}_last_three_month".format(config["order_by_column_prefix"],
                                                     "weekly" if read_from == 'l2' else "monthly"),
        grouping="weekly" if read_from == 'l2' else "monthly"
    ))

    sql_stmt = sql_stmt.format(',\n'.join(set(features)),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = SparkSession.builder.getOrCreate()
    df = spark.sql(sql_stmt)

    return df


def l4_rolling_ranked_window_first_rank(
        input_df,
        config
) -> Dict[str, DataFrame]:

    ranked_df = l4_rolling_ranked_window(input_df, config)

    result_df = {}
    read_from = config["read_from"]

    if read_from == 'l2':
        result_df["last_week"] = ranked_df.where(F.col("weekly_rank_last_week") == 1)
        result_df["last_two_week"] = ranked_df.where(F.col("weekly_rank_last_two_week") == 1)

    grouping = "weekly" if read_from == 'l2' else "monthly"
    result_df["last_month"] = ranked_df.where(F.col("{}_rank_last_month".format(grouping)) == 1)
    result_df["last_three_month"] = ranked_df.where(F.col("{}_rank_last_three_month".format(grouping)) == 1)

    return result_df
