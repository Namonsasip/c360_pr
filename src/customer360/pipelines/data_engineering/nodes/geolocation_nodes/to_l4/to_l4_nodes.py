from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config, l4_rolling_window, create_weekly_lookback_window, \
    create_daily_lookback_window, __get_l4_time_granularity_column, create_monthly_lookback_window, _get_full_data
from customer360.utilities.re_usable_functions import l3_massive_processing, l1_massive_processing, __divide_chunks, \
    check_empty_dfs, data_non_availability_and_missing_check
from kedro.context.context import load_context
from pathlib import Path
import logging
from customer360.utilities.spark_util import get_spark_session, get_spark_empty_df


def create_window_statement_geo(
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


def create_lookback_window_geo(
        partition_type,
        num_of_days,
        partition_column
):
    if partition_type == 'daily':
        max_seconds = num_of_days * 24 * 60 * 60
        order_by_column = "event_partition_date"
    elif partition_type == 'weekly':
        max_seconds = num_of_days * 7 * 24 * 60 * 60
        order_by_column = "start_of_week"
    else:
        max_seconds = num_of_days * 31 * 24 * 60 * 60
        order_by_column = "start_of_month"

    window_statement = create_window_statement_geo(
        partition_column=partition_column,
        order_by_column=order_by_column,
        start_interval="{} preceding".format(max_seconds),
        end_interval="current row"
    )

    return window_statement


def l4_rolling_window_geo(input_df: DataFrame, config: dict):

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
                    window=create_lookback_window_geo('daily', 6, config["partition_by"]),
                    column_name="{}_{}_past_seven_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('daily', 13, config["partition_by"]),
                    column_name="{}_{}_past_fourteen_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('daily', 29, config["partition_by"]),
                    column_name="{}_{}_past_thirty_day".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('daily', 89, config["partition_by"]),
                    column_name="{}_{}_past_ninety_day".format(agg_function, each_feature_column)
                ))

            elif read_from == 'l2':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('weekly', 0, config["partition_by"]),
                    column_name="{}_{}".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('weekly', 1, config["partition_by"]),
                    column_name="{}_{}_past_two_week".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('weekly', 3, config["partition_by"]),
                    column_name="{}_{}_past_four_week".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('weekly', 11, config["partition_by"]),
                    column_name="{}_{}_past_twelve_week".format(agg_function, each_feature_column)
                ))
            else:
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('monthly', 0, config["partition_by"]),
                    column_name="{}_{}".format(agg_function, each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_lookback_window_geo('monthly', 2, config["partition_by"]),
                    column_name="{}_{}_past_three_month".format(agg_function, each_feature_column)
                ))

    sql_stmt = sql_stmt.format(',\n'.join(features),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()
    df = spark.sql(sql_stmt)

    return df


def l4_geo_top3_voice_location(input_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    result_df = l4_rolling_window_geo(input_df, params_config)
    column_result_df = result_df.columns
    for i in ['subscription_identifier', 'mobile_no', 'imsi', 'start_of_week']:
        column_result_df.remove(i)

    result_df.printSchema()
    print(column_result_df)

    output_df = input_df.join(result_df, ['subscription_identifier', 'mobile_no', 'imsi', 'start_of_week'],
                              'inner').select(
        input_df.subscription_identifier,
        input_df.mobile_no,
        input_df.imsi,
        input_df.start_of_week,
        input_df.total_call,
        input_df.total_call_minute,
        input_df.top_voice_location_id_1st,
        input_df.top_voice_latitude_1st,
        input_df.top_voice_longitude_1st,
        input_df.top_voice_location_id_2nd,
        input_df.top_voice_latitude_2nd,
        input_df.top_voice_longitude_2nd,
        input_df.top_voice_location_id_3rd,
        input_df.top_voice_latitude_3rd,
        input_df.top_voice_longitude_3rd,
        *column_result_df
    )
    return output_df


def l4_join_customer_profile_geo(input_df: DataFrame, cust_df: DataFrame, param_config: str) -> DataFrame:
    read_from = param_config.get("read_from")
    col_partition = 'start_of_week' if read_from == 'l2' else 'start_of_month'
    input_df_col = input_df.columns
    input_df_col.remove('imsi')
    input_df_col.remove(col_partition)
    output_df = input_df.join(cust_df, ['imsi', col_partition], 'inner').select(
        cust_df.subscription_identifier, cust_df.mobile_no, input_df.imsi,
        input_df[col_partition], *input_df_col
    )
    return output_df


def l4_geo_time_spent_by_location(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = l4_rolling_window_geo(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_time_spent_by_store(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = l4_rolling_window_geo(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_count_visit_by_location(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = l4_rolling_window_geo(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_total_distance_km(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = l4_rolling_window_geo(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_home_work_location_id(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = node_from_config(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_top3_visit_exclude_hw(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = node_from_config(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_work_area_center_average(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = node_from_config(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_home_weekday_city_citizens(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    result_df = l4_rolling_window_geo(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_geo_visit_ais_store_location(input_df: DataFrame, cust_df: DataFrame, params_config: str) -> DataFrame:
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    result_df = node_from_config(input_df, params_config)
    output_df = l4_join_customer_profile_geo(result_df, cust_df, params_config)
    return output_df


def l4_rolling_window_de(input_df: DataFrame, config: dict):
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
                    column_name="{}_daily_last_seven_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(14, config["partition_by"]),
                    column_name="{}_daily_last_fourteen_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(30, config["partition_by"]),
                    column_name="{}_daily_last_thirty_day".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_daily_lookback_window(90, config["partition_by"]),
                    column_name="{}_daily_last_ninety_day".format(each_feature_column)
                ))

            elif read_from == 'l2':
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(1, config["partition_by"]),
                    column_name="{}_weekly_last_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(2, config["partition_by"]),
                    column_name="{}_weekly_last_two_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(4, config["partition_by"]),
                    column_name="{}_weekly_last_four_week".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_weekly_lookback_window(12, config["partition_by"]),
                    column_name="{}_weekly_last_twelve_week".format(each_feature_column)
                ))
            else:
                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(1, config["partition_by"]),
                    column_name="{}_monthly_last_month".format(each_feature_column)
                ))

                features.append("{function}({feature_column}) over ({window}) as {column_name}".format(
                    function=agg_function,
                    feature_column=each_feature_column,
                    window=create_monthly_lookback_window(3, config["partition_by"]),
                    column_name="{}_monthly_last_three_month".format(each_feature_column)
                ))

    sql_stmt = sql_stmt.format(',\n'.join(features),
                               config.get("where_clause", ""))

    logging.info("SQL QUERY {}".format(sql_stmt))

    spark = get_spark_session()
    df = spark.sql(sql_stmt)

    return df