from pyspark.sql import DataFrame
from kedro.context import load_context
import logging, os
from pathlib import Path
from pyspark.sql import functions as F

from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, gen_max_sql, execute_sql, \
    union_dataframes_with_missing_cols, gen_min_sql
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def l4_usage_filter_date_rolling_window_weekly(input_df: DataFrame, config: dict):
    start_period = '2020-07-13'
    end_period = '2021-05-24'

    rolling_df = l4_rolling_window(input_df, config)

    return_df = rolling_df.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    logging.info("WHERE Condition: start_of_week between '" + start_period + "' and '" + end_period + "'")

    return return_df


def l4_usage_rolling_window_weekly(input_one: DataFrame, input_two: DataFrame,
                                   input_three: DataFrame, input_four: DataFrame,
                                   input_five: DataFrame, input_six: DataFrame,
                                   input_seven: DataFrame, input_eight: DataFrame,
                                   input_nine: DataFrame, input_ten: DataFrame,
                                   input_eleven: DataFrame, input_twelve: DataFrame,
                                   input_thirteen: DataFrame) -> DataFrame:

    start_period = '2020-06-22'
    end_period = '2020-06-29'
    input_1 = input_one.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_2 = input_two.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_3 = input_three.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_4 = input_four.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_5 = input_five.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_6 = input_six.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_7 = input_seven.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_8 = input_eight.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_9 = input_nine.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_10 = input_ten.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_11 = input_eleven.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_12 = input_twelve.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_13 = input_thirteen.where("start_of_week between '" + start_period + "' and '" + end_period + "'")

    # input_1 = input_one.where("start_of_week = '" + start_period + "'")
    # input_2 = input_two.where("start_of_week = '" + start_period + "'")
    # input_3 = input_three.where("start_of_week = '" + start_period + "'")
    # input_4 = input_four.where("start_of_week = '" + start_period + "'")
    # input_5 = input_five.where("start_of_week = '" + start_period + "'")
    # input_6 = input_six.where("start_of_week = '" + start_period + "'")
    # input_7 = input_seven.where("start_of_week = '" + start_period + "'")
    # input_8 = input_eight.where("start_of_week = '" + start_period + "'")
    # input_9 = input_nine.where("start_of_week = '" + start_period + "'")
    # input_10 = input_ten.where("start_of_week = '" + start_period + "'")
    # input_11 = input_eleven.where("start_of_week = '" + start_period + "'")
    # input_12 = input_twelve.where("start_of_week = '" + start_period + "'")
    # input_13 = input_thirteen.where("start_of_week = '" + start_period + "'")

    logging.info(start_period+" "+end_period)

    if check_empty_dfs([input_1, input_2,
                        input_3, input_4,
                        input_5, input_6,
                        input_7, input_8,
                        input_9, input_10,
                        input_11, input_12, input_13]):
        return get_spark_empty_df()

    group_cols = ["subscription_identifier", "start_of_week"]

    union_df = union_dataframes_with_missing_cols([input_1, input_2,
                                                   input_3, input_4,
                                                   input_5, input_6,
                                                   input_7, input_8,
                                                   input_9, input_10,
                                                   input_11, input_12, input_13])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df


def l4_usage_filter_date_rolling_window_weekly_min(input_df: DataFrame, config: dict):
    start_period = '2020-11-30'
    end_period = '2020-12-07'

    rolling_df = l4_rolling_window(input_df, config)

    return_df = rolling_df.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    logging.info("WHERE Condition: start_of_week between '" + start_period + "' and '" + end_period + "'")

    return return_df


def l4_usage_rolling_window_weekly_min(input_one: DataFrame, input_two: DataFrame,
                                       input_three: DataFrame, input_four: DataFrame,
                                       input_five: DataFrame, input_six: DataFrame,
                                       input_seven: DataFrame, input_eight: DataFrame,
                                       input_nine: DataFrame, input_ten: DataFrame,
                                       input_eleven: DataFrame, input_twelve: DataFrame) -> DataFrame:

    start_period = '2020-11-30'
    end_period = '2020-12-07'
    input_1 = input_one.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_2 = input_two.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_3 = input_three.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_4 = input_four.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_5 = input_five.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_6 = input_six.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_7 = input_seven.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_8 = input_eight.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_9 = input_nine.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_10 = input_ten.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_11 = input_eleven.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    input_12 = input_twelve.where("start_of_week between '" + start_period + "' and '" + end_period + "'")

    logging.info(start_period+" "+end_period)

    if check_empty_dfs([input_1, input_2,
                        input_3, input_4,
                        input_5, input_6,
                        input_7, input_8,
                        input_9, input_10,
                        input_11, input_12]):
        return get_spark_empty_df()

    group_cols = ["subscription_identifier", "start_of_week"]

    union_df = union_dataframes_with_missing_cols([input_1, input_2,
                                                   input_3, input_4,
                                                   input_5, input_6,
                                                   input_7, input_8,
                                                   input_9, input_10,
                                                   input_11, input_12])

    final_df_str = gen_min_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df


def build_l4_usage_rolling_window_split_column(input_df: DataFrame, first_dict: dict,
                                               second_dict: dict, third_dict: dict,
                                               fourth_dict: dict, fifth_dict: dict,
                                               sixth_dict: dict, seventh_dict: dict,
                                               eighth_dict: dict, ninth_dict: dict,
                                               tenth_dict: dict, eleventh_dict: dict,
                                               twelfth_dict: dict, thirteenth_dict: dict,
                                               target_table: str) -> DataFrame:

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)

    # metadata = CNTX.catalog.load("util_audit_metadata_table")
    # max_date = metadata.filter(F.col("table_name") == target_table) \
    #     .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
    #     .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
    #     .collect()[0].max_date
    min_date = '2020-11-30'
    max_date = '2020-12-07'

    input_df = input_df.cache()
    first_df = l4_rolling_window(input_df, first_dict)
    first_df = first_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_first", first_df)

    second_df = l4_rolling_window(input_df, second_dict)
    second_df = second_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_second", second_df)

    third_df = l4_rolling_window(input_df, third_dict)
    third_df = third_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_third", third_df)

    fourth_df = l4_rolling_window(input_df, fourth_dict)
    fourth_df = fourth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_fourth", fourth_df)

    fifth_df = l4_rolling_window(input_df, fifth_dict)
    fifth_df = fifth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_fifth", fifth_df)

    sixth_df = l4_rolling_window(input_df, sixth_dict)
    sixth_df = sixth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_sixth", sixth_df)

    seventh_df = l4_rolling_window(input_df, seventh_dict)
    seventh_df = seventh_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_seventh", seventh_df)

    eighth_df = l4_rolling_window(input_df, eighth_dict)
    eighth_df = eighth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_eighth", eighth_df)

    ninth_df = l4_rolling_window(input_df, ninth_dict)
    ninth_df = ninth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_ninth", ninth_df)

    tenth_df = l4_rolling_window(input_df, tenth_dict)
    tenth_df = tenth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_tenth", tenth_df)

    eleventh_df = l4_rolling_window(input_df, eleventh_dict)
    eleventh_df = eleventh_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_eleventh", eleventh_df)

    twelfth_df = l4_rolling_window(input_df, twelfth_dict)
    twelfth_df = twelfth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_twelfth", twelfth_df)

    thirteenth_df = l4_rolling_window(input_df, thirteenth_dict)
    thirteenth_df = thirteenth_df.filter(F.col("start_of_week").between(min_date, max_date))
    CNTX.catalog.save(target_table + "_thirteenth", thirteenth_df)

    first_df = CNTX.catalog.load(target_table + "_first")
    second_df = CNTX.catalog.load(target_table + "_second")
    third_df = CNTX.catalog.load(target_table + "_third")
    fourth_df = CNTX.catalog.load(target_table + "_fourth")
    fifth_df = CNTX.catalog.load(target_table + "_fifth")
    sixth_df = CNTX.catalog.load(target_table + "_sixth")
    seventh_df = CNTX.catalog.load(target_table + "_seventh")
    eighth_df = CNTX.catalog.load(target_table + "_eighth")
    ninth_df = CNTX.catalog.load(target_table + "_ninth")
    tenth_df = CNTX.catalog.load(target_table + "_tenth")
    eleventh_df = CNTX.catalog.load(target_table + "_eleventh")
    twelfth_df = CNTX.catalog.load(target_table + "_twelfth")
    thirteenth_df = CNTX.catalog.load(target_table + "_thirteenth")

    group_cols = ["subscription_identifier", "start_of_week"]

    merged_df = union_dataframes_with_missing_cols(first_df, second_df, third_df, fourth_df, fifth_df, sixth_df,
                                                   seventh_df, eighth_df, ninth_df, tenth_df, eleventh_df, twelfth_df,
                                                   thirteenth_df)
    sql_query = gen_max_sql(merged_df, "test_table", group_cols)

    return_df = execute_sql(merged_df, "test_table", sql_query)
    return return_df


def l4_usage_rolling_window_split_column_maxdate(input_df: DataFrame, first_dict: dict,
                                                 second_dict: dict, third_dict: dict,
                                                 fourth_dict: dict, fifth_dict: dict,
                                                 sixth_dict: dict, seventh_dict: dict,
                                                 eighth_dict: dict, ninth_dict: dict,
                                                 tenth_dict: dict, eleventh_dict: dict,
                                                 twelfth_dict: dict, thirteenth_dict: dict,
                                                 target_table: str) -> DataFrame:

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == target_table) \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    input_df = input_df.cache()
    first_df = l4_rolling_window(input_df, first_dict)
    first_df = first_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_first", first_df)

    second_df = l4_rolling_window(input_df, second_dict)
    second_df = second_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_second", second_df)

    third_df = l4_rolling_window(input_df, third_dict)
    third_df = third_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_third", third_df)

    fourth_df = l4_rolling_window(input_df, fourth_dict)
    fourth_df = fourth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_fourth", fourth_df)

    fifth_df = l4_rolling_window(input_df, fifth_dict)
    fifth_df = fifth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_fifth", fifth_df)

    sixth_df = l4_rolling_window(input_df, sixth_dict)
    sixth_df = sixth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_sixth", sixth_df)

    seventh_df = l4_rolling_window(input_df, seventh_dict)
    seventh_df = seventh_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_seventh", seventh_df)

    eighth_df = l4_rolling_window(input_df, eighth_dict)
    eighth_df = eighth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_eighth", eighth_df)

    ninth_df = l4_rolling_window(input_df, ninth_dict)
    ninth_df = ninth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_ninth", ninth_df)

    tenth_df = l4_rolling_window(input_df, tenth_dict)
    tenth_df = tenth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_tenth", tenth_df)

    eleventh_df = l4_rolling_window(input_df, eleventh_dict)
    eleventh_df = eleventh_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_eleventh", eleventh_df)

    twelfth_df = l4_rolling_window(input_df, twelfth_dict)
    twelfth_df = twelfth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_twelfth", twelfth_df)

    thirteenth_df = l4_rolling_window(input_df, thirteenth_dict)
    thirteenth_df = thirteenth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save(target_table + "_thirteenth", thirteenth_df)

    first_df = CNTX.catalog.load(target_table + "_first")
    second_df = CNTX.catalog.load(target_table + "_second")
    third_df = CNTX.catalog.load(target_table + "_third")
    fourth_df = CNTX.catalog.load(target_table + "_fourth")
    fifth_df = CNTX.catalog.load(target_table + "_fifth")
    sixth_df = CNTX.catalog.load(target_table + "_sixth")
    seventh_df = CNTX.catalog.load(target_table + "_seventh")
    eighth_df = CNTX.catalog.load(target_table + "_eighth")
    ninth_df = CNTX.catalog.load(target_table + "_ninth")
    tenth_df = CNTX.catalog.load(target_table + "_tenth")
    eleventh_df = CNTX.catalog.load(target_table + "_eleventh")
    twelfth_df = CNTX.catalog.load(target_table + "_twelfth")
    thirteenth_df = CNTX.catalog.load(target_table + "_thirteenth")

    group_cols = ["subscription_identifier", "start_of_week"]

    merged_df = union_dataframes_with_missing_cols(first_df, second_df, third_df, fourth_df, fifth_df, sixth_df,
                                                   seventh_df, eighth_df, ninth_df, tenth_df, eleventh_df, twelfth_df,
                                                   thirteenth_df)
    sql_query = gen_max_sql(merged_df, "test_table", group_cols)

    return_df = execute_sql(merged_df, "test_table", sql_query)
    return return_df


def merge_all_usage_massive_processing(df1: DataFrame, df2: DataFrame,
                                       df3: DataFrame, df4: DataFrame,
                                       df5: DataFrame, df6: DataFrame,
                                       df7: DataFrame, df8: DataFrame,
                                       df9: DataFrame, df10: DataFrame,
                                       df11: DataFrame, df12: DataFrame,
                                       df13: DataFrame) -> DataFrame:

    start_period = '2021-04-12'
    end_period = '2021-05-24'
    df1 = df1.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df2 = df2.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df3 = df3.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df4 = df4.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df5 = df5.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df6 = df6.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df7 = df7.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df8 = df8.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df9 = df9.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df10 = df10.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df11 = df11.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df12 = df12.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    df13 = df13.where("start_of_week between '" + start_period + "' and '" + end_period + "'")
    logging.info("WHERE CONDITION: " + start_period + " " + end_period)

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    CNTX = load_context(Path.cwd(), env=conf)
    dates_list = df1.select('start_of_week').distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    group_cols = ["subscription_identifier", "start_of_week"]

    mvv_new = list(divide_chunks(mvv_array, 2))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        first_df = df1.filter(F.col("start_of_week").isin(*[curr_item]))
        second_df = df2.filter(F.col("start_of_week").isin(*[curr_item]))
        third_df = df3.filter(F.col("start_of_week").isin(*[curr_item]))
        fourth_df = df4.filter(F.col("start_of_week").isin(*[curr_item]))
        fifth_df = df5.filter(F.col("start_of_week").isin(*[curr_item]))
        sixth_df = df6.filter(F.col("start_of_week").isin(*[curr_item]))
        seventh_df = df7.filter(F.col("start_of_week").isin(*[curr_item]))
        eighth_df = df8.filter(F.col("start_of_week").isin(*[curr_item]))
        ninth_df = df9.filter(F.col("start_of_week").isin(*[curr_item]))
        tenth_df = df10.filter(F.col("start_of_week").isin(*[curr_item]))
        eleventh_df = df11.filter(F.col("start_of_week").isin(*[curr_item]))
        twelfth_df = df12.filter(F.col("start_of_week").isin(*[curr_item]))
        thirteenth = df13.filter(F.col("start_of_week").isin(*[curr_item]))

        if check_empty_dfs([first_df, second_df,
                           third_df, fourth_df,
                           fifth_df, sixth_df,
                           seventh_df, eighth_df,
                           ninth_df, tenth_df,
                           eleventh_df, twelfth_df, thirteenth]):
            return get_spark_empty_df()

        union_df = union_dataframes_with_missing_cols([first_df, second_df,
                                                       third_df, fourth_df,
                                                       fifth_df, sixth_df,
                                                       seventh_df, eighth_df,
                                                       ninth_df, tenth_df,
                                                       eleventh_df, twelfth_df, thirteenth])

        final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
        merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

        CNTX.catalog.save("l4_usage_postpaid_prepaid_weekly_features_max", merged_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    first_df = df1.filter(F.col("start_of_week").isin(*[first_item]))
    second_df = df2.filter(F.col("start_of_week").isin(*[first_item]))
    third_df = df3.filter(F.col("start_of_week").isin(*[first_item]))
    fourth_df = df4.filter(F.col("start_of_week").isin(*[first_item]))
    fifth_df = df5.filter(F.col("start_of_week").isin(*[first_item]))
    sixth_df = df6.filter(F.col("start_of_week").isin(*[first_item]))
    seventh_df = df7.filter(F.col("start_of_week").isin(*[first_item]))
    eighth_df = df8.filter(F.col("start_of_week").isin(*[first_item]))
    ninth_df = df9.filter(F.col("start_of_week").isin(*[first_item]))
    tenth_df = df10.filter(F.col("start_of_week").isin(*[first_item]))
    eleventh_df = df11.filter(F.col("start_of_week").isin(*[first_item]))
    twelfth_df = df12.filter(F.col("start_of_week").isin(*[first_item]))
    thirteenth = df13.filter(F.col("start_of_week").isin(*[first_item]))

    if check_empty_dfs([first_df, second_df,
                        third_df, fourth_df,
                        fifth_df, sixth_df,
                        seventh_df, eighth_df,
                        ninth_df, tenth_df,
                        eleventh_df, twelfth_df, thirteenth]):
        return get_spark_empty_df()

    union_df2 = union_dataframes_with_missing_cols([first_df, second_df,
                                                   third_df, fourth_df,
                                                   fifth_df, sixth_df,
                                                   seventh_df, eighth_df,
                                                   ninth_df, tenth_df,
                                                   eleventh_df, twelfth_df, thirteenth])

    final_df_str2 = gen_max_sql(union_df2, 'tmp_table_name', group_cols)
    return_df = execute_sql(union_df2, 'tmp_table_name', final_df_str2)

    return return_df


def l4_usage_rolling_window_split_column_massive(input_df: DataFrame, first_dict: dict,
                                                 second_dict: dict, third_dict: dict,
                                                 fourth_dict: dict, fifth_dict: dict,
                                                 sixth_dict: dict, seventh_dict: dict,
                                                 eighth_dict: dict, ninth_dict: dict,
                                                 tenth_dict: dict, eleventh_dict: dict,
                                                 twelfth_dict: dict, thirteenth_dict: dict,
                                                 target_table: str) -> DataFrame:

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = input_df.cache()
    CNTX = load_context(Path.cwd(), env=conf)
    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == target_table) \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date
    logging.info("Max date of table {0} is {1}".format(target_table, str(max_date)))

    def divide_chunks(l, n):
        # looping till length l
        for i in range(0, len(l), n):
            yield l[i:i + n]

    df_filter_date = input_df.select('start_of_week').filter(F.col("start_of_week") > max_date)
    dates_list = df_filter_date.distinct().collect()
    mvv_array = [row[0] for row in dates_list if row[0] != "SAMPLING"]
    mvv_array = sorted(mvv_array)
    logging.info("Dates to run for {0}".format(str(mvv_array)))
    group_cols = ["subscription_identifier", "start_of_week"]

    mvv_new = list(divide_chunks(mvv_array, 1))
    add_list = mvv_new

    first_item = add_list[-1]

    add_list.remove(first_item)
    for curr_item in add_list:
        logging.info("running for dates {0}".format(str(curr_item)))
        first_df = l4_rolling_window(input_df, first_dict)
        first_df = first_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_first", first_df)

        second_df = l4_rolling_window(input_df, second_dict)
        second_df = second_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_second", second_df)

        third_df = l4_rolling_window(input_df, third_dict)
        third_df = third_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_third", third_df)

        fourth_df = l4_rolling_window(input_df, fourth_dict)
        fourth_df = fourth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_fourth", fourth_df)

        fifth_df = l4_rolling_window(input_df, fifth_dict)
        fifth_df = fifth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_fifth", fifth_df)

        sixth_df = l4_rolling_window(input_df, sixth_dict)
        sixth_df = sixth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_sixth", sixth_df)

        seventh_df = l4_rolling_window(input_df, seventh_dict)
        seventh_df = seventh_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_seventh", seventh_df)

        eighth_df = l4_rolling_window(input_df, eighth_dict)
        eighth_df = eighth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_eighth", eighth_df)

        ninth_df = l4_rolling_window(input_df, ninth_dict)
        ninth_df = ninth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_ninth", ninth_df)

        tenth_df = l4_rolling_window(input_df, tenth_dict)
        tenth_df = tenth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_tenth", tenth_df)

        eleventh_df = l4_rolling_window(input_df, eleventh_dict)
        eleventh_df = eleventh_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_eleventh", eleventh_df)

        twelfth_df = l4_rolling_window(input_df, twelfth_dict)
        twelfth_df = twelfth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_twelfth", twelfth_df)

        thirteenth_df = l4_rolling_window(input_df, thirteenth_dict)
        thirteenth_df = thirteenth_df.filter(F.col("start_of_week").isin(*[curr_item]))
        CNTX.catalog.save(target_table + "_thirteenth", thirteenth_df)

        first_df = CNTX.catalog.load(target_table + "_first")
        second_df = CNTX.catalog.load(target_table + "_second")
        third_df = CNTX.catalog.load(target_table + "_third")
        fourth_df = CNTX.catalog.load(target_table + "_fourth")
        fifth_df = CNTX.catalog.load(target_table + "_fifth")
        sixth_df = CNTX.catalog.load(target_table + "_sixth")
        seventh_df = CNTX.catalog.load(target_table + "_seventh")
        eighth_df = CNTX.catalog.load(target_table + "_eighth")
        ninth_df = CNTX.catalog.load(target_table + "_ninth")
        tenth_df = CNTX.catalog.load(target_table + "_tenth")
        eleventh_df = CNTX.catalog.load(target_table + "_eleventh")
        twelfth_df = CNTX.catalog.load(target_table + "_twelfth")
        thirteenth_df = CNTX.catalog.load(target_table + "_thirteenth")

        merged_df = union_dataframes_with_missing_cols(first_df, second_df, third_df, fourth_df, fifth_df, sixth_df,
                                                       seventh_df, eighth_df, ninth_df, tenth_df, eleventh_df,
                                                       twelfth_df,
                                                       thirteenth_df)
        sql_query = gen_max_sql(merged_df, "test_table", group_cols)

        save_df = execute_sql(merged_df, "test_table", sql_query)

        CNTX.catalog.save(target_table, save_df)

    logging.info("running for dates {0}".format(str(first_item)))
    first_df = l4_rolling_window(input_df, first_dict)
    first_df = first_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_first", first_df)

    second_df = l4_rolling_window(input_df, second_dict)
    second_df = second_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_second", second_df)

    third_df = l4_rolling_window(input_df, third_dict)
    third_df = third_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_third", third_df)

    fourth_df = l4_rolling_window(input_df, fourth_dict)
    fourth_df = fourth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_fourth", fourth_df)

    fifth_df = l4_rolling_window(input_df, fifth_dict)
    fifth_df = fifth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_fifth", fifth_df)

    sixth_df = l4_rolling_window(input_df, sixth_dict)
    sixth_df = sixth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_sixth", sixth_df)

    seventh_df = l4_rolling_window(input_df, seventh_dict)
    seventh_df = seventh_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_seventh", seventh_df)

    eighth_df = l4_rolling_window(input_df, eighth_dict)
    eighth_df = eighth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_eighth", eighth_df)

    ninth_df = l4_rolling_window(input_df, ninth_dict)
    ninth_df = ninth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_ninth", ninth_df)

    tenth_df = l4_rolling_window(input_df, tenth_dict)
    tenth_df = tenth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_tenth", tenth_df)

    eleventh_df = l4_rolling_window(input_df, eleventh_dict)
    eleventh_df = eleventh_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_eleventh", eleventh_df)

    twelfth_df = l4_rolling_window(input_df, twelfth_dict)
    twelfth_df = twelfth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_twelfth", twelfth_df)

    thirteenth_df = l4_rolling_window(input_df, thirteenth_dict)
    thirteenth_df = thirteenth_df.filter(F.col("start_of_week").isin(*[first_item]))
    CNTX.catalog.save(target_table + "_thirteenth", thirteenth_df)

    first_df = CNTX.catalog.load(target_table + "_first")
    second_df = CNTX.catalog.load(target_table + "_second")
    third_df = CNTX.catalog.load(target_table + "_third")
    fourth_df = CNTX.catalog.load(target_table + "_fourth")
    fifth_df = CNTX.catalog.load(target_table + "_fifth")
    sixth_df = CNTX.catalog.load(target_table + "_sixth")
    seventh_df = CNTX.catalog.load(target_table + "_seventh")
    eighth_df = CNTX.catalog.load(target_table + "_eighth")
    ninth_df = CNTX.catalog.load(target_table + "_ninth")
    tenth_df = CNTX.catalog.load(target_table + "_tenth")
    eleventh_df = CNTX.catalog.load(target_table + "_eleventh")
    twelfth_df = CNTX.catalog.load(target_table + "_twelfth")
    thirteenth_df = CNTX.catalog.load(target_table + "_thirteenth")

    merged_df = union_dataframes_with_missing_cols(first_df, second_df, third_df, fourth_df, fifth_df, sixth_df,
                                                   seventh_df, eighth_df, ninth_df, tenth_df, eleventh_df, twelfth_df,
                                                   thirteenth_df)
    sql_query = gen_max_sql(merged_df, "test_table", group_cols)

    return_df = execute_sql(merged_df, "test_table", sql_query)
    return return_df


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
