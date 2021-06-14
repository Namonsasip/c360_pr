from pyspark.sql import DataFrame
from kedro.context import load_context
import logging, os
from pathlib import Path
from pyspark.sql import functions as F

from customer360.utilities.re_usable_functions import check_empty_dfs, gen_max_sql, execute_sql, \
    union_dataframes_with_missing_cols
from customer360.utilities.spark_util import get_spark_empty_df

conf = os.getenv("CONF", None)


def l4_usage_rolling_window_weekly(input_one: DataFrame, input_two: DataFrame,
                                   input_three: DataFrame, input_four: DataFrame,
                                   input_five: DataFrame, input_six: DataFrame,
                                   input_seven: DataFrame, input_eight: DataFrame,
                                   input_nine: DataFrame, input_ten: DataFrame,
                                   input_eleven: DataFrame, input_twelve: DataFrame,
                                   input_thirteen: DataFrame) -> DataFrame:

    start_period = '2020-01-27'
    end_period = '2020-02-17'
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
