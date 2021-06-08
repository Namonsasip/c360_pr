from pyspark.sql import DataFrame
from kedro.context import load_context
import logging, os
from pathlib import Path
from pyspark.sql import functions as f
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, get_spark_empty_df,\
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql

conf = os.getenv("CONF", None)


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
        first_df = df1.filter(f.col("start_of_week").isin(*[curr_item]))
        second_df = df2.filter(f.col("start_of_week").isin(*[curr_item]))
        third_df = df3.filter(f.col("start_of_week").isin(*[curr_item]))
        fourth_df = df4.filter(f.col("start_of_week").isin(*[curr_item]))

        final_df = first_df.join(second_df, join_key)
        final_df = final_df.join(third_df, join_key)
        final_df = final_df.join(fourth_df, join_key)

        CNTX.catalog.save("l4_usage_postpaid_prepaid_weekly_features", final_df)

    logging.info("Final date to run for {0}".format(str(first_item)))
    first_df = df1.filter(f.col("start_of_week").isin(*[first_item]))
    second_df = df2.filter(f.col("start_of_week").isin(*[first_item]))
    third_df = df3.filter(f.col("start_of_week").isin(*[first_item]))
    fourth_df = df4.filter(f.col("start_of_week").isin(*[first_item]))

    return_df = first_df.join(second_df, join_key)
    return_df = return_df.join(third_df, join_key)
    return_df = return_df.join(fourth_df, join_key)

    return return_df

def usage_l4_rolling_window(input_df: DataFrame,
                            rolling_window_dict_first: dict,
                            rolling_window_dict_second: dict,
                            rolling_window_dict_third: dict,
                            rolling_window_dict_fourth: dict,
                            rolling_window_dict_fifth: dict,
                            table_name: str
                            ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :param rolling_window_dict_third:
    :param rolling_window_dict_fourth:
    :param rolling_window_dict_fifth:
    :param table_name
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()
    input_df = input_df.where("start_of_week <= '2020-03-02'")
    CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["subscription_identifier", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == table_name) \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    rolling_df_first = l4_rolling_window(input_df, rolling_window_dict_first)
    rolling_df_first = rolling_df_first.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_usage_temp_1", rolling_df_first)

    rolling_df_second = l4_rolling_window(input_df, rolling_window_dict_second)
    rolling_df_second = rolling_df_second.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_usage_temp_2", rolling_df_second)

    rolling_df_third = l4_rolling_window(input_df, rolling_window_dict_third)
    rolling_df_third = rolling_df_third.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_usage_temp_3", rolling_df_third)

    rolling_df_fourth = l4_rolling_window(input_df, rolling_window_dict_fourth)
    rolling_df_fourth = rolling_df_fourth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_usage_temp_4", rolling_df_fourth)

    rolling_df_fifth = l4_rolling_window(input_df, rolling_window_dict_fifth)
    rolling_df_fifth = rolling_df_fifth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_usage_temp_5", rolling_df_fifth)

    rolling_df_first = CNTX.catalog.load("l4_usage_temp_1")
    rolling_df_second = CNTX.catalog.load("l4_usage_temp_2")
    rolling_df_third = CNTX.catalog.load("l4_usage_temp_3")
    rolling_df_fourth = CNTX.catalog.load("l4_usage_temp_4")
    rolling_df_fifth = CNTX.catalog.load("l4_usage_temp_5")

    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second, rolling_df_third,
                                                   rolling_df_fourth, rolling_df_fifth])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df