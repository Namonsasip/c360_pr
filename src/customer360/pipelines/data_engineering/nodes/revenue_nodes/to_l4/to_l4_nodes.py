from customer360.utilities.spark_util import get_spark_empty_df
from customer360.utilities.re_usable_functions import check_empty_dfs, data_non_availability_and_missing_check, \
    get_spark_session, union_dataframes_with_missing_cols, gen_max_sql, execute_sql
from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_window_by_metadata, get_date_new_rolling
from customer360.utilities.config_parser import node_from_config
from pyspark.sql import DataFrame, functions as f
from pyspark.sql import functions as F
import datetime
import os
from pathlib import Path
from kedro.context.context import load_context
from dateutil.relativedelta import relativedelta
import logging


conf = os.getenv("CONF", None)


def df_copy_for_l4_customer_profile_ltv_to_date(input_df):
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    input_df = data_non_availability_and_missing_check(df=input_df, grouping="monthly",
                                                       par_col="partition_month",
                                                       target_table_name="l4_customer_profile_ltv_to_date")

    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    return input_df


def calculate_ltv_to_date(
        prepaid_revenue_df: DataFrame,
        postpaid_revenue_df: DataFrame
) -> DataFrame:
    ################################# Start Implementing Data availability checks #############################
    if check_empty_dfs([prepaid_revenue_df, postpaid_revenue_df]):
        return get_spark_empty_df()

    prepaid_revenue_df = data_non_availability_and_missing_check(df=prepaid_revenue_df, grouping="monthly",
                                                                 par_col="start_of_month",
                                                                 target_table_name="l4_revenue_ltv_to_date")

    postpaid_revenue_df = data_non_availability_and_missing_check(df=postpaid_revenue_df, grouping="monthly",
                                                                  par_col="start_of_month",
                                                                  target_table_name="l4_revenue_ltv_to_date")

    # new section to handle data latency
    min_value = union_dataframes_with_missing_cols(
        [
            prepaid_revenue_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
            postpaid_revenue_df.select(
                f.max(f.col("start_of_month")).alias("max_date")),
        ]
    ).select(f.min(f.col("max_date")).alias("min_date")).collect()[0].min_date

    prepaid_revenue_df = prepaid_revenue_df.filter(f.col("start_of_month") <= min_value)
    postpaid_revenue_df = postpaid_revenue_df.filter(f.col("start_of_month") <= min_value)

    if check_empty_dfs([prepaid_revenue_df, postpaid_revenue_df]):
        return get_spark_empty_df()

    ################################# End Implementing Data availability checks ###############################

    identifier = ["subscription_identifier"]
    granularity_col = identifier + ["start_of_month"]

    prepaid_revenue_df = prepaid_revenue_df.select(granularity_col + ["rev_arpu_total_revenue"])
    postpaid_revenue_df = postpaid_revenue_df.select(granularity_col + ["rev_arpu_total_revenue"])

    combined_revenue_df = prepaid_revenue_df.unionByName(postpaid_revenue_df)

    combined_revenue_df.createOrReplaceTempView("combined_revenue_df")

    spark = get_spark_session()
    df = spark.sql("""
        with combined_rpu as (
            select
                {granularity_col},
                sum(rev_arpu_total_revenue) as rev_arpu_total_revenue
            from combined_revenue_df
            group by {granularity_col}
        ) 
        select 
            {granularity_col},
            sum(rev_arpu_total_revenue) over (partition by {identifier}
                                              order by start_of_month asc) as ltv_to_date
        from combined_rpu
    """.format(granularity_col=", ".join(granularity_col),
               identifier=", ".join(identifier)))

    return df


def revenue_l4_dataset_monthly_datasets(input_df: DataFrame,
                                        rolling_window_dict_min: dict,
                                        rolling_window_dict_max: dict,
                                        rolling_window_dict_sum: dict,
                                        rolling_window_dict_avg: dict,
                                        rolling_window_dict_stddev: dict,
                                        node_from_config_dict: dict
                                        ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_min:
    :param rolling_window_dict_max:
    :param rolling_window_dict_sum:
    :param rolling_window_dict_avg:
    :param rolling_window_dict_stddev:
    :param node_from_config_dict:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    group_cols = ["subscription_identifier", "start_of_month"]
    rolling_df_min = l4_rolling_window(input_df, rolling_window_dict_min)
    rolling_df_max = l4_rolling_window(input_df, rolling_window_dict_max)
    rolling_df_sum = l4_rolling_window(input_df, rolling_window_dict_sum)
    rolling_df_avg = l4_rolling_window(input_df, rolling_window_dict_avg)
    rolling_df_stddev = l4_rolling_window(input_df, rolling_window_dict_stddev)

    union_df = union_dataframes_with_missing_cols([rolling_df_min, rolling_df_max, rolling_df_sum, rolling_df_avg
                                                   , rolling_df_stddev])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    node_df = node_from_config(merged_df, node_from_config_dict)
    return node_df


def revenue_l4_dataset_weekly_datasets(input_df: DataFrame,
                                       rolling_window_dict_first: dict,
                                       rolling_window_dict_second: dict,
                                       rolling_window_dict_third: dict,
                                       rolling_window_dict_fourth: dict,
                                       rolling_window_dict_fifth: dict,
                                       rolling_window_dict_all_tg: dict
                                       ) -> DataFrame:
    """
    :param input_df:
    :param rolling_window_dict_first:
    :param rolling_window_dict_second:
    :param rolling_window_dict_third:
    :param rolling_window_dict_fourth:
    :param rolling_window_dict_fifth:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["subscription_identifier", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(f.col("table_name") == "l4_revenue_prepaid_pru_f_usage_multi_features") \
        .select(f.max(f.col("target_max_data_load_date")).alias("max_date"))\
        .withColumn("max_date", f.coalesce(f.col("max_date"), f.to_date(f.lit('1970-01-01'), 'yyyy-MM-dd')))\
        .collect()[0].max_date

    max_date = get_date_new_rolling(max_date,"l2")

    rolling_window_first = l4_rolling_window_by_metadata(input_df, rolling_window_dict_first, rolling_window_dict_all_tg)
    rolling_window_first = rolling_window_first.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_revenue_prepaid_pru_f_usage_multi_features_first_set", rolling_window_first)

    rolling_window_second = l4_rolling_window_by_metadata(input_df, rolling_window_dict_second, rolling_window_dict_all_tg)
    rolling_window_second = rolling_window_second.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_revenue_prepaid_pru_f_usage_multi_features_second_set", rolling_window_second)

    rolling_window_third = l4_rolling_window_by_metadata(input_df, rolling_window_dict_third, rolling_window_dict_all_tg)
    rolling_window_third = rolling_window_third.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_revenue_prepaid_pru_f_usage_multi_features_third_set", rolling_window_third)

    rolling_window_fourth = l4_rolling_window_by_metadata(input_df, rolling_window_dict_fourth, rolling_window_dict_all_tg)
    rolling_window_fourth = rolling_window_fourth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_revenue_prepaid_pru_f_usage_multi_features_fourth_set", rolling_window_fourth)

    rolling_df_fifth = l4_rolling_window_by_metadata(input_df, rolling_window_dict_fifth, rolling_window_dict_all_tg)
    rolling_df_fifth = rolling_df_fifth.filter(f.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_revenue_prepaid_pru_f_usage_multi_features_fifth_set", rolling_df_fifth)

    rolling_df_first = CNTX.catalog.load("l4_revenue_prepaid_pru_f_usage_multi_features_first_set")
    rolling_df_second = CNTX.catalog.load("l4_revenue_prepaid_pru_f_usage_multi_features_second_set")
    rolling_df_third = CNTX.catalog.load("l4_revenue_prepaid_pru_f_usage_multi_features_third_set")
    rolling_df_fourth = CNTX.catalog.load("l4_revenue_prepaid_pru_f_usage_multi_features_fourth_set")
    rolling_df_fifth = CNTX.catalog.load("l4_revenue_prepaid_pru_f_usage_multi_features_fifth_set")

    union_df = union_dataframes_with_missing_cols([rolling_df_first, rolling_df_second, rolling_df_third,
                                                   rolling_df_fourth, rolling_df_fifth])

    final_df_str = gen_max_sql(union_df, 'tmp_table_name', group_cols)
    merged_df = execute_sql(union_df, 'tmp_table_name', final_df_str)

    return merged_df
