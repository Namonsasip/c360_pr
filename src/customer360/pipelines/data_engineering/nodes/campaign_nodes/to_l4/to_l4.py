from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, get_spark_empty_df,\
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql
import os
from pathlib import Path
from kedro.context.context import load_context


conf = os.getenv("CONF", None)


def build_campaign_weekly_features(input_df: DataFrame,
                                   first_dict: dict,
                                   second_dict: dict,
                                   third_dict: dict,
                                   fourth_dict: dict) -> DataFrame:
    """
    :param input_df:
    :param first_dict:
    :param second_dict:
    :param third_dict:
    :param fourth_dict:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == "l4_campaign_postpaid_prepaid_features") \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    input_df = input_df.cache()

    first_df = l4_rolling_window(input_df, first_dict)
    first_df = first_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_first", first_df)

    second_df = l4_rolling_window(input_df, second_dict)
    second_df = second_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_second", second_df)

    third_df = l4_rolling_window(input_df, third_dict)
    third_df = third_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_third", third_df)

    fourth_df = l4_rolling_window(input_df, fourth_dict)
    fourth_df = fourth_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_fourth", fourth_df)

    first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_first")
    second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_second")
    third_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_third")
    fourth_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_fourth")

    group_cols = ["subscription_identifier", "start_of_week"]
    merged_df = union_dataframes_with_missing_cols(first_df, second_df, third_df, fourth_df)
    sql_query = gen_max_sql(merged_df, "test_table", group_cols)

    return_df = execute_sql(merged_df, "test_table", sql_query)
    return return_df




def add_relative_time_features(data_frame: DataFrame) -> DataFrame:
    """
    :param data_frame:
    :return:
    """
    if len(data_frame.head(1)) == 0:
        return data_frame

    data_frame = data_frame.withColumn(
        "sum_campaign_total_upsell_xsell_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_retention_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_eligible_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_eligible_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_eligible_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_success_by_call_center_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_success_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_success_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_eligible_by_call_center_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_eligible_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_eligible_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_upsell_xsell_success_by_call_center_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_upsell_xsell_success_by_call_center_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_upsell_xsell_success_by_call_center_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_retention_eligible_by_call_center_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_eligible_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_eligible_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_retention_success_by_call_center_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_retention_success_by_call_center_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_retention_success_by_call_center_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_others_eligible_by_call_center_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_eligible_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_eligible_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_success_by_call_center_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_success_by_call_center_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_success_by_call_center_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_retention_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_success_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_success_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_retention_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_retention_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_others_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_retention_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_success_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_success_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_retention_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_retention_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_others_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_retention_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_success_by_sms_sum_weekly_last_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_success_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_upsell_xsell_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_upsell_xsell_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_retention_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_retention_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn("sum_campaign_total_retention_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
                    , F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_four_week")
                    / F.col("sum_campaign_total_retention_success_by_sms_sum_weekly_last_twelve_week")
                    ) \
        .withColumn(
        "sum_campaign_total_others_eligible_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_eligible_by_sms_sum_weekly_last_twelve_week")
    ) \
        .withColumn(
        "sum_campaign_total_others_success_by_sms_sum_weekly_four_week_over_twelve_weeks"
        , F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_four_week")
          / F.col("sum_campaign_total_others_success_by_sms_sum_weekly_last_twelve_week")
    )
    return data_frame
