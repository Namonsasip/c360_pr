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
                                   min: dict,
                                   max: dict,
                                   sum: dict,
                                   avg: dict) -> DataFrame:
    """
    :param input_df:
    :param min:
    :param max:
    :param sum:
    :param avg:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)
    group_cols = ["subscription_identifier", "start_of_week"]

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == "l4_campaign_postpaid_prepaid_features") \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .collect()[0].max_date

    input_df = input_df.cache()

    min_df = l4_rolling_window(input_df, min)
    min_df = min_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_min", min_df)

    max_df = l4_rolling_window(input_df, max)
    max_df = max_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_max", max_df)

    sum_df = l4_rolling_window(input_df, sum)
    sum_df = sum_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_sum", sum_df)

    avg_df = l4_rolling_window(input_df, avg)
    avg_df = avg_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_avg", avg_df)

    min_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_min")
    max_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_max")
    sum_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_sum")
    avg_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_avg")

    group_cols = ["subscription_identifier", "start_of_week"]
    merged_df = union_dataframes_with_missing_cols(min_df, max_df, sum_df, avg_df)
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
