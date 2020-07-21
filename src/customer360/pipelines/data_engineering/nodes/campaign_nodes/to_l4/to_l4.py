from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, get_spark_empty_df,\
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql


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

    input_df = input_df.cache()
    min_df = l4_rolling_window(input_df, min)
    max_df = l4_rolling_window(input_df, max)
    sum_df = l4_rolling_window(input_df, sum)
    avg_df = l4_rolling_window(input_df, avg)

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
