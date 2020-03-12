from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def add_relative_time_features(data_frame: DataFrame) -> DataFrame:
    """
    :param data_frame:
    :return:
    """
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
