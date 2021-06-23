from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from customer360.utilities.config_parser import l4_rolling_window
from customer360.utilities.re_usable_functions import check_empty_dfs, get_spark_empty_df, \
    union_dataframes_with_missing_cols, gen_max_sql, execute_sql
import os
from pathlib import Path
from kedro.context.context import load_context

conf = os.getenv("CONF", None)


def build_campaign_weekly_features(input_df: DataFrame,
                                   first_first_dict: dict,
                                   first_second_dict: dict,
                                   second_first_dict: dict,
                                   second_second_dict: dict,
                                   third_first_dict: dict,
                                   third_second_dict: dict,
                                   fourth_first_dict: dict,
                                   fourth_second_dict: dict,
                                   fifth_first_dict: dict,
                                   fifth_second_dict: dict,
                                   sixth_first_dict: dict,
                                   sixth_second_dict: dict,
                                   ) -> DataFrame:
    """
    :param input_df:
    :param first_first_dict:
    :param first_second_dict:
    :param second_first_dict:
    :param second_second_dict:
    :param third_first_dict:
    :param third_second_dict:
    :param fourth_first_dict:
    :param fourth_second_dict:
    :param fifth_first_dict:
    :param fifth_second_dict:
    :param sixth_first_dict:
    :param sixth_second_dict:
    :return:
    """
    if check_empty_dfs([input_df]):
        return get_spark_empty_df()

    CNTX = load_context(Path.cwd(), env=conf)

    metadata = CNTX.catalog.load("util_audit_metadata_table")
    max_date = metadata.filter(F.col("table_name") == "l4_campaign_postpaid_prepaid_features") \
        .select(F.max(F.col("target_max_data_load_date")).alias("max_date")) \
        .withColumn("max_date", F.coalesce(F.col("max_date"), F.to_date(F.lit('1970-01-01'), 'yyyy-MM-dd'))) \
        .withColumn("max_date", F.date_sub(F.col("max_date"), 65)) \
        .collect()[0].max_date

    input_df = input_df.cache()

    # first_first_df = l4_rolling_window(input_df, first_first_dict)
    # first_first_df = first_first_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_first_first", first_first_df)
    #
    # first_second_df = l4_rolling_window(input_df, first_second_dict)
    # first_second_df = first_second_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_first_second", first_second_df)
    #
    # second_first_df = l4_rolling_window(input_df, second_first_dict)
    # second_first_df = second_first_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_second_first", second_first_df)
    #
    # second_second_df = l4_rolling_window(input_df, second_second_dict)
    # second_second_df = second_second_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_second_second", second_second_df)
    #
    # third_first_df = l4_rolling_window(input_df, third_first_dict)
    # third_first_df = third_first_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_third_first", third_first_df)
    #
    # third_second_df = l4_rolling_window(input_df, third_second_dict)
    # third_second_df = third_second_df.filter(F.col("start_of_week") > max_date)
    # CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_third_second", third_second_df)

    fourth_first_df = l4_rolling_window(input_df, fourth_first_dict)
    fourth_first_df = fourth_first_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_fourth_first", fourth_first_df)

    fourth_second_df = l4_rolling_window(input_df, fourth_second_dict)
    fourth_second_df = fourth_second_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_fourth_second", fourth_second_df)

    fifth_first_df = l4_rolling_window(input_df, fifth_first_dict)
    fifth_first_df = fifth_first_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_fifth_first", fifth_first_df)

    fifth_second_df = l4_rolling_window(input_df, fifth_second_dict)
    fifth_second_df = fifth_second_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_fifth_second", fifth_second_df)

    sixth_first_df = l4_rolling_window(input_df, sixth_first_dict)
    sixth_first_df = sixth_first_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_sixth_first", sixth_first_df)

    sixth_second_df = l4_rolling_window(input_df, sixth_second_dict)
    sixth_second_df = sixth_second_df.filter(F.col("start_of_week") > max_date)
    CNTX.catalog.save("l4_campaign_postpaid_prepaid_features_sixth_second", sixth_second_df)

    first_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_first_first")
    first_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_first_second")
    second_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_second_first")
    second_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_second_second")
    third_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_third_first")
    third_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_third_second")
    fourth_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_fourth_first")
    fourth_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_fourth_second")
    fifth_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_fifth_first")
    fifth_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_fifth_second")
    sixth_first_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_sixth_first")
    sixth_second_df = CNTX.catalog.load("l4_campaign_postpaid_prepaid_features_sixth_second")

    group_cols = ["subscription_identifier", "start_of_week"]

    merged_df = union_dataframes_with_missing_cols(first_first_df, first_second_df, second_first_df, second_second_df,
                                                   third_first_df, third_second_df, fourth_first_df, fourth_second_df,
                                                   fifth_first_df, fifth_second_df, sixth_first_df, sixth_second_df)

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

    data_frame = data_frame.drop('run_date')
    data_frame = data_frame.withColumn("run_date", F.current_date())
    return data_frame


def add_column_run_date(data_frame: DataFrame) -> DataFrame:
    """

    :param data_frame:
    :return:
    """
    if len(data_frame.head(1)) == 0:
        return data_frame

    data_frame = data_frame.drop('run_date')
    data_frame = data_frame.withColumn("run_date", F.current_date())

    return data_frame
