import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.customer360.utilities.spark_util import get_spark_session

spark = get_spark_session()


def create_report_campaign_tracking_table(
    cvm_prepaid_customer_groups: DataFrame,
    dm996_cvm_ontop_pack: DataFrame,
    use_case_campaign_mapping: DataFrame,
    report_create_campaign_tracking_table_parameters: Dict[str, Any],
    day: str,
) -> DataFrame:
    """

    Args:
        cvm_prepaid_customer_groups: cvm sandbox target group
        dm996_cvm_ontop_pack: campaign response data
        use_case_campaign_mapping: campaign child code mapping table of each usecase
        report_create_campaign_tracking_table_parameters: parameters use to create campaign tracking table
        day: day string #TODO make dynamic

    Returns: DataFrame of campaign data for report making

    """

    # reduce data period to 90 days #TODO change to proper number
    tracking_day_d = datetime.date(datetime.strptime(day, "%Y-%m-%d"))
    down_scoped_date = tracking_day_d - timedelta(90)
    campaign_tracking_sdf_filter = dm996_cvm_ontop_pack.filter(
        F.col(report_create_campaign_tracking_table_parameters["date_filter_column"])
        > F.unix_timestamp(F.lit(down_scoped_date)).cast("timestamp")
    )

    # Joining campaign tracking data with sandbox group
    df_cvm_campaign_tracking = cvm_prepaid_customer_groups.join(
        campaign_tracking_sdf_filter.select(
            report_create_campaign_tracking_table_parameters[
                "campaign_table_selected_columns"
            ]
        ),
        report_create_campaign_tracking_table_parameters[
            "campaign_and_group_join_keys"
        ],
        "inner",
    ).join(
        use_case_campaign_mapping,
        report_create_campaign_tracking_table_parameters["campaign_mapping_join_keys"],
        "inner",
    )
    # Create integer response feature
    df_cvm_campaign_tracking = df_cvm_campaign_tracking.withColumn(
        "response_integer", F.when(F.col("response") == "Y", 1).otherwise(0)
    )

    return df_cvm_campaign_tracking


def create_agg_data_for_report(
    cvm_prepaid_customer_groups: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    dm43_promotion_prepaid: DataFrame,
    dm01_fin_top_up: DataFrame,
    dm15_mobile_usage_aggr_prepaid: DataFrame,
    day: str,
    aggregate_period: List[int],
):
    """

    Args:
        cvm_prepaid_customer_groups: cvm_sandbox_target_group
        dm42_promotion_prepaid: daily data on-top transaction
        dm43_promotion_prepaid: daily voice on-top transaction
        dm01_fin_top_up:  daily top-up transaction
        dm15_mobile_usage_aggr_prepaid: daily usage data, contains data/voice usage Pay per use charge sms
        day: day string #TODO make dynamic
        aggregate_period: list with all number of days to look back for the metrics

    Returns: dataFrame of aggregated features for campaign report tracking

    """

    # Create date period dataframe that will be use in cross join
    # to create main table for features aggregation
    start_day = datetime.date(datetime.strptime(day, "%Y-%m-%d")) - timedelta(
        max(aggregate_period) + 31  # To make available data for arpu uplift comparison
    )
    df_date_period = spark.sql(
        f"SELECT sequence("
        f"  to_date('{ start_day.strftime('%Y-%m-%d')}'),"
        f"  to_date('{day}'), interval 1 day"
        f") as date"
    ).withColumn("date", F.explode(F.col("date")))

    # Cross join all customer in sandbox control group with date period
    df_customer_date_period = cvm_prepaid_customer_groups.crossJoin(
        F.broadcast(df_date_period)
    )

    # Filter data-sources on recent period to minimize computation waste
    dm42_promotion_prepaid_filtered = dm42_promotion_prepaid.filter(
        dm42_promotion_prepaid.date_id >= start_day
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction").alias("ontop_data_number_of_transaction"),
        F.col("total_net_tariff").alias("ontop_data_total_net_tariff"),
        F.col("date_id").alias("date"),
    )
    dm43_promotion_prepaid_filtered = dm43_promotion_prepaid.filter(
        dm43_promotion_prepaid.date_id >= start_day
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction").alias("ontop_voice_number_of_transaction"),
        F.col("total_net_tariff").alias("ontop_voice_total_net_tariff"),
        F.col("date_id").alias("date"),
    )

    # data_charge is Pay per use data charge, voice/sms have onnet and offnet, onnet mean call within AIS network
    dm01_fin_top_up_filtered = dm01_fin_top_up.filter(
        dm01_fin_top_up.ddate >= start_day
    ).select(
        "analytic_id",
        "register_date",
        "top_up_tran",
        "top_up_value",
        F.col("ddate").alias("date"),
    )

    dm15_mobile_usage_aggr_prepaid_filtered = dm15_mobile_usage_aggr_prepaid.filter(
        dm15_mobile_usage_aggr_prepaid.ddate >= start_day
    ).select(
        "analytic_id",
        "register_date",
        (
            F.col("data_charge")
            + F.col("voice_onnet_charge_out")
            + F.col("voice_offnet_charge_out")
            + F.col("sms_onnet_charge_out")
            + F.col("sms_offnet_charge_out")
            + F.col("voice_roaming_charge_out")
            + F.col("sms_roaming_charge_out")
            + F.col("data_roaming_charge_data")
        ).alias("all_ppu_charge"),
        F.col("ddate").alias("date"),
    )

    # Join all table to consolidate all required data
    join_keys = ["analytic_id", "register_date", "date"]
    df_aggregate_table = (
        df_customer_date_period.join(dm42_promotion_prepaid_filtered, join_keys, "left")
        .join(dm43_promotion_prepaid_filtered, join_keys, "left")
        .join(dm01_fin_top_up_filtered, join_keys, "left")
        .join(dm15_mobile_usage_aggr_prepaid_filtered, join_keys, "left")
    )

    # Convert date column to timestamp for window function
    # Should be change if date format can be use
    df_aggregate_table = df_aggregate_table.withColumn(
        "timestamp", F.col("date").astype("Timestamp").cast("long"),
    )

    df_aggregate_table = df_aggregate_table.withColumn(
        "total_revenue",
        F.col("ontop_data_total_net_tariff")
        + F.col("ontop_voice_total_net_tariff")
        + F.col("all_ppu_charge"),
    )
    df_aggregate_table = df_aggregate_table.withColumn(
        "total_number_ontop_purchase",
        F.col("ontop_data_number_of_transaction")
        + F.col("ontop_voice_number_of_transaction"),
    )
    columns_to_aggregate = [
        "ontop_data_number_of_transaction",
        "ontop_data_total_net_tariff",
        "ontop_voice_number_of_transaction",
        "ontop_voice_total_net_tariff",
        "all_ppu_charge",
        "top_up_value",
        "total_revenue",
        "total_number_ontop_purchase",
    ]

    for period in aggregate_period:
        window_func = (
            Window.partitionBy("analytic_id")
            .orderBy(F.col("timestamp"))
            .rangeBetween(
                -((period + 1) * 86400), Window.currentRow
            )  # 86400 is the number of seconds in a day
        )

        df_aggregate_table = df_aggregate_table.select(
            *(
                df_aggregate_table.columns
                + [
                    F.sum(column).over(window_func).alias(f"{column}_{period}_day")
                    for column in columns_to_aggregate
                ]
            )
        )

    # Filter only the days for which we have all the info
    df_aggregate_table = df_aggregate_table.filter(F.col("date") == day)

    return df_aggregate_table


def create_use_case_view_report(
    cvm_prepaid_customer_groups: DataFrame,
    campaign_response_input_table: DataFrame,
    reporting_kpis: DataFrame,
    day: str,
    aggregate_period: List[int],
) -> DataFrame:
    """
    This function create use case view report.
        -aggregate campaign response tracking data to use case based
        -combine report input data

    Args:
        cvm_prepaid_customer_groups: cvm sandbox target group
        campaign_response_input_table: campaign response table created on focus campaigns
        reporting_kpis: ontop, topup, and revenue features
        day: report running date
        aggregate_period: list of aggregate period for campaign data aggregatation

    Returns: DataFrame of use case view report, contain all use case report currently support ARD and CHURN

    """
    # Get number of Freeze customer in control group
    current_size = cvm_prepaid_customer_groups.groupby("target_group").agg(
        F.countDistinct("crm_sub_id").alias("distinct_targeted_subscriber")
    )
    # TODO Make sure that rows of usecase and target_group combination exists report generating day same issue with cross join in previous function
    # Group data by customer to create number of distinct customer who accept campaign
    campaign_group_by = [
        "usecase",
        "contact_date",
        "target_group",
    ]

    # Create campaign features
    expr = [
        F.sum("response_integer").alias("n_campaign_accepted"),
        F.count("*").alias("n_campaign_sent"),
        F.countDistinct("analytic_id").alias("n_subscriber_targeted"),
        F.countDistinct(
            F.when(F.col("response_integer") == 1, F.col("analytic_id"))
        ).alias("n_subscriber_accepted"),
    ]
    df_campaign_aggregate_input = campaign_response_input_table.groupBy(
        campaign_group_by
    ).agg(*expr)

    # Aggregate window period campaign features
    df_campaign_aggregate_input = df_campaign_aggregate_input.withColumn(
        "timestamp", F.col("contact_date").astype("Timestamp").cast("long"),
    )
    columns_to_aggregate = [
        "n_subscriber_targeted",
        "n_campaign_accepted",
        "n_campaign_sent",
        "n_subscriber_accepted",
    ]
    for period in aggregate_period:
        window_func = (
            Window.partitionBy("target_group")
            .orderBy(F.col("timestamp"))
            .rangeBetween(
                -((period + 1) * 86400), Window.currentRow
            )  # 86400 is the number of seconds in a day
        )

        df_campaign_aggregate_input = df_campaign_aggregate_input.select(
            *(
                df_campaign_aggregate_input.columns
                + [
                    F.sum(column).over(window_func).alias(f"{column}_{period}_day")
                    for column in columns_to_aggregate
                ]
            )
        )

    # Group data into target group basis
    columns_to_sum = [c for c in reporting_kpis.columns if re.search(r"_[0-9]+_day$", c)]


    exprs = [F.sum(x).alias(x) for x in columns_to_sum]
    df_usage_features = reporting_kpis.groupBy(["target_group"]).agg(
        *exprs
    )

    # Join Number of Freeze customer with Campaign Feature
    df_use_case_view_report = current_size.join(
        df_campaign_aggregate_input.filter(F.col("contact_date") == day),
        ["target_group"],
        "left",
    ).join(df_usage_features, ["target_group"], "inner")
    # TODO add inactivity features, join previous arpu data and calculate revenue uplift
    return df_use_case_view_report
