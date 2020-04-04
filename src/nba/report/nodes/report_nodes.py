import re
from datetime import datetime
from datetime import timedelta
from typing import Dict, Any, List

import pandas as pd
import plotnine
from plotnine import *
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from customer360.utilities.spark_util import get_spark_session


def create_report_campaign_tracking_table(
    cvm_prepaid_customer_groups: DataFrame,
    l0_campaign_tracking_contact_list_pre: DataFrame,
    use_case_campaign_mapping: DataFrame,
    day: str,
) -> DataFrame:
    """
    Args:
        cvm_prepaid_customer_groups: cvm sandbox target group
        l0_campaign_tracking_contact_list_pre: C360 l0 campaign response data
        use_case_campaign_mapping: campaign child code mapping table of each usecase
        report_create_campaign_tracking_table_parameters: parameters use to create campaign tracking table
        day: day string #TODO make dynamic
    Returns: DataFrame of campaign data for report making
    """
    # reduce data period to 90 days #TODO change to proper number
    tracking_day_d = datetime.date(datetime.strptime(day, "%Y-%m-%d"))
    down_scoped_date = tracking_day_d - timedelta(90)
    campaign_tracking_sdf_filter = l0_campaign_tracking_contact_list_pre.filter(
        F.col("contact_date")
        > F.unix_timestamp(F.lit(down_scoped_date)).cast("timestamp")
    )

    campaign_tracking_sdf_filter = campaign_tracking_sdf_filter.selectExpr(
        "campaign_child_code",
        "subscription_identifier",
        "date(register_date) as register_date",
        "response",
        "date(contact_date) as contact_date",
    )
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "crm_sub_id as subscription_identifier",
        "target_group",
        "date(created_date) as control_group_created_date",
    )
    use_case_campaign_mapping = use_case_campaign_mapping.selectExpr(
        "campaign_child_code",
        "campaign_project_group",
        "target_group as defined_campaign_target_group",
        "report_campaign_group",
        "usecase",
    )
    # Joining campaign tracking data with sandbox group

    df_cvm_campaign_tracking = cvm_prepaid_customer_groups.join(
        campaign_tracking_sdf_filter,
        ["subscription_identifier", "register_date"],
        "inner",
    ).join(use_case_campaign_mapping, ["campaign_child_code"], "inner",)
    # Create integer response feature
    df_cvm_campaign_tracking = df_cvm_campaign_tracking.withColumn(
        "response_integer", F.when(F.col("response") == "Y", 1).otherwise(0)
    )

    return df_cvm_campaign_tracking


def node_reporting_kpis(
    cvm_prepaid_customer_groups: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    dm43_promotion_prepaid: DataFrame,
    dm01_fin_top_up: DataFrame,
    dm15_mobile_usage_aggr_prepaid: DataFrame,
    prepaid_no_activity_daily: DataFrame,
    date_from: datetime,
    date_to: datetime,
    arpu_days_agg_periods: List[int],
    dormant_days_agg_periods: List[int],
):
    """
    Args:
        cvm_prepaid_customer_groups: cvm_sandbox_target_group
        dm42_promotion_prepaid: daily data on-top transaction
        dm43_promotion_prepaid: daily voice on-top transaction
        dm01_fin_top_up:  daily top-up transaction
        dm15_mobile_usage_aggr_prepaid: daily usage data, contains data/voice usage Pay per use charge sms
        prepaid_no_activity_daily: table that contains inactivity data
        date_from: minimum date to generate the KPIs
        date_to: maximum date to generate the KPIs
        arpu_days_agg_periods: List of days back to aggregate ARPU-like KPIs
        dormant_days_agg_periods: List of days back to aggregate dormancy KPIs

    Returns: dataFrame of aggregated features for campaign report tracking
    """
    spark = get_spark_session()

    # Create date period dataframe that will be use in cross join
    # to create main table for features aggregation
    min_day_required_for_arpu_window = date_from - timedelta(
        days=max(arpu_days_agg_periods)
    )
    df_date_period = spark.sql(
        f"SELECT sequence("
        f"  to_date('{ min_day_required_for_arpu_window.strftime('%Y-%m-%d')}'),"
        f"  to_date('{ date_to.strftime('%Y-%m-%d')}'), interval 1 day"
        f") as join_date"
    ).withColumn("join_date", F.explode(F.col("join_date")))
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "crm_sub_id as subscription_identifier",
        "target_group",
        "created_date as control_group_created_date",
    )
    # Cross join all customer in sandbox control group with date period
    df_customer_date_period = cvm_prepaid_customer_groups.crossJoin(
        F.broadcast(df_date_period)
    )

    # Filter data-sources on recent period to minimize computation waste
    dm42_promotion_prepaid_filtered = dm42_promotion_prepaid.filter(
        F.col("date_id").between(min_day_required_for_arpu_window, date_to)
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction").alias("ontop_data_number_of_transaction"),
        F.col("total_net_tariff").alias("ontop_data_total_net_tariff"),
        F.col("date_id").alias("join_date"),
    )
    dm43_promotion_prepaid_filtered = dm43_promotion_prepaid.filter(
        F.col("date_id").between(min_day_required_for_arpu_window, date_to)
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction").alias("ontop_voice_number_of_transaction"),
        F.col("total_net_tariff").alias("ontop_voice_total_net_tariff"),
        F.col("date_id").alias("join_date"),
    )

    # data_charge is Pay per use data charge, voice/sms have onnet and offnet, onnet mean call within AIS network
    dm01_fin_top_up_filtered = dm01_fin_top_up.filter(
        F.col("ddate").between(min_day_required_for_arpu_window, date_to)
    ).select(
        "analytic_id",
        "register_date",
        "top_up_tran",
        "top_up_value",
        F.col("ddate").alias("join_date"),
    )

    dm15_mobile_usage_aggr_prepaid_filtered = dm15_mobile_usage_aggr_prepaid.filter(
        F.col("ddate").between(min_day_required_for_arpu_window, date_to)
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
        F.col("ddate").alias("join_date"),
    )

    # Create inactivity KPI features
    inactivity_kpis = (
        prepaid_no_activity_daily.withColumnRenamed("ddate", "join_date")
        # For dormancy we don't need to load extra data in the past because no
        # window function aggregation is necessary
        .filter(F.col("ddate").between(date_from, date_to)).select(
            "analytic_id",
            "register_date",
            "join_date",
            *[
                F.when(F.col("no_activity_n_days") >= min_days_dormant, 1)
                .otherwise(0)
                .alias(f"dormant_{min_days_dormant}_day")
                for min_days_dormant in dormant_days_agg_periods
            ],
        )
    )

    # Join all table to consolidate all required data
    join_keys = ["analytic_id", "register_date", "join_date"]
    df_reporting_kpis = (
        df_customer_date_period.join(dm42_promotion_prepaid_filtered, join_keys, "left")
        .join(dm43_promotion_prepaid_filtered, join_keys, "left")
        .join(dm01_fin_top_up_filtered, join_keys, "left")
        .join(dm15_mobile_usage_aggr_prepaid_filtered, join_keys, "left")
        .join(inactivity_kpis, join_keys, "left")
    )

    # Convert date column to timestamp for window function
    # Should be change if date format can be use
    df_reporting_kpis = df_reporting_kpis.withColumn(
        "timestamp", F.col("join_date").astype("Timestamp").cast("long"),
    )

    # These aren't all the columns we need to aggregate, but first we need to
    # impute these ones so that the combined ones don't have problems of
    # NA arithmetics

    columns_to_aggregate = [
        "ontop_data_number_of_transaction",
        "ontop_data_total_net_tariff",
        "ontop_voice_number_of_transaction",
        "ontop_voice_total_net_tariff",
        "all_ppu_charge",
        "top_up_value",
    ]

    # When we don't have data it means the KPI is 0 (e.g subscriber didn't consume)
    df_reporting_kpis = df_reporting_kpis.fillna(0, subset=columns_to_aggregate)

    # Now that NAs are imputed we can create these additional columns
    df_reporting_kpis = df_reporting_kpis.withColumn(
        "total_revenue",
        F.col("ontop_data_total_net_tariff")
        + F.col("ontop_voice_total_net_tariff")
        + F.col("all_ppu_charge"),
    )
    df_reporting_kpis = df_reporting_kpis.withColumn(
        "total_number_ontop_purchase",
        F.col("ontop_data_number_of_transaction")
        + F.col("ontop_voice_number_of_transaction"),
    )

    columns_to_aggregate += [
        "total_revenue",
        "total_number_ontop_purchase",
    ]

    for period in arpu_days_agg_periods:
        window_func = (
            Window.partitionBy("subscription_identifier")
            .orderBy(F.col("timestamp"))
            .rangeBetween(
                -((period + 1) * 86400), Window.currentRow
            )  # 86400 is the number of seconds in a day
        )

        df_reporting_kpis = df_reporting_kpis.select(
            *(
                df_reporting_kpis.columns
                + [
                    F.sum(column).over(window_func).alias(f"{column}_{period}_day")
                    for column in columns_to_aggregate
                ]
            )
        )

    # This will eliminate the extra auxiliary dates we loaded for window aggregation,
    # Just leaving the data that is complete
    df_reporting_kpis = df_reporting_kpis.filter(
        F.col("join_date").between(date_from, date_to)
    )

    return df_reporting_kpis


def node_daily_kpis_by_group_report(reporting_kpis: DataFrame) -> DataFrame:
    """
    Creates a report table with the daily average value of the reporting KPIs
    for each group
    Args:
        reporting_kpis: reportings_kpis as returned by node_reporting_kpis

    Returns:
        A spark DataFrame with the KPIs for each day
    """
    df_daily_kpis_by_group_report = reporting_kpis.groupby(
        ["target_group", "join_date"]
    ).agg(
        *[
            F.mean(c).alias(f"mean_{c}")
            for c in reporting_kpis.columns
            if c.endswith("_day")
        ]
    )
    return df_daily_kpis_by_group_report


def node_plot_daily_kpis_by_group_report(
    daily_kpis_by_group_report: DataFrame,
) -> Dict[str, plotnine.ggplot]:
    """
    Plots the daily kpis report results, creates a different plot for each KPI
    Args:
        daily_kpis_by_group_report: daily KPIs report table as returned by
            node_daily_kpis_by_group_report

    Returns:
        A dictionary where each key is the name of the KPI and the value the plot
    """

    pdf_daily_kpis_by_group_report = daily_kpis_by_group_report.toPandas()
    pdf_daily_kpis_by_group_report["join_date"] = pd.to_datetime(
        pdf_daily_kpis_by_group_report["join_date"]
    )
    plots_dict = {}
    cols_to_plot = [
        c for c in pdf_daily_kpis_by_group_report.columns if c.endswith("_day")
    ]
    for col_to_plot in cols_to_plot:
        plots_dict[col_to_plot] = (
            ggplot(
                pdf_daily_kpis_by_group_report,
                aes(x="join_date", y=col_to_plot, color="target_group"),
            )
            + geom_line()
            + theme(axis_text_x=element_text(rotation=60, hjust=1))
        )
    return plots_dict


def create_use_case_view_report(
    use_case_campaign_mapping: DataFrame,
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
        use_case_campaign_mapping:
        cvm_prepaid_customer_groups: cvm sandbox target group
        campaign_response_input_table: campaign response table created on focus campaigns
        reporting_kpis: ontop, topup, and revenue features
        prepaid_no_activity_daily: Inactivity data
        day: report running date
        aggregate_period: list of aggregate period for campaign data aggregatation
    Returns: DataFrame of use case view report, contain all use case report currently support ARD and CHURN
    """

    spark = get_spark_session()
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "crm_sub_id as subscription_identifier",
        "target_group",
        "created_date as control_group_created_date",
    )
    # Get number of Freeze customer in control group
    current_size = cvm_prepaid_customer_groups.groupby("target_group").agg(
        F.countDistinct("subscription_identifier").alias("distinct_targeted_subscriber")
    )

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
        F.countDistinct("subscription_identifier").alias("n_subscriber_targeted"),
        F.countDistinct(
            F.when(F.col("response_integer") == 1, F.col("subscription_identifier"))
        ).alias("n_subscriber_accepted"),
    ]

    # Make sure that rows of usecase and target_group combination exists report generating day
    start_day = datetime.date(datetime.strptime(day, "%Y-%m-%d")) - timedelta(90)
    df_date_period = spark.sql(
        f"SELECT sequence("
        f"  to_date('{ start_day.strftime('%Y-%m-%d')}'),"
        f"  to_date('{day}'), interval 1 day"
        f") as contact_date"
    ).withColumn("contact_date", F.explode(F.col("contact_date")))
    df_usecases = (
        use_case_campaign_mapping.groupBy(["usecase"])
        .agg(F.count("*").alias("Total_campaigns"))
        .select("usecase")
    )
    df_groups = (
        cvm_prepaid_customer_groups.groupBy(["target_group"])
        .agg(F.count("*").alias("Total_campaigns"))
        .select("target_group")
    )
    df_usecases_period = df_date_period.crossJoin(df_usecases).crossJoin(df_groups)
    df_campaign_aggregate_input = campaign_response_input_table.groupBy(
        campaign_group_by
    ).agg(*expr)
    df_campaign_aggregate_input = df_usecases_period.join(
        df_campaign_aggregate_input, ["usecase", "target_group", "contact_date"], "left"
    )

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
    # Filter only the days for which we calculate report
    reporting_kpis_present = reporting_kpis.filter(F.col("join_date") == day)

    # Group data into target group basis
    columns_to_sum = [
        c for c in reporting_kpis_present.columns if re.search(r"_[0-9]+_day$", c)
    ]

    exprs = [F.sum(x).alias(x) for x in columns_to_sum]
    df_usage_features = reporting_kpis_present.groupBy(["target_group"]).agg(*exprs)

    # Join Number of Freeze customer with Campaign Feature
    df_use_case_view_report = current_size.join(
        df_campaign_aggregate_input.filter(F.col("contact_date") == day),
        ["target_group"],
        "left",
    ).join(df_usage_features, ["target_group"], "inner")

    # Join with ARPU Last month for uplift calculation
    last_month_day = (
        datetime.date(datetime.strptime(day, "%Y-%m-%d")) - timedelta(31)
    ).strftime("%Y-%m-%d")
    reporting_kpis_last_month = reporting_kpis.filter(
        F.col("join_date") == last_month_day
    )
    df_arpu_last_month = reporting_kpis_last_month.select(
        "subscription_identifier",
        "register_date",
        "target_group",
        "total_revenue_1_day",
        "total_revenue_7_day",
        "total_revenue_30_day",
    )
    columns_to_sum = [
        c for c in df_arpu_last_month.columns if re.search(r"_[0-9]+_day$", c)
    ]
    exprs = [F.sum(x).alias(x + "_last_month") for x in columns_to_sum]
    df_arpu_last_month_features = df_arpu_last_month.groupBy(["target_group"]).agg(
        *exprs
    )

    # Calculate ARPU uplift
    df_use_case_view_report = (
        df_use_case_view_report.join(
            df_arpu_last_month_features, ["target_group"], "left"
        )
        .withColumn(
            "arpu_uplift_1_day_vs_last_month",
            (F.col("total_revenue_1_day") - F.col("total_revenue_1_day_last_month"))
            / F.col("total_revenue_1_day_last_month"),
        )
        .withColumn(
            "arpu_uplift_7_day_vs_last_month",
            (F.col("total_revenue_7_day") - F.col("total_revenue_7_day_last_month"))
            / F.col("total_revenue_7_day_last_month"),
        )
        .withColumn(
            "arpu_uplift_30_day_vs_last_month",
            (F.col("total_revenue_30_day") - F.col("total_revenue_30_day_last_month"))
            / F.col("total_revenue_30_day_last_month"),
        )
    )

    # TODO discuss on how churn feature should be added into reporting
    return df_use_case_view_report


def create_use_case_campaign_mapping_table(
    campaign_churn_cvm_master: DataFrame,
    campaign_churn_bau_master: DataFrame,
    campaign_ard_cvm_master: DataFrame,
) -> DataFrame:
    campaign_churn_cvm_master = campaign_churn_cvm_master.selectExpr(
        "child_code as campaign_child_code",
        "campaign_group as campaign_project_group",
        "'bau' as target_group",
        "'cvm' as report_campaign_group",
        "'CHURN' as usecase",
    )
    campaign_churn_bau_master = campaign_churn_bau_master.selectExpr(
        "child_code as campaign_child_code",
        "campaign_group as campaign_project_group",
        "'bau' as target_group",
        "'bau' as report_campaign_group",
        "'CHURN' as usecase",
    )
    campaign_ard_cvm_master = campaign_ard_cvm_master.selectExpr(
        "child_code as campaign_child_code",
        "'Anti_revenue_dilution' as campaign_project_group",
        "'bau' as target_group",
        "'cvm' as report_campaign_group",
        "'ARD' as usecase",
    )

    campaign_mapping_master = campaign_churn_cvm_master.union(
        campaign_churn_bau_master
    ).union(campaign_ard_cvm_master)
    return campaign_mapping_master
