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


def drop_data_by_date(date_from, date_to, table, key_col):
    spark = get_spark_session()
    spark.sql(
        "DELETE FROM "
        + table
        + " WHERE date('"
        + key_col
        + "') >= date('"
        + date_from.strftime("%Y-%m-%d")
        + "') AND date('"
        + key_col
        + "') <= date('"
        + date_to.strftime("%Y-%m-%d")
        + "')"
    )
    spark.sql("REFRESH TABLE " + table)


def create_report_campaign_tracking_table(
    cvm_prepaid_customer_groups: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    use_case_campaign_mapping: DataFrame,
    date_from: datetime,
    date_to: datetime,
    drop_update_table=False,
) -> DataFrame:
    """
    Args:
        cvm_prepaid_customer_groups: cvm sandbox target group
        l0_campaign_tracking_contact_list_pre_full_load: C360 l0 campaign response data
        use_case_campaign_mapping: campaign child code mapping table of each usecase
        report_create_campaign_tracking_table_parameters: parameters use to create campaign tracking table
        day: day string
    Returns: DataFrame of campaign data for report making
    """
    spark = get_spark_session()
    # Drop data for update
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.campaign_response_input_table",
            key_col="contact_date",
        )
    # reduce data period to 90 days
    campaign_tracking_sdf_filter = l0_campaign_tracking_contact_list_pre_full_load.filter(
        F.col("contact_date").between(date_from, date_to)
    )
    campaign_tracking_sdf_filter = campaign_tracking_sdf_filter.selectExpr(
        "campaign_child_code",
        "subscription_identifier",
        "date(register_date) as register_date",
        "response",
        "date(contact_date) as contact_date",
        "date(response_date) as response_date",
        "date(update_date) as update_date",
    )

    # Select latest L0 transaction from campaign data
    latest_transaction = campaign_tracking_sdf_filter.groupby(
        "subscription_identifier",
        "register_date",
        "campaign_child_code",
        "contact_date",
    ).agg(F.max("update_date").alias("update_date"))
    campaign_tracking_sdf_filter = campaign_tracking_sdf_filter.join(
        latest_transaction,
        [
            "subscription_identifier",
            "register_date",
            "campaign_child_code",
            "contact_date",
            "update_date",
        ],
        "inner",
    ).drop("update_date")
    # TODO Modify report process to be able to recognize difference version of sampling
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "crm_sub_id as subscription_identifier",
        """CASE WHEN target_group = 'BAU_2020_CVM_V2' THEN 'BAU' 
        WHEN target_group = 'TG_2020_CVM_V2' THEN 'TG'
        WHEN target_group = 'CG_2020_CVM_V2' THEN 'CG'
        ELSE 'old' END AS target_group""",
        "date(created_date) as control_group_created_date",
    ).where("target_group != 'old'")
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
    if not drop_update_table:
        df_cvm_campaign_tracking.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.campaign_response_input_table")
        spark.sql(
            """CREATE TABLE nba_dev.campaign_response_input_table 
            USING DELTA 
            PARTITIONED BY (contact_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        df_cvm_campaign_tracking.write.format("delta").mode("append").partitionBy(
            "contact_date"
        ).saveAsTable("nba_dev.campaign_response_input_table")
    return df_cvm_campaign_tracking


def create_input_data_for_reporting_kpis(
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    l0_churn_status_daily: DataFrame,
    cvm_prepaid_customer_groups: DataFrame,
    dm42_promotion_prepaid: DataFrame,
    dm43_promotion_prepaid: DataFrame,
    dm01_fin_top_up: DataFrame,
    dm15_mobile_usage_aggr_prepaid: DataFrame,
    dm07_sub_clnt_info: DataFrame,
    prepaid_no_activity_daily: DataFrame,
    date_from: datetime,
    date_to: datetime,
    drop_update_table=False,
) -> DataFrame:
    """

    Args:
        cvm_prepaid_customer_groups: cvm_sandbox_target_group
        dm42_promotion_prepaid: daily data on-top transaction
        dm43_promotion_prepaid: daily voice on-top transaction
        dm01_fin_top_up:  daily top-up transaction
        dm15_mobile_usage_aggr_prepaid: daily usage data, contains data/voice usage Pay per use charge sms
        dm07_sub_clnt_info: demo
        prepaid_no_activity_daily: table that contains inactivity data
        date_from:
        date_to:

    Returns:

    """
    spark = get_spark_session()
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.reporting_kpis_input",
            key_col="join_date",
        )
    # Create date period dataframe that will be use in cross join
    # to create main table for features aggregation
    df_date_period = spark.sql(
        f"SELECT sequence("
        f"  to_date('{ date_from.strftime('%Y-%m-%d')}'),"
        f"  to_date('{ date_to.strftime('%Y-%m-%d')}'), interval 1 day"
        f") as join_date"
    ).withColumn("join_date", F.explode(F.col("join_date")))
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "date(created_date) as event_partition_date",
        "crm_sub_id as subscription_identifier",
        """CASE WHEN target_group = 'BAU_2020_CVM_V2' THEN 'BAU' 
        WHEN target_group = 'TG_2020_CVM_V2' THEN 'TG'
        WHEN target_group = 'CG_2020_CVM_V2' THEN 'CG'
        ELSE 'old' END AS target_group""",
        "date(created_date) as control_group_created_date",
    ).where("target_group != 'old'")

    #
    l1_customer_profile_union_daily_feature_full_load = l1_customer_profile_union_daily_feature_full_load.selectExpr(
        "old_subscription_identifier as subscription_identifier",
        "access_method_num",
        "date(register_date) as register_date",
        "date(event_partition_date) as event_partition_date",
        "subscription_status",
    )
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.join(
        l1_customer_profile_union_daily_feature_full_load.drop("subscription_status"),
        ["subscription_identifier", "register_date", "event_partition_date",],
        "inner",
    ).drop("event_partition_date")
    # Only use the latest profile data
    max_ddate = (
        dm07_sub_clnt_info.agg({"ddate": "max"}).collect()[0][0].strftime("%Y-%m-%d")
    )
    dm07_sub_clnt_info = dm07_sub_clnt_info.where(
        "ddate = date('" + max_ddate + "') AND charge_type = 'Pre-paid'"
    ).selectExpr(
        "analytic_id",
        "date(activation_date) as register_date",
        "crm_sub_id as subscription_identifier",
    )

    cvm_prepaid_customer_groups = dm07_sub_clnt_info.join(
        cvm_prepaid_customer_groups,
        ["analytic_id", "register_date", "subscription_identifier"],
        "left",
    )

    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "register_date",
        "access_method_num",
        "subscription_identifier",
        "COALESCE(target_group, 'default') as target_group",
        "control_group_created_date",
    )

    # Cross join all customer in sandbox control group with date period
    df_customer_date_period = cvm_prepaid_customer_groups.crossJoin(
        F.broadcast(df_date_period)
    )

    # Filter daily churn data
    customer_churn_daily = l0_churn_status_daily.selectExpr(
        "access_method_num",
        "date(register_date) as register_date",
        "churn_type",
        "date(day_code) as join_date",
    )

    l1_customer_profile_union_daily_feature_full_load_cap = l1_customer_profile_union_daily_feature_full_load.filter(
        F.col("event_partition_date").between(date_from, date_to)
    )

    customer_profile_daily = l1_customer_profile_union_daily_feature_full_load_cap.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date(event_partition_date) as join_date",
        "subscription_status",
    )

    # Filter data-sources on recent period to minimize computation waste
    dm42_promotion_prepaid_filtered = dm42_promotion_prepaid.filter(
        F.col("date_id").between(date_from, date_to)
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction"),
        F.col("total_net_tariff"),
        F.col("date_id").alias("join_date"),
    )
    dm42_promotion_prepaid_filtered = dm42_promotion_prepaid_filtered.groupBy(
        ["analytic_id", "register_date", "join_date"]
    ).agg(
        F.sum("number_of_transaction").alias("ontop_data_number_of_transaction"),
        F.sum("total_net_tariff").alias("ontop_data_total_net_tariff"),
    )

    dm43_promotion_prepaid_filtered = dm43_promotion_prepaid.filter(
        F.col("date_id").between(date_from, date_to)
    ).select(
        "analytic_id",
        "register_date",
        F.col("number_of_transaction"),
        F.col("total_net_tariff"),
        F.col("date_id").alias("join_date"),
    )
    dm43_promotion_prepaid_filtered = dm43_promotion_prepaid_filtered.groupBy(
        ["analytic_id", "register_date", "join_date"]
    ).agg(
        F.sum("number_of_transaction").alias("ontop_voice_number_of_transaction"),
        F.sum("total_net_tariff").alias("ontop_voice_total_net_tariff"),
    )

    # data_charge is Pay per use data charge, voice/sms have onnet and offnet, onnet mean call within AIS network
    dm01_fin_top_up_filtered = dm01_fin_top_up.filter(
        F.col("ddate").between(date_from, date_to)
    ).selectExpr(
        "analytic_id",
        "register_date",
        "top_up_tran",
        "top_up_tran * top_up_value as top_up_value",
        "ddate as join_date",
    )

    dm01_fin_top_up_filtered = dm01_fin_top_up_filtered.groupBy(
        ["analytic_id", "register_date", "join_date"]
    ).agg(
        F.sum("top_up_tran").alias("top_up_tran"),
        F.sum("top_up_value").alias("top_up_value"),
    )

    dm15_mobile_usage_aggr_prepaid_filtered = dm15_mobile_usage_aggr_prepaid.filter(
        F.col("ddate").between(date_from, date_to)
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

    # Join all table to consolidate all required data
    prepaid_no_activity_daily = prepaid_no_activity_daily.withColumnRenamed(
        "ddate", "join_date"
    ).select("analytic_id", "register_date", "no_activity_n_days", "join_date")
    join_keys = ["analytic_id", "register_date", "join_date"]
    sdf_reporting_kpis_input = (
        df_customer_date_period.join(dm42_promotion_prepaid_filtered, join_keys, "left")
        .join(dm43_promotion_prepaid_filtered, join_keys, "left")
        .join(dm01_fin_top_up_filtered, join_keys, "left")
        .join(dm15_mobile_usage_aggr_prepaid_filtered, join_keys, "left")
        .join(prepaid_no_activity_daily, join_keys, "left")
    )
    sdf_reporting_kpis_input = sdf_reporting_kpis_input.join(
        customer_churn_daily,
        ["access_method_num", "register_date", "join_date"],
        "left",
    )
    sdf_reporting_kpis_input = sdf_reporting_kpis_input.join(
        customer_profile_daily,
        ["subscription_identifier", "register_date", "join_date"],
        "left",
    )
    sdf_reporting_kpis_input = sdf_reporting_kpis_input.dropDuplicates(join_keys)
    if not drop_update_table:
        sdf_reporting_kpis_input.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.reporting_kpis_input")
        spark.sql(
            """CREATE TABLE nba_dev.reporting_kpis_input 
            USING DELTA 
            PARTITIONED BY (join_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        sdf_reporting_kpis_input.write.format("delta").mode("append").partitionBy(
            "join_date"
        ).saveAsTable("nba_dev.reporting_kpis_input")
    return sdf_reporting_kpis_input


def node_reporting_kpis(
    reporting_kpis_input: DataFrame,
    unused_memory_reporting_kpis_input: DataFrame,
    date_from: datetime,
    date_to: datetime,
    arpu_days_agg_periods: List[int],
    drop_update_table=False,
):
    """
    Args:
        date_from: minimum date to generate the KPIs
        date_to: maximum date to generate the KPIs
        arpu_days_agg_periods: List of days back to aggregate ARPU-like KPIs
        dormant_days_agg_periods: List of days back to aggregate dormancy KPIs

    Returns: dataFrame of aggregated features for campaign report tracking
    """
    spark = get_spark_session()
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.reporting_kpis",
            key_col="join_date",
        )
    # Convert date column to timestamp for window function
    # Should be change if date format can be use
    covered_history_date_from = date_from - timedelta(45)
    df_reporting_kpis = reporting_kpis_input.filter(
        F.col("join_date").between(covered_history_date_from, date_to)
    )
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
    for col in columns_to_aggregate:
        df_reporting_kpis = df_reporting_kpis.withColumnRenamed(col, col + "_1_day")
    # This will eliminate the extra auxiliary dates we loaded for window aggregation,
    # Just leaving the data that is complete
    df_reporting_kpis = df_reporting_kpis.filter(
        F.col("join_date").between(date_from, date_to)
    )
    if not drop_update_table:
        df_reporting_kpis.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.reporting_kpis")
        spark.sql(
            """CREATE TABLE nba_dev.reporting_kpis 
            USING DELTA 
            PARTITIONED BY (join_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        df_reporting_kpis.write.format("delta").mode("append").partitionBy(
            "join_date"
        ).saveAsTable("nba_dev.reporting_kpis")
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
    unused_memory_campaign_response_input_table: DataFrame,
    unused_memory_reporting_kpis_input: DataFrame,
    unused_memory_reporting_kpis: DataFrame,
    reporting_kpis: DataFrame,
    reporting_kpis_input: DataFrame,
    day_list: List[str],
    aggregate_period: List[int],
    dormant_days_agg_periods: List[int],
    date_from,
    date_to,
    drop_update_table=False,
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

    # use_case_campaign_mapping = catalog.load("use_case_campaign_mapping")
    # cvm_prepaid_customer_groups = catalog.load("cvm_prepaid_customer_groups")
    # campaign_response_input_table = catalog.load("campaign_response_input_table")
    # reporting_kpis = catalog.load("reporting_kpis")
    # reporting_kpis_input = catalog.load("reporting_kpis_input")
    # day_list = ["2020-02-01", "2020-02-02"]
    # aggregate_period = [7, 30]
    # dormant_days_agg_periods = [5, 7, 14, 30, 60, 90]
    # day = "2020-07-13"
    spark = get_spark_session()
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.use_case_view_report_table",
            key_col="contact_date",
        )
    cvm_prepaid_customer_groups = cvm_prepaid_customer_groups.selectExpr(
        "analytic_id",
        "date(register_date) as register_date",
        "crm_sub_id as subscription_identifier",
        """CASE WHEN target_group = 'BAU_2020_CVM_V2' THEN 'BAU' 
        WHEN target_group = 'TG_2020_CVM_V2' THEN 'TG'
        WHEN target_group = 'CG_2020_CVM_V2' THEN 'CG'
        ELSE 'old' END AS target_group""",
        "date(created_date) as control_group_created_date",
    ).where("target_group != 'old'")
    # Get number of Freeze customer in control group
    current_size = cvm_prepaid_customer_groups.groupby("target_group").agg(
        F.countDistinct("subscription_identifier").alias("distinct_targeted_subscriber")
    )
    iterate_count = 0
    for day in day_list:
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

        # Group data by customer to create number of distinct customer who accept campaign
        campaign_group_by = [
            "usecase",
            "contact_date",
            "target_group",
        ]
        # Create Churn Reactive report KPI
        inactive_tm1 = reporting_kpis_input.where(
            "subscription_identifier is not null"
        ).selectExpr(
            "subscription_identifier",
            "register_date",
            "date_add(join_date,1) as contact_date",
            "no_activity_n_days as no_activity_n_days_tm1",
        )
        inactive_t0 = reporting_kpis_input.where(
            "subscription_identifier is not null"
        ).selectExpr(
            "subscription_identifier",
            "register_date",
            "join_date as contact_date",
            "no_activity_n_days as no_activity_n_days_t0",
        )
        inactive_t4 = reporting_kpis_input.where(
            "subscription_identifier is not null"
        ).selectExpr(
            "subscription_identifier",
            "register_date",
            "date_add(join_date,-3) as contact_date",
            "no_activity_n_days as no_activity_n_days_t4",
        )
        reactive_input = inactive_tm1.join(
            inactive_t0,
            ["subscription_identifier", "register_date", "contact_date"],
            "left",
        ).join(
            inactive_t4,
            ["subscription_identifier", "register_date", "contact_date"],
            "left",
        )
        reactive_kpi = reactive_input.selectExpr(
            "subscription_identifier",
            "register_date",
            "contact_date",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 7 AND no_activity_n_days_t0 == 0 
                    THEN 1 ELSE 0 END AS reactive_1_7_contact_p0""",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 15 AND no_activity_n_days_t0 == 0 
                    THEN 1 ELSE 0 END AS reactive_1_15_contact_p0""",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 30 AND no_activity_n_days_t0 == 0 
                    THEN 1 ELSE 0 END AS reactive_1_30_contact_p0""",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 7 AND 
                    no_activity_n_days_tm1 > no_activity_n_days_t4
                    THEN 1 ELSE 0 END AS reactive_1_7_contact_p4""",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 15 AND 
                    no_activity_n_days_tm1 > no_activity_n_days_t4
                    THEN 1 ELSE 0 END AS reactive_1_15_contact_p4""",
            """CASE WHEN no_activity_n_days_tm1 >= 1 AND no_activity_n_days_tm1 <= 30 AND 
                    no_activity_n_days_tm1 > no_activity_n_days_t4
                    THEN 1 ELSE 0 END AS reactive_1_30_contact_p4""",
        )
        # create Churn report KPI
        churn_kpi = reporting_kpis_input.selectExpr(
            "subscription_identifier",
            "register_date",
            "date(join_date) as contact_date",
            "CASE WHEN churn_type is not NULL THEN 1 ELSE 0 END AS churn_sub_yn_integer",
            "CASE WHEN subscription_status is NULL THEN 1 ELSE 0 END AS churned_integer",
        )

        campaign_response_input_df = campaign_response_input_table.join(
            churn_kpi,
            ["subscription_identifier", "register_date", "contact_date"],
            "left",
        )
        campaign_response_input_df = campaign_response_input_df.join(
            reactive_kpi,
            ["subscription_identifier", "register_date", "contact_date"],
            "left",
        )
        # Create campaign based features
        expr = [
            F.sum("response_integer").alias("n_campaign_accepted"),
            F.count("*").alias("n_campaign_sent"),
            F.countDistinct("subscription_identifier").alias("n_subscriber_targeted"),
            F.countDistinct(
                F.when(F.col("response_integer") == 1, F.col("subscription_identifier"))
            ).alias("n_subscriber_accepted"),
            F.countDistinct(
                F.when(
                    F.col("churn_sub_yn_integer") == 1,
                    F.col("subscription_identifier"),
                )
            ).alias("n_subscriber_targeted_churned"),
            F.countDistinct(
                F.when(
                    F.col("reactive_1_7_contact_p0") == 1,
                    F.col("subscription_identifier"),
                )
            ).alias("n_subscriber_reactive_1_7_contact_p0"),
            F.countDistinct(
                F.when(
                    F.col("reactive_1_15_contact_p0") == 1,
                    F.col("subscription_identifier"),
                )
            ).alias("n_subscriber_reactive_1_15_contact_p0"),
            F.countDistinct(
                F.when(
                    F.col("reactive_1_30_contact_p0") == 1,
                    F.col("subscription_identifier"),
                )
            ).alias("n_subscriber_reactive_1_30_contact_p0"),
            # F.countDistinct(
            #     F.when(
            #         F.col("reactive_1_7_contact_p4") == 1,
            #         F.col("subscription_identifier"),
            #     )
            # ).alias("n_subscriber_reactive_1_7_contact_p4"),
            # F.countDistinct(
            #     F.when(
            #         F.col("reactive_1_15_contact_p4") == 1,
            #         F.col("subscription_identifier"),
            #     )
            # ).alias("n_subscriber_reactive_1_15_contact_p4"),
            # F.countDistinct(
            #     F.when(
            #         F.col("reactive_1_30_contact_p4") == 1,
            #         F.col("subscription_identifier"),
            #     )
            # ).alias("n_subscriber_reactive_1_30_contact_p4"),
        ]
        df_campaign_aggregate_input = campaign_response_input_df.groupBy(
            campaign_group_by
        ).agg(*expr)
        df_campaign_aggregate_input = df_usecases_period.join(
            df_campaign_aggregate_input,
            ["usecase", "target_group", "contact_date"],
            "left",
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
            "n_subscriber_targeted_churned",
            "n_subscriber_reactive_1_7_contact_p0",
            "n_subscriber_reactive_1_15_contact_p0",
            "n_subscriber_reactive_1_30_contact_p0",
        ]
        for period in aggregate_period:
            window_func = (
                Window.partitionBy(["target_group", "usecase"])
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
        for col in columns_to_aggregate:
            df_campaign_aggregate_input = df_campaign_aggregate_input.withColumnRenamed(
                col, col + "_1_day"
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

        # Join with ARPU Last week for uplift calculation
        last_week_day = (
            datetime.date(datetime.strptime(day, "%Y-%m-%d")) - timedelta(8)
        ).strftime("%Y-%m-%d")

        df_campaign_aggregate_input_last_week = df_campaign_aggregate_input.filter(
            F.col("contact_date") == last_week_day
        )
        columns_to_rename = [
            c
            for c in df_campaign_aggregate_input_last_week.columns
            if re.search(r"_[0-9]+_day$", c)
        ]
        for col_name in columns_to_rename:
            df_campaign_aggregate_input_last_week = df_campaign_aggregate_input_last_week.withColumnRenamed(
                col_name, col_name + "_Last_week"
            )
        df_use_case_view_report = df_use_case_view_report.join(
            df_campaign_aggregate_input_last_week.drop("timestamp").drop(
                "contact_date"
            ),
            ["usecase", "target_group"],
            "left",
        )

        reporting_kpis_last_week = reporting_kpis.filter(
            F.col("join_date") == last_week_day
        )
        df_arpu_last_week = reporting_kpis_last_week.select(
            "subscription_identifier",
            "register_date",
            "target_group",
            "total_revenue_1_day",
            "total_revenue_7_day",
            "total_revenue_30_day",
        )

        columns_to_sum = [
            c for c in df_arpu_last_week.columns if re.search(r"_[0-9]+_day$", c)
        ]
        exprs = [F.sum(x).alias(x + "_Last_week") for x in columns_to_sum]
        df_arpu_last_week_features = df_arpu_last_week.groupBy(["target_group"]).agg(
            *exprs
        )

        df_use_case_view_report = df_use_case_view_report.join(
            df_arpu_last_week_features, ["target_group"], "left"
        )

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
        date_from = (
            datetime.date(datetime.strptime(day, "%Y-%m-%d")) - timedelta(90)
        ).strftime("%Y-%m-%d")

        # Create churn KPI features (non-campaign depended)
        churn_kpis_all = (
            # For dormancy we don't need to load extra data in the past because no
            # window function aggregation is necessary
            reporting_kpis_input.filter(
                F.col("join_date").between(
                    date_from, datetime.date(datetime.strptime(day, "%Y-%m-%d"))
                )
            ).select(
                "analytic_id",
                "register_date",
                "join_date",
                *[
                    F.when(
                        (F.col("churn_type") == F.lit("CT"))
                        | (F.col("churn_type") == F.lit("Terminate")),
                        1,
                    )
                    .otherwise(0)
                    .alias(f"total_churn_{min_days_churn}_day")
                    for min_days_churn in aggregate_period
                ],
            )
        )
        churn_kpis_all = cvm_prepaid_customer_groups.select(
            "analytic_id", "register_date", "target_group"
        ).join(churn_kpis_all, ["analytic_id", "register_date"], "left")
        columns_to_sum = [
            c for c in churn_kpis_all.columns if re.search(r"_[0-9]+_day$", c)
        ]
        exprs = [F.sum(x).alias(x) for x in columns_to_sum]
        churn_features = churn_kpis_all.groupBy(["target_group", "join_date"]).agg(
            *exprs
        )
        churn_features_today = churn_features.filter(F.col("join_date") == day)
        churn_features_today = churn_features_today.selectExpr(
            "target_group",
            "total_churn_7_day",
            "total_churn_15_day",
            "total_churn_30_day",
        )

        churn_features_lastweek = churn_features.filter(
            F.col("join_date") == last_week_day
        )

        churn_features_lastweek = churn_features_lastweek.selectExpr(
            "target_group",
            "total_churn_7_day as total_churn_7_day_lastweek",
            "total_churn_15_day as total_churn_15_day_lastweek",
            "total_churn_30_day as total_churn_30_day_lastweek",
        )

        # churn_features_today.join(
        #     churn_features_lastweek, ["target_group"], "left"
        # ).createOrReplaceTempView("temp_view_load")
        # spark.sql("DROP TABLE IF EXISTS tmp_churn_features")
        # spark.sql("CREATE TABLE tmp_churn_features AS SELECT * FROM temp_view_load")
        # tmp_churn_features = spark.sql("SELECT * FROM tmp_churn_features")
        # df_use_case_view_report = df_use_case_view_report.join(
        #     tmp_churn_features, ["target_group"], "left"
        # )

        df_use_case_view_report = df_use_case_view_report.join(
            churn_features_today, ["target_group"], "left"
        )

        df_use_case_view_report = df_use_case_view_report.join(
            churn_features_lastweek, ["target_group"], "left"
        )

        # Create inactivity KPI features
        inactivity_kpis = (
            # For dormancy we don't need to load extra data in the past because no
            # window function aggregation is necessary
            reporting_kpis_input.filter(
                F.col("join_date").between(
                    date_from, datetime.date(datetime.strptime(day, "%Y-%m-%d"))
                )
            ).select(
                "analytic_id",
                "register_date",
                "join_date",
                *[
                    F.when(F.col("no_activity_n_days") >= min_days_dormant, 1)
                    .otherwise(0)
                    .alias(f"total_dormant_{min_days_dormant}_day")
                    for min_days_dormant in dormant_days_agg_periods
                ],
            )
        )
        active_sub = reporting_kpis_input.filter(
            F.col("join_date").between(
                date_from, datetime.date(datetime.strptime(day, "%Y-%m-%d"))
            )
        ).select(
            "analytic_id",
            "register_date",
            "join_date",
            F.when(F.col("no_activity_n_days") == 0, 1)
            .otherwise(0)
            .alias(f"active_prepaid_sub"),
        )
        inactivity_kpis = (
            cvm_prepaid_customer_groups.select(
                "analytic_id", "register_date", "target_group"
            )
            .join(inactivity_kpis, ["analytic_id", "register_date"], "left")
            .join(active_sub, ["analytic_id", "register_date", "join_date"], "left")
        )

        columns_to_sum = [
            c for c in inactivity_kpis.columns if re.search(r"_[0-9]+_day$", c)
        ]
        columns_to_sum += ["active_prepaid_sub"]
        exprs = [F.sum(x).alias(x) for x in columns_to_sum]
        inactivity_features = inactivity_kpis.groupBy(
            ["target_group", "join_date"]
        ).agg(*exprs)
        inactivity_features_today = inactivity_features.filter(
            F.col("join_date") == day
        )

        inactivity_features_today = inactivity_features_today.selectExpr(
            "target_group",
            "total_dormant_5_day",
            "total_dormant_7_day",
            "total_dormant_14_day",
            "total_dormant_30_day",
            "total_dormant_60_day",
            "total_dormant_90_day",
            "active_prepaid_sub as total_active_prepaid_sub_today",
        )

        inactivity_features_lastweek = inactivity_features.filter(
            F.col("join_date") == last_week_day
        )

        inactivity_features_lastweek = inactivity_features_lastweek.selectExpr(
            "target_group",
            "total_dormant_5_day as total_dormant_5_day_lastweek",
            "total_dormant_7_day as total_dormant_7_day_lastweek",
            "total_dormant_14_day as total_dormant_14_day_lastweek",
            "total_dormant_30_day as total_dormant_30_day_lastweek",
            "total_dormant_60_day as total_dormant_60_day_lastweek",
            "total_dormant_90_day as total_dormant_90_day_lastweek",
            "active_prepaid_sub as total_active_prepaid_sub_lastweek",
        )

        # inactivity_features_today.join(
        #     inactivity_features_lastweek, ["target_group"], "left"
        # ).createOrReplaceTempView("temp_view_load")
        # spark.sql("DROP TABLE IF EXISTS tmp_inactivity_features")
        # spark.sql(
        #     "CREATE TABLE tmp_inactivity_features AS SELECT * FROM temp_view_load"
        # )
        # tmp_inactivity_features = spark.sql("SELECT * FROM tmp_inactivity_features")
        # df_use_case_view_report = df_use_case_view_report.join(
        #     tmp_inactivity_features, ["target_group"], "left"
        # )

        df_use_case_view_report = df_use_case_view_report.join(
            inactivity_features_today, ["target_group"], "left"
        )

        df_use_case_view_report = df_use_case_view_report.join(
            inactivity_features_lastweek, ["target_group"], "left"
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
                (
                    F.col("total_revenue_30_day")
                    - F.col("total_revenue_30_day_last_month")
                )
                / F.col("total_revenue_30_day_last_month"),
            )
        )
        df_use_case_view_report_all = df_use_case_view_report.select(
            sorted(df_use_case_view_report.columns)
        )
        # if iterate_count > 0:
        #     df_use_case_view_report_all = df_use_case_view_report_all.select(
        #         sorted(df_use_case_view_report.columns)
        #     ).union(
        #         df_use_case_view_report.select(sorted(df_use_case_view_report.columns))
        #     )
        # else:
        #     df_use_case_view_report_all = df_use_case_view_report.select(
        #         sorted(df_use_case_view_report.columns)
        #     )

        if not drop_update_table and iterate_count == 0:
            df_use_case_view_report_all.createOrReplaceTempView("temp_view_load")
            spark.sql("DROP TABLE IF EXISTS nba_dev.use_case_view_report_table")
            spark.sql(
                """CREATE TABLE nba_dev.use_case_view_report_table 
                USING DELTA 
                PARTITIONED BY (contact_date) 
                AS 
                SELECT * FROM temp_view_load"""
            )
        else:
            df_use_case_view_report_all.write.format("delta").mode(
                "append"
            ).partitionBy("contact_date").saveAsTable(
                "nba_dev.use_case_view_report_table"
            )
        iterate_count = iterate_count + 1
    return df_use_case_view_report_all


def process_date(date):
    """
    This function processes date from string form (e.g. "2018-01-01") to proper date format.
    """
    date = datetime.strptime(date, "%Y-%m-%d").date()
    return date


def daterange(start_date, end_date):
    """
    This function returns list of dates: one for each day between start and end date passed as arguments.
    """
    start_date = process_date(start_date)
    end_date = process_date(end_date)
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def create_distinct_aggregate_campaign_feature(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    date_from,
    date_to,
    drop_update_table=False,
) -> DataFrame:
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load("l0_campaign_tracking_contact_list_pre_full_load")
    spark = get_spark_session()
    start_day_data = date_from - timedelta(60)

    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.distinct_aggregate_campaign_feature_tbl",
            key_col="join_date",
        )

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*", "date(contact_date) as join_date"
    ).filter(
        F.col("join_date").between(start_day_data, date_to)
    )
    campaign_child_tbl = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(["campaign_child_code"])
        .agg(F.count("*").alias("c"))
        .drop("c")
    )
    campaign_response_yn = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(
            ["campaign_child_code", "response"]
        )
        .agg(F.count("*").alias("c"))
        .drop("c")
    )

    campaign_response_yn.createOrReplaceTempView("campaign_response_yn")
    campaign_child_tbl.createOrReplaceTempView("campaign_child_tbl")
    campaign_tracking_yn = spark.sql(
        """SELECT main.campaign_child_code,
CASE WHEN COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'empty' THEN 0 
ELSE 1 END AS response_tracking_yn,
CASE WHEN  COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'N' THEN 1
ELSE 0 END AS zero_response_campaign_yn,
f_yes.response as response_y_flag,f_no.response as response_n_flag FROM 
(SELECT campaign_child_code FROM campaign_child_tbl) main
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'Y') f_yes
ON f_yes.campaign_child_code = main.campaign_child_code
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'N') f_no
ON f_no.campaign_child_code = main.campaign_child_code"""
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        campaign_tracking_yn, ["campaign_child_code"], "left"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.withColumn(
        "response_integer", F.when(F.col("response") == "Y", 1).otherwise(0)
    )
    distinct_campaign_kpis = l0_campaign_tracking_contact_list_pre_full_load.withColumn(
        "G", F.lit("1")
    )
    distinct_campaign_features_daily = (
        distinct_campaign_kpis.filter(F.col("join_date").between(date_from, date_to))
        .groupby("join_date")
        .agg(
            F.countDistinct("subscription_identifier").alias(
                "Distinct_prepaid_sub_targeted_1_day_Today"
            ),
            F.countDistinct(
                F.when(F.col("response_integer") == 1, F.col("subscription_identifier"))
            ).alias("Distinct_prepaid_sub_responders_1_day_Today"),
            F.countDistinct("campaign_child_code").alias(
                "Distinct_campaigns_running_1_Day_Today"
            ),
        )
    )
    iterate_distinct = 0
    for d in list(
        daterange(date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d"))
    ):
        d_str = d.strftime("%Y-%m-%d")
        start_window = d - timedelta(6)
        tmp = (
            distinct_campaign_kpis.where(
                "date(contact_date) >= date('"
                + start_window.strftime("%Y-%m-%d")
                + "') AND date(contact_date) <= date('"
                + d_str
                + "')"
            )
            .groupby("G")
            .agg(
                F.countDistinct("subscription_identifier").alias(
                    "Distinct_prepaid_sub_targeted_7_day_Today"
                ),
                F.countDistinct(
                    F.when(
                        F.col("response_integer") == 1, F.col("subscription_identifier")
                    )
                ).alias("Distinct_prepaid_sub_responders_7_day_Today"),
                F.countDistinct("campaign_child_code").alias(
                    "Distinct_campaigns_running_7_Day_Today"
                ),
            )
            .selectExpr(
                "date('" + d_str + "') as join_date",
                "Distinct_prepaid_sub_targeted_7_day_Today",
                "Distinct_prepaid_sub_responders_7_day_Today",
                "Distinct_campaigns_running_7_Day_Today",
            )
        )
        if iterate_distinct == 0:
            distinct_campaign_features_weekly = tmp
        else:
            distinct_campaign_features_weekly = distinct_campaign_features_weekly.union(
                tmp
            )
        iterate_distinct = iterate_distinct + 1

    distinct_campaign_features = distinct_campaign_features_daily.join(
        distinct_campaign_features_weekly, ["join_date"], "left"
    )
    if not drop_update_table:
        distinct_campaign_features.createOrReplaceTempView("temp_view_load")
        spark.sql(
            "DROP TABLE IF EXISTS nba_dev.distinct_aggregate_campaign_feature_tbl"
        )
        spark.sql(
            """CREATE TABLE nba_dev.distinct_aggregate_campaign_feature_tbl 
            USING DELTA 
            PARTITIONED BY (join_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        distinct_campaign_features.write.format("delta").mode("append").partitionBy(
            "join_date"
        ).saveAsTable("nba_dev.distinct_aggregate_campaign_feature_tbl")
    return distinct_campaign_features


def create_general_marketing_performance_report(
    reporting_kpis: DataFrame,
    unused_memory_distinct_aggregate_campaign_feature_tbl: DataFrame,
    unused_memory_reporting_kpis: DataFrame,
    distinct_aggregate_campaign_feature_tbl: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    prepaid_no_activity_daily: DataFrame,
    aggregate_period: List[int],
    dormant_days_agg_periods: List[int],
    date_from,
    date_to,
    drop_update_table=False,
) -> DataFrame:
    # prepaid_no_activity_daily = catalog.load("prepaid_no_activity_daily")
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    # distinct_aggregate_campaign_feature_tbl = catalog.load(
    #     "distinct_aggregate_campaign_feature_tbl"
    # )
    # reporting_kpis = catalog.load("reporting_kpis")
    # aggregate_period = [7, 30]
    # dormant_days_agg_periods = [5, 7, 14, 30, 45, 60, 90]
    # day = "2020-04-28"

    spark = get_spark_session()

    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.general_marketing_performance_report_tbl",
            key_col="join_date",
        )

    start_day_data = date_from - timedelta(30)

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "*", "date(contact_date) as join_date"
    ).filter(
        F.col("join_date").between(start_day_data, date_to)
    )
    campaign_child_tbl = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(["campaign_child_code"])
        .agg(F.count("*").alias("c"))
        .drop("c")
    )
    campaign_response_yn = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(
            ["campaign_child_code", "response"]
        )
        .agg(F.count("*").alias("c"))
        .drop("c")
    )

    campaign_response_yn.createOrReplaceTempView("campaign_response_yn")
    campaign_child_tbl.createOrReplaceTempView("campaign_child_tbl")
    campaign_tracking_yn = spark.sql(
        """SELECT main.campaign_child_code,
CASE WHEN COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'empty' THEN 0 
ELSE 1 END AS response_tracking_yn,
CASE WHEN  COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'N' THEN 1
ELSE 0 END AS zero_response_campaign_yn,
f_yes.response as response_y_flag,f_no.response as response_n_flag FROM 
(SELECT campaign_child_code FROM campaign_child_tbl) main
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'Y') f_yes
ON f_yes.campaign_child_code = main.campaign_child_code
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'N') f_no
ON f_no.campaign_child_code = main.campaign_child_code"""
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        campaign_tracking_yn, ["campaign_child_code"], "left"
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.withColumn(
        "response_integer", F.when(F.col("response") == "Y", 1).otherwise(0)
    )
    campaign_daily_kpis = l0_campaign_tracking_contact_list_pre_full_load.groupby(
        "join_date"
    ).agg(
        F.count("*").alias("All_campaign_transactions"),
        F.sum("response_tracking_yn").alias(
            "All_campaign_transactions_with_response_tracking"
        ),
        F.sum("response_integer").alias("Campaign_Transactions_Responded"),
    )

    campaign_daily_kpis = campaign_daily_kpis.withColumn(
        "timestamp", F.col("join_date").astype("Timestamp").cast("long"),
    ).withColumn("G", F.lit("1"))

    columns_to_aggregate = [
        "All_campaign_transactions",
        "All_campaign_transactions_with_response_tracking",
        "Campaign_Transactions_Responded",
    ]

    for period in aggregate_period:
        window_func = (
            Window.partitionBy(["G"])
            .orderBy(F.col("timestamp"))
            .rangeBetween(
                -((period + 1) * 86400), Window.currentRow
            )  # 86400 is the number of seconds in a day
        )

        campaign_daily_kpis = campaign_daily_kpis.select(
            *(
                campaign_daily_kpis.columns
                + [
                    F.sum(column)
                    .over(window_func)
                    .alias(f"{column}_{period}_Day_Today")
                    for column in columns_to_aggregate
                ]
            )
        )
    campaign_daily_kpis = campaign_daily_kpis.drop("timestamp", "G",)
    for c in columns_to_aggregate:
        campaign_daily_kpis = campaign_daily_kpis.withColumnRenamed(
            c, c + "_1_Day_Today"
        )

    reporting_kpis = reporting_kpis.filter(
        F.col("join_date").between(start_day_data, date_to)
    )
    active_sub_kpis = reporting_kpis.where("no_activity_n_days = 0").selectExpr(
        "analytic_id",
        "register_date",
        "subscription_identifier",
        "join_date",
        "CASE WHEN total_revenue_1_day > 0 THEN 1 ELSE 0 END AS nonzero_1_Day_revenue_active_prepaid_subscribers",
        "CASE WHEN total_revenue_7_day > 0 THEN 1 ELSE 0 END AS nonzero_7_Day_revenue_active_prepaid_subscribers",
        "CASE WHEN total_revenue_30_day > 0 THEN 1 ELSE 0 END AS nonzero_30_Day_revenue_active_prepaid_subscribers",
        "1 as active_prepaid_subscribers_1_Day",
    )

    inactivity_kpis = reporting_kpis.select(
        "analytic_id",
        "register_date",
        "join_date",
        *[
            F.when(F.col("no_activity_n_days") >= min_days_dormant, 1)
            .otherwise(0)
            .alias(f"total_dormant_{min_days_dormant}_day")
            for min_days_dormant in dormant_days_agg_periods
        ],
    )
    inactivity_kpis_agg = inactivity_kpis.groupby("join_date").agg(
        F.sum("total_dormant_5_day").alias("total_dormant_5_day"),
        F.sum("total_dormant_7_day").alias("total_dormant_7_day"),
        F.sum("total_dormant_14_day").alias("total_dormant_14_day"),
        F.sum("total_dormant_30_day").alias("total_dormant_30_day"),
        F.sum("total_dormant_45_day").alias("total_dormant_45_day"),
        F.sum("total_dormant_60_day").alias("total_dormant_60_day"),
        F.sum("total_dormant_90_day").alias("total_dormant_90_day"),
    )

    reporting_kpis_agg = reporting_kpis.groupby("join_date").agg(
        F.sum("total_revenue_1_day").alias("Total_Revenue_1_day_Today"),
        F.sum("total_revenue_7_day").alias("Total_Revenue_7_day_Today"),
        F.sum("total_revenue_30_day").alias("Total_Revenue_30_day_Today"),
        F.avg("total_revenue_1_day").alias("ARPU_1_day_Today"),
        F.avg("total_revenue_7_day").alias("ARPU_7_day_Today"),
        F.avg("total_revenue_30_day").alias("ARPU_30_day_Today"),
    )

    active_sub_kpis_agg = active_sub_kpis.groupby("join_date").agg(
        F.sum("active_prepaid_subscribers_1_Day").alias(
            "Total_active_prepaid_subscribers_1_Day_Today"
        ),
        F.sum("nonzero_1_Day_revenue_active_prepaid_subscribers").alias(
            "Total_nonzero_1_Day_revenue_active_prepaid_subscribers_1_Day_Today"
        ),
        F.sum("nonzero_7_Day_revenue_active_prepaid_subscribers").alias(
            "Total_nonzero_7_Day_revenue_active_prepaid_subscribers_1_Day_Today"
        ),
        F.sum("nonzero_30_Day_revenue_active_prepaid_subscribers").alias(
            "Total_nonzero_30_Day_revenue_active_prepaid_subscribers_1_Day_Today"
        ),
    )
    spine_table = reporting_kpis_agg.filter(
        F.col("join_date").between(date_from, date_to)
    )
    spine_table = spine_table.join(
        inactivity_kpis_agg.filter(F.col("join_date").between(date_from, date_to)),
        ["join_date"],
        "left",
    )
    spine_table = spine_table.join(
        active_sub_kpis_agg.filter(F.col("join_date").between(date_from, date_to)),
        ["join_date"],
        "left",
    )
    inactivity_kpis_agg_last_week = inactivity_kpis_agg.selectExpr(
        "date_add(join_date,7) as join_date",
        "total_dormant_5_day as total_dormant_5_day_Last_week",
        "total_dormant_7_day as total_dormant_7_day_Last_week",
        "total_dormant_14_day as total_dormant_14_day_Last_week",
        "total_dormant_30_day as total_dormant_30_day_Last_week",
        "total_dormant_45_day as total_dormant_45_day_Last_week",
        "total_dormant_60_day as total_dormant_60_day_Last_week",
        "total_dormant_90_day as total_dormant_90_day_Last_week",
    )

    spine_table = spine_table.join(inactivity_kpis_agg_last_week, ["join_date"], "left")

    reporting_kpis_agg_last_week = reporting_kpis_agg.selectExpr(
        "date_add(join_date,7) as join_date",
        "Total_Revenue_1_day_Today as Total_Revenue_1_day_Last_week",
        "Total_Revenue_7_day_Today as Total_Revenue_7_day_Last_week",
        "Total_Revenue_30_day_Today as Total_Revenue_30_day_Last_week",
        "ARPU_1_day_Today as ARPU_1_day_Last_week",
        "ARPU_7_day_Today as ARPU_7_day_Last_week",
        "ARPU_30_day_Today as ARPU_30_day_Last_week",
    )
    spine_table = spine_table.join(reporting_kpis_agg_last_week, ["join_date"], "left")

    active_sub_kpis_agg_last_week = active_sub_kpis_agg.selectExpr(
        "date_add(join_date,7) as join_date",
        "Total_active_prepaid_subscribers_1_Day_Today as Total_active_prepaid_subscribers_1_Day_Last_week",
        "Total_nonzero_1_Day_revenue_active_prepaid_subscribers_1_Day_Today as Total_nonzero_1_Day_revenue_active_prepaid_subscribers_1_Day_Last_week",
        "Total_nonzero_7_Day_revenue_active_prepaid_subscribers_1_Day_Today as Total_nonzero_7_Day_revenue_active_prepaid_subscribers_1_Day_Last_week",
        "Total_nonzero_30_Day_revenue_active_prepaid_subscribers_1_Day_Today as Total_nonzero_30_Day_revenue_active_prepaid_subscribers_1_Day_Last_week",
    )

    spine_table = spine_table.join(active_sub_kpis_agg_last_week, ["join_date"], "left")

    spine_table = spine_table.join(campaign_daily_kpis, ["join_date"], "left")

    campaign_kpis_last_week = campaign_daily_kpis.selectExpr(
        "date_add(join_date,7) as join_date",
        "All_campaign_transactions_1_Day_Today as All_campaign_transactions_1_Day_Last_week",
        "All_campaign_transactions_7_Day_Today as All_campaign_transactions_7_Day_Last_week",
        "All_campaign_transactions_30_Day_Today as All_campaign_transactions_30_Day_Last_week",
        "All_campaign_transactions_with_response_tracking_1_day_Today as All_campaign_transactions_with_response_tracking_1_day_Last_week",
        "All_campaign_transactions_with_response_tracking_7_day_Today as All_campaign_transactions_with_response_tracking_7_day_Last_week",
        "All_campaign_transactions_with_response_tracking_30_day_Today as All_campaign_transactions_with_response_tracking_30_day_Last_week",
        "Campaign_Transactions_Responded_1_day_Today as Campaign_Transactions_Responded_1_day_Last_week",
        "Campaign_Transactions_Responded_7_day_Today as Campaign_Transactions_Responded_7_day_Last_week",
        "Campaign_Transactions_Responded_30_day_Today as Campaign_Transactions_Responded_30_day_Last_week",
    )
    spine_table = spine_table.join(campaign_kpis_last_week, ["join_date"], "left")

    spine_table = spine_table.join(
        distinct_aggregate_campaign_feature_tbl, ["join_date"], "left"
    )

    distinct_campaign_features_last_week = distinct_aggregate_campaign_feature_tbl.selectExpr(
        "date_add(join_date,7) as join_date",
        "Distinct_prepaid_sub_targeted_1_day_Today as Distinct_prepaid_sub_targeted_1_day_Last_week",
        "Distinct_prepaid_sub_responders_1_day_Today as Distinct_prepaid_sub_responders_1_day_Last_week",
        "Distinct_campaigns_running_1_Day_Today as Distinct_campaigns_running_1_Day_Last_week",
        "Distinct_prepaid_sub_targeted_7_day_Today as Distinct_prepaid_sub_targeted_7_day_Last_week",
        "Distinct_prepaid_sub_responders_7_day_Today as Distinct_prepaid_sub_responders_7_day_Last_week",
        "Distinct_campaigns_running_7_Day_Today as Distinct_campaigns_running_7_Day_Last_week",
    )
    spine_table = spine_table.join(
        distinct_campaign_features_last_week, ["join_date"], "left"
    )
    spine_table = spine_table.withColumn(
        "Candidate_transactions_7_Day_Today", F.lit("")
    ).withColumn("Candidate_transactions_7_Day_Last_week", F.lit(""))

    total_sub_daily = (
        prepaid_no_activity_daily.groupby("ddate")
        .agg(
            F.countDistinct("analytic_id").alias(
                "Total_prepaid_subscribers_1_Day_Today"
            )
        )
        .selectExpr("Total_prepaid_subscribers_1_Day_Today", "date(ddate) as join_date")
    )

    spine_table = spine_table.join(total_sub_daily, ["join_date"], "left")

    total_sub_last_week = total_sub_daily.selectExpr(
        "date_add(join_date,7) as join_date",
        "Total_prepaid_subscribers_1_Day_Today as Total_prepaid_subscribers_1_Day_Last_week",
    )

    spine_table = spine_table.join(total_sub_last_week, ["join_date"], "left")

    if not drop_update_table:
        spine_table.createOrReplaceTempView("temp_view_load")
        spark.sql(
            "DROP TABLE IF EXISTS nba_dev.general_marketing_performance_report_tbl"
        )
        spark.sql(
            """CREATE TABLE nba_dev.general_marketing_performance_report_tbl 
            USING DELTA 
            PARTITIONED BY (join_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        spine_table.write.format("delta").mode("append").partitionBy(
            "join_date"
        ).saveAsTable("nba_dev.general_marketing_performance_report_tbl")
    return spine_table


def create_use_case_campaign_mapping_table(
    campaign_churn_cvm_master: DataFrame,
    campaign_churn_bau_master: DataFrame,
    campaign_ard_cvm_master: DataFrame,
    campaign_ard_churn_mck_master: DataFrame,
) -> DataFrame:
    campaign_ard_churn_mck_master = (
        campaign_ard_churn_mck_master.groupby(
            "use_case", "campaign_code", "macrosegment"
        )
        .agg(F.count("*"))
        .selectExpr(
            "macrosegment as campaign_project_group",
            "campaign_code as campaign_child_code",
            "'mck' as target_group",
            "'cvm' as report_campaign_group",
            "CASE WHEN use_case = 'ard' THEN 'ARD' ELSE 'CHURN' END as usecase",
        )
    )
    campaign_churn_cvm_master = campaign_churn_cvm_master.groupBy(["child_code"]).agg(
        F.first("campaign_group").alias("campaign_project_group")
    )
    campaign_churn_cvm_master = campaign_churn_cvm_master.selectExpr(
        "campaign_project_group",
        "child_code as campaign_child_code",
        "'bau' as target_group",
        "'cvm' as report_campaign_group",
        "'CHURN' as usecase",
    )
    campaign_churn_bau_master = campaign_churn_bau_master.groupBy(["child_code"]).agg(
        F.first("campaign_group").alias("campaign_project_group")
    )
    campaign_churn_bau_master = campaign_churn_bau_master.selectExpr(
        "campaign_project_group",
        "child_code as campaign_child_code",
        "'bau' as target_group",
        "'bau' as report_campaign_group",
        "'CHURN' as usecase",
    )
    # campaign_churn_bau_master = campaign_churn_bau_master
    campaign_ard_cvm_master = campaign_ard_cvm_master.groupBy(["child_code"]).agg(
        F.first("campaign_system").alias("campaign_system")
    )
    campaign_ard_cvm_master = campaign_ard_cvm_master.selectExpr(
        "'Anti_revenue_dilution' as campaign_project_group",
        "child_code as campaign_child_code",
        "'bau' as target_group",
        "'cvm' as report_campaign_group",
        "'ARD' as usecase",
    )

    campaign_mapping_master = (
        campaign_churn_cvm_master.union(campaign_churn_bau_master)
        .union(campaign_ard_cvm_master)
        .union(campaign_ard_churn_mck_master)
    )
    return campaign_mapping_master


def store_historical_usecase_view_report(
    use_case_view_report_table: DataFrame,
    unused_memory_use_case_view_report_table: DataFrame,
) -> DataFrame:
    spark = get_spark_session()
    group_by_campaign_group_tbl = use_case_view_report_table.groupby(
        ["target_group", "contact_date"]
    ).agg(
        *[
            F.first(c).alias(c)
            for c in use_case_view_report_table.columns
            if c.endswith("_day")
            or c.endswith("_last_month")
            or c.endswith("_lastweek")
            or c.endswith("_Last_week")
            or c.endswith("_today")
        ]
    )
    for a in [1, 7, 30]:
        for p in ["", "_Last_week"]:
            group_by_campaign_group_tbl = group_by_campaign_group_tbl.drop(
                "n_subscriber_targeted_{0}_day{1}",
                "n_campaign_accepted_{0}_day{1}",
                "n_campaign_sent_{0}_day{1}",
                "n_subscriber_accepted_{0}_day{1}",
            )
    group_by_campaign_group_tbl = group_by_campaign_group_tbl.join(
        use_case_view_report_table.groupby(["target_group"]).agg(
            F.first("distinct_targeted_subscriber").alias(
                "distinct_targeted_subscriber"
            )
        ),
        ["target_group"],
        "inner",
    )
    for usecase in ("CHURN", "ARD"):
        sdf_pivot = use_case_view_report_table.selectExpr(
            "target_group",
            "contact_date",
            "n_subscriber_targeted_1_day as n_{}_subscriber_targeted_1_day".format(
                usecase
            ),
            "n_campaign_accepted_1_day as n_{}_campaign_accepted_1_day".format(usecase),
            "n_campaign_sent_1_day as n_{}_campaign_sent_1_day".format(usecase),
            "n_subscriber_accepted_1_day as n_{}_subscriber_accepted_1_day".format(
                usecase
            ),
            "n_subscriber_targeted_7_day as n_{}_subscriber_targeted_7_day".format(
                usecase
            ),
            "n_campaign_accepted_7_day as n_{}_campaign_accepted_7_day".format(usecase),
            "n_campaign_sent_7_day as n_{}_campaign_sent_7_day".format(usecase),
            "n_subscriber_accepted_7_day as n_{}_subscriber_accepted_7_day".format(
                usecase
            ),
            "n_subscriber_targeted_30_day as n_{}_subscriber_targeted_30_day".format(
                usecase
            ),
            "n_campaign_accepted_30_day as n_{}_campaign_accepted_30_day".format(
                usecase
            ),
            "n_campaign_sent_30_day as n_{}_campaign_sent_30_day".format(usecase),
            "n_subscriber_accepted_30_day as n_{}_subscriber_accepted_30_day".format(
                usecase
            ),
            # lastweek
            "n_subscriber_targeted_1_day_Last_week as n_{}_subscriber_targeted_1_day_Last_week".format(
                usecase
            ),
            "n_campaign_accepted_1_day_Last_week as n_{}_campaign_accepted_1_day_Last_week".format(
                usecase
            ),
            "n_campaign_sent_1_day_Last_week as n_{}_campaign_sent_1_day_Last_week".format(
                usecase
            ),
            "n_subscriber_accepted_1_day_Last_week as n_{}_subscriber_accepted_1_day_Last_week".format(
                usecase
            ),
            "n_subscriber_targeted_7_day_Last_week as n_{}_subscriber_targeted_7_day_Last_week".format(
                usecase
            ),
            "n_campaign_accepted_7_day_Last_week as n_{}_campaign_accepted_7_day_Last_week".format(
                usecase
            ),
            "n_campaign_sent_7_day_Last_week as n_{}_campaign_sent_7_day_Last_week".format(
                usecase
            ),
            "n_subscriber_accepted_7_day_Last_week as n_{}_subscriber_accepted_7_day_Last_week".format(
                usecase
            ),
            "n_subscriber_targeted_30_day_Last_week as n_{}_subscriber_targeted_30_day_Last_week".format(
                usecase
            ),
            "n_campaign_accepted_30_day_Last_week as n_{}_campaign_accepted_30_day_Last_week".format(
                usecase
            ),
            "n_campaign_sent_30_day_Last_week as n_{}_campaign_sent_30_day_Last_week".format(
                usecase
            ),
            "n_subscriber_accepted_30_day_Last_week as n_{}_subscriber_accepted_30_day_Last_week".format(
                usecase
            ),
        ).where(F.col("usecase") == usecase)
        group_by_campaign_group_tbl = group_by_campaign_group_tbl.join(
            sdf_pivot, ["target_group", "contact_date"], "inner"
        )
    group_by_campaign_group_tbl = group_by_campaign_group_tbl.withColumnRenamed(
        "target_group", "campaign_control_group"
    ).withColumnRenamed("contact_date", "report_date")

    group_by_campaign_group_tbl.createOrReplaceTempView("temp_view_load")
    spark.sql("DROP TABLE IF EXISTS nba_dev.historical_use_case_view_report_table")
    spark.sql(
        """CREATE TABLE nba_dev.historical_use_case_view_report_table 
        USING DELTA 
        PARTITIONED BY (report_date) 
        AS 
        SELECT * FROM temp_view_load"""
    )
    return group_by_campaign_group_tbl


def create_campaign_view_report_input(
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    l0_campaign_history_master_active: DataFrame,
    use_case_campaign_mapping: DataFrame,
    reporting_kpis: DataFrame,
    date_from,
    date_to,
    drop_update_table=False,
) -> DataFrame:
    # Test
    # mock_report_running_date = (datetime.now() + timedelta(hours=7)).strftime(
    #     "%Y-%m-%d"
    # )
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    # l0_campaign_history_master_active = catalog.load(
    #     "l0_campaign_history_master_active"
    # )
    # use_case_campaign_mapping = catalog.load("use_case_campaign_mapping")
    # reporting_kpis = catalog.load("reporting_kpis")
    # date_from = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
    #     days=-50
    # )
    # date_to = datetime.strptime(mock_report_running_date, "%Y-%m-%d")
    spark = get_spark_session()

    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.campaign_view_report_input_tbl",
            key_col="contact_date",
        )

    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.filter(
        F.col("contact_date").between(date_from, date_to)
    )

    # only select relavant columns for report
    # TODO in the future more column might be use as the business required
    l0_campaign_history_master_active = l0_campaign_history_master_active.selectExpr(
        "child_code as campaign_child_code",
        "campaign_type",
        "campaign_sub_type",
        "campaign_category",
        "offer_category",
        "offer_type",
        "effort_condition",
    )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "campaign_type as campaign_treatment_type",
        "campaign_group",
        "response_type",
        "campaign_channel",
        "campaign_child_code",
        "campaign_name",
        "date(contact_date) as contact_date",
        "contact_control_group",
        "response",
        "response_date",
    )
    # Joining campaign transaction with Campaign Master table
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        l0_campaign_history_master_active, ["campaign_child_code"], "left"
    )
    # Transform response Y/N into integer value
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.withColumn(
        "response_integer", F.when(F.col("response") == "Y", 1).otherwise(0)
    )
    # Joining with CHURN AND ARD usecase Campaign Mapping
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.join(
        use_case_campaign_mapping, ["campaign_child_code"], "left"
    )

    reporting_kpis = reporting_kpis.drop("timestamp",)
    reporting_kpis = reporting_kpis.selectExpr("*", "date(join_date) as contact_date")

    ######## Create campaign view report input data
    # This part of code is computationally expensive,
    # required atleast 15 Nodes of Standard_E16s_v3 to run the data for 1 months
    # Which mean This part made the whole function
    # Require to be optimize in daily process by limiting the number of day to re-run/update

    # Currently, We define that revenue after feature is calculate after 4 days of campaign invitation (Campaign period)
    # Since Reporting KPIs already calculate revenue feature for the 1D(Today) 7D(Last 7D include today) and 30D
    # So we could join by specific date and use them right away
    # Example in case of Revenue 7D features we will add 10 day to the contact date
    # So if the customer was contact on 1st Jun we are taking feature of 11sh Jun to be join
    # Hence Revenue 7D features of 11th Jun calculate from 5th 6th 7th 8th 9th 10th 11st Jun

    reporting_kpis_before = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "contact_date",
        "target_group as use_case_control_group",
        "ontop_data_number_of_transaction_1_day as ontop_data_number_of_transaction_1_day_before",
        "ontop_data_total_net_tariff_1_day as ontop_data_total_net_tariff_1_day_before",
        "ontop_voice_number_of_transaction_1_day as ontop_voice_number_of_transaction_1_day_before",
        "ontop_voice_total_net_tariff_1_day as ontop_voice_total_net_tariff_1_day_before",
        "all_ppu_charge_1_day as all_ppu_charge_1_day_before",
        "top_up_value_1_day as top_up_value_1_day_before",
        "total_revenue_1_day as total_revenue_1_day_before",
        "total_number_ontop_purchase_1_day as total_number_ontop_purchase_1_day_before",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_1d_before",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_1d_before",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_1d_before",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_1d_before",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_1d_before",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0    END as dormant_90_day_1d_before",
        "ontop_data_number_of_transaction_7_day as ontop_data_number_of_transaction_7_day_before",
        "ontop_data_total_net_tariff_7_day as ontop_data_total_net_tariff_7_day_before",
        "ontop_voice_number_of_transaction_7_day as ontop_voice_number_of_transaction_7_day_before",
        "ontop_voice_total_net_tariff_7_day as ontop_voice_total_net_tariff_7_day_before",
        "all_ppu_charge_7_day as all_ppu_charge_7_day_before",
        "top_up_value_7_day as top_up_value_7_day_before",
        "total_revenue_7_day as total_revenue_7_day_before",
        "total_number_ontop_purchase_7_day as total_number_ontop_purchase_7_day_before",
        "ontop_data_number_of_transaction_30_day as ontop_data_number_of_transaction_30_day_before",
        "ontop_data_total_net_tariff_30_day as ontop_data_total_net_tariff_30_day_before",
        "ontop_voice_number_of_transaction_30_day as ontop_voice_number_of_transaction_30_day_before",
        "ontop_voice_total_net_tariff_30_day as ontop_voice_total_net_tariff_30_day_before",
        "all_ppu_charge_30_day as all_ppu_charge_30_day_before",
        "top_up_value_30_day as top_up_value_30_day_before",
        "total_revenue_30_day as total_revenue_30_day_before",
        "total_number_ontop_purchase_30_day as total_number_ontop_purchase_30_day_before",
    )

    reporting_kpis_7d_before = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date_add(contact_date,-10) as contact_date",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_7d_before",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_7d_before",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_7d_before",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_7d_before",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_7d_before",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0    END as dormant_90_day_7d_before",
    )
    reporting_kpis_30d_before = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date_add(contact_date,-33) as contact_date",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_30d_before",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_30d_before",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_30d_before",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_30d_before",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_30d_before",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0    END as dormant_90_day_30d_before",
    )
    reporting_kpis_1d_after = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date_add(contact_date,4) as contact_date",
        "ontop_data_number_of_transaction_1_day as ontop_data_number_of_transaction_1_day_after",
        "ontop_data_total_net_tariff_1_day as ontop_data_total_net_tariff_1_day_after",
        "ontop_voice_number_of_transaction_1_day as ontop_voice_number_of_transaction_1_day_after",
        "ontop_voice_total_net_tariff_1_day as ontop_voice_total_net_tariff_1_day_after",
        "all_ppu_charge_1_day as all_ppu_charge_1_day_after",
        "top_up_value_1_day as top_up_value_1_day_after",
        "total_revenue_1_day as total_revenue_1_day_after",
        "total_number_ontop_purchase_1_day as total_number_ontop_purchase_1_day_after",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_1d_after",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_1d_after",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_1d_after",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_1d_after",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_1d_after",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0 END as dormant_90_day_1d_after",
    )
    reporting_kpis_7d_after = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date_add(contact_date,10) as contact_date",
        "ontop_data_number_of_transaction_7_day as ontop_data_number_of_transaction_7_day_after",
        "ontop_data_total_net_tariff_7_day as ontop_data_total_net_tariff_7_day_after",
        "ontop_voice_number_of_transaction_7_day as ontop_voice_number_of_transaction_7_day_after",
        "ontop_voice_total_net_tariff_7_day as ontop_voice_total_net_tariff_7_day_after",
        "all_ppu_charge_7_day as all_ppu_charge_7_day_after",
        "top_up_value_7_day as top_up_value_7_day_after",
        "total_revenue_7_day as total_revenue_7_day_after",
        "total_number_ontop_purchase_7_day as total_number_ontop_purchase_7_day_after",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_7d_after",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_7d_after",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_7d_after",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_7d_after",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_7d_after",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0 END as dormant_90_day_7d_after",
    )
    reporting_kpis_30d_after = reporting_kpis.selectExpr(
        "subscription_identifier",
        "date(register_date) as register_date",
        "date_add(contact_date,33) as contact_date",
        "ontop_data_number_of_transaction_30_day as ontop_data_number_of_transaction_30_day_after",
        "ontop_data_total_net_tariff_30_day as ontop_data_total_net_tariff_30_day_after",
        "ontop_voice_number_of_transaction_30_day as ontop_voice_number_of_transaction_30_day_after",
        "ontop_voice_total_net_tariff_30_day as ontop_voice_total_net_tariff_30_day_after",
        "all_ppu_charge_30_day as all_ppu_charge_30_day_after",
        "top_up_value_30_day as top_up_value_30_day_after",
        "total_revenue_30_day as total_revenue_30_day_after",
        "total_number_ontop_purchase_30_day as total_number_ontop_purchase_30_day_after",
        "CASE WHEN no_activity_n_days >= 5 THEN 1 ELSE 0 END as dormant_5_day_30d_after",
        "CASE WHEN no_activity_n_days >= 7 THEN 1 ELSE 0 END as dormant_7_day_30d_after",
        "CASE WHEN no_activity_n_days >= 14 THEN 1 ELSE 0 END as dormant_14_day_30d_after",
        "CASE WHEN no_activity_n_days >= 30 THEN 1 ELSE 0 END as dormant_30_day_30d_after",
        "CASE WHEN no_activity_n_days >= 60 THEN 1 ELSE 0 END as dormant_60_day_30d_after",
        "CASE WHEN no_activity_n_days >= 90 THEN 1 ELSE 0 END as dormant_90_day_30d_after",
    )

    mapping_tbl = use_case_campaign_mapping.selectExpr(
        "campaign_child_code", "target_group as defined_campaign_target_group",
    )
    campaign_view_report_input = l0_campaign_tracking_contact_list_pre_full_load.join(
        reporting_kpis_1d_after,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )
    campaign_view_report_input = campaign_view_report_input.join(
        reporting_kpis_7d_after,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )
    campaign_view_report_input = campaign_view_report_input.join(
        reporting_kpis_30d_after,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )
    campaign_view_report_input = campaign_view_report_input.join(
        reporting_kpis_before,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )
    campaign_view_report_input = campaign_view_report_input.join(
        reporting_kpis_7d_before,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )

    campaign_view_report_input = campaign_view_report_input.join(
        reporting_kpis_30d_before,
        ["subscription_identifier", "register_date", "contact_date"],
        "left",
    )

    campaign_view_report_input = campaign_view_report_input.join(
        mapping_tbl, ["campaign_child_code"], "left",
    )
    if not drop_update_table:
        campaign_view_report_input.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.campaign_view_report_input_tbl")
        spark.sql(
            """CREATE TABLE nba_dev.campaign_view_report_input_tbl 
            USING DELTA 
            PARTITIONED BY (contact_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        campaign_view_report_input.write.format("delta").mode("append").partitionBy(
            "contact_date"
        ).saveAsTable("nba_dev.campaign_view_report_input_tbl")
    return campaign_view_report_input


def create_aggregate_campaign_view_features(
    campaign_view_report_input: DataFrame,
    date_to,
    date_from,
    aggregate_period,
    drop_update_table=False,
) -> DataFrame:
    # aggregate_period = [7, 30]
    # mock_report_running_date = (datetime.now() + timedelta(hours=7)).strftime(
    #     "%Y-%m-%d"
    # )
    # date_from = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
    #     days=-30
    # )
    # date_to = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
    #     days=-20
    # )
    # campaign_view_report_input = catalog.load("campaign_view_report_input_tbl")
    spark = get_spark_session()
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.aggregate_campaign_view_features_tbl",
            key_col="contact_date",
        )
    # Create all date within running period to make sure that every campaign has record
    # for each day even if the value is 0, so that we could show in the report.
    df_date_period = spark.sql(
        f"SELECT sequence("
        f"  to_date('{date_from.strftime('%Y-%m-%d')}'),"
        f"  to_date('{date_to.strftime('%Y-%m-%d')}'), interval 1 day"
        f") as contact_date"
    ).withColumn("contact_date", F.explode(F.col("contact_date")))
    columns_to_avg = [
        # 1D Feature after (Day after End campaign period)
        "ontop_data_number_of_transaction_1_day_after",
        "ontop_data_total_net_tariff_1_day_after",
        "ontop_voice_number_of_transaction_1_day_after",
        "ontop_voice_total_net_tariff_1_day_after",
        "all_ppu_charge_1_day_after",
        "top_up_value_1_day_after",
        "total_revenue_1_day_after",
        "total_number_ontop_purchase_1_day_after",
        # 7D feature after
        "ontop_data_number_of_transaction_7_day_after",
        "ontop_data_total_net_tariff_7_day_after",
        "ontop_voice_number_of_transaction_7_day_after",
        "ontop_voice_total_net_tariff_7_day_after",
        "all_ppu_charge_7_day_after",
        "top_up_value_7_day_after",
        "total_revenue_7_day_after",
        "total_number_ontop_purchase_7_day_after",
        # 30D Feature after
        "ontop_data_number_of_transaction_30_day_after",
        "ontop_data_total_net_tariff_30_day_after",
        "ontop_voice_number_of_transaction_30_day_after",
        "ontop_voice_total_net_tariff_30_day_after",
        "all_ppu_charge_30_day_after",
        "top_up_value_30_day_after",
        "total_revenue_30_day_after",
        "total_number_ontop_purchase_30_day_after",
        # 1D Feature before
        "ontop_data_number_of_transaction_1_day_before",
        "ontop_data_total_net_tariff_1_day_before",
        "ontop_voice_number_of_transaction_1_day_before",
        "ontop_voice_total_net_tariff_1_day_before",
        "all_ppu_charge_1_day_before",
        "top_up_value_1_day_before",
        "total_revenue_1_day_before",
        "total_number_ontop_purchase_1_day_before",
        # 7D Feature before
        "ontop_data_number_of_transaction_7_day_before",
        "ontop_data_total_net_tariff_7_day_before",
        "ontop_voice_number_of_transaction_7_day_before",
        "ontop_voice_total_net_tariff_7_day_before",
        "all_ppu_charge_7_day_before",
        "top_up_value_7_day_before",
        "total_revenue_7_day_before",
        "total_number_ontop_purchase_7_day_before",
        # 30D Feature before
        "ontop_data_number_of_transaction_30_day_before",
        "ontop_data_total_net_tariff_30_day_before",
        "ontop_voice_number_of_transaction_30_day_before",
        "ontop_voice_total_net_tariff_30_day_before",
        "all_ppu_charge_30_day_before",
        "top_up_value_30_day_before",
        "total_revenue_30_day_before",
        "total_number_ontop_purchase_30_day_before",
    ]
    columns_to_sum = [
        "dormant_5_day_1d_after",
        "dormant_7_day_1d_after",
        "dormant_14_day_1d_after",
        "dormant_30_day_1d_after",
        "dormant_60_day_1d_after",
        "dormant_90_day_1d_after",
        "dormant_5_day_7d_after",
        "dormant_7_day_7d_after",
        "dormant_14_day_7d_after",
        "dormant_30_day_7d_after",
        "dormant_60_day_7d_after",
        "dormant_90_day_7d_after",
        "dormant_5_day_30d_after",
        "dormant_7_day_30d_after",
        "dormant_14_day_30d_after",
        "dormant_30_day_30d_after",
        "dormant_60_day_30d_after",
        "dormant_90_day_30d_after",
        "dormant_5_day_1d_before",
        "dormant_7_day_1d_before",
        "dormant_14_day_1d_before",
        "dormant_30_day_1d_before",
        "dormant_60_day_1d_before",
        "dormant_90_day_1d_before",
        "dormant_5_day_7d_before",
        "dormant_7_day_7d_before",
        "dormant_14_day_7d_before",
        "dormant_30_day_7d_before",
        "dormant_60_day_7d_before",
        "dormant_90_day_7d_before",
        "dormant_5_day_30d_before",
        "dormant_7_day_30d_before",
        "dormant_14_day_30d_before",
        "dormant_30_day_30d_before",
        "dormant_60_day_30d_before",
        "dormant_90_day_30d_before",
    ]
    exprs = [
        F.count("*").alias("n_campaign_sent"),
        F.sum("response_integer").alias("n_campaign_accepted"),
        F.countDistinct("subscription_identifier").alias("n_subscriber_targeted"),
        F.countDistinct(
            F.when(F.col("response_integer") == 1, F.col("subscription_identifier"))
        ).alias("n_subscriber_accepted"),
    ]
    for column in columns_to_avg:
        exprs.append(F.avg(column).alias(column))
    for column in columns_to_sum:
        exprs.append(F.sum(column).alias(column))
    aggregate_campaign_view_features = campaign_view_report_input.groupBy(
        [
            "campaign_child_code",
            "campaign_name",
            "campaign_type",
            "campaign_sub_type",
            "campaign_category",
            "offer_category",
            "offer_type",
            "effort_condition",
            "campaign_treatment_type",
            "campaign_group",
            "response_type",
            "campaign_channel",
            "contact_control_group",
            "contact_date",
            "defined_campaign_target_group",
            "usecase",
        ]
    ).agg(*exprs)
    campaign_combination_list = (
        aggregate_campaign_view_features.groupBy("campaign_child_code")
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
    )
    # Cross join all existing campaign with running date period
    campaign_combination_date = campaign_combination_list.crossJoin(df_date_period)

    aggregate_campaign_view_features = campaign_combination_date.join(
        aggregate_campaign_view_features,
        ["campaign_child_code", "contact_date"],
        "left",
    )

    # Aggregate campaign common features
    columns_to_aggregate = [
        "n_campaign_sent",
        "n_campaign_accepted",
        "n_subscriber_targeted",
        "n_subscriber_accepted",
    ]
    aggregate_campaign_view_features = aggregate_campaign_view_features.withColumn(
        "timestamp", F.col("contact_date").astype("Timestamp").cast("long"),
    )

    for period in aggregate_period:
        window_func = (
            Window.partitionBy(
                ["campaign_child_code", "contact_control_group", "usecase"]
            )
            .orderBy(F.col("timestamp"))
            .rangeBetween(
                -((period + 1) * 86400), Window.currentRow
            )  # 86400 is the number of seconds in a day
        )

        aggregate_campaign_view_features = aggregate_campaign_view_features.select(
            *(
                aggregate_campaign_view_features.columns
                + [
                    F.sum(column).over(window_func).alias(f"{column}_{period}_day")
                    for column in columns_to_aggregate
                ]
            )
        )
    for a in columns_to_aggregate:
        aggregate_campaign_view_features = aggregate_campaign_view_features.withColumnRenamed(
            a, a + "_1_day"
        )
    if not drop_update_table:
        aggregate_campaign_view_features.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.aggregate_campaign_view_features_tbl")
        spark.sql(
            """CREATE TABLE nba_dev.aggregate_campaign_view_features_tbl 
            USING DELTA 
            PARTITIONED BY (contact_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        aggregate_campaign_view_features.write.format("delta").mode(
            "append"
        ).partitionBy("contact_date").saveAsTable(
            "nba_dev.aggregate_campaign_view_features_tbl"
        )
    return aggregate_campaign_view_features


def create_campaign_view_report(
    aggregate_campaign_view_features_tbl: DataFrame,
    l0_campaign_tracking_contact_list_pre_full_load: DataFrame,
    date_from,
    date_to,
    drop_update_table=False,
) -> DataFrame:
    # l0_campaign_tracking_contact_list_pre_full_load = catalog.load(
    #     "l0_campaign_tracking_contact_list_pre_full_load"
    # )
    # mock_report_running_date = (datetime.now() + timedelta(hours=7)).strftime(
    #     "%Y-%m-%d"
    # )
    # date_from = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
    #     days=-50
    # )
    # date_to = datetime.strptime(mock_report_running_date, "%Y-%m-%d")
    # aggregate_campaign_view_features_tbl = catalog.load(
    #     "aggregate_campaign_view_features_tbl"
    # )
    # drop_update_table = False
    spark = get_spark_session()
    if drop_update_table:
        drop_data_by_date(
            date_from=date_from,
            date_to=date_to,
            table="nba_dev.campaign_view_report_tbl",
            key_col="contact_date",
        )
    l0_campaign_tracking_contact_list_pre_full_load = l0_campaign_tracking_contact_list_pre_full_load.filter(
        F.col("contact_date").between(date_from, date_to)
    )
    # Only rerun campaign view report starting from date_from till date_to
    aggregate_campaign_view_features = aggregate_campaign_view_features_tbl.filter(
        F.col("contact_date").between(date_from, date_to)
    )
    campaign_child_tbl = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(["campaign_child_code"])
        .agg(F.count("*").alias("c"))
        .drop("c")
    )
    campaign_response_yn = (
        l0_campaign_tracking_contact_list_pre_full_load.groupby(
            ["campaign_child_code", "response"]
        )
        .agg(F.count("*").alias("c"))
        .drop("c")
    )

    campaign_response_yn.createOrReplaceTempView("campaign_response_yn")
    campaign_child_tbl.createOrReplaceTempView("campaign_child_tbl")
    campaign_tracking_yn = spark.sql(
        """SELECT main.campaign_child_code,
CASE WHEN COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'empty' THEN 0 
ELSE 1 END AS response_tracking_yn,
CASE WHEN  COALESCE(f_yes.response,'empty') = 'empty' AND COALESCE(f_no.response,'empty') = 'N' THEN 1
ELSE 0 END AS zero_response_campaign_yn,
f_yes.response as response_y_flag,f_no.response as response_n_flag FROM 
(SELECT campaign_child_code FROM campaign_child_tbl) main
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'Y') f_yes
ON f_yes.campaign_child_code = main.campaign_child_code
LEFT JOIN
(SELECT campaign_child_code,response FROM campaign_response_yn WHERE response = 'N') f_no
ON f_no.campaign_child_code = main.campaign_child_code"""
    )

    daily_cmp_table_contacted = aggregate_campaign_view_features.where(
        "contact_control_group = 'Contact group'"
    )
    daily_cmp_table_control = aggregate_campaign_view_features.where(
        "contact_control_group = 'Control group'"
    )
    expr = [
        "campaign_child_code",
        "campaign_name",
        "campaign_type",
        "campaign_sub_type",
        "campaign_category",
        "offer_category",
        "offer_type",
        "effort_condition",
        "campaign_treatment_type",
        "campaign_group",
        "response_type",
        "campaign_channel",
        "contact_control_group",
        "contact_date",
        "defined_campaign_target_group",
        "usecase",
    ]

    for col in daily_cmp_table_contacted.columns:
        if col not in (
            "campaign_child_code",
            "campaign_name",
            "campaign_type",
            "campaign_sub_type",
            "campaign_category",
            "offer_category",
            "offer_type",
            "effort_condition",
            "campaign_treatment_type",
            "campaign_group",
            "response_type",
            "campaign_channel",
            "contact_control_group",
            "contact_date",
            "defined_campaign_target_group",
            "usecase",
        ):

            expr.append(col + " as " + col + "_Targeted")
    daily_cmp_table_contacted = daily_cmp_table_contacted.selectExpr(expr)
    expr_control = [
        "campaign_child_code",
        "contact_control_group",
        "contact_date",
        "defined_campaign_target_group",
        "usecase",
    ]
    for col in daily_cmp_table_control.columns:
        if col not in (
            "campaign_child_code",
            "contact_control_group",
            "contact_date",
            "defined_campaign_target_group",
            "usecase",
        ):
            expr_control.append(col + " as " + col + "_Not_Targeted")
    daily_cmp_table_control = daily_cmp_table_control.selectExpr(expr_control)
    daily_cmp_table_contacted = daily_cmp_table_contacted.withColumnRenamed(
        "contact_control_group", "contact_group_flag_value"
    )
    daily_cmp_table_control = daily_cmp_table_control.withColumnRenamed(
        "contact_control_group", "control_group_flag_value"
    )

    known_control_group_with_different_campaign_id = {"200623-11": "200623-12"}
    for key, value in known_control_group_with_different_campaign_id.items():
        print(key, value)
        daily_cmp_table_control = daily_cmp_table_control.withColumn(
            "campaign_child_code",
            F.when(F.col("campaign_child_code") == value, key).otherwise(
                F.col("campaign_child_code")
            ),
        )
    campaign_view_report = daily_cmp_table_contacted.join(
        daily_cmp_table_control,
        [
            "campaign_child_code",
            "contact_date",
            "defined_campaign_target_group",
            "usecase",
        ],
        "left",
    )
    campaign_view_report = campaign_view_report.join(
        campaign_tracking_yn, ["campaign_child_code"], "left"
    )
    if not drop_update_table:
        campaign_view_report.createOrReplaceTempView("temp_view_load")
        spark.sql("DROP TABLE IF EXISTS nba_dev.campaign_view_report_tbl")
        spark.sql(
            """CREATE TABLE nba_dev.campaign_view_report_tbl 
            USING DELTA 
            PARTITIONED BY (contact_date) 
            AS 
            SELECT * FROM temp_view_load"""
        )
    else:
        campaign_view_report.write.format("delta").mode("append").partitionBy(
            "contact_date"
        ).saveAsTable("nba_dev.campaign_view_report_tbl")
    return campaign_view_report
