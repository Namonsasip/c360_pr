from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import functions as F
from typing import Dict, Any, List
from pyspark.sql import DataFrame
import pandas as pd
from src.customer360.utilities.spark_util import get_spark_session

spark = get_spark_session()


def get_date_object_from_string(date_str):
    return datetime.date(datetime.strptime(date_str, "%Y-%m-%d"))


def get_delta_date(date_obj, delta_day):
    return date_obj - timedelta(delta_day)


def create_report_campaign_tracking_table(
    group_tbl: DataFrame,
    campaign_tbl: DataFrame,
    mapping_tbl: DataFrame,
    parameters: Dict[str, Any],
    day: str,
) -> DataFrame:
    tracking_day_d = datetime.date(datetime.strptime(day, "%Y-%m-%d"))
    down_scoped_date = tracking_day_d - timedelta(90)
    campaign_tracking_sdf_filter = campaign_tbl.filter(
        F.col(parameters["date_filter_column"])
        > F.unix_timestamp(F.lit(down_scoped_date)).cast("timestamp")
    )
    cvm_campaign_tracking = group_tbl.join(
        campaign_tracking_sdf_filter.select(
            parameters["campaign_table_selected_columns"]
        ),
        parameters["campaign_and_group_join_keys"],
        "inner",
    ).join(mapping_tbl, parameters["campaign_mapping_join_keys"], "inner")
    return cvm_campaign_tracking


def create_user_current_size(
    group_tbl: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    current_size = group_tbl.groupby(parameters["group_by_column"]).agg(
        F.countDistinct(parameters["users_unique_identifier"]).alias(
            parameters["output_alias"]
        )
    )
    return current_size


def agg_group_by_key_over_period(
    target_tbl: DataFrame,
    parameters: Dict[str, Any],
    expr,
    end_date_str,
    aggregate_period,
):
    end_date = get_date_object_from_string(end_date_str)
    start_period = get_delta_date(
        end_date, parameters[aggregate_period]["aggregate_window"] + 1
    )
    selected_period_sdf = target_tbl.filter(
        F.col(parameters["date_col"]).between(
            pd.to_datetime(start_period.strftime("%Y-%m-%d")),
            pd.to_datetime(end_date.strftime("%Y-%m-%d")),
        )
    )
    result = selected_period_sdf.groupBy(parameters["group_by_identifiers"]).agg(*expr)
    return result


def create_report_target_user_agg_tbl(
    target_tbl: DataFrame, parameters: Dict[str, Any], end_date_str, aggregate_period
) -> DataFrame:
    expr = [
        F.count(parameters["aggregate_col"]).alias(
            parameters[aggregate_period]["alias_name"]
        ),
        F.countDistinct(parameters["aggregate_col_2"]).alias(
            parameters[aggregate_period]["alias_name_2"]
        ),
    ]
    return agg_group_by_key_over_period(
        target_tbl, parameters, expr, end_date_str, aggregate_period,
    )


def create_ontop_table(
    group_tbl: DataFrame,
    ontop_data: DataFrame,
    ontop_voice: DataFrame,
    parameters: Dict[str, Any],
    end_date_str,
) -> DataFrame:
    tracking_day_d = get_date_object_from_string(end_date_str)
    down_scoped_date = get_delta_date(
        tracking_day_d, parameters["scope_down_delta_date"]
    )
    ontop_data_filter = ontop_data.filter(
        F.col(parameters["ontop_data_date_filter_column"])
        > F.unix_timestamp(F.lit(down_scoped_date)).cast("timestamp")
    )
    ontop_voice_filter = ontop_voice.filter(
        F.col(parameters["ontop_voice_date_filter_column"])
        > F.unix_timestamp(F.lit(down_scoped_date)).cast("timestamp")
    )
    ontop_all = ontop_data_filter.select(
        parameters["ontop_data_selected_column"]
    ).union(ontop_voice_filter.select(parameters["ontop_voice_selected_column"]))
    ontop_table = ontop_all.join(group_tbl, parameters["join_keys"], "inner")
    return ontop_table


def create_agg_data_for_report(
    cvm_prepaid_customer_groups: DataFrame,
    dm996_cvm_ontop_pack: DataFrame,
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
        dm996_cvm_ontop_pack: campaign response data
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
        max(aggregate_period)
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

    #TODO join with campaign detailed info (dm996_cvm_ontop_pack)

    return df_aggregate_table
