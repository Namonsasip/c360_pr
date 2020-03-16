from datetime import date
from datetime import timedelta
from datetime import datetime
from pyspark.sql import functions as F
from typing import Dict, Any
from pyspark.sql import DataFrame
import pandas as pd


def get_date_object_from_string(date_str):
    datetime_object = datetime.strptime(date_str, "%Y-%m-%d")
    return datetime.date(datetime_object)


def get_delta_date(date_obj, delta_day):
    delta_day = timedelta(delta_day)
    return date_obj - delta_day


def create_report_campaign_tracking_table(
    group_tbl: DataFrame,
    campaign_tbl: DataFrame,
    mapping_tbl: DataFrame,
    parameters: Dict[str, Any],
    day,
) -> DataFrame:
    tracking_day_d = get_date_object_from_string(day)
    down_scoped_date = get_delta_date(
        tracking_day_d, parameters["scope_down_delta_date"]
    )
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
        target_tbl,
        parameters,
        expr,
        end_date_str,
        aggregate_period,
    )


# def create_agg_group_by_key_over_period(
#     target_tbl: DataFrame, parameters, end_date_str, aggregate_period
# ) -> DataFrame:
#     end_date = get_date_object_from_string(end_date_str)
#     start_period = get_delta_date(
#         end_date, parameters[aggregate_period]["aggregate_window"] + 1
#     )
#     selected_period_sdf = target_tbl.filter(
#         F.col(parameters["date_col"]).between(
#             pd.to_datetime(start_period.strftime("%Y-%m-%d")),
#             pd.to_datetime(end_date.strftime("%Y-%m-%d")),
#         )
#     )
#     if parameters["mode"] == "custom_ontop":
#         expr = [
#             F.sum(parameters["aggregate_col"]).alias(
#                 parameters[aggregate_period]["alias_name"]
#             ),
#             F.sum(parameters["aggregate_col_2"]).alias(
#                 parameters[aggregate_period]["alias_name_2"]
#             ),
#         ]
#     result = selected_period_sdf.groupBy(parameters["group_by_identifiers"]).agg(*expr)
#     return result


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
