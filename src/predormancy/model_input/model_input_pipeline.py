from functools import partial
from kedro.pipeline import Pipeline, node

from predormancy.model_input.model_input_nodes import create_predormancy_target_variable

from nba.model_input.model_input_nodes import node_l5_nba_master_table


def create_predorm_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                create_predormancy_target_variable,
                inputs={
                    "prepaid_no_activity_daily": "prepaid_no_activity_daily",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info",
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                },
                outputs="l4_predorm_target_variable",
                name="predorm_target_variable",
                tags=["predorm_target_variable"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:predorm_model_input_features",
                    "l5_nba_master_table_spine": "l4_predorm_target_variable",
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "l4_touchpoints_to_call_center_features": "l4_touchpoints_to_call_center_features",
                },
                outputs="l5_predorm_master_table",
                name="l5_predorm_master_table",
                tags=["l5_predorm_master_table", "predorm_masters"],
            ),
        ]
    )
