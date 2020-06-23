from functools import partial

from kedro.pipeline import Pipeline, node

from du.model_input.model_input_nodes import (
    node_l5_du_target_variable_table,
    node_l5_du_master_spine_table,
)

from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table,
)

def create_du_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(node_l5_du_target_variable_table, running_day="2020-06-01",),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "mapping_for_model_training": "mapping_for_model_training",
                },
                outputs="l5_du_target_variable_tbl",
                name="l5_du_target_variable_tbl",
                tags=["l5_du_target_variable_tbl"],
            ),
            node(
                partial(node_l5_du_master_spine_table, min_feature_days_lag=5,),
                inputs={
                    "l5_du_target_variable_tbl": "l5_du_target_variable_tbl",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                },
                outputs="l5_du_master_spine_tbl",
                name="l5_du_master_spine_tbl",
                tags=["l5_du_master_spine_tbl"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:du_model_input_features",
                    "l5_nba_master_table_spine": "l5_du_master_spine_tbl",
                    "l5_nba_customer_profile": "l5_nba_customer_profile",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    # "l4_streaming_visit_count_and_download_traffic_feature": "l4_streaming_visit_count_and_download_traffic_feature",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "l4_product_activated_deactivated_features":"l4_product_activated_deactivated_features"
                },
                outputs="l5_du_master_tbl",
                name="l5_du_master_tbl",
                tags=["l5_du_master_tbl", "du_masters"],
            ),
        ]
    )
