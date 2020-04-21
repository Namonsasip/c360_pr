from functools import partial

from kedro.pipeline import Pipeline, node

from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table_spine,
    node_l5_nba_master_table,
    node_l5_nba_master_table_only_accepted,
    node_l5_nba_master_table_chunk_debug_arpu,
    node_l5_nba_master_table_chunk_debug_acceptance,
    node_l5_nba_customer_profile,
)


def create_nba_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                node_l5_nba_customer_profile,
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                },
                outputs="l5_nba_customer_profile",
                name="l5_nba_customer_profile",
                tags=["l5_nba_customer_profile"],
            ),
            node(
                node_l5_nba_master_table_spine,
                inputs={
                    "l0_campaign_tracking_contact_list_pre": "l0_campaign_tracking_contact_list_pre_full_load",
                    "campaign_history_master_active": "campaign_history_master_active",
                    "important_campaigns": "params:nba_prioritized_campaigns_child_codes",
                    "reporting_kpis": "reporting_kpis",
                    "min_feature_days_lag": "params:nba_min_feature_days_lag",
                },
                outputs="l5_nba_master_table_spine",
                name="l5_nba_master_table_spine",
                tags=["l5_nba_master_table_spine"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:nba_model_input_features",
                    "l5_nba_master_table_spine": "l5_nba_master_table_spine",
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
                },
                outputs="l5_nba_master_table",
                name="l5_nba_master_table",
                tags=["l5_nba_master_table"],
            ),
            node(
                node_l5_nba_master_table_only_accepted,
                inputs={"l5_nba_master_table": "l5_nba_master_table"},
                outputs="l5_nba_master_table_only_accepted",
                name="l5_nba_master_table_only_accepted",
                tags=["l5_nba_master_table_only_accepted"],
            ),
            node(
                partial(
                    node_l5_nba_master_table_chunk_debug_acceptance,
                    child_code="1-63919285101",
                    sampling_rate=1e-2,
                ),
                inputs={"l5_nba_master_table": "l5_nba_master_table",},
                outputs=[
                    "l5_nba_master_table_chunk_debug_acceptance",
                    "master_table_chunk_debug_extra_pai_metrics_acceptance",
                ],
                name="l5_nba_master_table_chunk_debug_acceptance",
                tags=["l5_nba_master_table_chunk_debug_acceptance",],
            ),
            node(
                partial(
                    node_l5_nba_master_table_chunk_debug_arpu,
                    child_code="1-63919285101",
                    sampling_rate=1e-1,
                ),
                inputs={
                    "l5_nba_master_table_only_accepted": "l5_nba_master_table_only_accepted",
                },
                outputs=[
                    "l5_nba_master_table_chunk_debug_arpu",
                    "master_table_chunk_debug_extra_pai_metrics_arpu",
                ],
                name="l5_nba_master_table_chunk_debug_arpu",
                tags=["l5_nba_master_table_chunk_debug_arpu",],
            ),
        ],
        tags="nba_model_input",
    )
