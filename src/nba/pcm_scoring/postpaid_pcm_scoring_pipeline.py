from kedro.pipeline import Pipeline, node

from nba.pcm_scoring.postpaid_pcm_scoring_nodes import (
    join_c360_postpaid_features_latest_date,
    l5_pcm_postpaid_candidate_with_campaign_info,
    l5_nba_pcm_postpaid_candidate_scored,
)


def create_nba_pcm_postpaid_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [

            node(
                l5_pcm_postpaid_candidate_with_campaign_info,
                inputs={
                    "postpaid_pcm_candidate": "pcm_candidate",
                    "l5_nba_postpaid_campaign_master": "l5_nba_postpaid_campaign_master",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "l0_campaign_tracking_contact_list_post": "l0_campaign_tracking_contact_list_post_full_load",
                    "pcm_date_min": "params: nba_postpaid_pcm_date_min",
                    "pcm_date_max": "params:nba_postpaid_pcm_date_max",
                    "postpaid_min_feature_days_lag": "params:nba_postpaid_min_feature_days_lag"
                },
                outputs="l5_pcm_postpaid_candidate_with_campaign_info",
                name="l5_pcm_postpaid_candidate_with_campaign_info",
                tags=["l5_pcm_postpaid_candidate_with_campaign_info"],
            ),
            node(
                join_c360_postpaid_features_latest_date,
                inputs={
                    "df_spine": "l5_pcm_postpaid_candidate_with_campaign_info",
                    "subset_features": "params:nba_postpaid_model_input_features",
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    # "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    # "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    # "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    # "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l0_revenue_nbo_postpaid_input_data": "l0_revenue_nbo_postpaid_input_data",
                    # "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    # "l4_streaming_visit_count_and_download_traffic_feature": "l4_streaming_visit_count_and_download_traffic_feature",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "l4_touchpoints_to_call_center_features": "l4_touchpoints_to_call_center_features",
                    # "l4_revenue_postpaid_average_by_bill_cycle": "l4_revenue_postpaid_average_by_bill_cycle",
                },
                outputs="l5_pcm_postpaid_scoring_master",
                name="l5_pcm_postpaid_scoring_master",
                tags=["l5_pcm_postpaid_scoring_master"],
            ),
            # node(
            #     l5_nba_pcm_postpaid_candidate_scored,
            #     inputs={
            #         "df_master": "l5_pcm_postpaid_scoring_master",
            #         "l5_postpaid_average_arpu_untie_lookup": "l5_postpaid_average_arpu_untie_lookup",
            #         "model_group_column": "params:nba_postpaid_model_scoring_group_column",
            #         # "prioritized_campaign_child_codes": "params:nba_prioritized_campaigns_child_codes",
            #         "nba_postpaid_model_group_column_push_campaign": "params:nba_postpaid_model_group_column_prioritized",
            #         "nba_postpaid_model_group_column_pull_campaign": "params:nba_postpaid_model_group_column_non_prioritized",
            #         # "nba_model_use_cases_child_codes": "params:nba_model_use_cases_child_codes",
            #         "acceptance_model_tag": "params:nba_postpaid_acceptance_model_tag",
            #         "arpu_model_tag": "params:nba_postpaid_arpu_model_tag",
            #         "pai_runs_uri": "params:nba_postpaid_pai_runs_uri",
            #         "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
            #         "mlflow_model_version": "params:nba_postpaid_mlflow_model_version_prediction",
            #         # "explanatory_features": "params:nba_postpaid_model_explanatory_features",
            #         "scoring_chunk_size": "params:backtesting_postpaid_scoring_chunk_size",
            #     },
            #     outputs="l5_pcm_postpaid_candidate_scored",
            #     name="l5_pcm_postpaid_candidate_scored",
            #     tags=["l5_pcm_postpaid_candidate_scored"],
            # ),
        ],
        tags="pcm_postpaid_scoring_pipeline",
    )
