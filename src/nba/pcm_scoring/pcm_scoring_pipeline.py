from kedro.pipeline import Pipeline, node

from nba.pcm_scoring.pcm_scoring_nodes import (
    join_c360_features_latest_date,
    l5_pcm_candidate_with_campaign_info,
)


def create_nba_pcm_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_pcm_candidate_with_campaign_info,
                inputs={
                    "pcm_candidate": "pcm_candidate",
                    "l5_nba_campaign_master": "l5_nba_campaign_master",
                },
                outputs="l5_pcm_candidate_with_campaign_info",
                name="l5_pcm_candidate_with_campaign_info",
                tags=["l5_pcm_candidate_with_campaign_info"],
            ),
            node(
                join_c360_features_latest_date,
                inputs={
                    "df_spine": "l5_pcm_candidate_with_campaign_info",
                    "subset_features": "params:nba_model_input_features",
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
                outputs="l5_pcm_scoring_master",
                name="l5_pcm_scoring_master",
                tags=["l5_pcm_scoring_master"],
            ),
            # node(
            #     l5_nba_pcm_candidate_scored,
            #     inputs={
            #         "df_master":"l5_pcm_scoring_master",
            # "l5_average_arpu_untie_lookup":"l5_average_arpu_untie_lookup",
            # "model_group_column": "params:nba_model_group_column",
            # acceptance_model_tag:
            # arpu_model_tag:
            # "pai_runs_uri": "params:nba_pai_runs_uri",
            # "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
            # "scoring_chunk_size": "params:backtesting_scoring_chunk_size",
            #
            #     },
            #     outputs="l5_pcm_candidate_with_campaign_info",
            #     name="l5_pcm_candidate_with_campaign_info",
            #     tags=["l5_pcm_candidate_with_campaign_info"],
            # ),
        ]
    )
