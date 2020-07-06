from functools import partial

from kedro.pipeline import Pipeline, node

from nba.backtesting.backtesting_nodes import (
    backtest_campaign_contacts,
    l5_nba_backtesting_master_expanded_scored,
    l5_nba_backtesting_master_scored,
    l5_nba_backtesting_pcm_eligible_spine,
    l5_nba_backtesting_master_expanded_queue_distribution_scored,
)
from nba.models.models_nodes import score_nba_models
from nba.model_input.model_input_nodes import node_l5_nba_master_table


def create_nba_backtesting_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_nba_backtesting_master_expanded_scored,
                inputs={
                    "df_master": "l5_nba_master_table",
                    "model_groups_to_score": "params:backtesting_model_groups_to_score",
                    "model_group_column": "params:nba_model_group_column",
                    "models_to_score": "params:backtesting_models_to_score",
                    "scoring_chunk_size": "params:backtesting_scoring_chunk_size",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "explanatory_features": "params:nba_model_explanatory_features",  ##TODO remove
                },
                outputs="l5_nba_backtesting_master_expanded_scored",
                name="l5_nba_backtesting_master_expanded_scored",
                tags=["l5_nba_backtesting_master_expanded_scored"],
            ),
            node(
                l5_nba_backtesting_master_scored,
                inputs={
                    "l5_nba_backtesting_master_expanded_scored": "l5_nba_backtesting_master_expanded_scored",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_nba_backtesting_master_scored",
                name="l5_nba_backtesting_master_scored",
                tags=["l5_nba_backtesting_master_scored"],
            ),
            node(
                partial(
                    l5_nba_backtesting_master_expanded_queue_distribution_scored,
                    partition_columns=["nba_spine_primary_key"],
                ),
                inputs={
                    "l5_nba_backtesting_master_expanded_scored": "l5_nba_backtesting_master_expanded_scored",
                    "queue_distribution": "params:nba_queue_distribution",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_nba_backtesting_master_expanded_queue_distribution_scored",
                name="l5_nba_backtesting_master_expanded_queue_distribution_scored",
                tags=["l5_nba_backtesting_master_expanded_queue_distribution_scored"],
            ),
            node(
                backtest_campaign_contacts,
                inputs={
                    "df_master_with_scores": "l5_nba_backtesting_master_scored",
                    "df_master_expanded_with_scores": "l5_nba_backtesting_master_expanded_scored",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_backtesting_results",
                name="backtest_campaign_contacts",
                tags=["backtest_campaign_contacts"],
            ),
            node(
                backtest_campaign_contacts,
                inputs={
                    "df_master_with_scores": "l5_nba_backtesting_master_scored",
                    "df_master_expanded_with_scores": "l5_nba_backtesting_master_expanded_queue_distribution_scored",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_backtesting_results_randomized_queue_size",
                name="backtest_campaign_contacts_randomized_queue_size",
                tags=["backtest_campaign_contacts_randomized_queue_size"],
            ),
            node(
                l5_nba_backtesting_pcm_eligible_spine,
                inputs={
                    "pcm_candidate": "pcm_candidate",
                    "l5_nba_campaign_master": "l5_nba_campaign_master",
                    "model_groups_to_score": "params:backtesting_model_groups_to_score",
                    "prioritized_campaign_child_codes": "params:nba_prioritized_campaigns_child_codes",
                    "nba_model_group_column_prioritized": "params:nba_model_group_column_prioritized",
                    "nba_model_group_column_non_prioritized": "params:nba_model_group_column_non_prioritized",
                    "min_feature_days_lag": "params:nba_min_feature_days_lag",
                },
                outputs="l5_nba_backtesting_pcm_eligible_spine",
                name="l5_nba_backtesting_pcm_eligible_spine",
                tags=["l5_nba_backtesting_pcm_eligible_spine"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:nba_model_input_features",
                    "l5_nba_master_table_spine": "l5_nba_backtesting_pcm_eligible_spine",
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
                    # "l4_streaming_visit_count_and_download_traffic_feature": "l4_streaming_visit_count_and_download_traffic_feature",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                    "l4_touchpoints_to_call_center_features": "l4_touchpoints_to_call_center_features",
                },
                outputs="l5_nba_pcm_eligible_master_table",
                name="l5_nba_pcm_eligible_master_table",
                tags=["l5_nba_pcm_eligible_master_table"],
            ),
            node(
                partial(
                    score_nba_models, primary_key_columns=["nba_spine_primary_key"]
                ),
                inputs={
                    "df_master": "l5_nba_pcm_eligible_master_table",
                    "model_group_column": "params:nba_model_group_column",
                    "models_to_score": "params:backtesting_models_to_score",
                    "scoring_chunk_size": "params:backtesting_scoring_chunk_size",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "explanatory_features": "params:nba_model_explanatory_features",
                    ##TODO remove
                },
                outputs="l5_nba_backtesting_master_expanded_scored_pcm_eligible",
                name="l5_nba_backtesting_master_expanded_scored_pcm_eligible",
                tags=["l5_nba_backtesting_master_expanded_scored_pcm_eligible"],
            ),
            node(
                backtest_campaign_contacts,
                inputs={
                    "df_master_with_scores": "l5_nba_backtesting_master_scored",
                    "df_master_expanded_with_scores": "l5_nba_backtesting_master_expanded_scored_pcm_eligible",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_backtesting_results_pcm_candidate",
                name="l5_backtesting_results_pcm_candidate",
                tags=["l5_backtesting_results_pcm_candidate"],
            ),
        ],
        tags="nba_backtesting",
    )
