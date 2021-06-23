from functools import partial

from kedro.pipeline import Pipeline, node

from nba.gender_age_imputation.gender_age_imputation_nodes import (
    l5_all_subscribers_master_table_spine,
    create_gender_age_imputation_clusters,
    l5_gender_age_imputation_clustering_summary_report,
    l5_gender_age_imputed,
)
from nba.model_input.model_input_nodes import node_l5_nba_master_table
from nba.pcm_scoring.pcm_scoring_nodes import (
    join_c360_features_latest_date,
    l5_pcm_candidate_with_campaign_info,
)


def create_nba_gender_age_imputation_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    l5_all_subscribers_master_table_spine,
                    date_min="2021-05-01",
                    date_max="2021-05-01",
                ),
                inputs={
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                },
                outputs="l5_all_subscribers_master_table_spine",
                name="l5_all_subscribers_master_table_spine",
                tags=["l5_all_subscribers_master_table_spine"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:nba_model_input_features",
                    "l5_nba_master_table_spine": "l5_all_subscribers_master_table_spine",
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
                    "l2_device_summary_with_config_weekly_full_load": "l2_device_summary_with_config_weekly_full_load",
                    "l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly":"l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly",
                },
                outputs="l5_all_subscribers_master_table",
                name="l5_all_subscribers_master_table",
                tags=["l5_all_subscribers_master_table",],
            ),
            node(
                create_gender_age_imputation_clusters,
                inputs={
                    "df_master": "l5_all_subscribers_master_table",
                    "clustering_features": "params:nba_gender_age_imputation_clustering_features",
                    "n_clusters": "params:nba_gender_age_imputation_n_clusters",
                },
                outputs="l5_gender_age_imputation_master_with_clusters",
                name="l5_gender_age_imputation_master_with_clusters",
                tags=["l5_gender_age_imputation_master_with_clusters"],
            ),
            node(
                l5_gender_age_imputed,
                inputs={
                    "l5_gender_age_imputation_master_with_clusters": "l5_gender_age_imputation_master_with_clusters",
                },
                outputs="l5_gender_age_imputed",
                name="l5_gender_age_imputed",
                tags=["l5_gender_age_imputed"],
            ),
            node(
                l5_gender_age_imputation_clustering_summary_report,
                inputs={"l5_gender_age_imputed": "l5_gender_age_imputed",},
                outputs="l5_gender_age_imputation_clustering_summary_report",
                name="l5_gender_age_imputation_clustering_summary_report",
                tags=["l5_gender_age_imputation_clustering_summary_report"],
            ),
        ],
        tags="gender_age_imputation",
    )
