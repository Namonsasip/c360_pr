from kedro.pipeline import Pipeline, node

from du.scoring.scoring_nodes import (
    l5_scoring_profile,
)

from nba.pcm_scoring.pcm_scoring_nodes import (
    join_c360_features_latest_date,)


def create_du_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_scoring_profile,
                inputs={
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                },
                outputs="l5_du_eligible_sub_to_score",
                name="l5_scoring_profile",
                tags=["l5_scoring_profile"],
            ),
            node(
                join_c360_features_latest_date,
                inputs={
                    "df_spine": "l5_du_eligible_sub_to_score",
                    "subset_features": "params:du_model_input_features",
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
                outputs="l5_du_scoring_master",
                name="l5_du_scoring_master",
                tags=["l5_du_scoring_master"],
            ),
        ],
        tags="du_scoring_pipeline",
    )
