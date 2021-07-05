from functools import partial
import datetime
from music.scoring.scoring_nodes import (
    l5_music_lift_scoring_new_sub,
    l5_music_lift_scoring_existing,
)
from kedro.pipeline import Pipeline, node
from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table,
    node_l5_nba_customer_profile,
)
from nba.pcm_scoring.pcm_scoring_nodes import join_c360_features_latest_date
from du.scoring.scoring_nodes import (
    l5_scoring_profile,
    l5_du_scored,
    du_join_preference,
)


def create_music_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     node_l5_nba_customer_profile,
            #     inputs={
            #         "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
            #     },
            #     outputs="l0_music_customer_profile",
            #     name="l0_music_customer_profile",
            #     tags=["l0_music_customer_profile"],
            # ),
            node(
                l5_scoring_profile,
                inputs={
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                },
                outputs="l5_music_eligible_sub_to_score",
                name="l5_scoring_profile",
                tags=["l5_scoring_profile"],
            ),
            node(
                join_c360_features_latest_date,
                inputs={
                    "df_spine": "l5_music_eligible_sub_to_score",
                    # "unused_memory_fix_id":"unused_memory_fix_id",
                    "subset_features": "params:music_model_input_features",
                    "l5_nba_customer_profile": "l0_music_customer_profile",
                    "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
                    "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
                    "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
                    "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
                    "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
                    "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_device_summary_features": "l4_device_summary_features",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
                    "l4_streaming_fav_youtube_video_streaming_day_of_week_feature": "l4_streaming_fav_youtube_video_streaming_day_of_week_feature",
                    "l4_streaming_fav_joox_music_streaming_day_of_week_feature":"l4_streaming_fav_joox_music_streaming_day_of_week_feature",
                    "l4_streaming_fav_spotify_music_streaming_day_of_week_feature":"l4_streaming_fav_spotify_music_streaming_day_of_week_feature",
                    "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
                    "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
                },
                outputs="l5_music_scoring_master",
                name="l5_music_scoring_master",
                tags=["l5_music_scoring_master"],
            ),
            node(
                l5_music_lift_scoring_new_sub,
                inputs={
                    "df_master": "l5_music_scoring_master",
                    "l5_average_arpu_untie_lookup": "l5_average_arpu_untie_lookup",
                    "model_group_column": "params:music_model_group_column",
                    "explanatory_features": "params:music_model_explanatory_features",
                    "acceptance_model_tag": "params:music_acceptance_model_tag",
                    "mlflow_model_version": "params:music_mlflow_model_version_prediction",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "scoring_chunk_size": "params:music_scoring_chunk_size",
                },
                outputs="unused_memory_du_scored_new",
                name="l5_music_lift_scoring_new_sub",
                tags=["l5_music_lift_scoring_new_sub"],
            ),
            node(
                l5_music_lift_scoring_existing,
                inputs={
                    "df_master": "l5_music_scoring_master",
                    "l5_average_arpu_untie_lookup": "l5_average_arpu_untie_lookup",
                    "model_group_column": "params:music_model_group_column",
                    "explanatory_features": "params:music_model_explanatory_features",
                    "acceptance_model_tag": "params:music_acceptance_model_tag",
                    "mlflow_model_version": "params:music_mlflow_model_version_prediction",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "scoring_chunk_size": "params:music_scoring_chunk_size",
                },
                outputs="unused_memory_du_scored_existing",
                name="l5_music_lift_scoring_existing",
                tags=["l5_music_lift_scoring_existing"],
            ),
        ]
    )
