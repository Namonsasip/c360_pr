from functools import partial

from kedro.pipeline import Pipeline, node

from music.model_input.model_input_nodes import (
    node_l0_calling_melody_campaign_target_variable_table,
    node_l5_music_master_spine_table,
node_l0_calling_melody_campaign_lift_table,
)

from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table,
    node_l5_nba_customer_profile,
)


def create_calling_melody_propensity_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                node_l5_nba_customer_profile,
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                },
                outputs="l0_music_customer_profile",
                name="l0_music_customer_profile",
                tags=["l0_music_customer_profile"],
            ),
            # node(
            #     partial(
            #         node_l0_calling_melody_campaign_target_variable_table,
            #         start_date="2020-03-01",
            #         end_date="2020-08-01",
            #     ),
            #     inputs={
            #         "daily_response_music_campaign": "daily_response_music_campaign",
            #         "dm07_sub_clnt_info": "dm07_sub_clnt_info"
            #     },
            #     outputs="l0_calling_melody_campaign_target_variable_table",
            #     name="l0_calling_melody_campaign_target_variable_table",
            #     tags=["l0_calling_melody_campaign_target_variable_table"],
            # ),
            # node(
            #     partial(node_l5_music_master_spine_table, min_feature_days_lag=10, ),
            #     inputs={
            #         "l0_calling_melody_campaign_target_variable_table": "l0_calling_melody_campaign_target_variable_table",
            #         "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
            #         "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
            #     },
            #     outputs="l5_music_master_spine_table",
            #     name="l5_music_master_spine_table",
            #     tags=["l5_music_master_spine_table"],
            # ),
            # node(
            #     node_l5_nba_master_table,
            #     inputs={
            #         "subset_features": "params:music_model_input_features",
            #         "l5_nba_master_table_spine": "l5_music_master_spine_table",
            #         "l0_music_customer_profile": "l0_music_customer_profile",
            #         "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
            #         "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
            #         "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
            #         "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
            #         "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
            #         "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
            #         "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
            #         "l4_device_summary_features": "l4_device_summary_features",
            #         "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
            #         "l4_streaming_fav_youtube_video_streaming_day_of_week_feature": "l4_streaming_fav_youtube_video_streaming_day_of_week_feature",
            #         "l4_streaming_fav_joox_music_streaming_day_of_week_feature":"l4_streaming_fav_joox_music_streaming_day_of_week_feature",
            #         "l4_streaming_fav_spotify_music_streaming_day_of_week_feature":"l4_streaming_fav_spotify_music_streaming_day_of_week_feature",
            #         "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
            #         "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
            #     },
            #     outputs="l5_music_master_tbl",
            #     name="l5_music_master_tbl",
            #     tags=["l5_music_master_tbl", "music_masters"],
            # ),
            node(
                partial(
                    node_l0_calling_melody_campaign_lift_table,
                    start_date="2020-09-01",
                    end_date="2020-10-15",
                ),
                inputs={
                    "daily_response_music_campaign": "daily_response_music_campaign",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info"
                },
                outputs="l0_calling_melody_campaign_lift_table",
                name="l0_calling_melody_campaign_lift_table",
                tags=["l0_calling_melody_campaign_lift_table"],
            ),
            node(
                partial(node_l5_music_master_spine_table, min_feature_days_lag=10, ),
                inputs={
                    "l0_calling_melody_campaign_target_variable_table": "l0_calling_melody_campaign_lift_table",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "l4_revenue_prepaid_daily_features": "l4_revenue_prepaid_daily_features",
                },
                outputs="l5_music_lift_spine_table",
                name="l5_music_lift_spine_table",
                tags=["l5_music_lift_spine_table"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "subset_features": "params:music_model_input_features",
                    "l5_nba_master_table_spine": "l5_music_lift_spine_table",
                    "l0_music_customer_profile": "l0_music_customer_profile",
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
                outputs="l5_music_lift_tbl",
                name="l5_music_lift_tbl",
                tags=["l5_music_lift_tbl", "music_masters"],
            ),
        ]
    )
