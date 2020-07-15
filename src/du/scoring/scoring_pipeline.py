from kedro.pipeline import Pipeline, node

from du.scoring.scoring_nodes import l5_scoring_profile, l5_du_scored

from nba.pcm_scoring.pcm_scoring_nodes import join_c360_features_latest_date

from du.models.package_prefer_nodes import (
    create_daily_ontop_pack,
    create_aggregate_ontop_package_preference_input,
    create_ontop_package_preference,
)
from functools import partial
import datetime


def create_package_preference_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_daily_ontop_pack,
                    hive_table="l1_data_ontop_purchase_daily_hive",
                    start_date=None,
                    end_date=None,
                    drop_replace_partition=True,
                ),
                inputs={
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "ontop_pack": "dm42_promotion_prepaid",
                    "usage_feature": "dm15_mobile_usage_aggr_prepaid",
                },
                outputs="unused_memory_dataset_1",
                name="l1_data_ontop_purchase_daily",
                tags=["package_preference_data","l1_data_ontop_purchase_daily"],
            ),
            node(
                partial(
                    create_aggregate_ontop_package_preference_input,
                    hive_table="l4_data_ontop_purchase_week_hive_aggregate_feature",
                    aggregate_periods=[90, 60, 30],
                    start_date=None,
                    drop_replace_partition=False,
                ),
                inputs={
                    "l1_data_ontop_purchase_daily": "l1_data_ontop_purchase_daily_hive",
                },
                outputs="unused_memory_dataset_2",
                name="l4_data_ontop_purchase_week_hive_aggregate_feature",
                tags=[
                    "package_preference_data",
                    "l4_data_ontop_purchase_week_hive_aggregate_feature",
                ],
            ),
            node(
                partial(
                    create_ontop_package_preference,
                    hive_table="l4_data_ontop_package_preference",
                    aggregate_periods=[90, 60, 30],
                    start_date=None,
                    drop_replace_partition=False,
                ),
                inputs={
                    "l4_data_ontop_purchase_week_hive_aggregate_feature": "l4_data_ontop_purchase_week_hive_aggregate_feature",
                },
                outputs="unused_memory_dataset_3",
                name="l4_data_ontop_package_preference",
                tags=["package_preference_data", "l4_data_ontop_package_preference"],
            ),
        ],
        tags="package_preference_pipeline",
    )


def create_du_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     l5_scoring_profile,
            #     inputs={
            #         "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
            #     },
            #     outputs="l5_du_eligible_sub_to_score",
            #     name="l5_scoring_profile",
            #     tags=["l5_scoring_profile"],
            # ),
            # node(
            #     join_c360_features_latest_date,
            #     inputs={
            #         "df_spine": "l5_du_eligible_sub_to_score",
            #         "subset_features": "params:du_model_input_features",
            #         "l5_nba_customer_profile": "l5_nba_customer_profile",
            #         "l4_billing_rolling_window_topup_and_volume": "l4_billing_rolling_window_topup_and_volume",
            #         "l4_billing_rolling_window_rpu": "l4_billing_rolling_window_rpu",
            #         "l4_billing_rolling_window_rpu_roaming": "l4_billing_rolling_window_rpu_roaming",
            #         "l4_billing_rolling_window_before_top_up_balance": "l4_billing_rolling_window_before_top_up_balance",
            #         "l4_billing_rolling_window_top_up_channels": "l4_billing_rolling_window_top_up_channels",
            #         "l4_daily_feature_topup_and_volume": "l4_daily_feature_topup_and_volume",
            #         "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
            #         "l4_device_summary_features": "l4_device_summary_features",
            #         "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly": "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
            #         # "l4_streaming_visit_count_and_download_traffic_feature": "l4_streaming_visit_count_and_download_traffic_feature",
            #         "l4_usage_prepaid_postpaid_daily_features": "l4_usage_prepaid_postpaid_daily_features",
            #         "l4_usage_postpaid_prepaid_weekly_features_sum": "l4_usage_postpaid_prepaid_weekly_features_sum",
            #     },
            #     outputs="l5_du_scoring_master",
            #     name="l5_du_scoring_master",
            #     tags=["l5_du_scoring_master"],
            # ),
            node(
                l5_du_scored,
                inputs={
                    "df_master": "l5_du_scoring_master",
                    "l5_average_arpu_untie_lookup": "l5_average_arpu_untie_lookup",
                    "model_group_column": "params:du_model_scoring_group_column",
                    "explanatory_features": "params:du_model_explanatory_features",
                    "acceptance_model_tag": "params:du_acceptance_model_tag",
                    "mlflow_model_version": "params:du_mlflow_model_version_prediction",
                    "arpu_model_tag": "params:du_arpu_model_tag",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "scoring_chunk_size": "params:du_scoring_chunk_size",
                },
                outputs="l5_du_scored",
                name="l5_du_scored",
                tags=["l5_du_scored"],
            ),
        ],
        tags="du_scoring_pipeline",
    )
