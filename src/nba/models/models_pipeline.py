from functools import partial

from kedro.pipeline import Pipeline, node

from nba.models.models_nodes import train_single_binary_model


def create_nba_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    train_single_binary_model,
                    group_column="campaign_child_code",
                    explanatory_features=[
                        "min_campaign_total_by_sms_sum_weekly_last_week",
                        "min_campaign_total_others_by_ussd_sum_weekly_last_week",
                        "max_campaign_total_by_sms_sum_weekly_last_week",
                        "max_campaign_total_by_sms_sum_weekly_last_twelve_week",
                        "sum_campaign_total_by_call_center_sum_weekly_last_four_week_over_twelve_weeks",
                        "avg_campaign_total_by_sms_sum_weekly_last_twelve_week",
                        "avg_campaign_total_by_sms_sum_weekly_last_week",
                        "avg_campaign_overall_count_sum_weekly_last_week",
                        "avg_campaign_overall_count_sum_weekly_last_two_week",
                    ],
                    target_column="target_response",
                    train_sampling_ratio=0.8,
                    model_params={
                        "num_leaves": 16,
                        "learning_rate": 0.05,
                        "n_estimators": 10,
                        "min_gain_to_split": 0.0,
                        "random_state": 123456,
                        "importance_type": "gain",
                    },
                    min_obs_per_class_for_model=100,
                    pai_storage_path=None,
                ),
                inputs={
                    "pdf_master_chunk": "l5_nba_master_table_chunk_debug",
                    "pdf_extra_pai_metrics": "master_table_chunk_debug_extra_pai_metrics",
                },
                outputs=None,
                name="debug_model_training",
                tags=["debug_model_training"],
            ),
        ],
        tags="nba_models",
    )
