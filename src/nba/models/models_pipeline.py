import getpass
from functools import partial

from kedro.pipeline import Pipeline, node

from customer360.utilities.datetime_utils import get_local_datetime
from nba.models.models_nodes import (
    train_multiple_binary_models,
    create_binary_model_function,
)


def create_nba_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                lambda pdf_master_chunk, pdf_extra_pai_metrics: create_binary_model_function(
                    as_pandas_udf=False,
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
                    pai_run_prefix=(
                        f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
                        f"{getpass.getuser()[0:6]}_"
                        "debug_"
                    ),
                    pai_storage_path=None,
                    pdf_extra_pai_metrics=pdf_extra_pai_metrics,
                )(
                    pdf_master_chunk
                ),
                inputs={
                    "pdf_master_chunk": "l5_nba_master_table_chunk_debug",
                    "pdf_extra_pai_metrics": "master_table_chunk_debug_extra_pai_metrics",
                },
                outputs="unused_memory_dataset_debug_model_training",
                name="debug_model_training",
                tags=["debug_model_training"],
            ),
            node(
                partial(
                    train_multiple_binary_models,
                    pai_run_prefix=(
                        f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
                        f"{getpass.getuser()[0:6]}_"
                    ),
                ),
                inputs={
                    "df_master": "l5_nba_master_table",
                    "group_column": "params:nba_model_group_column",
                    "explanatory_features": "params:nba_model_explanatory_features",
                    "target_column": "params:nba_model_target_column",
                    "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
                    "model_params": "params:nba_model_model_params",
                    "min_obs_per_class_for_model": "params:nba_model_min_obs_per_class_for_model",
                    "pai_storage_path": "params:nba_model_pai_storage_path",
                },
                outputs="unused_memory_dataset_nba_models_training",
                name="nba_models_training",
                tags=["nba_models_training"],
            ),
        ],
        tags="nba_models",
    )
