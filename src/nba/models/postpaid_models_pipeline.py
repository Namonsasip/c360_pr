import getpass
from functools import partial

from kedro.pipeline import Pipeline, node

from customer360.utilities.datetime_utils import get_local_datetime
from nba.models.models_nodes import (
    train_multiple_models,
    create_model_function,
)

from nba.models.ngcm_model_nodes import create_ngcm_nba_model_classifier


def create_nba_postpaid_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     create_ngcm_nba_model_classifier,
            #     inputs={
            #         "l5_nba_master_table": "l5_nba_master_table",
            #         "nba_model_group_column": "params:nba_model_group_column",
            #         "target_column": "params:nba_acceptance_model_target_column",
            #         "max_rows_per_group": "params:nba_model_max_rows_per_group",
            #         "model_params": "params:nba_model_model_params",
            #         "extra_keep_columns": "params:nba_extra_tag_columns_pai",
            #         "explanatory_features": "params:nba_model_explanatory_features",
            #         "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
            #         "nba_model_min_obs_per_class_for_model": "params:nba_model_min_obs_per_class_for_model",
            #     },
            #     outputs="unused_memory",
            #     name="create_ngcm_nba_model_classifier",
            #     tags=["create_ngcm_nba_model_classifier",],
            # ),
            # node(
            #     lambda pdf_master_chunk, pdf_extra_pai_metrics: create_model_function(
            #         as_pandas_udf=False,
            #         model_type="binary",
            #         group_column="campaign_child_code",
            #         explanatory_features=[
            #             "sum_payments_arpu_gprs_last_three_month",
            #             "sum_payments_arpu_voice_last_month",
            #         ],
            #         target_column="target_response",
            #         train_sampling_ratio=0.8,
            #         model_params={
            #             "num_leaves": 16,
            #             "learning_rate": 0.05,
            #             "n_estimators": 10,
            #             "min_gain_to_split": 0.0,
            #             "random_state": 123456,
            #             "importance_type": "gain",
            #         },
            #         min_obs_per_class_for_model=100,
            #         pai_run_prefix=(
            #             f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
            #             f"acceptance_"
            #             f"{getpass.getuser()[0:6]}_"
            #             "debug_"
            #         ),
            #         pai_storage_path=None,
            #         pdf_extra_pai_metrics=pdf_extra_pai_metrics,
            #     )(pdf_master_chunk),
            #     inputs={
            #         "pdf_master_chunk": "l5_nba_master_table_chunk_debug_acceptance",
            #         "pdf_extra_pai_metrics": "master_table_chunk_debug_extra_pai_metrics_acceptance",
            #     },
            #     outputs="unused_memory_dataset_debug_model_training_acceptance",
            #     name="debug_model_training_acceptance",
            #     tags=["debug_model_training_acceptance"],
            # ),
            # node(
            #     lambda pdf_master_chunk, pdf_extra_pai_metrics: create_model_function(
            #         as_pandas_udf=False,
            #         model_type="regression",
            #         group_column="campaign_child_code",
            #         explanatory_features=[
            #             "sum_usg_outgoing_total_call_duration_sum_weekly_last_four_week",
            #             "sum_usg_outgoing_total_sms_sum_weekly_last_week",
            #         ],
            #         target_column="target_relative_arpu_increase_30d",
            #         train_sampling_ratio=0.8,
            #         model_params={
            #             "num_leaves": 16,
            #             "learning_rate": 0.05,
            #             "n_estimators": 10,
            #             "min_gain_to_split": 0.0,
            #             "random_state": 123456,
            #             "importance_type": "gain",
            #         },
            #         min_obs_per_class_for_model=100,
            #         pai_run_prefix=(
            #             f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
            #             f"arpu"
            #             f"{getpass.getuser()[0:6]}_"
            #             "debug_"
            #         ),
            #         pai_storage_path=None,
            #         pdf_extra_pai_metrics=pdf_extra_pai_metrics,
            #     )(pdf_master_chunk),
            #     inputs={
            #         "pdf_master_chunk": "l5_nba_master_table_chunk_debug_arpu",
            #         "pdf_extra_pai_metrics": "master_table_chunk_debug_extra_pai_metrics_arpu",
            #     },
            #     outputs="unused_memory_dataset_debug_model_training_arpu",
            #     name="debug_model_training_arpu",
            #     tags=["debug_model_training_arpu"],
            # ),
            # node(
            #     partial(
            #         train_multiple_models,
            #         model_type="binary",
            #         pai_run_prefix=(
            #             f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
            #             f"acceptance_"
            #             f"thanakse_"
            #         ),
            #     ),
            #     inputs={
            #         "df_master": "l5_nba_postpaid_master_table",
            #         "group_column": "params:nba_postpaid_model_group_binary_column",
            #         "explanatory_features": "params:nba_postpaid_model_explanatory_features",
            #         "target_column": "params:nba_postpaid_acceptance_model_target_column",
            #         "train_sampling_ratio": "params:nba_postpaid_model_train_sampling_ratio",
            #         "model_params": "params:nba_postpaid_model_model_params",
            #         "max_rows_per_group": "params:nba_postpaid_model_max_rows_per_group",
            #         "min_obs_per_class_for_model": "params:nba_postpaid_model_min_obs_per_class_for_model",
            #         "mlflow_model_version": "params:nba_postpaid_mlflow_model_version_training",
            #         "mlflow_path": "params:nba_postpaid_mlflow_path",
            #         "extra_keep_columns": "params:nba_postpaid_extra_tag_columns_pai",
            #         "pai_runs_uri": "params:nba_postpaid_pai_runs_uri",
            #         "pai_artifacts_uri": "params:nba_postpaid_pai_artifacts_uri",
            #     },
            #     outputs="nba_postpaid_acceptance_models_train_set",
            #     name="nba_postpaid_acceptance_models_training",
            #     tags=["nba_postpaid_acceptance_models_training", "nba_postpaid_models"],
            # ),
            node(
                partial(
                    train_multiple_models,
                    model_type="regression",
                    pai_run_prefix=(
                        f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
                        f"arpu_uplift_"
                        f"thanakse_"
                    ),
                ),
                inputs={
                    "df_master": "l5_nba_postpaid_master_table_only_accepted",
                    "group_column": "params:nba_postpaid_model_group_regression_column",
                    "explanatory_features": "params:nba_postpaid_model_explanatory_features",
                    "target_column": "params:nba_postpaid_revenue_model_target_column",
                    "train_sampling_ratio": "params:nba_postpaid_model_train_sampling_ratio",
                    "model_params": "params:nba_postpaid_model_model_params",
                    "max_rows_per_group": "params:nba_postpaid_model_max_rows_per_group",
                    "min_obs_per_class_for_model": "params:nba_postpaid_model_min_obs_per_class_for_model",
                    "mlflow_model_version": "params:nba_postpaid_mlflow_model_version_training",
                    "mlflow_path": "params:nba_postpaid_mlflow_path",
                    "extra_keep_columns": "params:nba_postpaid_extra_tag_columns_pai",
                    "pai_runs_uri": "params:nba_postpaid_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_postpaid_pai_artifacts_uri",
                    "regression_clip_target_quantiles": "params:postpaid_regression_clip_target_quantiles_arpu",
                },
                outputs="nba_postpaid_revenue_uplift_models_train_set",
                name="nba_postpaid_revenue_uplift_models_training",
                tags=["nba_postpaid_revenue_uplift_models_training", "nba_postpaid_models"],
            ),
            # node(
            #     partial(
            #         train_multiple_models,
            #         model_type="regression",
            #         pai_run_prefix=(
            #             f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
            #             f"arpu_7d_"
            #             f"thanasiy_"
            #         ),
            #     ),
            #     inputs={
            #         "df_master": "l5_nba_master_table_only_accepted",
            #         "group_column": "params:nba_model_group_column",
            #         "explanatory_features": "params:nba_model_explanatory_features",
            #         "target_column": "params:nba_arpu_7d_model_target_column",
            #         "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
            #         "model_params": "params:nba_model_model_params",
            #         "max_rows_per_group": "params:nba_model_max_rows_per_group",
            #         "min_obs_per_class_for_model": "params:nba_model_min_obs_per_class_for_model",
            #         "mlflow_model_version": "params:nba_mlflow_model_version_training",
            #         "extra_keep_columns": "params:nba_extra_tag_columns_pai",
            #         "pai_runs_uri": "params:nba_pai_runs_uri",
            #         "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
            #         "regression_clip_target_quantiles": "params:regression_clip_target_quantiles_arpu",
            #     },
            #     outputs="nba_arpu_7d_models_train_set",
            #     name="nba_arpu_7d_models_training",
            #     tags=["nba_arpu_7d_models_training", "nba_models"],
            # ),
        ],
        tags="nba_postpaid_models_pipeline",
    )
