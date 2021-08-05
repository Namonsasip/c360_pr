import getpass
from functools import partial
from kedro.pipeline import Pipeline, node
from customer360.utilities.datetime_utils import get_local_datetime
from nba.models.models_nodes import (
    train_multiple_models,
    calculate_feature_importance
)

from nba.models.ngcm_model_nodes import create_ngcm_nba_model_classifier


def create_nba_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     calculate_feature_importance,
            #     inputs={
            #         "df_master": "l5_nba_master_table",
            #         "group_column": "params:nba_model_group_column_prioritized",
            #         "explanatory_features": "params:nba_model_explanatory_features",
            #         "binary_target_column": "params:nba_acceptance_model_target_column",
            #         "regression_target_column": "params:nba_arpu_30d_model_target_column",
            #         "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
            #         "model_params": "params:nba_model_model_params",
            #         "model_type": "params:nba_acceptance_model_tag",
            #         "campaigns_child_codes_list": "params:nba_prioritized_campaigns_child_codes",
            #         "filepath": "params:nba_binary_top_features_path"
            #     },
            #     outputs="nba_feature_importance_binary_model",
            #     name="nba_acceptance_models_feature_importance",
            #     tags=["nba_acceptance_models_feature_importance", "nba_models"]
            # ),
            node(
                    calculate_feature_importance,
                    inputs={
                        "df_master": "l5_nba_master_table_only_accepted",
                        "group_column": "params:nba_model_group_column_prioritized",
                        "explanatory_features": "params:nba_model_explanatory_features",
                        "binary_target_column": "params:nba_acceptance_model_target_column",
                        "regression_target_column": "params:nba_arpu_30d_model_target_column",
                        "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
                        "model_params": "params:nba_model_model_params",
                        "model_type": "params:nba_arpu_model_tag",
                        "campaigns_child_codes_list": "params:nba_prioritized_campaigns_child_codes",
                        "filepath": "params:nba_regression_top_features_path"
                    },
                outputs="nba_feature_importance_regression_model",
                name="nba_arpu_30d_models_feature_importance",
                tags=["nba_arpu_30d_models_feature_importance", "nba_models"]
            ),
            # node(
            #     partial(
            #         train_multiple_models,
            #         model_type="binary",
            #         pai_run_prefix=(
            #             f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
            #             f"acceptance_"
            #             f"sitticsr"
            #         ),
            #         undersampling=True
            #     ),
            #     inputs={
            #         "df_master": "l5_nba_master_table",
            #         "group_column": "params:nba_model_group_column_prioritized",
            #         "target_column": "params:nba_acceptance_model_target_column",
            #         "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
            #         "model_params": "params:nba_model_model_params",
            #         "max_rows_per_group": "params:nba_model_max_rows_per_group",
            #         "minimun_row": "params:nba_model_min_obs_per_class",
            #         "min_obs_per_class_for_model": "params:nba_model_min_obs_per_class_for_model",
            #         "mlflow_model_version": "params:nba_mlflow_model_version_training",
            #         "extra_keep_columns": "params:nba_extra_tag_columns_pai",
            #         "pai_runs_uri": "params:nba_pai_runs_uri",
            #         "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
            #         "campaigns_child_codes_list": "params:nba_prioritized_campaigns_child_codes",
            #         "nba_top_features": "nba_feature_importance_binary_model"
            #     },
            #     outputs="nba_acceptance_models_train_set",
            #     name="nba_acceptance_models_training",
            #     tags=["nba_acceptance_models_training", "nba_models"],
            # ),
            node(
                partial(
                    train_multiple_models,
                    model_type="regression",
                    pai_run_prefix=(
                        f"{get_local_datetime().strftime('%Y%m%d_%H%M%S')}_"
                        f"arpu_30d_"
                        f"sitticsr"
                    ),
                    undersampling=True
                ),
                inputs={
                    "df_master": "l5_nba_master_table_only_accepted",
                    "group_column": "params:nba_model_group_column_prioritized",
                    "target_column": "params:nba_arpu_30d_model_target_column",
                    "train_sampling_ratio": "params:nba_model_train_sampling_ratio",
                    "model_params": "params:nba_model_model_params",
                    "max_rows_per_group": "params:nba_model_max_rows_per_group",
                    "minimun_row": "params:nba_model_min_obs_per_class",
                    "min_obs_per_class_for_model": "params:nba_model_min_obs_per_class_for_model",
                    "mlflow_model_version": "params:nba_mlflow_model_version_training",
                    "extra_keep_columns": "params:nba_extra_tag_columns_pai",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "regression_clip_target_quantiles": "params:regression_clip_target_quantiles_arpu",
                    "campaigns_child_codes_list": "params:nba_prioritized_campaigns_child_codes",
                    "nba_top_features": "nba_feature_importance_regression_model"
                },
                outputs="nba_arpu_30d_models_train_set",
                name="nba_arpu_30d_models_training",
                tags=["nba_arpu_30d_models_training", "nba_models"],
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
        tags="nba_models_pipeline",
    )
