from functools import partial

from du.models.models_nodes import (
    train_multiple_models,
    train_disney_models,
    calculate_feature_importance,
    get_top_features
)
from kedro.pipeline import Pipeline, node


def create_du_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     calculate_feature_importance,
            #     inputs={
            #         "df_master": "l5_du_master_tbl",
            #         "model_params": "params:du_model_model_params",
            #         "binary_target_column": "params:du_acceptance_model_target_column",
            #         "regression_target_column": "params:du_arpu_30d_model_target_column",
            #         "train_sampling_ratio": "params:du_model_train_sampling_ratio",
            #         "model_type": "params:du_acceptance_model_tag",
            #         "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
            #     },
            #     outputs="feature_importance_binary_model",
            #     name="du_acceptance_models_feature_importance",
            #     tags=["du_acceptance_models_feature_importance", "du_models"]
            # ),
            # node(
            #     calculate_feature_importance,
            #     inputs={
            #         "df_master": "l5_du_master_tbl",
            #         "model_params": "params:du_model_model_params",
            #         "binary_target_column": "params:du_acceptance_model_target_column",
            #         "regression_target_column": "params:du_arpu_30d_model_target_column",
            #         "train_sampling_ratio": "params:du_model_train_sampling_ratio",
            #         "model_type": "params:du_arpu_model_tag",
            #         "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
            #     },
            #     outputs="feature_importance_regression_model",
            #     name="du_arpu_30d_models_feature_importance",
            #     tags=["du_arpu_30d_models_feature_importance", "du_models"]
            # ),
            node(
                partial(
                    train_multiple_models,
                    model_type="binary",
                    pai_run_prefix="dummy_acceptance_",
                    undersampling=True
                ),
                inputs={
                    "df_master": "l5_du_master_tbl",
                    "df_disney": "disneyplus_train_set_july",
                    "group_column": "params:du_model_group_column",
                    "target_column": "params:du_acceptance_model_target_column",
                    "train_sampling_ratio": "params:du_model_train_sampling_ratio",
                    "model_params": "params:du_model_model_params",
                    "max_rows_per_group": "params:du_model_max_rows_per_group",
                    "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
                    "mlflow_model_version": "params:du_mlflow_model_version_training",
                    "extra_keep_columns": "params:du_extra_tag_columns_pai",
                    "pai_runs_uri": "params:du_pai_runs_uri",
                    "pai_artifacts_uri": "params:du_pai_artifacts_uri",
                    "du_top_features": "feature_importance_binary_model"
                },
                outputs="du_acceptance_models_train_set",
                name="du_acceptance_models_training",
                tags=["du_acceptance_models_training", "du_models"],
            ),
            # node(
            #     partial(
            #         train_disney_models,
            #         model_type="binary",
            #         pai_run_prefix="dummy_acceptance_",
            #     ),
            #     inputs={
            #         "df_disney": "disneyplus_train_set_july",
            #         "group_column": "params:du_model_group_column",
            #         "target_column": "params:du_acceptance_model_target_column",
            #         "train_sampling_ratio": "params:du_model_train_sampling_ratio",
            #         "model_params": "params:du_model_model_params",
            #         "max_rows_per_group": "params:du_model_max_rows_per_group",
            #         "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
            #         "mlflow_model_version": "params:disney_mlflow_model_version_training",
            #         "extra_keep_columns": "params:du_extra_tag_columns_pai",
            #         "pai_runs_uri": "params:du_pai_runs_uri",
            #         "pai_artifacts_uri": "params:du_pai_artifacts_uri",
            #         "du_top_features": "feature_importance_binary_model"
            #     },
            #     outputs="disney_acceptance_models_train_set",
            #     name="disney_acceptance_models_training",
            #     tags=["disney_acceptance_models_training", "du_models"],
            # ),

            # node(
            #     partial(
            #         train_multiple_models,
            #         model_type="regression",
            #         pai_run_prefix="dummy_regression_",
            #         undersampling=False
            #     ),
            #     inputs={
            #         "df_master": "l5_du_master_table_only_accepted",
            #         "group_column": "params:du_model_group_column",
            #         "target_column": "params:du_arpu_30d_model_target_column",
            #         "train_sampling_ratio": "params:du_model_train_sampling_ratio",
            #         "model_params": "params:du_model_model_params",
            #         "max_rows_per_group": "params:du_model_max_rows_per_group",
            #         "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
            #         "mlflow_model_version": "params:du_mlflow_model_version_training",
            #         "extra_keep_columns": "params:du_extra_tag_columns_pai",
            #         "pai_runs_uri": "params:du_pai_runs_uri",
            #         "pai_artifacts_uri": "params:du_pai_artifacts_uri",
            #         "regression_clip_target_quantiles": "params:du_regression_clip_target_quantiles_arpuu",
            #         "du_top_features": "feature_importance_regression_model"
            #     },
            #     outputs="du_arpu_30d_models_train_set",
            #     name="du_arpu_30d_models_training",
            #     tags=["du_arpu_30d_models_training", "du_models"],
            # ),
            # node(
            #     lambda pdf_master_chunk, pdf_extra_pai_metrics: create_model_function(
            #         as_pandas_udf=False,
            #         model_type="binary",
            #         group_column="rework_macro_product",
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
            #         pai_run_prefix="dummy_acceptance_",
            #         pdf_extra_pai_metrics=pdf_extra_pai_metrics,
            #         pai_runs_uri="dbfs:/mnt/customer360-blob-data/DU/dev/",
            #         pai_artifacts_uri="/dbfs/mnt/customer360-blob-data/DU/dev/",
            #         extra_tag_columns=[
            #             "Package_name",
            #             "campaign_child_code",
            #             "macro_product",
            #             "Discount_predefine_range",
            #         ],
            #     )(pdf_master_chunk),
            #     inputs={
            #         "pdf_master_chunk": "l5_du_master_table_chunk_debug_acceptance",
            #         "pdf_extra_pai_metrics": "master_table_chunk_debug_extra_pai_metrics_acceptance_du",
            #     },
            #     outputs="unused_memory_dataset_debug_model_training_acceptance",
            #     name="debug_model_training_acceptance",
            #     tags=["debug_model_training_acceptance"],
            # ),
        ],
        tags="du_models_pipeline",
    )
