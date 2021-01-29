import getpass
from functools import partial

from kedro.pipeline import Pipeline, node

from customer360.utilities.datetime_utils import get_local_datetime
from music.models.models_nodes import (
    train_multiple_models,
    create_model_function,
)


def create_music_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    train_multiple_models,
                    model_type="binary",
                    pai_run_prefix="dummy_acceptance_",
                ),
                inputs={
                    "df_master": "l5_music_master_fix_tbl",
                    "group_column": "params:music_model_group_column",
                    "explanatory_features": "params:music_model_explanatory_features",
                    "target_column": "params:music_acceptance_model_target_column",
                    "train_sampling_ratio": "params:music_model_train_sampling_ratio",
                    "model_params": "params:music_model_model_params",
                    "max_rows_per_group": "params:music_model_max_rows_per_group",
                    "min_obs_per_class_for_model": "params:music_model_min_obs_per_class_for_model",
                    "mlflow_model_version": "params:music_mlflow_model_version_training",
                    "extra_keep_columns": "params:music_extra_tag_columns_pai",
                    "pai_runs_uri": "params:music_pai_runs_uri",
                    "pai_artifacts_uri": "params:music_pai_artifacts_uri",
                },
                outputs="music_acceptance_models_train_set",
                name="music_acceptance_models_training",
                tags=["music_acceptance_models_training", "music_models"],
            ),
        ]
    )
