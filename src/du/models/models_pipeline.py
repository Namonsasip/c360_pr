import getpass
from functools import partial

from kedro.pipeline import Pipeline, node

from customer360.utilities.datetime_utils import get_local_datetime
from du.models.models_nodes import (
    train_multiple_models,
    create_model_function,
)


def create_du_models_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(train_multiple_models, model_type="binary",),
                inputs={
                    "df_master": "l5_du_master_tbl",
                    "group_column": "params:du_model_group_column",
                    "explanatory_features": "params:du_model_explanatory_features",
                    "target_column": "params:du_acceptance_model_target_column",
                    "train_sampling_ratio": "params:du_model_train_sampling_ratio",
                    "model_params": "params:du_model_model_params",
                    "max_rows_per_group": "params:du_model_max_rows_per_group",
                    "min_obs_per_class_for_model": "params:du_model_min_obs_per_class_for_model",
                    "extra_keep_columns": "params:du_extra_tag_columns_pai",
                    "pai_runs_uri": "params:du_pai_runs_uri",
                    "pai_artifacts_uri": "params:du_pai_artifacts_uri",
                },
                outputs="du_acceptance_models_train_set",
                name="du_acceptance_models_training",
                tags=["du_acceptance_models_training", "du_models"],
            ),
        ],
        tags="du_models_pipeline",
    )
