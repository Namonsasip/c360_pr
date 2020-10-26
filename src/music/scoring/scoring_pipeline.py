from functools import partial
import datetime
from music.scoring.scoring_nodes import l5_music_lift_scoring
from kedro.pipeline import Pipeline, node


def create_music_scoring_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_music_lift_scoring,
                inputs={
                    "df_master": "l5_music_lift_fix_tbl",
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
                outputs="unused_memory_du_scored",
                name="l5_du_scored",
                tags=["l5_du_scored"],
            ),
            ]
    )
