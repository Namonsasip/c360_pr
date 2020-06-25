from kedro.pipeline import Pipeline, node

from nba.personnas_clustering.personnas_clustering_nodes import (
    personnas_clustering,
    l5_personnas_clustering_summary,
)


def create_nba_personnas_clustering_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                personnas_clustering,
                inputs={
                    "df_master": "l5_all_subscribers_master_table",
                    "clustering_features": "params:nba_personnas_clustering_features",
                    "n_pca_components": "params:nba_personnas_clustering_n_pca_components",
                    "n_clusters": "params:nba_personnas_clustering_n_clusters",
                },
                outputs=[
                    "l5_personnas_clustering_master",
                    "personnas_clustering_model",
                ],
                name="l5_personnas_clustering_master",
                tags=["l5_personnas_clustering_master"],
            ),
            node(
                l5_personnas_clustering_summary,
                inputs={
                    "l5_personnas_clustering_master": "l5_personnas_clustering_master",
                    "clustering_features": "params:nba_personnas_clustering_features",
                    "features_to_summarize": "params:nba_personnas_clustering_reporting_features",
                },
                outputs="l5_personnas_clustering_summary",
                name="l5_personnas_clustering_summary",
                tags=["l5_personnas_clustering_summary"],
            ),
        ],
        tags="personnas_clustering_pipeline",
    )
