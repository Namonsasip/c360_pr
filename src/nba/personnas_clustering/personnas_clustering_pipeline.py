from kedro.pipeline import Pipeline, node

from nba.personnas_clustering.personnas_clustering_nodes import (
    personnas_clustering,
    l5_personnas_clustering_summary,
    l5_all_subscribers_master_table_customer_level,
)


def create_nba_personnas_clustering_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_all_subscribers_master_table_customer_level,
                inputs={
                    "df_master": "l5_all_subscribers_master_table",
                    "l5_customer_ids": "l5_customer_ids",
                    "l4_streaming_visit_count_and_download_traffic_feature_full_load_data_blob": "l4_streaming_visit_count_and_download_traffic_feature_full_load_data_blob",
                    "subset_features": "params:nba_model_input_features",
                },
                outputs="l5_all_subscribers_master_table_customer_level",
                name="l5_all_subscribers_master_table_customer_level",
                tags=["l5_all_subscribers_master_table_customer_level"],
            ),
            node(
                personnas_clustering,
                inputs={
                    "df_master": "l5_all_subscribers_master_table_customer_level",
                    "clustering_features": "params:nba_personnas_clustering_features",
                    "n_pca_components": "params:nba_personnas_clustering_n_pca_components",
                    "n_clusters": "params:nba_personnas_clustering_n_clusters",
                },
                outputs=[
                    "l5_personnas_clustering_master_customer_level",
                    "personnas_clustering_model",
                ],
                name="l5_personnas_clustering_master_customer_level",
                tags=["l5_personnas_clustering_master_customer_level"],
            ),
            node(
                l5_personnas_clustering_summary,
                inputs={
                    "l5_personnas_clustering_master": "l5_personnas_clustering_master_customer_level",
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
