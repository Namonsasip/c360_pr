from kedro.pipeline import Pipeline, node

from nba.backtesting.backtesting_nodes import (
    backtest_campaign_contacts,
    l5_nba_backtesting_master_expanded_scored,
    l5_nba_backtesting_master_scored,
)


def create_nba_backtesting_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                l5_nba_backtesting_master_expanded_scored,
                inputs={
                    "df_master": "l5_nba_master_table",
                    "model_groups_to_score": "params:backtesting_model_groups_to_score",
                    "model_group_column": "params:nba_model_group_column",
                    "models_to_score": "params:backtesting_models_to_score",
                    "scoring_chunk_size": "params:backtesting_scoring_chunk_size",
                    "pai_runs_uri": "params:nba_pai_runs_uri",
                    "pai_artifacts_uri": "params:nba_pai_artifacts_uri",
                    "explanatory_features": "params:nba_model_explanatory_features",  ##TODO remove
                },
                outputs="l5_nba_backtesting_master_expanded_scored",
                name="l5_nba_backtesting_master_expanded_scored",
                tags=["l5_nba_backtesting_master_expanded_scored"],
            ),
            node(
                l5_nba_backtesting_master_scored,
                inputs={
                    "l5_nba_backtesting_master_expanded_scored": "l5_nba_backtesting_master_expanded_scored",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_nba_backtesting_master_scored",
                name="l5_nba_backtesting_master_scored",
                tags=["l5_nba_backtesting_master_scored"],
            ),
            node(
                backtest_campaign_contacts,
                inputs={
                    "df_master_with_scores": "l5_nba_backtesting_master_scored",
                    "df_master_expanded_with_scores": "l5_nba_backtesting_master_expanded_scored",
                    "model_group_column": "params:nba_model_group_column",
                },
                outputs="l5_backtesting_results",
                name="backtest_campaign_contacts",
                tags=["backtest_campaign_contacts"],
            ),
        ],
        tags="nba_backtesting",
    )
