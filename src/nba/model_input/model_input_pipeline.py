from kedro.pipeline import Pipeline, node

from nba.model_input.model_input_nodes import (
    node_l5_nba_master_table_spine,
    node_l5_nba_master_table,
)


def create_nba_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                node_l5_nba_master_table_spine,
                inputs={
                    "l0_campaign_tracking_contact_list_pre": "l0_campaign_tracking_contact_list_pre"
                },
                outputs="l5_nba_master_table_spine",
                name="l5_nba_master_table_spine",
                tags=["l5_nba_master_table_spine"],
            ),
            node(
                node_l5_nba_master_table,
                inputs={
                    "l5_nba_master_table_spine": "l5_nba_master_table_spine",
                    "l4_campaign_postpaid_prepaid_features": "l4_campaign_postpaid_prepaid_features",
                    "l4_campaign_top_channel_features": "l4_campaign_top_channel_features",
                    # "l4_customer_profile_ltv_to_date": "l4_customer_profile_ltv_to_date",
                },
                outputs="l5_nba_master_table",
                name="l5_nba_master_table",
                tags=["l5_nba_master_table"],
            ),
        ],
        tags="nba_model_input",
    )
