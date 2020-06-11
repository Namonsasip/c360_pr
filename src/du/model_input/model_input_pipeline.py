from functools import partial

from kedro.pipeline import Pipeline, node

from du.model_input.model_input_nodes import node_l5_du_target_variable_table


def create_nba_model_input_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(node_l5_du_target_variable_table, running_day="2020-04-01",),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "mapping_for_model_training": "mapping_for_model_training",
                },
                outputs="l5_du_target_variable_tbl",
                name="l5_du_target_variable_tbl",
                tags=["l5_du_target_variable_tbl"],
            ),
        ]
    )
