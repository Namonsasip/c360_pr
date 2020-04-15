from functools import partial
from kedro.pipeline import Pipeline, node
from src.nba.report.nodes.campaign_importance_volume_node import *


def campaign_importance_volume(run_type: str = None) -> Pipeline:
    return Pipeline(
        [
            # Non's notes:
            # This file is for creating pipelines, a combination of nodes calling
            # The normal 'node' is for calling nodes (functions) from the node file
            # The 'partial node' is for calling nodes with additional parameters
            # The parameter after node's function is 'input', referred from catalog(s) across the project
            # The parameter after input is 'output', write destination from catalog(s) across the project
            # Name is for calling when there is a need to run individual node
            # Tag is a short version of name, multiple nodes with the same tag can be used to run at once
            node(
                create_l0_campaign_history_master_active,
                inputs=[
                    "l0_campaign_history_master_active",
                    "params:create_l0_campaign_history_master_active_param"
                ],
                outputs=["campaign_history_master_active"],
                name="create_l0_campaign_history_master_active",
                tags=["l0_camp_mst", "campaign_importance_volume"],
            ),
            node(
                create_l5_campaign_distinct_contact_response,
                inputs=[
                    "distinct_child_code_contact_response",
                    "params:create_l5_campaign_distinct_contact_response_param",
                ],
                outputs=["distinct_child_response_aggregated"],
                name="create_l5_campaign_distinct_contact_response",
                tags=["l5_cmp_res_agg", "campaign_importance_volume"],
            ),
            node(
                create_l5_response_percentage_report,
                inputs=[
                    "distinct_child_response_aggregated",
                    "campaign_history_master_active",
                    "params:create_l5_response_percentage_report_param"
                ],
                outputs=["response_percentage_report"],
                name="create_l5_response_percentage_report",
                tags=["l5_res_perc_report", "campaign_importance_volume"],
            ),
            node(
                partial(
                    create_focus_campaign_by_volume_low_response,
                    percentage_threshold_low=10,
                    percentage_threshold_high=20,
                    campaign_amount_per_category=20,
                ),
                inputs={
                    "response_percentage_report": "response_percentage_report",
                    "focus_campaign_param": "params:create_l5_focus_campaign_param",
                },
                outputs=['important_campaign_by_contact_volume_low_response',
                         'important_campaign_by_contact_volume_high_response'],
                name="create_focus_campaign_by_volume_low_response",
                tags=["l5_focus_campaign", "campaign_importance_volume"]
            )
        ], tags=[]
    )
