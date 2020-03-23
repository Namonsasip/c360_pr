from functools import partial
from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix
from src.nba.report.nodes.campaign_importance_volume_node import *


def campaign_importance_volume(run_type: str = None) -> Pipeline:
    suffix = get_suffix(run_type)
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
                create_l0_campaign_tracking_contact_list_pre,
                [
                    "l0_campaign_tracking_contact_list_pre",
                    #"params:create_l0_campaign_history_master_active_param"
                ],
                "l0_campaign_tracking_contact_list_pre",
                name="create_l0_campaign_history_master_active",
                tags=["l0_camp_tracking_pre"],
            ),
            node(
                create_l0_campaign_history_master_active,
                [
                    "l0_campaign_history_master_active",
                    "params:create_l0_campaign_history_master_active_param"
                ],
                "campaign_history_master_active",
                name="create_l0_campaign_history_master_active",
                tags=["l0_camp_mst"],
            ),
            node(
                create_l5_campaign_distinct_contact_response,
                [
                    "distinct_child_code_contact_response",
                    "params:create_l5_campaign_distinct_contact_response_param"
                ],
                "distinct_child_response_aggregated",
                name="create_l5_campaign_distinct_contact_response",
                tags=["l5_cmp_res_agg"],
            ),
            node(
                create_l5_response_percentage_report,
                [
                    "distinct_child_response_aggregated",
                    "campaign_history_master_active",
                    "params:create_l5_response_percentage_report_param"
                ],
                "response_percentage_report",
                name="create_l5_response_percentage_report",
                tags=["l5_res_perc_report"],
            ),
        ], tags=[]
    )
