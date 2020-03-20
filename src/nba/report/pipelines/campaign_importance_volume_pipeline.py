from functools import partial
from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix
from src.nba.report.nodes.campaign_importance_volume_node import *

def campaign_importance_volume(run_type:str = None) -> Pipeline:
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
                    create_l0_campaign_history_master_active,
                [
                    "l0_campaign_history_master_active",
                ],
                "campaign_history_master_active",
                name="create_l0_campaign_history_master_active",
                tags=["l0_camp_mst"],
            ),
            node(
                read_table,
                [
                    "campaign_history_master_active2",
                ],
                None,
                tags=["cmp_mst_read"]
            )
        ], tags=[]
    )
