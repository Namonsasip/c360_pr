from functools import partial
from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix
from src.nba.report.nodes.campaign_importance_volume_node import *

def campaign_importance_volume(run_type:str = None) -> Pipeline:
    suffix = get_suffix(run_type)
    return Pipeline(
        [
            node(
                create_table_delta,
                [
                    "campaign_history_master_active",
                    "params:report_control_group_current_size",
                ],
                "l0_campaign_report_current_size",
                name="user_current_size",
            )
        ]
    )
