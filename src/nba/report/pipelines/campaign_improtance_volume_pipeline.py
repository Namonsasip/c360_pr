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
                    "distinct_child_code_contact_response",
                ],
                "distinct_child_response_agg",
                name="create distinct_child_response_agg table",
            ),
            node(
                read_table,
                [
                    "distinct_child_response_agg",
                ],
                "read_success",
                name = "test reading table"
            )
        ]
    )
