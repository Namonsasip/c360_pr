from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import *
from src.customer360.utilities.config_parser import *

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_billing_statement_history_monthly","params:l4_payments_bill_shock"],
                "l4_billing_statement_history_billshock"
            ),
        ]
    )