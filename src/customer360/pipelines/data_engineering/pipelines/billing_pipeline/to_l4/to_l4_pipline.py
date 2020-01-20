from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import bill_shock

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                bill_shock,
                ["l0_billing_statement_history_monthly"],
                "l0_billing_statement_history_billshock"
            ),
        ]
    )