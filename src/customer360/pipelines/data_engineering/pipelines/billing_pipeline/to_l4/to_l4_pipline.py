from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import billshock

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                billshock,
                ["l0_billing_statement_history_monthly"],
                "l0_billing_statement_history_billshock"
            ),
        ]
    )