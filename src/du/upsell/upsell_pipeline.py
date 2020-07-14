from kedro.pipeline import Pipeline, node

from du.upsell.experiment4_nodes import (
    create_btl_experiment_score_distribution,
)

def create_du_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [
        node(
            create_btl_experiment_score_distribution,
            inputs={
                "l4_revenue_prepaid_pru_f_usage_multi_features_sum": "l4_revenue_prepaid_pru_f_usage_multi_features_sum",
                "l5_du_scored": "l5_du_scored",
            },
            outputs="l5_experiment4_eligible_upsell",
            name="l5_experiment4_eligible_upsell",
            tags=["l5_experiment4_eligible_upsell"],
        ),
    ],
    tags = ["experiment4_eligible_upsell"],
    )