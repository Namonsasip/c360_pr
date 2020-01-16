from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config


def billing_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:l3_billing_and_payment_arpu"],
                "l3_billing_and_payments_arpu"
            ),
        ]
    )