from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config


def billing_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l0_billing_and_payment_feature_top_up_and_count"],
                "l1_billing_and_payments_topup_and_volume"
            ),
        ]
    )