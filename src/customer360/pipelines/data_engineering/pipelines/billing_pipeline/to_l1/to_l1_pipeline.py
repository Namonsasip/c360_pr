from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config


def billing_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_feature_top_up_and_count"],
                "l1_billing_and_payments_daily_topup_and_volume"
            ),
            node(
                node_from_config,
                ["l0_billing_ir_a_usg_daily",
                 "params:l1_billing_and_payment_rpu_roaming"],
                "l1_billing_and_payments_daily_rpu_roaming"
            ),
        ]
    )