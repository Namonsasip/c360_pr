from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import popular_top_up_channel


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
            node(
                node_from_config,
                ["l0_billing_sa_t_account_recharge_daily",
                 "params:l1_billing_and_payment_before_top_up_balance"],
                "l1_billing_and_payments_daily_before_top_up_balance"
            ),
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_top_up_channels"],
                "l1_billing_and_payments_daily_top_up_channels"
            ),
            node(
                popular_top_up_channel,
                ["l0_billing_and_payments_rt_t_recharge_daily"],
                "l1_billing_and_payments_daily_most_popular_top_up_channel"
            ),
        ]
    )