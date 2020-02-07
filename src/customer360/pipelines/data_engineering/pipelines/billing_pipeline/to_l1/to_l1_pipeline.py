from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config



def billing_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # Top up count and top up volume feature
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_feature_top_up_and_count"],
                "l1_billing_and_payments_daily_topup_and_volume"
            ),

            # Average revenue for roaming feature
            node(
                node_from_config,
                ["l0_billing_ir_a_usg_daily",
                 "params:l1_billing_and_payment_rpu_roaming"],
                "l1_billing_and_payments_daily_rpu_roaming"
            ),

            # Balance before top up feature
            node(
                node_from_config,
                ["l0_billing_sa_t_account_recharge_daily",
                 "params:l1_billing_and_payment_before_top_up_balance"],
                "l1_billing_and_payments_daily_before_top_up_balance"
            ),

            # Top up channels feature
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_top_up_channels"],
                "l1_billing_and_payments_daily_top_up_channels"
            ),

            # Most popular top up channel feature
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_most_popular_topup_channel"],
                "l1_billing_and_payments_daily_most_popular_top_up_channel"
            ),

            # Popular top up day and hour feature
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_billing_and_payment_popular_topup_day"],
                "l1_billing_and_payments_daily_popular_topup_day"
            ),

            # Time since last top up feature
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily",
                 "params:l1_time_since_last_top_up"],
                "l1_billing_and_payments_daily_time_since_last_top_up"
            ),
        ]
    )