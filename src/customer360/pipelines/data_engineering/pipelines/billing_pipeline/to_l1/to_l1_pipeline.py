from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *


def billing_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            # Top up count and top up volume feature pre-paid
            node(
                billing_topup_count_and_volume_node,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l1_billing_and_payments_daily_topup_and_volume",
                 "params:l1_billing_and_payment_feature_top_up_and_count"],
                "l1_billing_and_payments_daily_topup_and_volume"
            ),
  
            # Average revenue for roaming feature post-paid
            node(
                billing_daily_rpu_roaming,
                ["l0_billing_ir_a_usg_daily",
                 "params:l1_billing_and_payment_rpu_roaming"],
                "l1_billing_and_payments_daily_rpu_roaming"
            ),

            # Balance before top up feature pre-paid
            node(
                billing_before_topup_balance,
                ["l0_billing_sa_t_account_recharge_daily", "l1_customer_profile_union_daily_feature_for_l1_billing_and_payments_daily_before_top_up_balance",
                 "params:l1_billing_and_payment_before_top_up_balance"],
                "l1_billing_and_payments_daily_before_top_up_balance"
            ),

            # Top up channels feature pre-paid
            node(
                billing_topup_channels,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l1_billing_and_payments_daily_top_up_channels",
                 "params:l1_billing_and_payment_top_up_channels"],
                "l1_billing_and_payments_daily_top_up_channels"
            ),

            # Most popular top up channel feature pre-paid
            node(
                billing_most_popular_topup_channel,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l1_billing_and_payments_daily_most_popular_top_up_channel",
                 "params:l1_billing_and_payment_most_popular_topup_channel"],
                "l1_billing_and_payments_daily_most_popular_top_up_channel"
            ),

            # Popular top up day and hour feature pre-paid
            node(
                billing_popular_topup_day_hour,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l1_billing_and_payments_daily_popular_topup_day",
                 "params:l1_billing_and_payment_popular_topup_day"],
                "l1_billing_and_payments_daily_popular_topup_day"
            ),

            # Time since last top up feature pre-paid
            node(
                billing_time_since_last_topup,
                ["l0_billing_and_payments_rt_t_recharge_daily_for_l1_billing_and_payments_daily_time_since_last_top_up",
                 "params:l1_time_since_last_top_up"],
                "l1_billing_and_payments_daily_time_since_last_top_up"
            ),
        ]
    )

