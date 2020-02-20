from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *



def billing_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # Join daily recharge data with customer profile
            node(
                daily_recharge_data_with_customer_profile,
                ["l1_customer_profile_union_daily_feature",
                 "l0_billing_and_payments_rt_t_recharge_daily"],
                "recharge_daily_data"
            ),

            # Top up count and top up volume feature
            node(
                billing_topup_count_and_volume_node,
                ["recharge_daily_data",
                 "params:l1_billing_and_payment_feature_top_up_and_count"],
                "l1_billing_and_payments_daily_topup_and_volume"
            ),

            # Join daily roaming data with customer profile
            node(
                daily_roaming_data_with_customer_profile,
                ["l1_customer_profile_union_daily_feature",
                 "l0_billing_ir_a_usg_daily"],
                "roaming_daily_data"
            ),

            # Average revenue for roaming feature
            node(
                billing_daily_rpu_roaming,
                ["roaming_daily_data",
                 "params:l1_billing_and_payment_rpu_roaming"],
                "l1_billing_and_payments_daily_rpu_roaming"
            ),

            # Join daily sat_t_account data with customer profile
            node(
                daily_sa_account_data_with_customer_profile,
                ["l1_customer_profile_union_daily_feature",
                 "l0_billing_sa_t_account_recharge_daily"],
                "sa_t_account_daily_data"
            ),

            # Balance before top up feature
            node(
                billing_before_topup_balance,
                ["sa_t_account_daily_data",
                 "params:l1_billing_and_payment_before_top_up_balance"],
                "l1_billing_and_payments_daily_before_top_up_balance"
            ),

            # Top up channels feature
            node(
                billing_topup_channels,
                ["recharge_daily_data",
                 "params:l1_billing_and_payment_top_up_channels"],
                "l1_billing_and_payments_daily_top_up_channels"
            ),

            # Most popular top up channel feature
            node(
                billing_most_popular_topup_channel,
                ["recharge_daily_data",
                 "params:l1_billing_and_payment_most_popular_topup_channel"],
                "l1_billing_and_payments_daily_most_popular_top_up_channel"
            ),

            # Popular top up day and hour feature
            node(
                billing_popular_topup_day_hour,
                ["recharge_daily_data",
                 "params:l1_billing_and_payment_popular_topup_day"],
                "l1_billing_and_payments_daily_popular_topup_day"
            ),

            # Time since last top up feature
            node(
                billing_time_since_last_topup,
                ["recharge_daily_data",
                 "params:l1_time_since_last_top_up"],
                "l1_billing_and_payments_daily_time_since_last_top_up"
            ),
        ]
    )