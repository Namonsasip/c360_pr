from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *

def billing_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_topup_and_volume", "params:l2_billing_and_payment_feature_top_up_and_count_weekly"],
                "l2_billing_and_payments_weekly_topup_and_volume"
            ),
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily","params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate"],
                "l2_billing_and_payments_weekly_topup_diff_time_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_topup_diff_time_intermediate","params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly"],
                "l2_billing_and_payments_weekly_topup_time_diff"
            ),
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_rpu_roaming","params:l2_billing_and_payment_feature_rpu_roaming_weekly"],
                "l2_billing_weekly_rpu_roaming"
            ),
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_before_top_up_balance",
                 "params:l2_billing_and_payment_before_top_up_balance_weekly"],
                "l2_billing_and_payments_weekly_before_top_up_balance"
            ),
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_top_up_channels",
                 "params:l2_billing_and_payment_top_up_channels_weekly"],
                "l2_billing_and_payments_weekly_top_up_channels"
            ),
            node(
                top_up_channel_joined_data,
                ["l1_billing_and_payments_daily_most_popular_top_up_channel","l0_billing_topup_type"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_1"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_1", "params:l2_popular_top_up_channel_1"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_2"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_2",
                 "params:l2_popular_top_up_channel_2"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate","params:l2_most_popular_topup_channel"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel"
            ),
            node(
                top_up_channel_joined_data,
                ["l0_billing_and_payments_rt_t_recharge_daily","l0_billing_topup_type"],
                "l2_billing_and_payments_weekly_last_top_up_channel_1"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_top_up_channel_1", "params:l2_last_topup_channel_1"],
                "l2_billing_and_payments_weekly_last_top_up_channel_2"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_top_up_channel_2", "params:l2_last_topup_channel_2"],
                "l2_billing_and_payments_weekly_last_top_up_channel"
            ),
        ]
    )