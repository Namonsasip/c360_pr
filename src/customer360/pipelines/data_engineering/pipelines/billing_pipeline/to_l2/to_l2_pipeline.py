from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *

def billing_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            # Weekly top up count and top up volume
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_topup_and_volume",
                 "params:l2_billing_and_payment_feature_top_up_and_count_weekly"],
                "l2_billing_and_payments_weekly_topup_and_volume"
            ),

            # Join daily recharge data with customer profile
            node(
                daily_recharge_data_with_customer_profile,
                ["l1_customer_profile_union_daily_feature",
                 "l0_billing_and_payments_rt_t_recharge_daily"],
                "recharge_daily_data"
            ),

            # Weekly Time difference between top ups
            node(
                node_from_config,
                ["recharge_daily_data",
                 "params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate"],
                "l2_billing_and_payments_weekly_topup_diff_time_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_topup_diff_time_intermediate",
                 "params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly"],
                "l2_billing_and_payments_weekly_topup_time_diff"
            ),

            # Weekly arpu of roaming
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_rpu_roaming",
                 "params:l2_billing_and_payment_feature_rpu_roaming_weekly"],
                "l2_billing_weekly_rpu_roaming"
            ),

            # Weekly balance before top up
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_before_top_up_balance",
                 "params:l2_billing_and_payment_before_top_up_balance_weekly"],
                "l2_billing_and_payments_weekly_before_top_up_balance"
            ),

            # Weekly top up channels
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_top_up_channels",
                 "params:l2_billing_and_payment_top_up_channels_weekly"],
                "l2_billing_and_payments_weekly_top_up_channels"
            ),

            # Weekly most popular top up channel
            node(
                top_up_channel_joined_data,
                ["l1_billing_and_payments_daily_most_popular_top_up_channel",
                 "l0_billing_topup_type"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_1"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_1",
                 "params:l2_popular_top_up_channel"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate",
                 "params:l2_most_popular_topup_channel"],
                "l2_billing_and_payments_weekly_most_popular_top_up_channel"
            ),

            # Weekly last top up channel
            node(
                top_up_channel_joined_data,
                ["recharge_daily_data",
                 "l0_billing_topup_type"],
                "l2_billing_and_payments_weekly_last_top_up_channel_1"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_top_up_channel_1",
                 "params:l2_last_topup_channel_1"],
                "l2_billing_and_payments_weekly_last_top_up_channel_2"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_top_up_channel_2",
                 "params:l2_last_topup_channel_2"],
                "l2_billing_and_payments_weekly_last_top_up_channel"
            ),

            # Weekly popular top up day
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_popular_topup_day",
                 "params:l2_popular_topup_day_1"],
                "l2_billing_and_payments_weekly_popular_topup_day_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_popular_topup_day_intermediate",
                 "params:l2_popular_topup_day_2"],
                "l2_billing_and_payments_weekly_popular_topup_day"
            ),

            # Weekly popular top up hour
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_popular_topup_day",
                 "params:l2_popular_topup_hour_1"],
                "l2_billing_and_payments_weekly_popular_topup_hour_intermediate"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate",
                 "params:l2_popular_topup_hour_2"],
                "l2_billing_and_payments_weekly_popular_topup_hour"
            ),

            # Weekly time since last top up
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_time_since_last_top_up",
                 "params:l2_time_since_last_top_up"],
                "l2_billing_and_payments_weekly_time_since_last_top_up"
            ),

            # Weekly last 3 top up volume
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_time_since_last_top_up",
                 "params:l2_last_three_topup_volume_ranked"],
                "l2_billing_and_payments_weekly_last_three_topup_volume_1"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_three_topup_volume_1",
                 "params:l2_last_three_topup_volume"],
                "l2_billing_and_payments_weekly_last_three_topup_volume"
            ),
        ]
    )