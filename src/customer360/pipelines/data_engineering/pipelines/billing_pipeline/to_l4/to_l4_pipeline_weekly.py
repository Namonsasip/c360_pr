from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.config_parser import *


def billing_to_l4_pipeline_weekly(**kwargs):
    return Pipeline(
        [
            # # Top up count and volume with dynamics
            #
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_topup_and_volume_for_l4_billing_rolling_window_topup_and_volume",
            #      "params:l4_billing_topup_and_volume"],
            #     "l4_billing_rolling_window_topup_and_volume_intermediate"
            # ),
            # node(
            #     node_from_config,
            #     ["l4_billing_rolling_window_topup_and_volume_intermediate",
            #      "params:l4_dynamics_topups_and_volume"],
            #     "l4_billing_rolling_window_topup_and_volume"
            # ),
            #
            # # ARPU roaming
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_rpu_roaming_for_l4_billing_rolling_window_rpu_roaming",
            #      "params:l4_billing_rpu_roaming"],
            #     "l4_billing_rolling_window_rpu_roaming"
            # ),
            #
            # # Time difference between top ups
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_topup_time_diff_for_l4_billing_rolling_window_time_diff_bw_top_ups",
            #      "params:l4_billing_time_diff_bw_topups"],
            #     "l4_billing_rolling_window_time_diff_bw_top_ups_1"
            # ),
            #
            # node(
            #     node_from_config,
            #     ["l4_billing_rolling_window_time_diff_bw_top_ups_1",
            #      "params:l4_dynamics_time_diff_bw_topups"],
            #     "l4_billing_rolling_window_time_diff_bw_top_ups"
            # ),
            #
            # # Balance before top up
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_before_top_up_balance_for_l4_billing_rolling_window_before_top_up_balance",
            #      "params:l4_billing_before_top_up_balance"],
            #     "l4_billing_rolling_window_before_top_up_balance"
            # ),

            # Top up channels
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels_for_l4_billing_rolling_window_top_up_channels",
                 "params:l4_billing_rolling_window_top_up_channels_features_first"],
                "l4_billing_rolling_window_top_up_channels_features_first"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels_for_l4_billing_rolling_window_top_up_channels",
                 "params:l4_billing_rolling_window_top_up_channels_features_second"],
                "l4_billing_rolling_window_top_up_channels_features_second"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels_for_l4_billing_rolling_window_top_up_channels",
                 "params:l4_billing_rolling_window_top_up_channels_features_third"],
                "l4_billing_rolling_window_top_up_channels_features_third"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels_for_l4_billing_rolling_window_top_up_channels",
                 "params:l4_billing_rolling_window_top_up_channels_features_fourth"],
                "l4_billing_rolling_window_top_up_channels_features_fourth"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels_for_l4_billing_rolling_window_top_up_channels",
                 "params:l4_billing_rolling_window_top_up_channels_features_fifth"],
                "l4_billing_rolling_window_top_up_channels_features_fifth"
            ),
            node(
                l4_billing_rolling_window_top_up_channels,
                ["l4_billing_rolling_window_top_up_channels_features_first",
                 "l4_billing_rolling_window_top_up_channels_features_second",
                 "l4_billing_rolling_window_top_up_channels_features_third",
                 "l4_billing_rolling_window_top_up_channels_features_fourth",
                 "l4_billing_rolling_window_top_up_channels_features_fifth"
                 ],
                "l4_billing_rolling_window_top_up_channels"
            ),
        ]
    )


def billing_to_l4_ranked_pipeline_weekly(**kwargs):
    return Pipeline(
        [
            # Popular top up day
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_popular_topup_day_intermediate_for_l4_rolling_window_popular_topup_day",
                 "params:l4_popular_topup_day_initial"],
                "l4_rolling_window_popular_topup_day_1"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_rolling_window_popular_topup_day_1",
                 "params:l4_popular_topup_day"],
                "l4_rolling_window_popular_topup_day"
            ),

            # Popular top up hour
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate_for_l4_rolling_window_popular_topup_hour",
                 "params:l4_popular_topup_hour_initial"],
                "l4_rolling_window_popular_topup_hour_1"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_rolling_window_popular_topup_hour_1",
                 "params:l4_popular_topup_hour"],
                "l4_rolling_window_popular_topup_hour"
            ),

            # Most popular top up channel
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate_for_l4_billing_rolling_window_most_popular_topup_channel",
                 "params:l4_most_popular_topup_channel_initial"],
                "l4_rolling_window_most_popular_topup_channel_1"
            ),
            node(
                l4_rolling_ranked_window,
                ["l4_rolling_window_most_popular_topup_channel_1",
                 "params:l4_most_popular_topup_channel"],
                "l4_billing_rolling_window_most_popular_topup_channel"
            ),
        ]
    )
