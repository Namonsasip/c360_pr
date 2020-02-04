from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import *
from src.customer360.utilities.config_parser import *

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_billing_statement_history_monthly","params:l4_payments_bill_shock"],
                "l4_billing_statement_history_billshock"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_topup_and_volume",
                 "params:l4_billing_topup_and_volume"],
                "l4_billing_rolling_window_topup_and_volume_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_topup_and_volume_intermediate","params:l4_dynamics_topups_and_volume"],
                "l4_billing_rolling_window_topup_and_volume"
            ),
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_rpu",
                 "params:l4_billing_rpu"],
                "l4_billing_rolling_window_rpu_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_rpu_intermediate","params:l4_dynamics_arpu"],
                "l4_billing_rolling_window_rpu"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_weekly_rpu_roaming",
                 "params:l4_billing_rpu_roaming"],
                "l4_billing_rolling_window_rpu_roaming"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_topup_time_diff",
                 "params:l4_billing_time_diff_bw_topups"],
                "l4_billing_rolling_window_time_diff_bw_top_ups"
            ),
            node(
                node_from_config,
                ["l0_billing_and_payments_rt_t_recharge_daily","params:l4_last_3_top_up_volume"],
                "l4_billing_rolling_window_last_3_top_up_volume"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_before_top_up_balance",
                 "params:l4_billing_before_top_up_balance"],
                "l4_billing_rolling_window_before_top_up_balance"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_top_up_channels",
                 "params:l4_billing_top_up_channels"],
                "l4_billing_rolling_window_top_up_channels"
            ),
            # node(
            #     l4_rolling_window,
            #     ["l3_billing_and_payments_monthly_bill_volume",
            #      "params:l4_payments_bill_volume"],
            #     "l4_billing_rolling_window_bill_volume_intermediate"
            # ),
            # node(
            #     node_from_config,
            #     ["l4_billing_rolling_window_bill_volume_intermediate","params:l4_dynamics_bill_volume"],
            #     "l4_billing_rolling_window_bill_volume"
            # ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_last_top_up_channel","params:l4_last_top_up_channel"],
                "l4_rolling_window_last_top_up_channel"
            ),
            #node(
            #    l4_rolling_window,
            #    ["l3_billing_and_payments_monthly_missed_bills", "params:l4_missed_bills"],
            #    "l4_rolling_window_missed_bills"
            #),
            # node(
            #    l4_rolling_window,
            #    ["l3_billing_and_payments_monthly_overdue_bills", "params:l4_overdue_bills"],
            #    "l4_rolling_window_overdue_bills"
            # ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_popular_topup_day_intermediate", "params:l4_popular_topup_day_1"],
                "l4_rolling_window_popular_topup_day_1"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_day_1", "params:l4_popular_topup_day_with_ranks"],
                "l4_rolling_window_popular_topup_day_2"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_day_2", "params:l4_popular_topup_day_last_week"],
                "l4_rolling_window_popular_topup_day_last_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_day_2", "params:l4_popular_topup_day_last_two_week"],
                "l4_rolling_window_popular_topup_day_last_two_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_day_2", "params:l4_popular_topup_day_last_month"],
                "l4_rolling_window_popular_topup_day_last_month"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_day_2", "params:l4_popular_topup_day_last_three_month"],
                "l4_rolling_window_popular_topup_day_last_three_month"
            ),
            node(
                joined_data_for_popular_topup_day,
                ["l4_rolling_window_popular_topup_day_last_week", "l4_rolling_window_popular_topup_day_last_two_week",
                 "l4_rolling_window_popular_topup_day_last_month", "l4_rolling_window_popular_topup_day_last_three_month"],
                "l4_rolling_window_popular_topup_day"
            ),

            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate", "params:l4_popular_topup_hour_1"],
                "l4_rolling_window_popular_topup_hour_1"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_hour_1", "params:l4_popular_topup_hour_with_ranks"],
                "l4_rolling_window_popular_topup_hour_2"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_hour_2", "params:l4_popular_topup_hour_last_week"],
                "l4_rolling_window_popular_topup_hour_last_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_hour_2", "params:l4_popular_topup_hour_last_two_week"],
                "l4_rolling_window_popular_topup_hour_last_two_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_hour_2", "params:l4_popular_topup_hour_last_month"],
                "l4_rolling_window_popular_topup_hour_last_month"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_popular_topup_hour_2", "params:l4_popular_topup_hour_last_three_month"],
                "l4_rolling_window_popular_topup_hour_last_three_month"
            ),
            node(
                joined_data_for_popular_topup_hour,
                ["l4_rolling_window_popular_topup_hour_last_week", "l4_rolling_window_popular_topup_hour_last_two_week",
                 "l4_rolling_window_popular_topup_hour_last_month",
                 "l4_rolling_window_popular_topup_hour_last_three_month"],
                "l4_rolling_window_popular_topup_hour"
            ),

            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate", "params:l4_most_popular_topup_channel_1"],
                "l4_rolling_window_most_popular_topup_channel_1"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_most_popular_topup_channel_1", "params:l4_most_popular_topup_channel_with_ranks"],
                "l4_rolling_window_most_popular_topup_channel_2"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_most_popular_topup_channel_2", "params:l4_most_popular_topup_channel_last_week"],
                "l4_rolling_window_most_popular_topup_channel_last_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_most_popular_topup_channel_2", "params:l4_most_popular_topup_channel_last_two_week"],
                "l4_rolling_window_most_popular_topup_channel_last_two_week"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_most_popular_topup_channel_2", "params:l4_most_popular_topup_channel_last_month"],
                "l4_rolling_window_most_popular_topup_channel_last_month"
            ),
            node(
                node_from_config,
                ["l4_rolling_window_most_popular_topup_channel_2", "params:l4_most_popular_topup_channel_last_three_month"],
                "l4_rolling_window_most_popular_topup_channel_last_three_month"
            ),
            node(
                joined_data_for_most_popular_topup_channel,
                ["l4_rolling_window_most_popular_topup_channel_last_week", "l4_rolling_window_most_popular_topup_channel_last_two_week",
                 "l4_rolling_window_most_popular_topup_channel_last_month",
                 "l4_rolling_window_most_popular_topup_channel_last_three_month"],
                "l4_billing_rolling_window_most_popular_topup_channel"
            ),
        ]
    )