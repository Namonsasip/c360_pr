from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from src.customer360.utilities.config_parser import *

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [

            # Join monthly billing statement hist data with customer profile
            # node(
            #     billing_statement_hist_data_with_customer_profile,
            #     ["l3_customer_profile_include_1mo_non_active",
            #      "l0_billing_statement_history_monthly"],
            #     "billing_stat_hist_monthly_data"
            # ),

            # Bill Shock feature
            # node(
            #     node_from_config,
            #     ["billing_stat_hist_monthly_data",
            #      "params:l4_payments_bill_shock"],
            #     "l4_billing_statement_history_billshock"
            # ),

            # Top up count and volume with dynamics

            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_topup_and_volume",
                 "params:l4_billing_topup_and_volume"],
                "l4_billing_rolling_window_topup_and_volume_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_topup_and_volume_intermediate",
                 "params:l4_dynamics_topups_and_volume"],
                "l4_billing_rolling_window_topup_and_volume"
            ),

            # ARPU with dynamics
            # node(
            #     l4_rolling_window,
            #     ["l3_billing_and_payments_monthly_rpu",
            #      "params:l4_billing_rpu"],
            #     "l4_billing_rolling_window_rpu_intermediate"
            # ),
            # node(
            #     node_from_config,
            #     ["l4_billing_rolling_window_rpu_intermediate",
            #      "params:l4_dynamics_arpu"],
            #     "l4_billing_rolling_window_rpu"
            # ),

            # ARPU roaming
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_weekly_rpu_roaming",
            #      "params:l4_billing_rpu_roaming"],
            #     "l4_billing_rolling_window_rpu_roaming"
            # ),

            # Time difference between top ups
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_topup_time_diff",
            #      "params:l4_billing_time_diff_bw_topups"],
            #     "l4_billing_rolling_window_time_diff_bw_top_ups"
            # ),

            # Balance before top up
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_before_top_up_balance",
            #      "params:l4_billing_before_top_up_balance"],
            #     "l4_billing_rolling_window_before_top_up_balance"
            # ),

            # Top up channels
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_top_up_channels",
            #      "params:l4_billing_top_up_channels"],
            #     "l4_billing_rolling_window_top_up_channels"
            # ),

            # Post paid bill volume with dynamics
            # node(
            #     l4_rolling_window,
            #     ["l3_billing_and_payments_monthly_bill_volume",
            #      "params:l4_payments_bill_volume"],
            #     "l4_billing_rolling_window_bill_volume_intermediate"
            # ),
            # node(
            #     node_from_config,
            #     ["l4_billing_rolling_window_bill_volume_intermediate",
            #      "params:l4_dynamics_bill_volume"],
            #     "l4_billing_rolling_window_bill_volume"
            # ),

            # Postpaid missed bills count
            # node(
            #     l4_rolling_window,
            #     ["l3_billing_and_payments_monthly_missed_bills",
            #      "params:l4_missed_bills"],
            #     "l4_rolling_window_missed_bills"
            # ),

            # Postpaid overdue bills count
            # node(
            #     l4_rolling_window,
            #     ["l3_billing_and_payments_monthly_overdue_bills",
            #      "params:l4_overdue_bills"],
            #     "l4_rolling_window_overdue_bills"
            # ),

            # Popular top up day
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_popular_topup_day_intermediate",
            #      "params:l4_popular_topup_day_initial"],
            #     "l4_rolling_window_popular_topup_day_1"
            # ),
            # node(
            #     l4_rolling_ranked_window,
            #     ["l4_rolling_window_popular_topup_day_1",
            #      "params:l4_popular_topup_day"],
            #     "l4_rolling_window_popular_topup_day"
            # ),

            # Popular top up hour
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_popular_topup_hour_intermediate",
            #      "params:l4_popular_topup_hour_initial"],
            #     "l4_rolling_window_popular_topup_hour_1"
            # ),
            # node(
            #     l4_rolling_ranked_window,
            #     ["l4_rolling_window_popular_topup_hour_1",
            #      "params:l4_popular_topup_hour"],
            #     "l4_rolling_window_popular_topup_hour"
            # ),

            # Most popular top up channel
            # node(
            #     l4_rolling_window,
            #     ["l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate",
            #      "params:l4_most_popular_topup_channel_initial"],
            #     "l4_rolling_window_most_popular_topup_channel_1"
            # ),
            # node(
            #     l4_rolling_ranked_window,
            #     ["l4_rolling_window_most_popular_topup_channel_1",
            #      "params:l4_most_popular_topup_channel"],
            #     "l4_billing_rolling_window_most_popular_topup_channel"
            # ),
        ]
    )