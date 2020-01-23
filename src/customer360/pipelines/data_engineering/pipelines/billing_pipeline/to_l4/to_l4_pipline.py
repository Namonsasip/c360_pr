from kedro.pipeline import Pipeline, node

from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import bill_shock, dynamics_topups_and_volume
from src.customer360.utilities.config_parser import l4_rolling_window

def billing_to_l4_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                bill_shock,
                ["l0_billing_statement_history_monthly"],
                "l4_billing_statement_history_billshock"
            ),
            node(
                l4_rolling_window,
                ["l2_billing_and_payments_weekly_topup_and_volume",
                 "params:l4_billing_topup_and_volume"],
                "l4_billing_rolling_window_topup_and_volume_intermediate"
            ),
            node(
                dynamics_topups_and_volume,
                ["l4_billing_rolling_window_topup_and_volume_intermediate"],
                "l4_billing_rolling_window_topup_and_volume"
            ),
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_rpu",
                 "params:l4_billing_rpu"],
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
        ]
    )