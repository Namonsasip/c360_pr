from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from customer360.utilities.config_parser import *


def billing_to_l4_pipeline_monthly(**kwargs):
    return Pipeline(
        [

            # ARPU with dynamics
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_rpu",
                 "params:l4_billing_rpu"],
                "l4_billing_rolling_window_rpu_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_rpu_intermediate",
                 "params:l4_dynamics_arpu"],
                "l4_billing_rolling_window_rpu"
            ),

            # Post paid bill volume with dynamics
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_bill_volume",
                 "params:l4_payments_bill_volume"],
                "l4_billing_rolling_window_bill_volume_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_bill_volume_intermediate",
                 "params:l4_dynamics_bill_volume"],
                "l4_billing_rolling_window_bill_volume"
            ),

            # Join monthly billing statement hist data with customer profile
            node(
                billing_statement_hist_data_with_customer_profile,
                ["l3_customer_profile_include_1mo_non_active",
                 "l0_billing_statement_history_monthly"],
                "billing_stat_hist_monthly_data"
            ),

            # Bill Shock feature
            node(
                node_from_config,
                ["billing_stat_hist_monthly_data",
                 "params:l4_payments_bill_shock"],
                "l4_billing_statement_history_billshock"
            ),

            # Postpaid missed bills count
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_missed_bills",
                 "params:l4_missed_bills"],
                "l4_rolling_window_missed_bills"
            ),

            # Postpaid overdue bills count
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_overdue_bills",
                 "params:l4_overdue_bills"],
                "l4_rolling_window_overdue_bills"
            ),
        ]
    )
