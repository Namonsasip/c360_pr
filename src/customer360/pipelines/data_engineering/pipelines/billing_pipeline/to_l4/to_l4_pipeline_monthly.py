from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from customer360.pipelines.data_engineering.nodes.billing_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.config_parser import *


def billing_to_l4_pipeline_monthly(**kwargs):
    return Pipeline(
        [

            # ARPU with dynamics
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_rpu_for_l4_billing_rolling_window_rpu",
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
                ["l3_billing_and_payments_monthly_bill_volume_for_l4_billing_rolling_window_bill_volume",
                 "params:l4_payments_bill_volume"],
                "l4_billing_rolling_window_bill_volume_intermediate"
            ),
            node(
                node_from_config,
                ["l4_billing_rolling_window_bill_volume_intermediate",
                 "params:l4_dynamics_bill_volume"],
                "l4_billing_rolling_window_bill_volume"
            ),
            #
            # # Join monthly billing statement hist data with customer profile
            # node(
            #     billing_statement_hist_data_with_customer_profile,
            #     ["l3_customer_profile_include_1mo_non_active_for_l4_billing_statement_history_billshock",
            #      "l0_billing_statement_history_monthly_for_l4_billing_statement_history_billshock","params:l4_billing_statement_history_billshock_tbl"],
            #     "billing_stat_hist_monthly_data"
            # ),
            #
            # # Bill Shock feature
            # node(
            #     node_from_config,
            #     ["billing_stat_hist_monthly_data",
            #      "params:l4_payments_bill_shock"],
            #     "l4_billing_statement_history_billshock"
            # ),
            #
            # Postpaid missed bills count
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_missed_bills_for_l4_rolling_window_billing_and_payments_missed_bills",
                 "params:l4_missed_bills"],
                "l4_rolling_window_billing_and_payments_missed_bills"
            ),

            # Postpaid overdue bills count
            node(
                l4_rolling_window,
                ["l3_billing_and_payments_monthly_overdue_bills_for_l4_rolling_window_billing_and_payments_overdue_bills",
                 "params:l4_overdue_bills"],
                "l4_rolling_window_billing_and_payments_overdue_bills"
            ),

            # last and most payment
            node(
                l4_billing_last_and_most_billing_payment_detail_payment,
                ["l0_billing_pc_t_payment_for_l4_billing_last_and_most_billing_payment_detail"],
                "int_l4_billing_last_and_most_billing_payment_detail_payment_stg"

            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_profile,
                ["l3_customer_profile_include_1mo_non_active_for_l4_billing_last_and_most_billing_payment_detail"],
                "int_l4_billing_last_and_most_billing_payment_detail_profile_stg"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_1,
                ["int_l4_billing_last_and_most_billing_payment_detail_payment_stg",
                 "int_l4_billing_last_and_most_billing_payment_detail_profile_stg",
                 "l0_billing_payment_channel_for_l4_billing_last_and_most_bill_payment_detail"],
                ["int_l4_billing_last_and_most_billing_payment_detail_payment_stg_1",
                 "int_l4_billing_last_and_most_billing_payment_detail_profile_stg_1"]
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_2,
                ["int_l4_billing_last_and_most_billing_payment_detail_profile_stg_1"],
                "int_l4_billing_last_and_most_billing_payment_detail_profile_stg_2"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_3,
                ["int_l4_billing_last_and_most_billing_payment_detail_payment_stg_1",
                 "int_l4_billing_last_and_most_billing_payment_detail_profile_stg_2"],
                "int_l4_billing_last_and_most_billing_payment_detail_stg_1"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_last,
                ["int_l4_billing_last_and_most_billing_payment_detail_stg_1"],
                "int_l4_billing_last_and_most_billing_payment_detail_last"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_most3m,
                ["int_l4_billing_last_and_most_billing_payment_detail_stg_1"],
                "int_l4_billing_last_and_most_billing_payment_detail_most3m"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_most6m,
                ["int_l4_billing_last_and_most_billing_payment_detail_stg_1"],
                "int_l4_billing_last_and_most_billing_payment_detail_most6m"
            ),
            node(
                l4_billing_last_and_most_billing_payment_detail_4,
                ["int_l4_billing_last_and_most_billing_payment_detail_last",
                 "int_l4_billing_last_and_most_billing_payment_detail_most3m",
                 "int_l4_billing_last_and_most_billing_payment_detail_most6m"],
                "int_l4_billing_last_and_most_billing_payment_detail_stg"
            ),
            node(
                node_from_config,
                ["int_l4_billing_last_and_most_billing_payment_detail_stg",
                 "params:l4_billing_last_and_most_billing_payment_detail"],
                "l4_billing_last_and_most_billing_payment_detail"
            ),
        ]
    )

