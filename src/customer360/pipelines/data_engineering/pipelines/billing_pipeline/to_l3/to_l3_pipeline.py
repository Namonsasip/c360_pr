from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import automated_payment_monthly

def billing_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_topup_and_volume",
                 "params:l3_billing_and_payment_feature_top_up_and_count_monthly"],
                "l3_billing_and_payments_monthly_topup_and_volume"
            ),
            node(
                node_from_config,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:l3_billing_and_payment_revenue_per_user_monthly"],
                "l3_billing_and_payments_monthly_rpu"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_topup_time_diff",
                 "params:l3_billing_and_payment_time_diff_bw_topups_monthly"],
                "l3_billing_and_payments_monthly_topup_time_diff"
            ),
            node(
                node_from_config,
                ["l2_billing_weekly_rpu_roaming",
                 "params:l3_billing_and_payment_feature_rpu_roaming_monthly"],
                "l3_billing_monthly_rpu_roaming"
            ),
            node(
                automated_payment_monthly,
                ["l0_billing_pc_t_payment_daily"],
                "l3_billing_monthly_automated_payments"
            ),
            node(
                node_from_config,
                ["l2_billing_and_payments_weekly_before_top_up_balance",
                 "params:l3_billing_and_payment_before_top_up_balance_monthly"],
                "l3_billing_and_payments_monthly_before_top_up_balance"
            ),
        ]
    )