from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *

def billing_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l1_billing_and_payments_daily_topup_and_volume", "params:l2_billing_and_payment_feature_top_up_and_count_weekly"],
                "l2_billing_and_payments_weekly_topup_and_volume"
            ),
            # node(
            #     top_up_time_diff_weekly_data,
            #     ["l0_billing_and_payments_rt_t_recharge_daily"],
            #     "l2_billing_and_payments_weekly_topup_time_diff"
            # ),
            # node(
            #     automated_payment_weekly,
            #     ["l0_billing_pc_t_payment_daily"],
            #     "l2_billing_weekly_automated_payments"
            # ),
        ]
    )