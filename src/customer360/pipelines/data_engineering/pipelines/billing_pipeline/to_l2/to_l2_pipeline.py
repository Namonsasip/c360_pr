from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *
def billing_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                top_up_weekly_converted_data,
                ["l1_billing_and_payments_topup_and_volume"],
                "l2_billing_and_payments_weekly_topup_and_volume"
            ),
            node(
                top_up_time_diff_weekly_data,
                ["l0_billing_and_payments_rt_t_recharge_daily"],
                "l2_billing_and_payments_weekly_topup_time_diff"
            ),
        ]
    )