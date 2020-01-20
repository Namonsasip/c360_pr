from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import node_from_config
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *

def billing_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                top_up_monthly_converted_data,
                ["l1_billing_and_payments_topup_and_volume"],
                "l3_billing_and_payments_monthly_topup_and_volume"
            ),
            node(
                arpu_monthly_converted_data,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly"],
                "l3_billing_and_payments_monthly_arpu"
            ),
            node(
                top_up_time_diff_monthly_data,
                ["l0_billing_and_payments_rt_t_recharge_daily"],
                "l3_billing_and_payments_monthly_topup_time_diff"
            ),
        ]
    )