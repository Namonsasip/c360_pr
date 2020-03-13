from kedro.pipeline import Pipeline, node
from src.customer360.utilities.config_parser import *


def billing_to_l4_pipeline_daily(**kwargs):
    return Pipeline(
        [

            node(
                l4_rolling_window,
                ["l1_billing_and_payments_daily_topup_and_volume",
                 "params:l4_billing_topup_and_volume_daily_feature"],
                "l4_daily_feature_topup_and_volume"
            ),
        ]
    )
