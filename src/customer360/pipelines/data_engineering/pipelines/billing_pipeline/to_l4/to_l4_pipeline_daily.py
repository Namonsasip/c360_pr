from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *


def billing_to_l4_pipeline_daily(**kwargs):
    return Pipeline(
        [

            # node(
            #     l4_rolling_window,
            #     ["l1_billing_and_payments_daily_topup_and_volume_for_l4_daily_feature_topup_and_volume",
            #      "params:l4_billing_topup_and_volume_daily_feature"],
            #     "l4_daily_feature_topup_and_volume"
            # ),
            node(
                l4_rolling_window_by_metadata,
                ["l1_billing_and_payments_daily_topup_and_volume_for_l4_daily_feature_topup_and_volume",
                 "params:l4_billing_topup_and_volume_daily_feature",
                 "params:l4_daily_feature_topup_and_volume_tg",
                 "l1_customer_profile_union_daily_feature_for_l4_daily_feature_topup_and_volume"],
                "l4_daily_feature_topup_and_volume"
            ),
        ]
    )
