from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.device_nodes.to_l1.to_l1_nodes import *


def device_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            # node(
            #     generate_l1_layer,
            #     ["l0_devices_summary_customer_handset", "l1_customer_profile_union_daily_feature_for_device"],
            #     "l1_devices_summary_customer_handset_daily"
            # ),

            node(
                l1_device_summary_customer_handset_daily,
                ["l0_device_summary_customer_handset_for_l1_device_summary_customer_handset_daily",
                 "l1_customer_profile_union_daily_feature_for_device_for_l1_device_summary_customer_handset_daily",
                 ],
                "l1_device_summary_customer_handset_daily"
            ),
        ], name="device_to_l1_pipeline"
    )
