from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *


def device_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # Weekly handset configuration related features
            node(
                device_summary_with_configuration,
                ["l1_devices_summary_customer_handset_daily",
                 "l0_devices_handset_configurations",
                 "params:exception_partition_list_for_l1_devices_summary_customer_handset_daily"
                 ],
                "device_summary_with_config"
            ),

            node(
                node_from_config,
                ["device_summary_with_config",
                 "params:l2_device_summary_with_config"],
                "l2_device_summary_with_config_weekly"
            ),
        ], name="device_to_l2_pipeline"
    )
