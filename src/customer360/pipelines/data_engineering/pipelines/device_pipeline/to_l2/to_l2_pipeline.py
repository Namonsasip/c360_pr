from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *

def device_to_l2_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                device_features_with_config,
                ["l0_devices_summary_customer_handset", "l0_devices_handset_configurations"],
                "l2_device_handset_summary_with_configuration_weekly"
            ),
        ]
    )