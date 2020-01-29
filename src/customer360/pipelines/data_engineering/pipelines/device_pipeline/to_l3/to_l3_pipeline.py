from kedro.pipeline import Pipeline, node

from src.customer360.utilities.config_parser import *


def device_to_l3_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                node_from_config,
                ["l2_device_handset_summary_with_configuration_weekly","params:l3_device_handset_summary_with_configuration"],
                "l3_device_handset_summary_with_configuration_monthly"
            ),
        ]
    )