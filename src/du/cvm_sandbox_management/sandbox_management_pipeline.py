from functools import partial
from kedro.pipeline import Pipeline, node

from du.cvm_sandbox_management.sandbox_management_nodes import (
    update_sandbox_control_group,
)
import datetime


SAMPLING_RATE = [0.20, 0.72, 0.08]
TEST_GROUP_NAME = [
    "new_experiment",
    "bau_2021",
    "bau_2020",
]


def update_sandbox_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    update_sandbox_control_group,
                    sampling_rate=SAMPLING_RATE,
                    test_group_name=TEST_GROUP_NAME,
                    test_group_flag=TEST_GROUP_NAME,
                ),
                inputs={
                    "sandbox_framework_2021": "sandbox_framework_2021",
                    "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
                },
                outputs="unused_memory_update_groups",
                name="update_sandbox_control_group",
                tags=["update_sandbox_control_group"],
            ),
        ]
    )
