from functools import partial
from kedro.pipeline import Pipeline, node

from du.cvm_sandbox_management.sandbox_management_nodes import (
    update_sandbox_control_group,
)
from du.data_upsell_sandbox_management.du_sandbox_management_nodes import (
    update_du_control_group_nodes,
)
from du.data_upsell_sandbox_management.disney_sandbox_management_nodes import (
    update_disney_cg_tg_group,
)
import datetime


SAMPLING_RATE = [0.20, 0.72, 0.08]
TEST_GROUP_NAME = [
    "new_experiment",
    "bau_2021",
    "bau_2020",
]


def update_sandbox_pipeline(mode: str) -> Pipeline:
    if mode == 'Production':
        delta_table_schema = 'prod_dataupsell'
    elif mode == 'Development':
        delta_table_schema = 'dev_dataupsell'
    return Pipeline(
        [
            node(
                partial(
                    update_sandbox_control_group,
                    sampling_rate=SAMPLING_RATE,
                    test_group_name=TEST_GROUP_NAME,
                    test_group_flag=TEST_GROUP_NAME,
                    delta_table_schema=delta_table_schema,
                ),
                inputs={
                    "sandbox_framework_2021": "sandbox_framework_2021",
                    "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
                },
                outputs="unused_memory_update_groups",
                name="update_sandbox_control_group",
                tags=["update_sandbox_control_group"],
            ),
            node(
                partial(
                    update_du_control_group_nodes,
                    delta_table_schema=delta_table_schema,
                    mode=mode,
                ),
                inputs={"unused_memory_update_groups": "unused_memory_update_groups",},
                outputs="unused_memory_update_du_groups",
                name="update_du_control_group",
                tags=["update_sandbox_control_group"],
            ),
        ]
    )

def create_disney_cg_tg_group_pipeline(mode: str) -> Pipeline:
    if mode == 'Production':
        delta_table_schema = 'prod_dataupsell'
    elif mode == 'Development':
        delta_table_schema = 'dev_dataupsell'
    return Pipeline(
        [
            node(
                partial(
                    update_disney_cg_tg_group,
                    delta_table_schema=delta_table_schema,
                ),
                inputs={
                    "dummy": "l0_customer_profile_profile_customer_profile_pre_current_full_load"
                },
                outputs="unused_memory_disney_cg_tg_group_output",
                name="create_disney_cg_tg_group",
                tags=["create_disney_cg_tg_group"],
            ),
        ]
    )
