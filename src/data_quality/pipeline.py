from kedro.pipeline import node

from data_quality.nodes import *
from data_quality.dq_util import *


def data_quality_pipeline(**kwargs):
    c360_catalog = get_c360_catalog()

    dq_nodes = generate_dq_nodes(c360_catalog)

    return Pipeline(
        [
            *dq_nodes
        ]
    )


def subscription_id_sampling_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                sample_subscription_identifier,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "param:sample_size"],
                "dq_sampled_subscription_identifier"
            )
        ]
    )