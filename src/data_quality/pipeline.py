from kedro.pipeline import Pipeline, node

from data_quality.nodes import *
from data_quality.dq_util import *


def data_quality_pipeline(**kwargs):
    dq_nodes = generate_dq_nodes()

    return Pipeline(
        [
            node(
                func=check_catalog_and_feature_exist,
                inputs=["params:features_for_dq"],
                outputs=None
            ),
            *dq_nodes
        ]
    )


def subscription_id_sampling_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                sample_subscription_identifier,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:sample_size"],
                "dq_sampled_subscription_identifier"
            )
        ]
    )