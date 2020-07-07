from functools import reduce

from kedro.pipeline import Pipeline, node

from data_quality.nodes import generate_dq_nodes, \
    check_catalog_and_feature_exist, \
    sample_subscription_identifier
from data_quality.dq_threshold import generate_dq_threshold_analysis


def data_quality_pipeline(**kwargs):
    dq_nodes = generate_dq_nodes()

    check_catalog_tags = list(reduce(lambda x, y: x.union(y.tags), dq_nodes, set()))

    return Pipeline(
        [
            node(
                func=check_catalog_and_feature_exist,
                inputs=["params:features_for_dq"],

                # MemoryDataSet to ensure execution order. Otherwise,
                # dq_nodes can run before the checks completed
                outputs="all_catalog_and_feature_exist",
                tags=check_catalog_tags
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


def threshold_analysis_pipeline(**kwargs):

    return Pipeline(
        [
            node(
                func=generate_dq_threshold_analysis,
                inputs=["dq_accuracy_and_completeness", "params:threshold_lookback_corresponding_dates"],
                outputs=["dq_threshold_output_accuracy_and_completeness",
                         "dq_threshold_output_accuracy_and_completeness_pivoted",
                         "dq_threshold_output_accuracy_and_completeness_grouped"],
                tags="dq_threshold_output_accuracy_and_completeness"
            )
        ]
    )
