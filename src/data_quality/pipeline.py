from functools import reduce

from kedro.pipeline import Pipeline, node

from data_quality.nodes import generate_dq_nodes, \
    check_catalog_and_feature_exist, \
    sample_subscription_identifier
from data_quality.dq_threshold import generate_dq_threshold_analysis


def data_quality_pipeline(**kwargs):
    """
    Data quality pipeline which creates nodes to execute data quality computation on all dimensions:
    accuracy and completeness, consistency, availability and timeliness.
    :param kwargs:
    :return: List of nodes to run entire data quality pipeline to execute metrics.
    """
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
    """
    Pipeline to execute sampling of customer profile table in order to generate the # of sampled
    subscription_identifiers needed for data_quality_pipeline.
    :param kwargs:
    :return: List of single node.
    """
    return Pipeline(
        [
            node(
                sample_subscription_identifier,
                ["l3_customer_profile_union_monthly_feature_for_data_quality_sampling",
                 "params:sample_size"],
                "dq_sampled_subscription_identifier"
            )
        ]
    )


def threshold_analysis_pipeline(**kwargs):
    """
    Pipeline to execute threshold analysis on accuracy and completeness dimension.
    :param kwargs:
    :return: List of single node to execute data quality threshold analysis.
    """
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
