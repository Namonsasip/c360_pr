from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.predictive_nodes.to_l4.to_l4_nodes import *


def predictive_to_l4_pipeline(**kwargs):
    """
    :param kwargs:
    :return:
    """
    return Pipeline(
        [
            node(
                predictive_copy_features_to_l4,
                "l0_predictive_cvm_propensity_scores",
                "l4_predictive_cvm_propensity_scores"
            )
        ]
    )
