from kedro.pipeline import Pipeline, node
from customer360.utilities.generate_dependency_dataset import generate_dependency_dataset


def lineage_dependency_pipeline(**kwargs):
    return Pipeline(
        [

            # Top up count and top up volume feature pre-paid
            node(
                generate_dependency_dataset,
                ["params:running_environment_parameter"],
                ["util_dependency_report", "util_feature_report"]
            ),
        ]
    )
