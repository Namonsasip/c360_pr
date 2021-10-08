from kedro.pipeline import Pipeline, node
from customer360.utilities.generate_dependency_dataset import generate_dependency_dataset, generate_list_of_feature


def lineage_dependency_pipeline(**kwargs):
    return Pipeline(
        [

            # Top up count and top up volume feature pre-paid
            node(
                generate_dependency_dataset,
                None,
                ["util_dependency_report", "list_of_dataset"]
            ),
            node(
                generate_list_of_feature,
                "list_of_dataset",
                "util_feature_report"
            ),

        ]
    )
