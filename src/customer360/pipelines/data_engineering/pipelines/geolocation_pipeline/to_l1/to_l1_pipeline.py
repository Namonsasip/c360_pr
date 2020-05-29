from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes_backup.to_l1.to_l1_nodes import *


def geo_to_l1_intermediate_pipeline(**kwargs):
    return Pipeline(
        [


        ],
        name="geo_to_l1_intermediate_pipeline"
    )


def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_union_pipeline"
    )
