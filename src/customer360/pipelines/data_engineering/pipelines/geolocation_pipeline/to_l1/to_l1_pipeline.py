from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import *


def geo_to_l1_intermediate_pipeline(**kwargs):
    return Pipeline(
        [


        ],
        name="geo_to_l1_intermediate_pipeline"
    )


def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l1_geo_test,
                ["l0_mst_poi_shape",
                 "l0_mst_cell_masterplan"
                 ],
                "l1_test_pipeline"
            ),

        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_union_pipeline"
    )
