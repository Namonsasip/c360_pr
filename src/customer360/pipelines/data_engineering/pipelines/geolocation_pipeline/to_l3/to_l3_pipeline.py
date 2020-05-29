from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes_backup.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [



        ], name="geo_to_l3_pipeline"
    )
