from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *




def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                l4_rolling_window,
                ["l2_geo_area_from_ais_store_weekly_for_l4_geo_area_from_ais_store",
                 "params:l4_area_from_ais_store"
                 ],
                "l4_geo_area_from_ais_store"
            ),

        ], name="geo_to_l4_pipeline"
    )



