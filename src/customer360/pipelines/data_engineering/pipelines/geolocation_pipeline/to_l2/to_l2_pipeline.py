from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l2.to_l2_nodes import *




def geo_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l2_geo_area_from_ais_store_weekly,
                ["l1_geo_area_from_ais_store_daily_for_l2_geo_area_from_ais_store_weekly",
                 "params:l2_area_from_ais_store_weekly"
                 ],
                "l2_geo_area_from_ais_store_weekly"
            ),

            node(
                l2_geo_area_from_competitor_store_weekly,
                ["l1_geo_area_from_competitor_store_daily_for_l2_geo_area_from_competitor_store_weekly",
                 "params:l2_area_from_competitor_store_weekly"
                 ],
                "l2_geo_area_from_competitor_store_weekly"
            ),

        ], name="geo_to_l2_pipeline"
    )

