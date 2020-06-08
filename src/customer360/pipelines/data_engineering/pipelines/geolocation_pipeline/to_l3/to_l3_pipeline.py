from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                l3_geo_time_spent_by_location_monthly,
                ["l1_geo_time_spent_by_location_daily_for_l3_geo_time_spent_by_location_monthly",
                 "params:l3_geo_time_spent_by_location_monthly"
                 ],
                "l3_geo_time_spent_by_location_monthly"
            ),


            # node(
            #     l3_geo_area_from_ais_store_monthly,
            #     ["l1_geo_area_from_ais_store_daily_for_l3_geo_area_from_ais_store_monthly",
            #      "params:l3_area_from_ais_store_monthly"
            #      ],
            #     "l3_geo_area_from_ais_store_monthly"
            # ),
            #
            # node(
            #     l3_geo_area_from_competitor_store_monthly,
            #     ["l1_geo_area_from_competitor_store_daily_for_l3_geo_area_from_competitor_store_monthly",
            #      "params:l3_area_from_competitor_store_monthly"
            #      ],
            #     "l3_geo_area_from_competitor_store_monthly"
            # )

        ], name="geo_to_l3_pipeline"
    )
