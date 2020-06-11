from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *




def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            
            node(
                l4_rolling_window,
                ["l2_geo_time_spent_by_location_weekly_for_l4_geo_time_spent_by_location",
                 "params:l4_geo_time_spent_by_location"
                 ],
                "l4_geo_time_spent_by_location"
            ),


            node(
                l4_rolling_window,
                ["l2_geo_area_from_ais_store_weekly_for_l4_geo_area_from_ais_store",
                 "params:l4_area_from_ais_store"
                 ],
                "l4_geo_area_from_ais_store"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_area_from_competitor_store_weekly_for_l4_geo_area_from_competitor_store",
                 "params:l4_area_from_competitor_store"
                 ],
                "l4_geo_area_from_competitor_store"
            ),

            node(
                l4_geo_top_visit_exclude_homework,
                ["l3_geo_time_spent_by_location_monthly_for_l4_geo_top_visit_exclude_homework",
                 "l4_geo_home_work_location_id",
                 # "params:l4_geo_top_visit_exclude_homework"
                 ],
                "l4_geo_top_visit_exclude_homework"
            ),
            
            # node(
            #     l4_geo_home_work_location_id,
            #     ["l0_geo_cust_cell_visit_time_for_l4_geo_home_work_location_id",
            #      "params:l4_geo_home_work_location_id"
            #      ],
            #     "l4_geo_home_work_location_id"
            # ),

            node(
                l4_geo_home_weekday_city_citizens,
                ["l4_geo_home_work_location_id_for_l4_geo_home_weekday_city_citizens",
                 "l0_mst_cell_masterplan_for_l4_geo_home_weekday_city_citizens",
                 "params:l4_geo_home_weekday_city_citizens"
                 ],
                "l4_geo_home_weekday_city_citizens"
            )

        ], name="geo_to_l4_pipeline"
    )



