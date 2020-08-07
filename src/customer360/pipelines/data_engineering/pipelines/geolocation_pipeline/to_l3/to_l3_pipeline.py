from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *

# Note that 6,9,11 Use data from HOME/WORK l3
# Then we will crate 'geo_to_l3_home_work_pipeline' for run before 'geo_to_l3_pipeline'
# =====================================================================================
def geo_to_l3_home_work_pipeline(**kwargs):
    return Pipeline(
        [
            ### WAIT
            node(
                massive_processing_for_int_home_work_monthly,
                ["l0_geo_cust_location_monthly_hr_for_int_l3_geo_home_work_location_id",
                 "params:int_l3_geo_home_location_id_monthly",
                 "params:int_l3_geo_work_location_id_monthly"
                 ],
                ["int_l3_geo_home_location_id_monthly",
                 "int_l3_geo_work_location_id_monthly"
                 ]
            ),
            ### WAIT
            node(
                int_geo_home_location_id_monthly,
                ["int_l3_geo_home_location_id_monthly"
                 ],
                "int_l3_geo_home_location_id_last3_monthly"
            ),
            ### WAIT
            node(
                int_geo_work_location_id_monthly,
                ["int_l3_geo_work_location_id_monthly"
                 ],
                "int_l3_geo_work_location_id_last3_monthly"
            ),
            ### WAIT
            node(
                l3_geo_home_work_location_id_monthly,
                ["int_l3_geo_home_location_id_last3_monthly",
                 "int_l3_geo_work_location_id_last3_monthly"
                 ],
                "l3_geo_home_work_location_id_monthly"
            ),

        ], name="geo_to_l3_home_work_pipeline"
    )


def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            ### WAIT
            node(
                l3_geo_top3_visit_exclude_hw_monthly,
                ["l0_geo_cust_location_monthly_hr_for_l3_geo_top3_visit_exclude_hw_monthly",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_top3_visit_exclude_homework_monthly",
                 "params:l3_geo_top3_visit_exclude_hw_monthly"
                 ],
                "l3_geo_top3_visit_exclude_hw_monthly"
            ),  # Will add flag 'Y' if top1 weekday equal top1 weekend --> same_fav_weekday_and_weekend
            #===============================================================================================

            ### FINISH
            node(
                l3_geo_work_area_center_average_monthly,
                ["l0_geo_cust_location_visit_hr_for_l3_geo_work_area_center_average_monthly",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_work_area_center_average_monthly",
                 "params:l3_geo_work_area_center_average_monthly"
                 ],
                "l3_geo_work_area_center_average_monthly"
            ),

            ### FINISH
            node(
                l3_geo_home_weekday_city_citizens_monthly,
                ["l3_geo_home_work_location_id_monthly_for_l3_geo_home_weekday_city_citizens_monthly",
                 "l0_mst_cell_masterplan_for_l3_geo_home_weekday_city_citizens_monthly",
                 "params:l3_geo_home_weekday_city_citizens_monthly"
                 ],
                "l3_geo_home_weekday_city_citizens_monthly"
            ),

            ### FINISH
            node(
                node_from_config,
                ["l2_geo_data_session_location_weekly",
                 "params:int_l3_geo_use_traffic_favorite_location_monthly"
                 ],
                "int_l3_geo_use_traffic_favorite_location_monthly"
            ),
            node(
                l3_geo_use_traffic_favorite_location_monthly,
                ["int_l3_geo_use_traffic_favorite_location_monthly",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_use_traffic_favorite_location_monthly",
                 "l3_geo_top3_visit_exclude_hw_monthly_for_l3_geo_use_traffic_favorite_location_monthly",
                 "params:l3_geo_use_traffic_favorite_location_monthly"
                 ],
                "l3_geo_use_traffic_favorite_location_monthly"
            )

        ], name="geo_to_l3_pipeline"
    )


