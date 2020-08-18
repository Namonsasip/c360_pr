from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *
from customer360.utilities.re_usable_functions import l3_massive_processing


# Note that 6,9,11 Use data from HOME/WORK l3
# Then we will crate 'geo_to_l3_home_work_pipeline' for run before 'geo_to_l3_pipeline'
# =====================================================================================


def geo_to_l3_home_work_pipeline(**kwargs):
    return Pipeline(
        [
            ## WAIT
            node(
                massive_processing_for_int_home_work_monthly,
                ["l0_geo_cust_location_monthly_hr_for_int_l3_geo_home_work_location_id",
                 "params:int_l3_geo_home_location_id_monthly",
                 "params:int_l3_geo_work_location_id_monthly"
                 ],
                ["int_l3_geo_home_location_id_filter_monthly",
                 "int_l3_geo_work_location_id_filter_monthly"
                 ]
            ),
            node(
                int_geo_home_location_id_monthly,
                ["int_l3_geo_home_location_id_filter_monthly"
                 ],
                "int_l3_geo_home_location_id_monthly"
            ),
            node(
                int_geo_work_location_id_monthly,
                ["int_l3_geo_work_location_id_filter_monthly"
                 ],
                ["int_l3_geo_work_location_id_monthly",
                 "int_l3_geo_work_location_id_last3_monthly"
                 ]
            ),
            node(
                l3_geo_home_work_location_id_monthly,
                ["int_l3_geo_home_location_id_monthly",
                 "int_l3_geo_work_location_id_monthly"
                 ],
                "l3_geo_home_work_location_id_monthly"
            ),
            node(
                l3_geo_work_area_center_average_monthly,
                ["int_l3_geo_work_location_id_last3_monthly",
                 "int_l3_geo_work_location_id_monthly",
                 "params:l3_geo_work_area_center_average_monthly"
                 ],
                "l3_geo_work_area_center_average_monthly"
            ),
            node(
                l3_geo_home_weekday_city_citizens_monthly,
                ["int_l3_geo_home_location_id_monthly",
                 "l0_geo_mst_cell_masterplan_master",
                 "params:l3_geo_home_weekday_city_citizens_monthly"
                 ],
                "l3_geo_home_weekday_city_citizens_monthly"
            ),

        ], name="geo_to_l3_home_work_pipeline"
    )


def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            ### WAIT
            node(
                int_l3_geo_top3_visit_exclude_hw_monthly,
                ["l0_geo_cust_location_monthly_hr_for_l3_geo_top3_visit_exclude_hw_monthly",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_top3_visit_exclude_hw_monthly",
                 "params:int_l3_geo_top3_visit_exclude_hw_monthly"
                 ],
                "int_l3_geo_top3_visit_exclude_hw_monthly"
            ),
            node(
                l3_geo_top3_visit_exclude_hw_monthly,
                ["int_l3_geo_top3_visit_exclude_hw_monthly",
                 "params:l3_geo_top3_visit_exclude_hw_monthly"
                 ],
                "l3_geo_top3_visit_exclude_hw_monthly"
            ),

            # ### WAIT
            # node(
            #     int_l3_geo_visit_ais_store_location_filter_monthly,
            #     ["l1_geo_visit_ais_store_location_daily",
            #      "params:int_l3_geo_visit_ais_store_location_filter_monthly"
            #      ],
            #     "int_l3_geo_visit_ais_store_location_filter_monthly"
            # ),
            # node(
            #     int_l3_geo_visit_ais_store_location_monthly,
            #     ["int_l3_geo_visit_ais_store_location_monthly",
            #      "l3_geo_home_work_location_id_monthly_for_l3_geo_visit_ais_store_location_monthly",
            #      "l3_geo_top3_visit_exclude_hw_monthly_for_l3_geo_visit_ais_store_location_monthly"
            #      ],
            #     ["int_l3_geo_ais_store_near_home_work_monthly",
            #      "int_l3_geo_ais_store_near_top3_visit_monthly"
            #      ]
            # ),
            # node(
            #     l3_geo_visit_ais_store_location_monthly,
            #     ["int_l3_geo_ais_store_near_home_work_monthly",
            #      "int_l3_geo_ais_store_near_top3_visit_monthly",
            #      "params:l3_geo_visit_ais_store_location_monthly",
            #      ],
            #     "l3_geo_visit_ais_store_location_monthly"
            # ),

            # ### FINISH
            # node(
            #     l3_massive_processing,
            #     ["l1_geo_data_session_location_daily",
            #      "params:l3_geo_data_session_location_monthly"
            #      ],
            #     "l3_geo_data_session_location_monthly"
            # ),

            # ### FINISH
            # node(
            #     int_l3_geo_use_traffic_favorite_location_monthly,
            #     ["l3_geo_data_session_location_monthly_for_l3_geo_use_traffic_favorite_location_monthly",
            #      "l3_geo_home_work_location_id_monthly_for_l3_geo_use_traffic_favorite_location_monthly",
            #      "l3_geo_top3_visit_exclude_hw_monthly_for_l3_geo_use_traffic_favorite_location_monthly"
            #      ],
            #     ["int_l3_geo_use_traffic_home_work_location_monthly",
            #      "int_l3_geo_use_traffic_1st_2nd_location_monthly"
            #      ]
            # ),
            # node(
            #     l3_geo_use_traffic_favorite_location_monthly,
            #     ["int_l3_geo_use_traffic_home_work_location_monthly",
            #      "int_l3_geo_use_traffic_1st_2nd_location_monthly",
            #      "params:l3_geo_use_traffic_favorite_location_monthly"
            #      ],
            #     "l3_geo_use_traffic_favorite_location_monthly"
            # ),

            # ### WAIT
            # node(
            #     int_l3_geo_favourite_data_session_location_monthly,
            #     ["l3_geo_data_session_location_monthly_for_l3_geo_favourite_data_session_location_monthly"
            #      ],
            #     ["int_l3_geo_favourite_data_session_location_all_monthly",
            #     "int_l3_geo_favourite_data_session_location_week_monthly"
            #     ]
            # ),
            # node(
            #     l3_geo_favourite_data_session_location_monthly,
            #     ["int_l3_geo_favourite_data_session_location_all_monthly",
            #      "int_l3_geo_favourite_data_session_location_week_monthly",
            #      "params:l3_geo_favourite_data_session_location_monthly"
            #      ],
            #     "l3_geo_favourite_data_session_location_monthly"
            # ),

            # node(
            #     int_l3_customer_profile_imsi_daily_feature,
            #     ["int_l2_customer_profile_imsi_daily_feature",
            #      "params:int_l3_customer_profile_imsi_daily_feature"
            #      ],
            #     "int_l3_customer_profile_imsi_daily_feature"
            # )

        ], name="geo_to_l3_pipeline"
    )
