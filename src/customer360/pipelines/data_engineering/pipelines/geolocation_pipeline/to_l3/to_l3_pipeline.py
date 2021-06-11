from kedro.pipeline import Pipeline, node
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *
from customer360.utilities.re_usable_functions import l3_massive_processing


def geo_to_l3_pipeline_1st(**kwargs):
    return Pipeline(
        [
            ### FINISH
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
            ### FINISH
            node(
                l3_massive_processing,
                ["l1_geo_data_session_location_daily_for_l3_geo_data_session_location_monthly",
                 "params:l3_geo_data_session_location_monthly"
                 ],
                "l3_geo_data_session_location_monthly"
            ),

        ], name="geo_to_l3_pipeline_1st"
    )


def geo_to_l3_home_work_pipeline(**kwargs):
    return Pipeline(
        [
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
                 "int_l3_geo_work_location_id_monthly_for_l3_geo_work_area_center_average_monthly",
                 "params:l3_geo_work_area_center_average_monthly"
                 ],
                "l3_geo_work_area_center_average_monthly"
            ),
            node(
                l3_geo_home_weekday_city_citizens_monthly,
                ["int_l3_geo_home_location_id_monthly_for_l3_geo_home_weekday_city_citizens_monthly",
                 "l0_geo_mst_cell_masterplan_master",
                 "params:l3_geo_home_weekday_city_citizens_monthly"
                 ],
                "l3_geo_home_weekday_city_citizens_monthly"
            ),

        ], name="geo_to_l3_home_work_pipeline"
    )


def geo_to_l3_pipeline_2nd(**kwargs):
    return Pipeline(
        [

            ### FINISH
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

        ], name="geo_to_l3_pipeline_2nd"
    )


def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            ### FINISH
            node(
                int_l3_geo_visit_ais_store_location_filter_monthly,
                ["l1_geo_visit_ais_store_location_daily",
                 "params:int_l3_geo_visit_ais_store_location_filter_monthly"
                 ],
                "int_l3_geo_visit_ais_store_location_filter_monthly_1"
            ),
            node(
                int_l3_geo_visit_ais_store_location_monthly,
                ["int_l3_geo_visit_ais_store_location_filter_monthly_1",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_visit_ais_store_location_monthly",
                 "l3_geo_top3_visit_exclude_hw_monthly_for_l3_geo_visit_ais_store_location_monthly"
                 ],
                ["int_l3_geo_ais_store_near_home_work_monthly_1",
                 "int_l3_geo_ais_store_near_top3_visit_monthly_1"
                 ]
            ),
            node(
                l3_geo_visit_ais_store_location_monthly,
                ["int_l3_geo_ais_store_near_home_work_monthly_1",
                 "int_l3_geo_ais_store_near_top3_visit_monthly_1",
                 "params:l3_geo_visit_ais_store_location_monthly",
                 ],
                "l3_geo_visit_ais_store_location_monthly_test"
            ),

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
            #
            # ### FINISH
            # node(
            #     int_l3_geo_favourite_data_session_location_monthly,
            #     ["l3_geo_data_session_location_monthly_for_l3_geo_favourite_data_session_location_monthly"
            #      ],
            #     ["int_l3_geo_favourite_data_session_location_all_monthly",
            #      "int_l3_geo_favourite_data_session_location_week_monthly"
            #      ]
            # ),
            # node(
            #     l3_geo_favourite_data_session_location_monthly,
            #     ["int_l3_geo_favourite_data_session_location_all_monthly",
            #      "int_l3_geo_favourite_data_session_location_week_monthly",
            #      "params:l3_geo_favourite_data_session_location_monthly"
            #      ],
            #     "l3_geo_favourite_data_session_location_monthly"
            # ),
            #
            # ### FINISH
            # node(
            #     int_l3_customer_profile_imsi_daily_feature,
            #     ["l1_customer_profile_imsi_daily_feature_for_int_l3_customer_profile_imsi_daily_feature",
            #      "params:int_l3_customer_profile_imsi_daily_feature"
            #      ],
            #     "int_l3_customer_profile_imsi_daily_feature"
            # )

        ], name="geo_to_l3_pipeline"
    )
