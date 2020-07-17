from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *




def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            ### runnig flag == 2
            # node(
            #     l3_geo_time_spent_by_location_monthly,
            #     ["l1_geo_time_spent_by_location_daily_for_l3_geo_time_spent_by_location_monthly",
            #      "params:l3_geo_time_spent_by_location_monthly"
            #      ],
            #     "l3_geo_time_spent_by_location_monthly"
            # ),

            ### runnig flag == 8
            # node(
            #     l3_geo_area_from_ais_store_monthly,
            #     ["l1_geo_area_from_ais_store_daily_for_l3_geo_area_from_ais_store_monthly",
            #      "params:l3_area_from_ais_store_monthly"
            #      ],
            #     "l3_geo_area_from_ais_store_monthly"
            # ),

            node(
                l3_geo_area_from_competitor_store_monthly,
                ["l1_geo_area_from_competitor_store_daily_for_l3_geo_area_from_competitor_store_monthly",
                 "params:l3_area_from_competitor_store_monthly"
                 ],
                "l3_geo_area_from_competitor_store_monthly"
            ),

            ### runnig flag == 3
            ###total_distance_km###
            # node(
            #     l3_geo_total_distance_km_monthly,
            #     ["l1_geo_total_distance_km_daily_for_l3_geo_total_distance_km_monthly",
            #      "params:l3_geo_total_distance_km_monthly"
            #      ],
            #     "l3_geo_total_distance_km_monthly"
            # ),

            ### runnig flag == 9
            # ###Traffic_fav_location###
            # node(
            #     l3_data_traffic_home_work_top1_top2,
            #     ["l0_geo_mst_cell_masterplan_current_for_l3_use_non_homework_features",
            #      "l3_geo_home_work_location_id_monthly_for_l3_data_traffic_home_work_top1_top2",
            #      "l0_profile_customer_profile_ma_for_l3_use_non_homework_features",
            #      "l0_usage_sum_data_location_daily_for_l3_use_non_homework_features",
            #      "l3_geo_time_spent_by_location_monthly_for_l3_data_traffic_home_work_top1_top2"
            #      ],
            #     "l3_geo_use_traffic_home_work"
            # ),

            node(
                l3_geo_use_Share_traffic_monthly,
                ["l3_geo_use_traffic_home_work",
                 "params:l3_geo_use_traffic_home_work_weekly"
                 ],
                "l3_geo_use_traffic_home_work_monthly"
            ),

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> MUST FIX L0 HOME/WORK
            # ### runnig flag == 3
            # ###feature_sum_voice_location###
            # node(
            #     l3_geo_call_location_home_work_monthly,
            #     ["l1_geo_call_location_home_work_daily_for_l3_geo_call_location_home_work_monthly", # No data from l1
            #      "params:l3_geo_call_location_home_work_monthly"
            #      ],
            #     "l3_geo_call_location_home_work_monthly"
            # ),

            # ### runnig flag == 1
            # ##Top_3_cells_on_voice_usage###
            # node(
            #     l3_geo_top3_cells_on_voice_usage,
            #     ["l1_geo_top3_cells_on_voice_usage",
            #      "params:l3_geo_top3_cells_on_voice_usage"
            #      ],
            #     "l3_geo_top3_cells_on_voice_usage"
            # ),

            ##distance_top_call###
            node(
                l3_geo_distance_top_call,
                "l1_geo_distance_top_call",
                "l3_geo_distance_top_call"
            ),

            ### runnig flag == 5
            # # 47 The favourite location
            # node(
            #     l3_the_favourite_locations_monthly,
            #     ["l1_the_favourite_locations_daily"],
            #     "l3_the_favourite_locations_monthly"
            # ),

            ### runnig flag == 4
            # ### Home and Work Feature
            # node(
            #     massive_processing_for_home_work,
            #     ["l0_geo_cust_cell_visit_time_for_int_l3_geo_home_work_location_id",
            #      "params:int_l3_geo_home_location_id_monthly",
            #      "params:int_l3_geo_work_location_id_monthly"
            #      ],
            #     ["int_l3_geo_home_location_id_monthly",
            #      "int_l3_geo_work_location_id_monthly"
            #      ]
            # ),

            node(
                int_geo_home_work_list_imsi_monthly,
                ["int_l3_geo_home_location_id_monthly",
                 "int_l3_geo_work_location_id_monthly"
                 ],
                "geo_home_work_list_imsi_stg"
            ),
            node(
                int_geo_work_location_id_monthly,
                ["int_l3_geo_work_location_id_monthly",
                 "geo_home_work_list_imsi_stg"
                 ],
                "int_work_location_id"  # In memory Dataframe
            ),

            ### runnig flag == ุ7
            node(
                int_geo_home_location_id_monthly,
                ["int_l3_geo_home_location_id_monthly"
                 ],
                ["int_home_weekday_location_id",
                 "int_home_weekend_location_id"
                 ]
            ),
            node(
                l3_geo_home_work_location_id_monthly,
                ["int_home_weekday_location_id",
                 "int_home_weekend_location_id",
                 "int_work_location_id",
                 "params:l3_geo_home_work_location_id_monthly"
                 ],
                "l3_geo_home_work_location_id_monthly"
            ),

            ### runnig flag == 6
            # ### Home weekday city citizens
            # node(
            #     l3_geo_home_weekday_city_citizens_monthly,
            #     ["l3_geo_home_work_location_id_monthly_for_l3_geo_home_weekday_city_citizens_monthly",
            #      "l0_mst_cell_masterplan_for_l3_geo_home_weekday_city_citizens_monthly",
            #      "params:l3_geo_home_weekday_city_citizens_monthly"
            #      ],
            #     "l3_geo_home_weekday_city_citizens_monthly"
            # ),

            ### Work area center average
            node(
                l3_geo_work_area_center_average_monthly,
                ["l0_geo_cust_location_visit_hr_for_l3_geo_work_area_center_average_monthly",
                 "l3_geo_home_work_location_id_monthly_for_l3_geo_work_area_center_average_monthly"
                 ],
                "l3_geo_work_area_center_average_monthly"
            )

        ], name="geo_to_l3_pipeline"
    )
