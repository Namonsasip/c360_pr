from kedro.pipeline import Pipeline, node
from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l3.to_l3_nodes import *

# Note that 6,9,11 Use data from HOME/WORK l3
# Then we will crate 'geo_to_l3_home_wor_kpipeline' for run before 'geo_to_l3_pipeline'
# =====================================================================================
def geo_to_l3_home_work_pipeline(**kwargs):
    return Pipeline(
        [
            ### FINISH
            ### Home and Work Feature
            node(
                massive_processing_for_home_work,
                ["l0_geo_cust_cell_visit_time_for_int_l3_geo_home_work_location_id",
                 "params:int_l3_geo_home_location_id_monthly",
                 "params:int_l3_geo_work_location_id_monthly"
                 ],
                ["int_l3_geo_home_location_id_monthly",
                 "int_l3_geo_work_location_id_monthly"
                 ]
            ),
            ### FINISH
            node(
                int_geo_home_work_list_imsi_monthly,
                ["int_l3_geo_home_location_id_monthly",
                 "int_l3_geo_work_location_id_monthly"
                 ],
                "geo_home_work_list_imsi_stg"
            ),
            ### FINISH
            node(
                int_geo_work_location_id_monthly,
                ["int_l3_geo_work_location_id_monthly",
                 "geo_home_work_list_imsi_stg"
                 ],
                "int_work_location_id"  # In memory Dataframe
            ),
            ### FINISH
            node(
                int_geo_home_location_id_monthly,
                ["int_l3_geo_home_location_id_monthly"
                 ],
                ["int_home_weekday_location_id",
                 "int_home_weekend_location_id"
                 ]
            ),
            ### FINISH
            node(
                l3_geo_home_work_location_id_monthly,
                ["int_home_weekday_location_id",
                 "int_home_weekend_location_id",
                 "int_work_location_id",
                 "params:l3_geo_home_work_location_id_monthly"
                 ],
                "l3_geo_home_work_location_id_monthly"
            )

        ], name="geo_to_l3_home_work_pipeline"
    )


def geo_to_l3_pipeline(**kwargs):
    return Pipeline(
        [

            # ### FINISH
            # ## runnig flag == 8
            # node(
            #     l3_geo_area_from_ais_store_monthly,
            #     ["l1_geo_area_from_ais_store_daily_for_l3_geo_area_from_ais_store_monthly",
            #      "params:l3_area_from_ais_store_monthly"
            #      ],
            #     "l3_geo_area_from_ais_store_monthly"
            # ),

            # ### FINISH
            # ## runnig flag == 13
            # node(
            #     l3_geo_area_from_competitor_store_monthly,
            #     ["l1_geo_area_from_competitor_store_daily_for_l3_geo_area_from_competitor_store_monthly",
            #      "params:l3_area_from_competitor_store_monthly"
            #      ],
            #     "l3_geo_area_from_competitor_store_monthly"
            # ),

            ## runnig flag == 3
            ##total_distance_km###
            node(
                l3_geo_total_distance_km_monthly,
                ["l1_geo_total_distance_km_daily_for_l3_geo_total_distance_km_monthly",
                 "params:l3_geo_total_distance_km_monthly"
                 ],
                "l3_geo_total_distance_km_monthly"
            ),

            ## runnig flag == 5
            # 47 The favourite location
            node(
                l3_the_favourite_locations_monthly,
                ["l1_the_favourite_locations_daily"],
                "l3_the_favourite_locations_monthly"
            ),

            # ### ISSUE: data does not have on month 201911
            # ## runnig flag == 11
            # ### Work area center average
            # node(
            #     l3_geo_work_area_center_average_monthly,
            #     ["l0_geo_cust_location_visit_hr_for_l3_geo_work_area_center_average_monthly",
            #      "l3_geo_home_work_location_id_monthly_for_l3_geo_work_area_center_average_monthly"
            #      ],
            #     "l3_geo_work_area_center_average_monthly"
            # )

        ], name="geo_to_l3_pipeline"
    )


def geo_to_l3_pipeline_call_data(**kwargs):
    return Pipeline(
        [

            # ### FINISH
            # ### runnig flag == 9.2
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
            #
            # ),

            # ### FINISH
            # ### runnig flag == 9.0
            # ###feature_sum_voice_location###
            # node(
            #     l3_call_location_home_work_monthly,
            #     ["l0_geo_mst_cell_masterplan_current_for_l3_call_location_home_work_monthly",
            #      "l3_geo_home_work_location_id_monthly_for_l3_call_location_home_work_monthly",
            #      "l0_profile_customer_profile_ma_for_l3_call_location_home_work_monthly",
            #      "l0_usage_sum_voice_location_daily_for_l3_call_location_home_work_monthly",
            #      "l3_geo_top_visit_exclude_homework_for_l3_call_location_home_work_monthly"
            #      ],
            #     "l3_geo_call_location_home_work_monthly"
            # )

        ], name="geo_to_l3_pipeline_call_data"
    )


def geo_to_l3_pipeline_interim(**kwargs):
    return Pipeline(
        [
            # ### FINISH
            # ### runnig flag == 6
            # ### Home weekday city citizens
            # node(
            #     l3_geo_home_weekday_city_citizens_monthly,
            #     ["l3_geo_home_work_location_id_monthly_for_l3_geo_home_weekday_city_citizens_monthly",
            #      "l0_mst_cell_masterplan_for_l3_geo_home_weekday_city_citizens_monthly",
            #      "params:l3_geo_home_weekday_city_citizens_monthly"
            #      ],
            #     "l3_geo_home_weekday_city_citizens_monthly"
            # ),

            # ### FINISH
            # ## runnig flag == 2
            # node(
            #     l3_geo_time_spent_by_location_monthly,
            #     ["l1_geo_time_spent_by_location_daily_for_l3_geo_time_spent_by_location_monthly",
            #      "params:l3_geo_time_spent_by_location_monthly"
            #      ],
            #     "l3_geo_time_spent_by_location_monthly"
            # ),

            # ### FINISH
            # ## runnig flag == 9.1
            # node(
            #     l3_geo_top_visit_exclude_homework,
            #     ["l3_geo_time_spent_by_location_monthly_for_l3_geo_top_visit_exclude_homework",
            #      "l3_geo_home_work_location_id_monthly_for_l3_geo_top_visit_exclude_homework"
            #      ],
            #     "l3_geo_top_visit_exclude_homework"
            # ),

            # ### FINISH
            # ## runnig flag == 12
            # ##distance_top_call###
            # node(
            #     l3_geo_distance_top_call,
            #     "l1_geo_distance_top_call_for_l3_geo_distance_top_call",
            #     "l3_geo_distance_top_call"
            # ),

            ### WAIT
            ### runnig flag == 1
            ##Top_3_cells_on_voice_usage###
            node(
                l3_geo_top3_cells_on_voice_usage,
                ["l1_geo_top3_cells_on_voice_usage_for_l3_geo_top3_cells_on_voice_usage",
                 "params:l3_geo_top3_cells_on_voice_usage"
                 ],
                "l3_geo_top3_cells_on_voice_usage"
            ),

        ], name="geo_to_l3_pipeline_interim"
    )

