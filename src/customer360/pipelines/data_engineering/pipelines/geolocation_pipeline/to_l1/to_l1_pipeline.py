from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l1.to_l1_nodes import *

def geo_to_l1_intermediate_pipeline(**kwargs):
    return Pipeline(
        [


        ],
        name="geo_to_l1_intermediate_pipeline"
    )

def geo_to_l1_pipeline(**kwargs):
    return Pipeline(
        [

            # node(
            #     l1_geo_time_spent_by_location_daily,
            #     ["l0_geo_cust_cell_visit_time_for_l1_geo_time_spent_by_location_daily",
            #      "params:l1_geo_time_spent_by_location_daily"
            #      ],
            #     "l1_geo_time_spent_by_location_daily"
            #
            # ),
            #
            node(
                l1_geo_area_from_ais_store_daily,
                ["l0_mst_poi_shape_for_l1_geo_area_from_ais_store_daily",
                 "l0_mst_cell_masterplan_for_l1_geo_area_from_ais_store_daily",
                 "l0_geo_cust_cell_visit_time_for_l1_geo_area_from_ais_store_daily",
                 "params:l1_area_from_ais_store_daily"
                 ],
                "l1_geo_area_from_ais_store_daily"
            ),
            #
            # node(
            #     l1_geo_area_from_competitor_store_daily,
            #     ["l0_mst_poi_shape_for_l1_geo_area_from_competitor_store_daily",
            #      "l0_mst_cell_masterplan_for_l1_geo_area_from_competitor_store_daily",
            #      "l0_geo_cust_cell_visit_time_for_l1_geo_area_from_competitor_store_daily",
            #      "params:l1_area_from_competitor_store_daily"
            #      ],
            #     "l1_geo_area_from_competitor_store_daily"
            # ),
            #
            # node(
            #     l1_geo_cust_subseqently_distance,
            #     ["l0_geo_cust_cell_visit_time_for_l1_geo_cust_subseqently_distance_daily",
            #      "params:l1_geo_cust_subseqently_distance_daily"
            #      ],
            #     "l1_geo_cust_subseqently_distance_daily"
            # ),
            #
            # ###total_distance_km###
            # node(l1_geo_total_distance_km_daily,
            #      ["l0_geo_cust_cell_visit_time_for_l1_geo_total_distance_km_daily",
            #       "params:l1_geo_total_distance_km_daily"
            #       ],
            #      "l1_geo_total_distance_km_daily"
            # ),
            #
            #
            # ###Number_of_base_station###
            # node(
            #     l1_geo_number_of_bs_used,
            #     ["l0_geo_cust_cell_visit_time_daily_for_l1_geo_number_of_bs_used",
            #      "params:l1_geo_data_count_location_id"
            #      ],
            #     "l1_geo_number_of_bs_used"
            # ),

            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> MUST FIX L0 HOME/WORK
            # ###feature_sum_voice_location###
            # node(
            #     l1_call_location_home_work,
            #     ["l0_geo_mst_cell_masterplan_current_for_l1_use_non_homework_features",
            #      "l0_geo_home_work_data_for_l1_use_non_homework_features",
            #      "l0_profile_customer_profile_ma_for_l1_use_non_homework_features",
            #      "l0_usage_sum_voice_location_daily_for_l1_homework_features",
            #      "l0_geo_top_visit_exclude_homework_for_l1_sum_voice_daily"
            #      ],
            #     "l1_geo_call_location_home_work_daily"
            # ),

            # ### runnig flag == 1
            # ###feature_AIS_store###
            # node(
            #     l1_location_of_visit_ais_store_daily,
            #     ["l0_mst_poi_shape_for_l1_geo_area_from_ais_store_daily",
            #      "l0_geo_cust_cell_visit_time_for_l1_geo_time_spent_by_location_daily",
            #      "params:l1_location_of_visit_ais_store_daily"
            #      ],
            #     "l1_location_of_visit_ais_store_daily"
            # ),
            #
            # ##Top_3_cells_on_voice_usage###
            # node(
            #     l1_geo_top3_cells_on_voice_usage,
            #     ["l0_usage_sum_voice_location_daily_for_l1_geo_top3_cells_on_voice_usage",
            #      "l0_geo_mst_cell_masterplan_for_l1_geo_top3_cells_on_voice_usage",
            #      "l0_profile_customer_profile_ma_for_l1_geo_top3_cells_on_voice_usage"
            #      ],
            #     "l1_geo_top3_cells_on_voice_usage"
            # ),
            #
            # ##distance_top_call###
            # node(
            #     l1_geo_distance_top_call,
            #     "l1_geo_top3_cells_on_voice_usage",
            #     "l1_geo_distance_top_call"
            # ),
            #
            # ##47 the_favourite_locations
            # node(
            #     l1_the_favourite_locations_daily,
            #     ["l0_usage_sum_data_location_daily_for_l1_the_favourite_locations",
            #      "l0_geo_mst_cell_masterplan_for_l1_the_favourite_locations"
            #      ],
            #     "l1_the_favourite_locations_daily"
            # ),
            #
            # ## Number of Unique Cells Used ###
            # node(
            #     l1_number_of_unique_cell_daily,
            #     ["l0_usage_sum_data_location_daily_for_l1_number_of_unique_cell_daily"
            #      ],
            #     "l1_number_of_unique_cell_daily"
            # )

        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_union_pipeline"
    )

