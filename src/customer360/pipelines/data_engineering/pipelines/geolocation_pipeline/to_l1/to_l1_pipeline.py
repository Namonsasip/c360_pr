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
            # node(
            #     l1_geo_area_from_ais_store_daily,
            #     ["l0_mst_poi_shape_for_l1_geo_area_from_ais_store_daily",
            #      "l0_mst_cell_masterplan_for_l1_geo_area_from_ais_store_daily",
            #      "l0_geo_cust_cell_visit_time_for_l1_geo_area_from_ais_store_daily",
            #      "params:l1_area_from_ais_store_daily"
            #      ],
            #     "l1_geo_area_from_ais_store_daily"
            # ),
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
            #     l1_geo_top_visit_exclude_homework_daily,
            #     ["l0_geo_cust_cell_visit_time_for_l1_geo_top_visit_exclude_homework",
            #      "l1_homework_master_for_l1_geo_top_visit_exclude_homework",
            #      "params:l1_geo_top_visit_exclude_homework"
            #      ],
            #     "l1_geo_top_visit_exclude_homework"
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
            # ##==============================Update 2020-06-12 by Thatt529==========================================##
            #
            # ###total_distance_km###
            # node(l1_geo_total_distance_km_daily,
            #      ["l0_geo_cust_cell_visit_time_simple_data_daily",
            #       "params:l1_geo_total_distance_km_daily"
            #       ],
            #      "l1_geo_total_distance_km_daily"
            # ),
            #
            # ##Traffic_fav_location###
            # node(
            #     L1_data_traffic_home_work_Top1_TOP2,
            #     ["l0_geo_mst_cell_masterplan_current_for_l1_use_non_homework_features",
            #      "l0_geo_home_work_data_for_l1_use_non_homework_features",
            #      "l0_profile_customer_profile_ma_for_l1_use_non_homework_features",
            #      "l0_usage_sum_data_location_daily_for_l1_use_non_homework_features"
            #      ],
            #     "l1_geo_use_traffic_home_work"
            # ),

            ###Number_of_base_station###
            node(
                l1_geo_number_of_bs_used,
                ["l0_geo_cust_cell_visit_time_daily_for_l1_geo_number_of_bs_used",
                 "params:l1_geo_data_count_location_id"
                 ],
                "l1_geo_number_of_bs_used"
            )

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
            #
            # ###feature_AIS_store###
            # node(
            #     l1_location_of_visit_ais_store_daily,
            #     ["l0_mst_poi_shape_for_l1_location_of_last_visit_ais_store_daily",
            #      "l0_geo_cust_cell_visit_time_for_l1_location_of_visit_ais_store_daily",
            #      "params:l1_location_of_visit_ais_store_daily"
            #      ],
            #     "l1_location_of_visit_ais_store_daily"
            # ),
            # ##==============================Update 2020-06-15 by Thatt529==========================================##
            #
            # ##Top_3_cells_on_voice_usage###
            # node(
            #     l1_geo_top3_cells_on_voice_usage,
            #     ["l0_usage_sum_voice_location_daily_for_l1_geo_top3_cells_on_voice_usage",
            #      "l0_geo_mst_cell_masterplan_for_l1_geo_top3_cells_on_voice_usage",
            #      "l0_profile_customer_profile_ma_for_l1_geo_top3_cells_on_voice_usage"
            #      ],
            #     "l1_geo_top3_cells_on_voice_usage"
            # )

        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_union_pipeline"
    )

