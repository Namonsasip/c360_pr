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


            node(
                l1_geo_cust_subseqently_distance,
                ["l0_geo_cust_cell_visit_time_for_l1_geo_cust_subseqently_distance_daily",
                 "params:l1_geo_cust_subseqently_distance_daily"
                 ],
                "l1_geo_cust_subseqently_distance_daily"
            )

        ], name="geo_to_l1_pipeline"
    )


def geo_to_l1_union_pipeline(**kwargs):
    return Pipeline(
        [


        ], name="geo_to_l1_union_pipeline"
    )
