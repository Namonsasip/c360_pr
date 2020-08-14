from kedro.pipeline import Pipeline, node

from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.re_usable_functions import l1_massive_processing


def geo_to_l4_pipeline(**kwargs):
    return Pipeline(
        [

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_time_spent_by_location_weekly",
                 "params:l4_geo_time_spent_by_location"
                 ],
                "l4_geo_time_spent_by_location"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_time_spent_by_store_weekly",
                 "params:l4_geo_time_spent_by_store"
                 ],
                "l4_geo_time_spent_by_store"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_count_visit_by_location_weekly",
                 "params:l4_geo_count_visit_by_location"
                 ],
                "l4_geo_count_visit_by_location"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l2_geo_total_distance_km_weekly",
                 "params:l4_geo_total_distance_km"
                 ],
                "l4_geo_total_distance_km"
            ),

            # ### FINISH
            # node(
            #     node_from_config,
            #     ["l3_geo_home_work_location_id_monthly",
            #      "params:l4_geo_home_work_location_id"
            #      ],
            #     "l4_geo_home_work_location_id"
            # ),

            ### FINISH
            node(
                node_from_config,
                ["l3_geo_top3_visit_exclude_hw_monthly",
                 "params:l4_geo_top3_visit_exclude_hw"
                 ],
                "l4_geo_top3_visit_exclude_hw"
            ),

            ### FINISH
            node(
                node_from_config,
                ["l3_geo_work_area_center_average_monthly",
                 "params:l4_geo_work_area_center_average"
                 ],
                "l4_geo_work_area_center_average"
            ),

            ### FINISH
            node(
                l4_rolling_window_geo,
                ["l3_geo_home_weekday_city_citizens_monthly",
                 "params:l4_geo_home_weekday_city_citizens"
                 ],
                "l4_geo_home_weekday_city_citizens"
            ),

            # ### WAIT
            # node(
            #     node_from_config,
            #     ["l3_geo_visit_ais_store_location_monthly",
            #      "params:l4_geo_visit_ais_store_location"
            #      ],
            #     "l4_geo_visit_ais_store_location"
            # ),

            # ### WAIT
            # node(
            #     node_from_config,
            #     ["l2_geo_top3_voice_location_weekly",
            #      "params:l4_geo_top3_voice_location"
            #      ],
            #     "l4_geo_top3_voice_location"
            # ),

            #             # Number of Unique Cells Used###
            #             node(
            #                 l4_geo_number_unique_cell_used,
            #                 ["l1_number_of_unique_cell_daily_for_l4_number_of_unique_cell_weekly"
            #                  ],
            #                 "l4_geo_number_unique_cell_used"
            #             ),
            #

        ], name="geo_to_l4_pipeline"
    )



# def geo_to_l4_pipeline(**kwargs):
#     return Pipeline(
#         [
#
#
#             ### FINISH
#             ###Traffic_fav_location###
#             node(
#                 l4_Share_traffic,
#                 ["l3_geo_use_traffic_home_work_for_l4_geo_use_traffic_home_work_most",
#                 "params:int_l4_geo_use_traffic_home_work_weekly",
#                 "params:l4_geo_use_traffic_home_work_weekly"
#                  ],
#                 "l4_geo_use_traffic_home_work_most"
#             ),
  #             ###feature_sum_voice_location###
            #             node(
            #                 l4_rolling_window,
            #                 ["l3_geo_call_location_home_work_monthly",
            #                  "params:l4_geo_call_home_work_location"
            #                  ],
            #                 "l4_geo_call_home_work_location"
            #             ),
#
#
#
#
#             # ###47 the_favourite_locations###
#             # node(
#             #     l4_the_favourite_locations_daily,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_favourite_locations_daily"],
#             #     "l4_the_favourite_locations_daily"
#             # ),
#             #
#             # # 48 The most frequently used Location for data sessions on weekdays (Mon to Fri)
#             # node(
#             #     l4_the_most_frequently_location_weekdays,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location_weekdays"],
#             #     "l4_the_most_frequently_location_weekdays"
#             # ),
#             #
#             # # #49 The most frequently used Location for data sessions on weekdays (Mon to Fri) is 4G flag
#             # node(
#             #     l4_the_most_frequently_location_weekdays_4g,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location_weekdays_4g"],
#             #     "l4_the_most_frequently_location_weekdays_4g"
#             # ),
#             #
#             # # 50 The most frequently used Location for data sessions on weekends
#             # node(
#             #     l4_the_most_frequently_location_weekends,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location_weekends"],
#             #     "l4_the_most_frequently_location_weekends"
#             # ),
#             #
#             # # 51 The most frequently used Location for data sessions on weekends is 4G flag
#             # node(
#             #     l4_the_most_frequently_location_weekends_4g,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location_weekends_4g"],
#             #     "l4_the_most_frequently_location_weekends_4g"
#             # ),
#             #
#             # # 52 The most frequently used Location for data sessions
#             # node(
#             #     l4_the_most_frequently_location,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location"],
#             #     "l4_the_most_frequently_location"
#             # ),
#             #
#             # # 53 The most frequently used Location for data sessions is 4G flag
#             # node(
#             #     l4_the_most_frequently_location_4g,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_most_frequently_location_4g"],
#             #     "l4_the_most_frequently_location_4g"
#             # ),
#             #
#             # # 54 The second most frequently used cell for data sessions on weekdays (Mon to Fri)
#             # node(
#             #     l4_the_second_frequently_location_weekdays,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_second_frequently_location_weekdays"],
#             #     "l4_the_second_frequently_location_weekdays"
#             # ),
#             #
#             # # 55 The second most frequently used cell for data sessions on weekdays (Mon to Fri) is 4G flag
#             # node(
#             #     l4_the_second_frequently_location_weekdays_4g,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_second_frequently_location_weekdays_4g"],
#             #     "l4_the_second_frequently_location_weekdays_4g"
#             # ),
#             #
#             # # 56 The second most frequently used cell for data sessions on weekends
#             # node(
#             #     l4_the_second_frequently_location_weekends,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_second_frequently_location_weekends"],
#             #     "l4_the_second_frequently_location_weekends"
#             # ),
#             #
#             # # 57 The second most frequently used cell for data sessions on weekends is 4G flag
#             # node(
#             #     l4_the_second_frequently_location_weekends_4g,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_second_frequently_location_weekends_4g"],
#             #     "l4_the_second_frequently_location_weekends_4g"
#             # ),
#             #
#             # # 58 The second most frequently used cell for data sessions
#             # node(
#             #     l4_the_second_frequently_location,
#             #     ["l1_the_favourite_locations_daily_for_l4_the_second_frequently_location"],
#             #     "l4_the_second_frequently_location"
#             # ),
#             #
#             # # 59 The second most frequently used cell for data sessions is 4G flag
#             # node(
#             #     l4_the_second_frequently_location_4g,
#             #     ["l1_the_favourite_locations_daily_for_the_second_frequently_location_4g"],
#             #     "l4_the_second_frequently_location_4g"
#             # ),
#             #
#             # # Number of used most frequent top five
#             # node(
#             #     l4_geo_number_most_frequent_top_five_weekday,
#             #     ["l1_the_favourite_locations_daily_for_l4_geo_number_most_frequent_top_five_weekday",
#             #      "l4_the_most_frequently_location_weekdays",
#             #      "params:l4_area_from_number_of_used_most_frequent_top_5_weekday"
#             #      ],
#             #     "l4_geo_number_most_frequent_top_five_weekdays"
#             # ),
#             # node(
#             #     l4_geo_number_most_frequent_top_five_weekend,
#             #     ["l1_the_favourite_locations_daily_for_l4_geo_number_most_frequent_top_five_weekend",
#             #      "l4_the_most_frequently_location_weekends",
#             #      "params:l4_area_from_number_of_used_most_frequent_top_5_weekend"
#             #      ],
#             #     "l4_geo_number_most_frequent_top_five_weekends"
#             # ),
#             # node(
#             #     l4_geo_number_most_frequent_top_five,
#             #     ["l2_the_favourite_locations_weekly"],
#             #     "l4_geo_number_most_frequent_top_five"
#             # ),
#             #
#             # # Number of used most frequent
#             # node(
#             #     l4_geo_number_most_frequent_weekday,
#             #     ["l1_the_favourite_locations_daily_for_l4_geo_number_most_frequent_weekday",
#             #      "l4_the_most_frequently_location_weekdays",
#             #      "params:l4_area_from_number_of_used_most_frequent_weekdays"
#             #      ],
#             #     "l4_geo_from_number_of_used_most_frequent_weekdays"
#             # ),
#             # node(
#             #     l4_geo_number_most_frequent_weekend,
#             #     ["l1_the_favourite_locations_daily_for_l4_geo_number_most_frequent_weekend",
#             #      "l4_the_most_frequently_location_weekends",
#             #      "params:l4_area_from_number_of_used_most_frequent_weekends"
#             #      ],
#             #     "l4_geo_from_number_of_used_most_frequent_weekends"
#             # ),
#
#         ], name="geo_to_l4_pipeline"
#     )