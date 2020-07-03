from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import *
from customer360.pipelines.data_engineering.nodes.geolocation_nodes.to_l4.to_l4_nodes import *
from customer360.utilities.re_usable_functions import l1_massive_processing



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
                 "l4_geo_home_work_location_id"
                 # ,"params:l4_geo_top_visit_exclude_homework"
                 ],
                "l4_geo_top_visit_exclude_homework"
            ),

            ### Home and Work Feature
            node(
                massive_processing_for_home_work,  # partition_date is main column to massive processing
                ["l0_geo_cust_cell_visit_time_for_int_l4_geo_home_work_location_id",
                 "params:int_l4_geo_home_location_id_monthly",
                 "params:int_l4_geo_work_location_id_monthly"
                 ],
                ["int_l4_geo_home_location_id_monthly",
                 "int_l4_geo_work_location_id_monthly"
                 ]
            ),

            ### List imsi from int_HOME/WORK
            node(
                int_geo_home_work_list_imsi,
                ["int_l4_geo_home_location_id_monthly",
                 "int_l4_geo_work_location_id_monthly"
                 ],
                "geo_home_work_list_imsi_stg"  # In memory Dataframe
            ),

            node(
                int_geo_work_location_id,
                ["int_l4_geo_work_location_id_monthly",
                 "geo_home_work_list_imsi_stg"
                 ],
                "int_work_location_id"  # In memory Dataframe
            ),

            node(
                int_geo_home_location_id,
                ["int_l4_geo_home_location_id_monthly"
                 ],
                ["int_home_weekday_location_id",  # In memory Dataframe
                 "int_home_weekend_location_id"  # In memory Dataframe
                 ]
            ),

            node(
                l4_geo_home_work_location_id,
                ["int_home_weekday_location_id",
                 "int_home_weekend_location_id",
                 "int_work_location_id",
                 "params:l4_geo_home_work_location_id"
                 ],
                "l4_geo_home_work_location_id"
            ),

            node(
                l4_geo_home_weekday_city_citizens,
                ["l4_geo_home_work_location_id_for_l4_geo_home_weekday_city_citizens",
                 "l0_mst_cell_masterplan_for_l4_geo_home_weekday_city_citizens",
                 "params:l4_geo_home_weekday_city_citizens"
                 ],
                "l4_geo_home_weekday_city_citizens"
            ),

            node(
                l4_rolling_window,
                ["l2_geo_cust_subseqently_distance_weekly_for_l4_geo_cust_subseqently_distance",
                 "params:l4_geo_cust_subseqently_distance"
                 ],
                "l4_geo_cust_subseqently_distance"
            ),
            ##==============================Update 2020-06-12 by Thatt529==========================================##

            ###total_distance_km###
            node(
                l4_rolling_window_de,
                ["l2_geo_total_distance_km_weekly_for_l4_geo_total_distance_km",
                 "params:l4_geo_total_distance_km"
                 ],
                "l4_geo_total_distance_km"
            ),

            ###Traffic_fav_location###
            node(
                l4_rolling_window,
                ["l2_geo_use_traffic_home_work_weekly",
                 "params:l4_geo_use_traffic_home_work_weekly"
                 ],
                "int_l4_geo_use_traffic_home_work_weekly"
            ),
            node(
                l4_Share_traffic,
                "int_l4_geo_use_traffic_home_work_weekly",
                "l4_geo_use_traffic_home_work_weekly"
            ),

            ###Number_of_base_station###
            node(
                l4_rolling_window,
                ["l2_geo_number_of_base_station_weekly_for_l4_geo_number_of_base_station",
                 "params:l4_geo_number_of_base_station"
                 ],
                "l4_geo_number_of_base_station"
            ),

            ###feature_sum_voice_location###
            node(
                l4_rolling_window,
                ["l2_geo_call_location_home_work_weekly_for_l4_geo_call_location_home_work_weekly",
                 "params:l4_geo_call_home_work_location_weekly"
                 ],
                "l4_geo_call_location_home_work_weekly"
            ),

            ###Number of Unique Cells Used###
            node(
                l4_geo_number_unique_cell_used,
                ["l0_usage_sum_data_location_daily_for_l4_geo_number_unique_cell_used",
                 "params:l4_geo_number_unique_cell_used"
                 ],
                "l4_geo_number_unique_cell_used"
            ),


            ##feature_AIS_store###
            node(
                l4_geo_last_AIS_store_visit,
                ["l1_location_of_visit_ais_store_daily_for_l4_location_of_last_visit_ais_store",
                 "params:l4_geo_last_AIS_store_visit"
                 ],
                "l4_geo_last_AIS_store_visit"
            ),
            node(
                l4_geo_most_AIS_store_visit,
                ["l1_location_of_visit_ais_store_daily_for_l4_location_of_last_visit_ais_store",
                 "params:l4_geo_most_AIS_store_visit"
                 ],
                "l4_geo_most_AIS_store_visit"
            ),
            node(
                l4_geo_store_close_to_home,
                ["l4_geo_home_work_location_id",
                 "l0_mst_poi_shape_for_l1_geo_area_from_ais_store_daily",
                 "params:l4_geo_store_close_to_home"
                 ],
                "l4_geo_store_close_to_home"
            ),
            node(
                l4_geo_store_close_to_work,
                ["l4_geo_home_work_location_id",
                "l0_mst_poi_shape_for_l1_geo_area_from_ais_store_daily",
                 "params:l4_geo_store_close_to_work"
                 ],
                "l4_geo_store_close_to_work"
            ),
            ##==============================Update 2020-06-15 by Thatt529==========================================##

            ##Top_3_cells_on_voice_usage###
            node(
                l4_rolling_window,
                ["l2_geo_top3_cells_on_voice_usage",
                 "params:l4_geo_top3_cells_on_voice_usage"
                 ],
                "l4_geo_top3_cells_on_voice_usage"
            ),

            node(
                l4_geo_work_area_center_average,
                ["l0_geo_cust_location_visit_hr_for_l4_geo_work_area_center_average",
                 "l4_geo_home_work_location_id_for_l4_geo_work_area_center_average",
                 "params:l4_geo_work_area_center_average"
                 ],
                "l4_geo_work_area_center_average"
            ),

            ##==============================Update 2020-06-17 by Thatt529==========================================##

            ##distance_top_call###
            node(
                l4_rolling_window_de,
                ["l2_geo_distance_top_call",
                 "params:l4_geo_distance_top_call"
                 ],
                "l4_geo_distance_top_call"
            ),

            ###Distance between nearest store and most visited store###
            node(
                l4_geo_range_from_most_visited,
                ["l1_location_of_visit_ais_store_daily_for_l4_location_of_last_visit_ais_store",
                 "l4_geo_store_close_to_home",
                 "params:l4_geo_range_from_most_visited"
                 ],
                "l4_geo_range_from_most_visited"
            ),

            # Form
            # 48 The most frequently used Location for data sessions on weekdays (Mon to Fri)
            node(
                l4_the_most_frequently_location_weekdays,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location_weekdays"
            ),

            # #49 The most frequently used Location for data sessions on weekdays (Mon to Fri) is 4G flag
            node(
                l4_the_most_frequently_location_weekdays_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location_weekdays_4g"
            ),

            # 50 The most frequently used Location for data sessions on weekends
            node(
                l4_the_most_frequently_location_weekends,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location_weekends"
            ),

            # 51 The most frequently used Location for data sessions on weekends is 4G flag
            node(
                l4_the_most_frequently_location_weekends_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location_weekends_4g"
            ),

            # 52 The most frequently used Location for data sessions
            node(
                l4_the_most_frequently_location,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location"
            ),

            # 53 The most frequently used Location for data sessions is 4G flag
            node(
                l4_the_most_frequently_location_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_most_frequently_location_4g"
            ),

            # 54 The second most frequently used cell for data sessions on weekdays (Mon to Fri)
            node(
                l4_the_second_frequently_location_weekdays,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location_weekdays"
            ),

            # 55 The second most frequently used cell for data sessions on weekdays (Mon to Fri) is 4G flag
            node(
                l4_the_second_frequently_location_weekdays_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location_weekdays_4g"
            ),

            # 56 The second most frequently used cell for data sessions on weekends
            node(
                l4_the_second_frequently_location_weekends,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location_weekends"
            ),

            # 57 The second most frequently used cell for data sessions on weekends is 4G flag
            node(
                l4_the_second_frequently_location_weekends_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location_weekends_4g"
            ),

            # 58 The second most frequently used cell for data sessions
            node(
                l4_the_second_frequently_location,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location"
            ),

            # 59 The second most frequently used cell for data sessions is 4G flag
            node(
                l4_the_second_frequently_location_4g,
                ["l1_the_favourite_locations_daily"],
                "l4_the_second_frequently_location_4g"
            ),

            ##============== Number of used most frequent top five =====================
            node(
                l4_geo_number_most_frequent_top_five_weekday,
                ["l1_the_favourite_locations_daily",
                 "l4_the_most_frequently_location_weekdays",
                 "params:l4_area_from_number_of_used_most_frequent_top_5_weekday"
                 ],
                "l4_geo_number_most_frequent_top_five_weekdays"
            ),
            node(
                l4_geo_number_most_frequent_top_five_weekend,
                ["l1_the_favourite_locations_daily",
                 "l4_the_most_frequently_location_weekends",
                 "params:l4_area_from_number_of_used_most_frequent_top_5_weekend"
                 ],
                "l4_geo_number_most_frequent_top_five_weekends"
            ),
            node(
                l4_geo_number_most_frequent_top_five,
                ["l1_the_favourite_locations_daily",
                 "l4_the_most_frequently_location",
                 "params:l4_area_from_number_of_used_most_frequent_top_5"
                 ],
                "l4_geo_number_most_frequent_top_five"
            ),
            ## ============== Number of used most frequent =====================
            node(
                l4_geo_number_most_frequent_weekday,
                ["l1_the_favourite_locations_daily",
                 "l4_the_most_frequently_location_weekdays",
                 "params:l4_area_from_number_of_used_most_frequent_weekdays"
                 ],
                "l4_geo_from_number_of_used_most_frequent_weekdays"
            ),
            node(
                l4_geo_number_most_frequent_weekend,
                ["l1_the_favourite_locations_daily",
                 "l4_the_most_frequently_location_weekends",
                 "params:l4_area_from_number_of_used_most_frequent_weekends"
                 ],
                "l4_geo_from_number_of_used_most_frequent_weekends"
            )

        ], name="geo_to_l4_pipeline"
    )



