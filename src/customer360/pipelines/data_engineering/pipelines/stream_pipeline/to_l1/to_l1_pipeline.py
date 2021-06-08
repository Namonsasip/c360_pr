# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
from kedro.pipeline import Pipeline, node
from kedro.context.context import load_context
from pyspark.sql import functions as f
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l1.to_l1_nodes import *
from customer360.utilities.re_usable_functions import l1_massive_processing


def streaming_sdr_sub_app_hourly_daily_for_l3_monthly(**kwargs):
    return Pipeline(
        [
            node(
                build_streaming_sdr_sub_app_hourly_for_l3_monthly,
                [
                    "l0_streaming_sdr_sub_app_hourly_for_l1_streaming_sdr_sub_app_hourly",
                    "l0_mobile_app_master",
                    "l1_customer_profile_union_daily_feature_for_l1_streaming_sdr_sub_app_hourly",
                ],
                "l1_streaming_sdr_sub_app_hourly",
            ),
            node(
                build_streaming_ufdr_streaming_quality_for_l3_monthly,
                [
                    "l0_streaming_soc_mobile_app_daily_for_l1_streaming_app_quality_features",
                    "l0_mobile_app_master",
                    "l1_customer_profile_union_daily_feature_for_l1_streaming_app_quality_features",
                ],
                "l1_streaming_app_quality_features",
            ),
            node(
                build_streaming_ufdr_streaming_favourite_base_station_for_l3_monthly,
                [
                    "l0_streaming_soc_mobile_app_daily_for_l1_streaming_base_station_features",
                    "l0_mobile_app_master",
                    "l0_geo_mst_cell_masterplan_current_for_l1_streaming_base_station_features",
                    "l1_customer_profile_union_daily_feature_for_l1_streaming_base_station_features",
                ],
                "l1_streaming_base_station_features",
            ),
        ]
    )


def streaming_to_l1_onair_vimmi_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                stream_process_ru_a_onair_vimmi,
                [
                    "l0_streaming_ru_a_onair_vimmi_usage_daily_for_multiple_outputs",
                    "l1_customer_profile_union_daily_feature_for_l1_streaming_fav_tv_show_by_episode_watched",
                    "l3_streaming_series_title_master",
                    # Content Type Features
                    "params:int_l1_streaming_content_type_features",
                    "params:l1_streaming_fav_content_group_by_volume",
                    "params:l1_streaming_fav_content_group_by_duration",
                    # TV Channel features
                    "params:int_l1_streaming_tv_channel_features",
                    "params:l1_streaming_fav_tv_channel_by_volume",
                    "params:l1_streaming_fav_tv_channel_by_duration",
                    # Favorite Episode
                    "params:int_l1_streaming_tv_show_features",
                    "params:l1_streaming_fav_tv_show_by_episode_watched",
                    # fav tv_show_by_share_of_completed_episodes
                    "params:int_l1_streaming_share_of_completed_episodes_features",
                    "params:int_l1_streaming_share_of_completed_episodes_ratio_features",
                    "params:l1_streaming_fav_tv_show_by_share_of_completed_episodes",
                ],
                [
                    # Content Type Features
                    "int_l1_streaming_content_type_features",
                    "l1_streaming_fav_content_group_by_volume",
                    "l1_streaming_fav_content_group_by_duration",
                    # TV Channel features
                    "int_l1_streaming_tv_channel_features",
                    "l1_streaming_fav_tv_channel_by_volume",
                    "l1_streaming_fav_tv_channel_by_duration",
                    # TV Show features
                    "int_l0_streaming_vimmi_table",
                    # fav tv_show_by_share_of_completed_episodes
                    "l1_streaming_fav_tv_show_by_share_of_completed_episodes",
                    # Favorite Episode
                    "l1_streaming_fav_tv_show_by_episode_watched",
                ],
            ),
        ]
    )


def streaming_to_l1_soc_mobile_data_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                stream_process_soc_mobile_data,
                [
                    "l0_streaming_soc_mobile_app_daily_for_multiple_outputs",
                    "l1_customer_profile_union_daily_feature_for_l1_streaming_visit_count_and_download_traffic_feature",
                    # # fav video service by download traffic
                    "params:int_l1_streaming_video_service_feature",
                    "params:l1_streaming_fav_video_service_by_download_feature",
                    "params:l1_streaming_2nd_fav_video_service_by_download_feature",
                    # fav music service by download traffic
                    "params:int_l1_streaming_music_service_feature",
                    "params:l1_streaming_fav_music_service_by_download_feature",
                    "params:l1_streaming_2nd_fav_music_service_by_download_feature",
                    # # # fav esport service by download traffic
                    "params:int_l1_streaming_esport_service_feature",
                    "params:l1_streaming_fav_esport_service_by_download_feature",
                    "params:l1_streaming_2nd_fav_esport_service_by_download_feature",
                    # # number of visit and volume of download traffic
                    "params:l1_streaming_visit_count_and_download_traffic_feature",
                ],
                [
                    "int_l1_streaming_video_service_feature",
                    "l1_streaming_fav_video_service_by_download_feature",
                    "l1_streaming_2nd_fav_video_service_by_download_feature",
                    "int_l1_streaming_music_service_feature",
                    "l1_streaming_fav_music_service_by_download_feature",
                    "l1_streaming_2nd_fav_music_service_by_download_feature",
                    "int_l1_streaming_esport_service_feature",
                    "l1_streaming_fav_esport_service_by_download_feature",
                    "l1_streaming_2nd_fav_esport_service_by_download_feature",
                    "l1_streaming_visit_count_and_download_traffic_feature",
                ],
            ),
        ]
    )


def streaming_to_l1_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                dac_for_streaming_to_l1_intermediate_pipeline,
                [
                    "l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                    "l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature",
                    "params:l1_streaming_session_duration_feature_tbl",
                ],
                [
                    "int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                    "int_l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature",
                ],
            ),
            node(
                application_duration,
                [
                    "int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature",
                    "l0_mobile_app_master",
                ],
                "int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature_application",
            ),
            node(
                l1_massive_processing,
                [
                    "int_l0_streaming_soc_mobile_app_daily_for_l1_streaming_session_duration_feature_application",
                    "params:l1_streaming_session_duration_feature",
                    "int_l1_customer_profile_union_daily_feature_l1_streaming_session_duration_feature",
                ],
                "l1_streaming_session_duration_feature",
            ),
        ],
        name="streaming_to_l1_session_duration_pipeline",
    )


def aib_category_cleanup_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=build_iab_category_table,
                inputs=["l0_iab_categories_raw", "l0_iab_category_priority_mapping"],
                outputs="l1_aib_categories_clean",
            ),
            node(
                func=build_stream_mobile_app_categories_master_table,
                inputs=[
                    "l0_stream_mobile_app_categories_master_raw",
                    "l0_iab_category_priority_mapping",
                ],
                outputs="l1_stream_mobile_app_categories_master_clean",
            ),
        ],
    )


def cxense_traffic_daily_agg_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_clean_datasets,
                inputs=[
                    "l0_cxense_traffic_raw",
                    "l0_cxense_content_profile_raw",
                ],
                outputs=[
                    "l1_cxense_traffic_int",
                    "l1_cxense_content_profile_int",
                ],
                tags=["node_clean_datasets"],
            ),
            node(
                func=node_create_content_profile_mapping,
                inputs=["l1_cxense_content_profile_int", "l1_aib_categories_clean"],
                outputs="l1_cxense_content_profile_mapping",
                tags=["node_create_content_profile_mapping"],
            ),
            node(
                func=node_agg_cxense_traffic,
                inputs="l1_cxense_traffic_int",
                outputs="l1_cxense_traffic_agg_daily",
                tags=["node_agg_cxense_traffic"],
            ),
            node(
                func=node_get_matched_and_unmatched_urls,
                inputs=[
                    "l1_cxense_traffic_agg_daily",
                    "l1_cxense_content_profile_mapping",
                ],
                outputs=["l1_matched_urls", "l1_unmatched_urls"],
                tags=["node_get_matched_and_unmatched_urls"],
            ),
            node(
                func=node_get_best_match_for_unmatched_urls,
                inputs=["l1_unmatched_urls", "l1_cxense_content_profile_mapping"],
                outputs="l1_best_match_for_unmatched_urls",
                tags=["node_get_best_match_for_unmatched_urls"],
            ),
            node(
                func=node_union_matched_and_unmatched_urls,
                inputs=["l1_matched_urls", "l1_best_match_for_unmatched_urls"],
                outputs="l1_cxense_traffic_complete_agg_daily",
                tags=["node_union_matched_and_unmatched_urls"],
            ),
            
        ]
    )


def soc_app_daily_agg_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_join_soc_hourly_with_aib_agg,
                inputs=[
                    "l0_soc_app_hourly_raw",
                    "l1_stream_mobile_app_categories_master_clean",
                ],
                outputs="l1_soc_app_hourly_with_iab@output",
                tags=["node_join_soc_hourly_with_aib_agg"],
            ),
            node(
                func=node_join_soc_daily_with_aib_agg,
                inputs=["l0_soc_app_daily_raw", "l1_aib_categories_clean"],
                outputs="l1_soc_app_daily_with_iab@output",
                tags=["node_join_soc_daily_with_aib_agg"],
            ),
            node(
                func=combine_soc_app_daily_and_hourly_agg,
                inputs=[
                    "l1_soc_app_daily_with_iab@l1_combined_soc_app_daily_and_hourly_agg",
                    "l1_soc_app_hourly_with_iab@l1_combined_soc_app_daily_and_hourly_agg",
                ],
                outputs="l1_combined_soc_app_daily_and_hourly_agg",
                tags=["combine_soc_app_daily_and_hourly_agg"],
            ),
            node(
                func=node_generate_soc_app_day_level_stats,
                inputs="l1_soc_app_daily_with_iab@l1_soc_app_day_level_stats",
                outputs="l1_soc_app_day_level_stats",
                tags=["node_generate_soc_day_level_stats"],
            ),
        ],
        tags=["soc_app"],
    )


def soc_app_feature_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_soc_app_daily_category_level_features_massive_processing,
                inputs=[
                    "l1_combined_soc_app_daily_and_hourly_agg@l1_soc_app_daily_category_level_features",
                    "l1_soc_app_day_level_stats@l1_soc_app_daily_category_level_features",
                    "l1_customer_profile_union_daily_feature@l1_soc_app_daily_category_level_features",
                    "params:l1_soc_app_daily_agg_features",
                    "params:l1_soc_app_daily_ratio_based_features",
                    "params:l1_soc_app_daily_popular_app_by_download_volume",
                    "params:l1_soc_app_daily_popular_app_by_frequency_access",
                    "params:l1_soc_app_daily_popular_app_by_visit_duration",
                    "params:l1_soc_app_daily_most_popular_app_by_download_volume",
                    "params:l1_soc_app_daily_most_popular_app_by_frequency_access",
                    "params:l1_soc_app_daily_most_popular_app_by_visit_duration",
                ],
                outputs="l1_soc_app_daily_category_level_features",
                tags=["node_soc_app_daily_category_level_features_massive_processing"],
            ),
            node(
                func=node_soc_app_daily_features_massive_processing,
                inputs=[
                    "l1_combined_soc_app_daily_and_hourly_agg@l1_soc_app_daily_features",
                    "l1_customer_profile_union_daily_feature@l1_soc_app_daily_features",
                    "params:l1_soc_app_daily_popular_category_by_frequency_access",
                    "params:l1_soc_app_daily_popular_category_by_visit_duration",
                    "params:l1_soc_app_daily_most_popular_category_by_frequency_access",
                    "params:l1_soc_app_daily_most_popular_category_by_visit_duration",
                ],
                outputs="l1_soc_app_daily_features",
                tags=["node_soc_app_daily_features_massive_processing"],
            ),
        ],
        tags=["soc_app"],
    )


def relay_to_l1_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_pageviews_daily_features,
                inputs=[
                    "l0_relay_page_views_raw_for_l1_relay_daily_pageviews_features",
                    "params:l1_relay_daily_total_pageviews_visits_count",
                    "params:l1_relay_daily_popular_url_by_pageviews",
                    "params:l1_relay_daily_popular_subcategory1_by_pageviews",
                    "params:l1_relay_daily_popular_subcategory2_by_pageviews",
                    "params:l1_relay_daily_popular_cid_by_pageviews",
                    "params:l1_relay_daily_popular_productname_by_pageviews",
                    "params:l1_relay_daily_most_popular_url_by_pageviews",
                    "params:l1_relay_daily_most_popular_subcategory1_by_pageviews",
                    "params:l1_relay_daily_most_popular_subcategory2_by_pageviews",
                    "params:l1_relay_daily_most_popular_cid_by_pageviews",
                    "params:l1_relay_daily_most_popular_productname_by_pageviews",
                ],
                outputs="l1_relay_daily_pageviews_features",
                tags=["node_pageviews_daily_features"],
            ),
            node(
                func=node_pageviews_cid_level_daily_features,
                inputs=["l0_relay_page_views_raw_for_l1_relay_page_views_cid_level_features",
                    "params:l1_relay_pageviews_visits_count_by_cid",
                ],
                outputs="l1_relay_page_views_cid_level_features",
                tags=["node_pageviews_cid_level_daily_features"],
            ),
            node(
                func=node_engagement_conversion_daily_features,
                inputs=[
                    "l0_relay_engagement_conversion_raw_for_l1_relay_daily_engagement_conversion_features",
                    "params:l1_relay_daily_popular_product_by_engagement_conversion",
                    "params:l1_relay_daily_popular_cid_by_engagement_conversion",
                    "params:l1_relay_daily_most_popular_product_by_engagement_conversion",
                    "params:l1_relay_daily_most_popular_cid_by_engagement_conversion",
                ],
                outputs="l1_relay_daily_engagement_conversion_features",
                tags=["node_engagement_conversion_daily_features"],
            ),
            node(
                func=node_engagement_conversion_cid_level_daily_features,
                inputs=[
                    "l0_relay_engagement_conversion_raw_for_l1_relay_daily_engagement_conversion_cid_level_features",
                    "params:l1_relay_daily_total_engagement_conversion_visits_count_by_cid",
                ],
                outputs="l1_relay_daily_engagement_conversion_cid_level_features",
                tags=["node_engagement_conversion_cid_level_daily_features"],
            ),
            node(
                func=node_engagement_conversion_package_daily_features,
                inputs=[
                    "l0_relay_engagement_conversion_package_raw_for_l1_relay_daily_engagement_conversion_package_features",
                    "params:l1_relay_daily_popular_product_by_engagement_conversion_package",
                    "params:l1_relay_daily_popular_cid_by_engagement_conversion_package",
                    "params:l1_relay_daily_most_popular_product_by_engagement_conversion_package",
                    "params:l1_relay_daily_most_popular_cid_by_engagement_conversion_package",
                ],
                outputs="l1_relay_daily_engagement_conversion_package_features",
                tags=["node_engagement_conversion_package_daily_features"],
            ),
            node(
                func=node_engagement_conversion_package_cid_level_daily_features,
                inputs=[
                    "l0_relay_engagement_conversion_package_raw_for_l1_relay_daily_engagement_conversion_package_cid_level_features",
                    "params:l1_relay_daily_total_engagement_conversion_package_visits_count_by_cid",
                ],
                outputs="l1_relay_daily_engagement_conversion_package_cid_level_features",
                tags=["node_engagement_conversion_package_cid_level_daily_features"],
            ),
        ]
    )


def soc_web_daily_agg_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_join_soc_web_daily_with_with_aib_agg,
                inputs=["l0_soc_web_daily_raw", "l1_aib_categories_clean"],
                outputs="l1_soc_web_daily_with_iab@output",
                tags=["node_join_soc_web_daily_with_with_aib_agg"],
            ),
            node(
                func=node_join_soc_web_hourly_with_with_aib_agg,
                inputs=["l0_soc_web_hourly_raw", "l1_aib_categories_clean"],
                outputs="l1_soc_web_hourly_with_iab@output",
                tags=["node_join_soc_web_hourly_with_with_aib_agg"],
            ),
            node(
                func=combine_soc_web_daily_and_hourly_agg,
                inputs=[
                    "l1_soc_web_daily_with_iab@l1_combined_soc_web_daily_and_hourly_agg",
                    "l1_soc_web_hourly_with_iab@l1_combined_soc_web_daily_and_hourly_agg",
                ],
                outputs="l1_combined_soc_web_daily_and_hourly_agg",
                tags=["node_combine_soc_app_daily_and_hourly_agg"],
            ),
            node(
                func=node_generate_soc_web_day_level_stats,
                inputs="l1_soc_web_daily_with_iab@l1_soc_web_day_level_stats",
                outputs="l1_soc_web_day_level_stats",
                tags=["node_generate_soc_web_day_level_stats"],
            ),
        ],
        tags=["soc_web"],
    )


def soc_web_feature_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_soc_web_daily_category_level_features_massive_processing,
                inputs=[
                    "l1_combined_soc_web_daily_and_hourly_agg@l1_soc_web_daily_category_level_features",
                    "l1_soc_web_day_level_stats@l1_soc_web_daily_category_level_features",
                    "l1_customer_profile_union_daily_feature@l1_soc_web_daily_category_level_features",
                    "params:l1_soc_web_daily_agg_features",
                    "params:l1_soc_web_daily_ratio_based_features",
                    "params:l1_soc_web_daily_popular_domain_by_download_volume",
                    "params:l1_soc_web_daily_most_popular_domain_by_download_volume",
                ],
                outputs="l1_soc_web_daily_category_level_features",
                tags=["node_soc_web_daily_category_level_features_massive_processing"],
            ),
            node(
                func=node_soc_web_daily_features_massive_processing,
                inputs=[
                    "l1_combined_soc_web_daily_and_hourly_agg@l1_soc_web_daily_features",
                    "l1_customer_profile_union_daily_feature@l1_soc_web_daily_features",
                    "params:l1_soc_web_daily_popular_category_by_download_volume",
                    "params:l1_soc_web_daily_most_popular_category_by_download_volume",
                ],
                outputs="l1_soc_web_daily_features",
                tags=["node_soc_web_daily_features_massive_processing"],
            ),
        ],
        tags=["soc_web"],
    )


def comb_all_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_combine_soc_all_and_cxense,
                inputs=[
                    "l1_cxense_traffic_complete_agg_daily_for_l1_comb_all",
                    "l1_comb_soc_web_and_app_for_l1_comb_all",
                ],
                outputs="l1_comb_all@output",
                tags=["node_combine_soc_all_and_cxense"],
            ),
            node(
                func=node_comb_all_features_massive_processing,
                inputs=[
                    "l1_comb_all@l1_comb_all_features",
                    "params:l1_comb_all_create_single_view",
                    "params:l1_com_all_day_level_stats",
                    "params:l1_comb_all_sum_features",
                    "params:l1_comb_all_sum_and_ratio_based_features",
                    "params:l1_comb_all_popular_app_or_url",
                    "params:l1_comb_all_most_popular_app_or_url_by_visit_count",
                    "params:l1_comb_all_most_popular_app_or_url_by_visit_duration",
                ],
                outputs="l1_comb_all_features",
                tags=["node_comb_all_features_massive_processing"],
            ),
            node(
                func=node_comb_all_daily_features_massive_processing,
                inputs=[
                    "l1_comb_all@l1_comb_all_daily_features",
                    "params:l1_comb_all_popular_category",
                    "params:l1_comb_all_most_popular_category_by_visit_counts",
                    "params:l1_comb_all_most_popular_category_by_visit_duration",
                ],
                outputs="l1_comb_all_daily_features",
                tags=["node_comb_all_daily_features_massive_processing"],
            ),
        ],
        tags=["comb_all"],
    )


def comb_soc_app_web_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_combine_soc_app_and_web_massive_processing,
                inputs=[
                    "l1_combined_soc_app_daily_and_hourly_agg_for_l1_comb_soc_web_and_app",
                    "l1_combined_soc_web_daily_and_hourly_agg_for_l1_comb_soc_web_and_app",
                    "l1_customer_profile_union_daily_feature_for_l1_comb_soc_web_and_app",
                ],
                outputs="l1_comb_soc_web_and_app@output",
                tags=["node_combine_soc_app_and_web_massive_processing"],
            ),
            node(
                func=node_comb_soc_app_web_features_massive_processing,
                inputs=[
                    "l1_comb_soc_web_and_app@l1_comb_soc_features",
                    "params:l1_comb_soc_sum_features",
                    "params:l1_comb_soc_daily_stats",
                    "params:l1_comb_soc_popular_app_or_url",
                    "params:l1_comb_soc_most_popular_app_or_url",
                    "params:l1_comb_soc_web_fea_all",
                ],
                outputs="l1_comb_soc_features",
                tags=["node_comb_soc_app_web_features_massive_processing"],
            ),
            node(
                func=node_comb_soc_app_web_daily_features_massive_processing,
                inputs=[
                    "l1_comb_soc_web_and_app@l1_comb_soc_daily_features",
                    "params:l1_comb_soc_app_web_popular_category_by_download_traffic",
                    "params:l1_comb_soc_app_web_most_popular_category_by_download_traffic",
                ],
                outputs="l1_comb_soc_daily_features",
                tags=["node_comb_soc_app_web_daily_features_massive_processing"],
            ),
        ],
        tags=["soc_comb"],
    )


def comb_web_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_comb_web_daily_agg_massive_processing,
                inputs=[
                    "l1_cxense_traffic_complete_agg_daily_for_l1_comb_web_agg",
                    "l1_combined_soc_web_daily_and_hourly_agg_for_l1_comb_web_agg",
                    "l1_customer_profile_union_daily_feature_for_l1_comb_web_agg",
                    "params:l1_comb_web_agg",
                ],
                outputs="l1_comb_web_agg@output",
                tags=["node_comb_web_daily_agg_massive_processing"],
            ),
            node(
                func=node_comb_web_daily_category_level_features_massive_processing,
                inputs=[
                    "l1_comb_web_agg@l1_comb_web_category_level_features",
                    "params:l1_comb_web_day_level_stats",
                    "params:l1_comb_web_total_category_sum_features",
                    "params:l1_comb_web_total_sum_and_ratio_features",
                    "params:l1_comb_web_daily_popular_url",
                    "params:l1_comb_web_daily_most_popular_url_by_visit_duration",
                    "params:l1_comb_web_daily_most_popular_url_by_visit_counts",
                ],
                outputs="l1_comb_web_category_level_features",
                tags=["node_comb_web_daily_category_level_features_massive_processing"],
            ),
            node(
                func=node_comb_web_daily_features_massive_processing,
                inputs=[
                    "l1_comb_web_agg@l1_comb_web_daily_features",
                    "params:l1_comb_web_daily_popular_category",
                    "params:l1_comb_web_daily_most_popular_category_by_visit_duration",
                    "params:l1_comb_web_daily_most_popular_category_by_visit_counts",
                ],
                outputs="l1_comb_web_daily_features",
                tags=["node_comb_web_daily_features_massive_processing"],
            ),
        ],
        tags=["comb_web_all_features"],
    )
