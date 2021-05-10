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
"""Example code for the nodes in the example pipeline. This code is meant
just for illustrating basic Kedro features.

PLEASE DELETE THIS FILE ONCE YOU START WORKING ON YOUR OWN PROJECT!
"""

from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import expansion
from customer360.pipelines.data_engineering.nodes.stream_nodes.to_l3.to_l3_nodes import *


def streaming_series_title_master(**kwargs):
    return Pipeline(
        [
            node(
                series_title_master,
                "l0_streaming_ru_a_onair_vimmi_usage_daily_for_series_title_master",
                "l3_streaming_series_title_master",
            ),
        ]
    )


def streaming_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            # # Content Type Features
            node(
                streaming_to_l3_content_type_features,
                [
                    "int_l1_streaming_content_type_features_for_l3_streaming_fav_content_group",
                    "params:int_l3_streaming_content_type_features",
                    "params:l3_streaming_fav_content_group_by_volume",
                    "params:l3_streaming_fav_content_group_by_duration",
                ],
                [
                    "l3_streaming_fav_content_group_by_volume",
                    "l3_streaming_fav_content_group_by_duration",
                ],
            ),
            # # TV Channel features
            node(
                streaming_to_l3_tv_channel_type_features,
                [
                    "int_l1_streaming_tv_channel_features_for_l3_streaming_fav_tv_channel",
                    "params:int_l3_streaming_tv_channel_features",
                    "params:l3_streaming_fav_tv_channel_by_volume",
                    "params:l3_streaming_fav_tv_channel_by_duration",
                ],
                [
                    "l3_streaming_fav_tv_channel_by_volume",
                    "l3_streaming_fav_tv_channel_by_duration",
                ],
            ),
            # # TV show features
            node(
                streaming_streaming_fav_tv_show_by_episode_watched_features,
                [
                    "int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_episode_watched",
                    "params:int_l3_streaming_tv_show_features",
                    "params:l3_streaming_fav_tv_show_by_episode_watched",
                    "params:int_l3_streaming_genre",
                    "params:l3_streaming_fav_genre",
                ],
                [
                    "l3_streaming_fav_tv_show_by_episode_watched",
                    "l3_streaming_fav_genre",
                ],
            ),
            # # fav video service by download traffic/visit count
            node(
                streaming_fav_service_download_traffic_visit_count,
                [
                    "int_l1_streaming_video_service_feature_for_l3_streaming_fav_video_service_feature",
                    "params:int_l3_streaming_service_feature",
                    "params:l3_streaming_fav_video_service_by_download_feature",
                    "params:l3_streaming_2nd_fav_video_service_by_download_feature",
                    "params:l3_streaming_fav_video_service_by_visit_count_feature",
                ],
                [
                    "l3_streaming_fav_video_service_by_download_feature",
                    "l3_streaming_2nd_fav_video_service_by_download_feature",
                    "l3_streaming_fav_video_service_by_visit_count_feature",
                ],
            ),
            # # fav music service by download traffic/visit count
            node(
                streaming_fav_service_download_traffic_visit_count,
                [
                    "int_l1_streaming_music_service_feature_for_l3_streaming_fav_music_service_feature",
                    "params:int_l3_streaming_service_feature",
                    "params:l3_streaming_fav_music_service_by_download_feature",
                    "params:l3_streaming_2nd_fav_music_service_by_download_feature",
                    "params:l3_streaming_fav_music_service_by_visit_count_feature",
                ],
                [
                    "l3_streaming_fav_music_service_by_download_feature",
                    "l3_streaming_2nd_fav_music_service_by_download_feature",
                    "l3_streaming_fav_music_service_by_visit_count_feature",
                ],
            ),
            # fav esport service by download traffic/visit count
            node(
                streaming_fav_service_download_traffic_visit_count,
                [
                    "int_l1_streaming_esport_service_feature_for_l3_streaming_fav_esport_service_feature",
                    "params:int_l3_streaming_service_feature",
                    "params:l3_streaming_fav_esport_service_by_download_feature",
                    "params:l3_streaming_2nd_fav_esport_service_by_download_feature",
                    "params:l3_streaming_fav_esport_service_by_visit_count_feature",
                ],
                [
                    "l3_streaming_fav_esport_service_by_download_feature",
                    "l3_streaming_2nd_fav_esport_service_by_download_feature",
                    "l3_streaming_fav_esport_service_by_visit_count_feature",
                ],
            ),
            # number of visit and volume of download traffic
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                [
                    "l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_visit_count_and_download_traffic_feature",
                    "params:l3_streaming_visit_count_and_download_traffic_feature_tbl",
                ],
                "intermediate_l3_streaming_visit_count_and_download_traffic_feature",
            ),
            node(
                expansion,
                [
                    "intermediate_l3_streaming_visit_count_and_download_traffic_feature",
                    "params:l3_streaming_visit_count_and_download_traffic_feature",
                ],
                "l3_streaming_visit_count_and_download_traffic_feature",
            ),
            # Favourite streaming day of week
            # get sum per day of week
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                [
                    "l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature",
                    "params:l3_streaming_fav_youtube_video_streaming_day_of_week_feature_tbl",
                ],
                "intermediate_l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature",
            ),
            node(
                expansion,
                [
                    "intermediate_l1_streaming_visit_count_and_download_traffic_feature_for_l3_streaming_fav_youtube_video_streaming_day_of_week_feature",
                    "params:int_l3_streaming_sum_per_day",
                ],
                "int_l3_streaming_sum_per_day",
            ),
            # rank of day per week
            node(
                node_from_config,
                [
                    "int_l3_streaming_sum_per_day",
                    "params:int_l3_streaming_ranked_of_day_per_month",
                ],
                "int_l3_streaming_ranked_of_day_per_month",
            ),
            # generate all the tables inside
            node(
                generate_l3_fav_streaming_day,
                ["int_l3_streaming_ranked_of_day_per_month", "params:streaming_app"],
                None,
            ),
            node(
                streaming_to_l3_fav_tv_show_by_share_of_completed_episodes,
                [
                    "int_l0_streaming_vimmi_table_for_l3_streaming_fav_tv_show_by_share_of_completed_episodes",
                    "l3_streaming_series_title_master",
                    "params:int_l3_streaming_share_of_completed_episodes_features",
                    "params:int_l3_streaming_share_of_completed_episodes_ratio_features",
                    "params:l3_streaming_fav_tv_show_by_share_of_completed_episodes",
                ],
                "l3_streaming_fav_tv_show_by_share_of_completed_episodes",
            ),
        ],
        name="streaming_to_l3_pipeline",
    )


def streaming_to_l3_sdr_sub_app_time_based_features(**kwargs):
    return Pipeline(
        [
            node(
                streaming_favourite_start_hour_of_day_func,
                "l1_streaming_sdr_sub_app_hourly",
                None,
            ),
        ],
        name="streaming_to_l3_sdr_sub_app_time_based_features",
    )


def streaming_to_l3_favourite_location_features(**kwargs):
    return Pipeline(
        [
            node(
                streaming_favourite_location_features_func,
                "l1_streaming_base_station_features",
                "l3_streaming_favourite_location_features",
            ),
            node(
                streaming_favourite_quality_features_func,
                "l1_streaming_app_quality_features",
                "l3_streaming_app_quality_features",
            ),
        ],
        name="streaming_to_l3_favourite_location_features",
    )


def streaming_to_l3_session_duration_pipeline(**kwargs):
    return Pipeline(
        [
            # session duration
            node(
                dac_for_streaming_to_l3_pipeline_from_l1,
                [
                    "l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature",
                    "params:l3_streaming_session_duration_feature_tbl",
                ],
                "intermediate_l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature",
            ),
            node(
                node_from_config,
                [
                    "intermediate_l1_streaming_session_duration_feature_for_l3_streaming_session_duration_feature",
                    "params:l3_streaming_session_duration_feature",
                ],
                "l3_streaming_session_duration_feature",
            ),
        ],
        name="streaming_to_l3_session_duration_pipeline",
    )


def soc_app_monthly_feature_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_compute_int_soc_app_monthly_features,
                inputs=[
                    "l1_soc_app_daily_category_level_features_for_l3_soc_app_monthly_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_sum_features",
                    "params:l3_soc_app_monthly_stats",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_count_merge_chunk",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk",
                    "params:l3_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk",
                ],
                outputs=None,
                tags=["node_compute_int_soc_app_monthly_features"],
            ),

            node(
                func=node_compute_int_soc_app_monthly_features_catlv2,
                inputs=[
                    "l1_soc_app_daily_category_level_features_for_l3_soc_app_monthly_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_sum_features_catlv2",
                    "params:l3_soc_app_monthly_stats_catlv2",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_count_merge_chunk_catlv2",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk_catlv2",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk_catlv2",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk_catlv2",
                    "params:l3_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk_catlv2",
                    "params:l3_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk_catlv2",
                ],
                outputs=None,
                tags=["node_compute_int_soc_app_monthly_features_catlv2"],
            ),

            node(
                func=node_compute_int_soc_app_monthly_features_catlv3,
                inputs=[
                    "l1_soc_app_daily_category_level_features_for_l3_soc_app_monthly_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_sum_features_catlv3",
                    "params:l3_soc_app_monthly_stats_catlv3",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_count_merge_chunk_catlv3",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk_catlv3",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk_catlv3",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk_catlv3",
                    "params:l3_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk_catlv3",
                    "params:l3_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk_catlv3",
                ],
                outputs=None,
                tags=["node_compute_int_soc_app_monthly_features_catlv3"],
            ),

            node(
                func=node_compute_int_soc_app_monthly_features_catlv4,
                inputs=[
                    "l1_soc_app_daily_category_level_features_for_l3_soc_app_monthly_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_sum_features_catlv4",
                    "params:l3_soc_app_monthly_stats_catlv4",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_count_merge_chunk_catlv4",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_count_merge_chunk_catlv4",
                    "params:l3_soc_app_monthly_popular_app_rank_visit_duration_merge_chunk_catlv4",
                    "params:l3_soc_app_monthly_most_popular_app_by_visit_duration_merge_chunk_catlv4",
                    "params:l3_soc_app_monthly_popular_app_rank_download_traffic_merge_chunk_catlv4",
                    "params:l3_soc_app_monthly_most_popular_app_by_download_traffic_merge_chunk_catlv4",
                ],
                outputs=None,
                tags=["node_compute_int_soc_app_monthly_features_catlv4"],
            ),


            node(
                func=node_compute_final_soc_app_monthly_features,
                inputs=[
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_final_sum",
                    "params:l3_soc_app_monthly_ratio_features",
                    "params:l3_soc_app_monthly_final_popular_app_rank_visit_count_merge_chunk",
                    "params:l3_soc_app_monthly_final_most_popular_app_by_visit_count_merge_chunk",
                    "params:l3_soc_app_monthly_final_popular_app_rank_visit_duration_merge_chunk",
                    "params:l3_soc_app_monthly_final_most_popular_app_by_visit_duration_merge_chunk",
                    "params:l3_soc_app_monthly_final_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_soc_app_monthly_final_most_popular_app_by_download_traffic_merge_chunk",
                    "params:l3_soc_app_monthly_agg"
                ],
                outputs="l3_soc_app_monthly_features",
                tags=["node_compute_final_soc_app_monthly_features"],
            ),
            node(
                func=node_soc_app_monthly_user_category_granularity_features,
                inputs=[
                    "l3_soc_app_monthly_features_for_l3_soc_app_monthly_user_category_grain_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_app_monthly_popular_category_by_frequency_access",
                    "params:l3_soc_app_monthly_most_popular_category_by_frequency_access",
                    "params:l3_soc_app_monthly_popular_category_by_visit_duration",
                    "params:l3_soc_app_monthly_most_popular_category_by_visit_duration",
                    "params:l3_soc_app_monthly_popular_category_by_download_traffic",
                    "params:l3_soc_app_monthly_most_popular_category_by_download_traffic"
                ],
                outputs="l3_soc_app_monthly_user_category_grain_features",
                tags=["node_soc_app_monthly_user_category_granularity_features"],
            ),
        ],
        tags=["soc_app"],
    )


def soc_web_monthly_feature_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_compute_int_soc_web_monthly_features,
                inputs=[
                    "l1_soc_web_daily_category_level_features_for_l3_soc_web_monthly_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_web_monthly_agg",
                    "params:l3_soc_web_monthly_stats",
                    "params:l3_soc_web_monthly_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_soc_web_monthly_most_popular_app_by_download_traffic_merge_chunk",
                ],
                outputs=None,
                tags=["node_compute_int_soc_web_monthly_features"],
            ),
            node(
                func=node_compute_final_soc_web_monthly_features,
                inputs=[
                    "l1_aib_categories_clean",
                    "params:l3_soc_web_monthly_final_sum",
                    "params:l3_soc_web_monthly_ratio_features",
                    "params:l3_soc_web_monthly_final_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_soc_web_monthly_final_most_popular_app_by_download_traffic_merge_chunk",
                    "params:l3_soc_web_monthly_level_stats",
                ],
                outputs="l3_soc_web_monthly_features",
                tags=["node_compute_final_soc_web_monthly_features"],
            ),
            node(
                func=node_soc_web_monthly_user_category_granularity_features,
                inputs=[
                    "l3_soc_web_monthly_features_for_l3_soc_web_monthly_user_category_grain_features",
                    "l1_aib_categories_clean",
                    "params:l3_soc_web_monthly_popular_category_by_download_traffic",
                    "params:l3_soc_web_monthly_most_popular_category_by_download_traffic",
                ],
                outputs="l3_soc_web_monthly_user_category_grain_features",
                tags=["node_soc_web_monthly_user_category_granularity_features"],
            ),
        ],
        tags=["soc_web"],
    )


def comb_soc_app_web_monthly_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_compute_int_comb_soc_monthly_features,
                inputs=[
                    "l1_comb_soc_features_for_l3_comb_soc_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_soc_monthly_sum_features",
                    "params:l3_comb_soc_monthly_stats",
                    "params:l3_comb_soc_monthly_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_comb_soc_monthly_most_popular_app_by_download_traffic_merge_chunk",
                ],
                outputs=None,
                tags=["node_compute_int_comb_soc_monthly_features"],
            ),
            node(
                func=node_compute_final_comb_soc_monthly_features,
                inputs=[
                    "l1_aib_categories_clean",
                    "params:l3_comb_soc_final_monthly_agg",
                    "params:l3_comb_soc_monthly_final_sum",
                    "params:l3_comb_soc_ratio_based_features",
                    "params:l3_comb_soc_monthly_final_popular_app_rank_download_traffic_merge_chunk",
                    "params:l3_comb_soc_monthly_final_most_popular_app_by_download_traffic_merge_chunk",
                ],
                outputs="l3_comb_soc_features",
                tags=["node_compute_final_comb_soc_monthly_features"],
            ),
            node(
                func=node_comb_soc_monthly_user_category_granularity_features,
                inputs=[
                    "l3_comb_soc_features_for_l3_comb_soc_monthly_user_category_grain_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_soc_monthly_popular_category_by_download_traffic",
                    "params:l3_comb_soc_app_monthly_most_popular_category_by_download_traffic",
                ],
                outputs="l3_comb_soc_monthly_user_category_grain_features",
                tags=["node_comb_soc_monthly_user_category_granularity_features"],
            ),
        ],
        tags=["soc_comb"],
    )

def comb_all_monthly_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_compute_int_comb_all_monthly_features,
                inputs=[
                    "l1_comb_all_category_level_features_for_l3_comb_all_category_level_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_all_monthly_sum_features",
                    "params:l3_comb_all_monthly_stats",
                    "params:l3_comb_all_monthly_popular_app_or_url_by_visit_counts_merge_chunk",
                    "params:l3_comb_all_monthly_popular_app_or_url_by_visit_duration_merge_chunk",
                    "params:l3_comb_all_monthly_most_popular_app_or_url_by_visit_counts_merge_chunk",
                    "params:l3_comb_all_monthly_most_popular_app_or_url_by_visit_duration_merge_chunk",
                ],
                outputs=None,
                tags=["node_compute_int_comb_all_monthly_features"],
            ),
            node(
                func=node_compute_final_comb_all_monthly_features,
                inputs=[
                    "l1_aib_categories_clean",
                    "params:l3_comb_all_monthly_stats_visit_counts_final",
                    "params:l3_comb_all_monthly_stats_visit_duration_final",                    
                    "params:l3_comb_all_monthly_sum_features_final",
                    "params:l3_comb_all_monthly_ratio_feature_visit_counts",
                    "params:l3_comb_all_monthly_ratio_feature_visit_duration",
                    "params:l3_comb_all_monthly_popular_app_or_url_by_visit_counts_final",
                    "params:l3_comb_all_monthly_most_popular_app_or_url_by_visit_counts_final",
                    "params:l3_comb_all_monthly_popular_app_or_url_by_visit_duration_final",
                    "params:l3_comb_all_monthly_most_popular_app_or_url_by_visit_duration_final",
                ],
                outputs="l3_comb_all_monthly_category_level_features",
                tags=["node_compute_final_comb_all_monthly_features"],
            ),
            node(
                func=node_comb_all_monthly_user_category_granularity_features,
                inputs=[
                    "l3_comb_all_monthly_category_level_features_for_l3_comb_all_user_granularity_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_all_monthly_popular_category_by_total_visit_counts",
                    "params:l3_comb_all_monthly_most_popular_category_by_total_visit_counts",
                    "params:l3_comb_all_monthly_popular_category_by_total_visit_duration",
                    "params:l3_comb_all_monthly_most_popular_category_by_total_visit_duration",
                ],
                outputs="l3_comb_all_user_granularity_features",
                tags=["node_comb_all_monthly_user_category_granularity_features"],
            ),

        ],
        tags=["comb_all_monthly_features","streaming_monthly_features"],
    )

def comb_web_monthly_features_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_compute_int_comb_web_monthly_features,
                inputs=[
                    "l1_comb_web_category_level_features_for_l3_comb_web_category_level_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_web_monthly_sum_features",
                    "params:l3_comb_web_monthly_stats",
                    "params:l3_comb_web_monthly_popular_url_by_visit_counts_merge_chunk",
                    "params:l3_comb_web_monthly_popular_url_by_visit_duration_merge_chunk",
                    "params:l3_comb_web_monthly_most_popular_url_by_visit_counts_merge_chunk",
                    "params:l3_comb_web_monthly_most_popular_url_by_visit_duration_merge_chunk",
                ],
                outputs=None,
                tags=["node_compute_int_comb_web_monthly_features"],
            ),
            node(
                func=node_compute_final_comb_web_monthly_features,
                inputs=[
                    "l1_aib_categories_clean",
                    "params:l3_comb_web_monthly_stats_visit_counts_final",
                    "params:l3_comb_web_monthly_stats_visit_duration_final",
                    "params:l3_comb_web_monthly_sum_features_final",
                    "params:l3_comb_web_monthly_ratio_feature_visit_counts",
                    "params:l3_comb_web_monthly_ratio_feature_visit_duration",
                    "params:l3_comb_web_monthly_popular_url_by_visit_counts_final",
                    "params:l3_comb_web_monthly_most_popular_url_by_visit_counts_final",
                    "params:l3_comb_web_monthly_popular_url_by_visit_duration_final",
                    "params:l3_comb_web_monthly_most_popular_url_by_visit_duration_final",
                ],
                outputs="l3_comb_web_monthly_category_level_features",
                tags=["node_compute_final_comb_web_monthly_features"],
            ),
            node(
                func=node_comb_web_monthly_user_category_granularity_features,
                inputs=[
                    "l3_comb_web_monthly_category_level_features_for_l3_comb_web_user_granularity_features",
                    "l1_aib_categories_clean",
                    "params:l3_comb_web_monthly_popular_category_by_total_visit_counts",
                    "params:l3_comb_web_monthly_most_popular_category_by_total_visit_counts",
                    "params:l3_comb_web_monthly_popular_category_by_total_visit_duration",
                    "params:l3_comb_web_monthly_most_popular_category_by_total_visit_duration",
                    
                ],
                outputs="l3_comb_web_user_granularity_features",
                tags=["node_comb_web_monthly_user_category_granularity_features"],
            ),

        ],
        tags=["comb_web_monthly_features","streaming_monthly_features"],
    )

def relay_to_l3_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=node_pageviews_monthly_features,
                inputs=[
                    "l0_relay_page_views_raw_for_l3_relay_monthly_pageviews_features",
                    "params:l3_relay_monthly_total_pageviews_visits_count",
                    "params:l3_relay_monthly_popular_url_by_pageviews",
                    "params:l3_relay_monthly_popular_subcategory1_by_pageviews",
                    "params:l3_relay_monthly_popular_subcategory2_by_pageviews",
                    "params:l3_relay_monthly_popular_cid_by_pageviews",
                    "params:l3_relay_monthly_popular_productname_by_pageviews",
                    "params:l3_relay_monthly_most_popular_url_by_pageviews",
                    "params:l3_relay_monthly_most_popular_subcategory1_by_pageviews",
                    "params:l3_relay_monthly_most_popular_subcategory2_by_pageviews",
                    "params:l3_relay_monthly_most_popular_cid_by_pageviews",
                    "params:l3_relay_monthly_most_popular_productname_by_pageviews",
                ],
                outputs="l3_relay_monthly_pageviews_features",
                tags=["node_pageviews_monthly_features"],
            ),
            node(
                func=node_engagement_conversion_monthly_features,
                inputs=[
                    "l0_relay_engagement_conversion_raw_for_l3_relay_monthly_engagement_conversion_features",
                    "params:l3_relay_monthly_popular_product_by_engagement_conversion",
                    "params:l3_relay_monthly_popular_cid_by_engagement_conversion",
                    "params:l3_relay_monthly_most_popular_product_by_engagement_conversion",
                    "params:l3_relay_monthly_most_popular_cid_by_engagement_conversion",
                ],
                outputs="l3_relay_monthly_engagement_conversion_features",
                tags=["node_engagement_conversion_monthly_features"],
            ),
            node(
                func=node_engagement_conversion_cid_level_monthly_features,
                inputs=[
                    "l0_relay_engagement_conversion_raw_for_l3_relay_monthly_engagement_conversion_cid_level_features",
                    "params:l3_relay_monthly_total_engagement_conversion_visits_count_by_cid",
                ],
                outputs="l3_relay_monthly_engagement_conversion_cid_level_features",
                tags=["node_engagement_conversion_cid_level_monthly_features"],
            ),
            node(
                func=node_engagement_conversion_package_monthly_features,
                inputs=[
                    "l0_relay_engagement_conversion_package_raw_for_l3_relay_monthly_engagement_conversion_package_features",
                    "params:l3_relay_monthly_popular_product_by_engagement_conversion_package",
                    "params:l3_relay_monthly_popular_cid_by_engagement_conversion_package",
                    "params:l3_relay_monthly_most_popular_product_by_engagement_conversion_package",
                    "params:l3_relay_monthly_most_popular_cid_by_engagement_conversion_package",
                ],
                outputs="l3_relay_monthly_engagement_conversion_package_features",
                tags=["node_engagement_conversion_package_monthly_features"],
            ),
            node(
                func=node_engagement_conversion_package_cid_level_monthly_features,
                inputs=[
                    "l0_relay_engagement_conversion_package_raw_for_l3_relay_monthly_engagement_conversion_package_cid_level_features",
                    "params:l3_relay_monthly_total_engagement_conversion_package_visits_count_by_cid",
                ],
                outputs="l3_relay_monthly_engagement_conversion_package_cid_level_features",
                tags=["node_engagement_conversion_package_cid_level_monthly_features"],
            ),
        ]
    ) 