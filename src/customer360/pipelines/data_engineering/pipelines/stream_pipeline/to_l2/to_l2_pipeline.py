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

from customer360.utilities.config_parser import node_from_config


def streaming_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            # Content Type Features
            node(
                node_from_config,
                ["int_l1_streaming_content_type_features",
                 "params:int_l2_streaming_content_type_features"],
                "int_l2_streaming_content_type_features"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_content_type_features",
                 "params:l2_streaming_fav_content_group_by_volume"],
                "l2_streaming_fav_content_group_by_volume"
            ),

            # TV Channel features
            node(
                node_from_config,
                ["int_l1_streaming_tv_channel_features",
                 "params:int_l2_streaming_tv_channel_features"],
                "int_l2_streaming_tv_channel_features"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_tv_channel_features",
                 "params:l2_streaming_fav_tv_channel_by_volume"],
                "l2_streaming_fav_tv_channel_by_volume"
            ),

            # TV show features
            node(
                node_from_config,
                ["int_l0_streaming_vimmi_table",
                 "params:int_l2_streaming_tv_show_features"],
                "int_l2_streaming_tv_show_features"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_tv_show_features",
                 "params:l2_streaming_fav_tv_show_by_episode_watched"],
                "l2_streaming_fav_tv_show_by_episode_watched"
            ),

            # fav video service by download traffic
            node(
                node_from_config,
                ["int_l1_streaming_video_service_feature",
                 "params:int_l2_streaming_video_service_feature"],
                "int_l2_streaming_video_service_feature"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_video_service_feature",
                 "params:l2_streaming_fav_video_service_by_download_feature"],
                "l2_streaming_fav_video_service_by_download_feature"
            ),

            # fav music service by download traffic
            node(
                node_from_config,
                ["int_l1_streaming_music_service_feature",
                 "params:int_l2_streaming_music_service_feature"],
                "int_l2_streaming_music_service_feature"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_music_service_feature",
                 "params:l2_streaming_fav_music_service_by_download_feature"],
                "l2_streaming_fav_music_service_by_download_feature"
            ),

            # fav esport service by download traffic
            node(
                node_from_config,
                ["int_l1_streaming_esport_service_feature",
                 "params:int_l2_streaming_esport_service_feature"],
                "int_l2_streaming_esport_service_feature"
            ),
            node(
                node_from_config,
                ["int_l2_streaming_esport_service_feature",
                 "params:l2_streaming_fav_esport_service_by_download_feature"],
                "l2_streaming_fav_esport_service_by_download_feature"
            ),
        ], name="streaming_to_l2_pipeline"
    )
