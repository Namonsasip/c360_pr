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

from src.customer360.utilities.config_parser import \
    l4_rolling_window, node_from_config, l4_rolling_ranked_window_first_rank


def streaming_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            # this will calculate sum per content_group
            node(
                l4_rolling_window,
                ["int_l2_streaming_content_type_features",
                 "params:int_l4_streaming_content_type_features"],
                "int_l4_streaming_content_type_features"
            ),
            node(
                l4_rolling_ranked_window_first_rank,
                ["int_l4_streaming_content_type_features",
                 "params:l4_streaming_fav_content_group_by_volume"],
                {"last_week": "l4_streaming_fav_content_group_by_volume_last_week",
                 "last_two_week": "l4_streaming_fav_content_group_by_volume_last_two_week",
                 "last_month": "l4_streaming_fav_content_group_by_volume_last_month",
                 "last_three_month": "l4_streaming_fav_content_group_by_volume_last_three_month"}
            ),

            node(
                l4_rolling_window,
                ["int_l2_streaming_tv_channel_features",
                 "params:int_l4_streaming_tv_channel_features"],
                "int_l4_streaming_tv_channel_features"
            ),
            node(
                l4_rolling_ranked_window_first_rank,
                ["int_l4_streaming_tv_channel_features",
                 "params:l4_streaming_fav_tv_channel_by_volume"],
                {"last_week": "l4_streaming_fav_tv_channel_by_volume_last_week",
                 "last_two_week": "l4_streaming_fav_tv_channel_by_volume_last_two_week",
                 "last_month": "l4_streaming_fav_tv_channel_by_volume_last_month",
                 "last_three_month": "l4_streaming_fav_tv_channel_by_volume_last_three_month"}
            ),

            node(
                l4_rolling_window,
                ["int_l0_streaming_vimmi_table",
                 "params:int_l4_streaming_tv_show_features_1"],
                "int_l4_streaming_tv_show_features_1"
            ),
            # group it per week because we read directly from L1
            node(
                node_from_config,
                ["int_l4_streaming_tv_show_features_1",
                 "params:int_l4_streaming_tv_show_features_2"],
                "int_l4_streaming_tv_show_features_2"
            ),
            node(
                l4_rolling_ranked_window_first_rank,
                ["int_l4_streaming_tv_show_features_2",
                 "params:l4_streaming_fav_tv_show_by_episode_watched"],
                {"last_week": "l4_streaming_fav_tv_show_by_episode_watched_last_week",
                 "last_two_week": "l4_streaming_fav_tv_show_by_episode_watched_last_two_week",
                 "last_month": "l4_streaming_fav_tv_show_by_episode_watched_last_month",
                 "last_three_month": "l4_streaming_fav_tv_show_by_episode_watched_last_three_month"}
            )

        ], name="streaming_to_l4_pipeline"
    )
