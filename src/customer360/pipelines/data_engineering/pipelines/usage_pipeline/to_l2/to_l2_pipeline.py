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
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l2 import prepare_prepaid_call_data \
    , prepare_postpaid_call_data, prepare_prepaid_gprs_data, prepare_postpaid_gprs_data


def usage_to_l2_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                prepare_prepaid_call_data, 'l1_usage_ru_f_cbs_prepaid_call_daily',
                'l1_usage_ru_f_cbs_prepaid_call_daily_stg'),
            node(
                expansion,
                ["l1_usage_ru_f_cbs_prepaid_call_daily_stg",
                 "params:l2_usage_ru_f_cbs_prepaid_call_weekly"],
                "l2_usage_ru_f_cbs_prepaid_call_weekly"
            ),
            node(
                prepare_postpaid_call_data, 'l1_usage_ru_a_voice_usg_daily'
                , 'l1_usage_ru_a_voice_usg_daily_stg'
            ),
            node(
                expansion,
                ["l1_usage_ru_a_voice_usg_daily_stg",
                 "params:l2_usage_ru_a_voice_usg_weekly"],
                "l2_usage_ru_a_voice_usg_weekly"
            ),
            node(
                prepare_prepaid_gprs_data, 'l1_usage_ru_a_gprs_cbs_usage_daily',
                'l1_usage_ru_a_gprs_cbs_usage_daily_stg'),
            node(
                expansion,
                ["l1_usage_ru_a_gprs_cbs_usage_daily_stg",
                 "params:l2_usage_ru_a_gprs_cbs_usage_weekly"],
                "l2_usage_ru_a_gprs_cbs_usage_weekly"
            ),
            node(
                prepare_postpaid_gprs_data, 'l1_usage_ru_a_vas_postpaid_usg_daily',
                'l1_usage_ru_a_vas_postpaid_usg_daily_stg'),
            node(
                expansion,
                ["l1_usage_ru_a_vas_postpaid_usg_daily_stg",
                 "params:l2_usage_ru_a_vas_postpaid_usg_weekly"],
                "l2_usage_ru_a_vas_postpaid_usg_weekly"
            ),

        ], name="usage_to_l2_pipeline"
    )
