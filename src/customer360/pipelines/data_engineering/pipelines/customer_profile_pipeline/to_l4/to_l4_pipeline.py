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

from src.customer360.utilities.config_parser import node_from_config


def city_of_residence(
        cust_pre,
        cust_post,
        cust_non_mobile
):
    columns = [
        "subscription_identifier",
        "mobile_no",
        "register_date",
        "zipcode"
    ]

    df = cust_pre.select(columns) \
        .union(cust_post.select(columns)) \
        .union(cust_non_mobile.select(columns))

    return df


def customer_profile_to_l4_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                node_from_config,
                ["l0_customer_profile_profile_drm_t_active_profile_customer_journey_monthly",
                 "params:int_l4_customer_profile_basic_features"],
                "int_l4_customer_profile_basic_features"
            ),
            node(
                city_of_residence,
                ["l0_customer_profile_profile_customer_profile_pre_current",
                 "l0_customer_profile_profile_customer_profile_post_current",
                 "l0_customer_profile_profile_customer_profile_post_non_mobile_current_non_mobile_current"],
                "int_l4_customer_profile_city_of_residence"
            )
        ], tags=["customer_profile_to_l4_pipeline"]
    )
