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
import re

from cvm.sample_inputs.nodes import (
    create_sample_dataset,
    create_users_from_active_users,
    create_users_from_cgtg,
)
from kedro.pipeline import Pipeline, node


def create_users_from_tg(sample_type: str) -> Pipeline:
    """ Creates users table to use during training / scoring using predefined target
    group.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    return Pipeline(
        [
            node(
                create_users_from_cgtg,
                [
                    "cvm_prepaid_customer_groups_sub_ids_mapped",
                    "params:{}".format(sample_type),
                    "parameters",
                ],
                "cvm_users_list_" + sample_type,
                name="create_users_list_tgcg_" + sample_type,
            ),
        ]
    )


def create_users_from_active(sample_type: str) -> Pipeline:
    """ Creates users table to use during training / scoring using list of active users.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    return Pipeline(
        [
            node(
                create_sample_dataset,
                [
                    "l3_customer_profile_include_1mo_non_active_sub_ids_mapped",
                    "parameters",
                    "params:" + sample_type,
                ],
                "active_users_sample_" + sample_type,
                name="create_active_users_sample_" + sample_type,
            ),
            node(
                create_users_from_active_users,
                [
                    "active_users_sample_" + sample_type,
                    "l0_product_pru_m_package_master_group_for_daily",
                    "params:" + sample_type,
                    "parameters",
                ],
                "cvm_users_list_" + sample_type,
                name="create_cvm_users_list_active_users_" + sample_type,
            ),
        ]
    )


def sample_inputs(sample_type: str) -> Pipeline:
    """ Creates samples for input datasets.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.
    """

    datasets_to_sample = [
        "l3_customer_profile_include_1mo_non_active",
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        "l4_usage_prepaid_postpaid_daily_features",
        "l4_daily_feature_topup_and_volume",
        "l4_usage_postpaid_prepaid_weekly_features_sum",
        "l4_touchpoints_to_call_center_features",
    ]

    nodes_list = [
        node(
            create_sample_dataset,
            inputs=[
                dataset_name + "_sub_ids_mapped",  # temp fix
                "parameters",
                "params:" + sample_type,
            ],
            # when not incremental version of dataset is used
            outputs=re.sub("_no_inc", "", dataset_name) + "_" + sample_type,
            name="sample_" + dataset_name + "_" + sample_type,
        )
        for dataset_name in datasets_to_sample
    ]

    return Pipeline(nodes_list)
