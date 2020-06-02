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

from cvm.src.temporary_fixes.sub_id_replace import get_mapped_dataset_name
from cvm.temp.nodes import create_sub_id_mapping, map_sub_ids


def create_sub_id_mapping_pipeline() -> Pipeline:
    """Creates sub id mapping table"""
    return Pipeline(
        [
            node(
                create_sub_id_mapping,
                ["l1_customer_profile_union_daily_feature"],
                "sub_id_mapping",
                name="create_sub_id_mapping",
            )
        ]
    )


def map_sub_ids_of_input_datasets(sample_type: str) -> Pipeline:
    """ Maps all input datasets to new sub ids

    Args:
        sample_type: type of sample, may be "scoring", "training" or custom
    """
    input_datasets = [
        "l3_customer_profile_include_1mo_non_active",
        "l4_usage_prepaid_postpaid_daily_features",
        "l4_daily_feature_topup_and_volume",
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        "l4_usage_postpaid_prepaid_weekly_features_sum",
        "cvm_prepaid_customer_groups_sub_ids_mapped",
    ]
    nodes_list = [
        node(
            func=map_sub_ids,
            inputs=[input_dataset, "sub_id_mapping"],
            outputs=get_mapped_dataset_name(input_dataset, sample_type),
            name="map_sub_ids_for_{}".format(input_dataset),
        )
        for input_dataset in input_datasets
    ]
    return Pipeline(nodes_list)
