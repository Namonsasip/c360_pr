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
from cvm.data_prep.nodes import subs_date_join
from cvm.report.nodes import (
    add_micro_macro,
    build_daily_kpis,
    filter_out_micro_macro,
    prepare_users,
)
from cvm.sample_inputs.nodes import create_sample_dataset
from kedro.pipeline import Pipeline, node


def sample_report_inputs() -> Pipeline:
    """ Creates samples for report input datasets. """

    datasets_to_sample = [
        "l3_customer_profile_include_1mo_non_active",
        "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly",
        "l4_usage_prepaid_postpaid_daily_features",
        "l4_usage_postpaid_prepaid_weekly_features_sum",
        "l4_daily_feature_topup_and_volume",
    ]
    sample_type = "report"

    nodes_list = [
        node(
            create_sample_dataset,
            [dataset_name, "parameters", "params:" + sample_type],
            dataset_name + "_" + sample_type,
            name="sample_" + dataset_name + "_" + sample_type,
        )
        for dataset_name in datasets_to_sample
    ]

    return Pipeline(nodes_list)


def prepare_users_report() -> Pipeline:
    """Prepares users to include in report."""
    return Pipeline(
        [
            node(
                prepare_users,
                ["cvm_prepaid_customer_groups", "parameters"],
                "users_report",
                name="create_users_report",
            )
        ]
    )


def join_features() -> Pipeline:
    """Joins the features from C360 and adds microsegments / macrosegments."""
    return Pipeline(
        [
            node(
                subs_date_join,
                [
                    "parameters",
                    "users_report",
                    "l4_daily_feature_topup_and_volume_report",
                    "l3_customer_profile_include_1mo_non_active_report",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_report",
                    "l4_usage_prepaid_postpaid_daily_features_report",
                    "l4_usage_postpaid_prepaid_weekly_features_sum_report",
                ],
                "features_report",
                name="join_report_features",
            ),
            node(
                add_micro_macro,
                [
                    "features_report",
                    "l3_customer_profile_include_1mo_non_active_report",
                    "parameters",
                ],
                "users_micro_macro",
                name="prepare_micro_macro",
            ),
            node(
                filter_out_micro_macro,
                "users_micro_macro",
                "users_micro_macro_only",
                name="prepare_micro_macro_only",
            ),
        ]
    )


def create_kpis() -> Pipeline:
    """Creates kpis used for tracking."""
    return Pipeline(
        [
            node(
                build_daily_kpis,
                [
                    "users_micro_macro_only",
                    "l1_revenue_prepaid_pru_f_usage_multi_daily",
                    "l1_customer_profile_union_daily_feature",
                    "l4_usage_prepaid_postpaid_daily_features",
                    "parameters",
                ],
                "daily_kpis",
                name="build_daily_kpis",
            )
        ]
    )


def prepare_user_microsegments() -> Pipeline:
    """ Join above pipelines"""
    return prepare_users_report() + sample_report_inputs() + join_features()


def run_report() -> Pipeline:
    """ Prepares data and creates kpis"""
    return prepare_user_microsegments() + create_kpis()
