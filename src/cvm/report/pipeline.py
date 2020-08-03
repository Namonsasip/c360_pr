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
    build_contacts_table,
    build_daily_kpis,
    create_contact_kpis,
    prepare_users,
)
from cvm.sample_inputs.nodes import create_sample_dataset
from cvm.src.report.kpis_build import get_most_recent_micro_macrosegment
from kedro.pipeline import Pipeline, node

from src.cvm.report.nodes import summarize_contact_kpis


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
            func=create_sample_dataset,
            inputs=[dataset_name, "parameters", "params:" + sample_type],
            outputs=dataset_name + "_" + sample_type,
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
                func=prepare_users,
                inputs=["cvm_prepaid_customer_groups", "sub_id_mapping", "parameters"],
                outputs="users_report",
                name="create_users_report",
            )
        ]
    )


def join_features() -> Pipeline:
    """Joins the features from C360 and adds microsegments / macrosegments."""
    return Pipeline(
        [
            node(
                func=subs_date_join,
                inputs=[
                    "parameters",
                    "users_report",
                    "l4_daily_feature_topup_and_volume_report",
                    "l3_customer_profile_include_1mo_non_active_report",
                    "l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_report",
                    "l4_usage_prepaid_postpaid_daily_features_report",
                    "l4_usage_postpaid_prepaid_weekly_features_sum_report",
                ],
                outputs="features_report",
                name="join_report_features",
            ),
            node(
                func=get_most_recent_micro_macrosegment,
                inputs="microsegments_macrosegments_history_input_scoring",
                outputs="users_micro_macro_only",
                name="prepare_micro_macro",
            ),
        ]
    )


def create_kpis() -> Pipeline:
    """Creates kpis used for tracking."""
    return Pipeline(
        [
            node(
                func=build_daily_kpis,
                inputs=[
                    "users_report",
                    "users_micro_macro_only",
                    "network_churn",
                    "l1_revenue_prepaid_pru_f_usage_multi_daily",
                    "l1_customer_profile_union_daily_feature",
                    "l4_usage_prepaid_postpaid_daily_features",
                    "parameters",
                    "prepaid_no_activity_daily",
                    "l0_revenue_prepaid_pru_f_usage_multi_daily",
                ],
                outputs="daily_kpis",
                name="build_daily_kpis",
            )
        ]
    )


def create_kpis_around_contacts() -> Pipeline:
    """ Creates kpis for time periods around contacts"""
    return Pipeline(
        [
            node(
                func=build_contacts_table,
                inputs=[
                    "child_code_tags",
                    "l0_campaign_tracking_contact_list_pre_full_load",
                    "parameters",
                ],
                outputs="contacts_only",
                name="list_contacts",
            ),
            node(
                func=create_contact_kpis,
                inputs=["daily_kpis", "contacts_only", "users_report", "parameters"],
                outputs="contact_kpis",
                name="create_contact_kpis",
            ),
            node(
                func=summarize_contact_kpis,
                inputs=["contact_kpis", "parameters"],
                outputs="contact_kpis_summarized",
                name="summarize_contact_kpis",
            ),
        ]
    )


def prepare_user_microsegments() -> Pipeline:
    """ Join above pipelines"""
    return prepare_users_report() + sample_report_inputs() + join_features()


def run_report() -> Pipeline:
    """ Prepares data and creates kpis"""
    return prepare_user_microsegments() + create_kpis() + create_kpis_around_contacts()
