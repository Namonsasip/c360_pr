from functools import partial
from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix
from src.nba.report.nodes.report_nodes import *

REPORT_DATE = "2020-03-06"


def create_use_case_view_report_data(run_type: str = None) -> Pipeline:
    suffix = get_suffix(run_type)
    return Pipeline(
        [
            node(
                partial(create_report_campaign_tracking_table, day=REPORT_DATE),
                [
                    "cvm_prepaid_customer_groups",
                    "dm996_cvm_ontop_pack",
                    "use_case_campaign_mapping",
                    "params:report_create_campaign_tracking_table",
                ],
                "l0_campaign_report_response_control_group",
                name="create_campaign_tracking_on_groups",
            ),
            node(
                create_user_current_size,
                [
                    "cvm_prepaid_customer_groups",
                    "params:report_control_group_current_size",
                ],
                "l0_campaign_report_current_size",
                name="user_current_size",
            ),
            node(
                partial(
                    create_report_target_user_agg_tbl,
                    end_date_str=REPORT_DATE,
                    aggregate_period="yesterday",
                ),
                [
                    "l0_campaign_report_response_control_group",
                    "params:report_targeted_user",
                ],
                "l0_churn_ard_targeted_customer_yesterday",
                name="churn_ard_targeted_customer_yesterday",
            ),
            node(
                partial(
                    create_report_target_user_agg_tbl,
                    end_date_str=REPORT_DATE,
                    aggregate_period="last_week",
                ),
                [
                    "l0_campaign_report_response_control_group",
                    "params:report_targeted_user",
                ],
                "l0_churn_ard_targeted_customer_last_week",
                name="churn_ard_targeted_customer_last_week",
            ),
            node(
                partial(
                    create_report_target_user_agg_tbl,
                    end_date_str=REPORT_DATE,
                    aggregate_period="last_month",
                ),
                [
                    "l0_campaign_report_response_control_group",
                    "params:report_targeted_user",
                ],
                "l0_churn_ard_targeted_customer_last_month",
                name="churn_ard_targeted_customer_last_month",
            ),
        ]
    )
