from functools import partial
from kedro.pipeline import Pipeline, node
from src.customer360.pipelines.cvm.src.utils.get_suffix import get_suffix
from src.nba.report.nodes.report_nodes import *

REPORT_DATE = "2020-03-06"


def create_use_case_view_report_data(run_type: str = None) -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_report_campaign_tracking_table,
                    day="2020-03-03",  # TODO make dynamic
                ),
                [
                    "cvm_prepaid_customer_groups",
                    "dm996_cvm_ontop_pack",
                    "use_case_campaign_mapping",
                    "params:report_create_campaign_tracking_table",
                ],
                "campaign_response_input_table",
                name="create_campaign_tracking_on_groups",
            ),
            node(
                partial(
                    create_agg_data_for_report,
                    day="2020-03-03",  # TODO make dynamic
                    aggregate_period=[1, 7, 30],
                ),
                inputs=[
                    "cvm_prepaid_customer_groups",
                    "dm42_promotion_prepaid",
                    "dm43_promotion_prepaid",
                    "dm01_fin_top_up",
                    "dm15_mobile_usage_aggr_prepaid",
                ],
                outputs="churn_ard_report_input_table",
                name="churn_ard_report_input_table",
            ),

            node(
                partial(
                    create_use_case_view_report,
                    day="2020-03-03",  # TODO make dynamic
                    aggregate_period=[1, 7, 30],
                ),
                inputs=[
                    "cvm_prepaid_customer_groups",
                    "campaign_response_input_table",
                    "churn_ard_report_input_table",
                ],
                outputs="use_case_view_report_table",
                name="churn_ard_report_use_case_view_table",
            ),
        ],
        name="churn_ard_report",
    )
