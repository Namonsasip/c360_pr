from functools import partial

from kedro.pipeline import Pipeline, node
from datetime import timedelta
from src.nba.report.nodes.report_nodes import *
from datetime import datetime


def date_range(start_dt, end_dt=None):
    start_dt = datetime.strptime(start_dt, "%Y-%m-%d")
    if end_dt:
        end_dt = datetime.strptime(end_dt, "%Y-%m-%d")
    while start_dt <= end_dt:
        yield start_dt.strftime("%Y-%m-%d")
        start_dt += timedelta(days=1)


def create_use_case_view_report_pipeline() -> Pipeline:
    mock_report_running_date = "2020-08-01"  # fixed date
    # mock_report_running_date = (datetime.now() + timedelta(hours=7)).strftime(
    #     "%Y-%m-%d"
    # )  # Dynamic Test
    report_running_date_start = datetime.strptime(
        mock_report_running_date, "%Y-%m-%d"
    ) + timedelta(days=-35)
    report_running_date_start = report_running_date_start.strftime("%Y-%m-%d")
    mock_report_running_date_list = [
        e for e in date_range(report_running_date_start, mock_report_running_date)
    ]
    start_date = datetime.strptime(mock_report_running_date, "%Y-%m-%d") + timedelta(
        days=-45
    )
    start_date_input = datetime.strptime(
        mock_report_running_date, "%Y-%m-%d"
    ) + timedelta(days=-35)
    return Pipeline(
        [
            node(
                create_use_case_campaign_mapping_table,
                {
                    "campaign_churn_cvm_master": "campaign_churn_cvm_master",
                    "campaign_churn_bau_master": "campaign_churn_bau_master",
                    "campaign_ard_cvm_master": "campaign_ard_cvm_master",
                    "campaign_ard_churn_mck_master": "campaign_ard_churn_mck_master",
                },
                outputs="use_case_campaign_mapping",
                name="create_use_case_campaign_mapping_table",
                tags=["create_use_case_campaign_mapping_table",],
            ),
            node(
                partial(
                    create_report_campaign_tracking_table,
                    date_from=start_date_input,
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                {
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                },
                outputs="unused_memory_campaign_response_input_table",
                name="campaign_response_input_table",
                tags=["campaign_response_input_table",],
            ),
            node(
                partial(
                    create_input_data_for_reporting_kpis,
                    date_from=start_date_input,
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                inputs={
                    "l1_customer_profile_union_daily_feature_full_load": "l1_customer_profile_union_daily_feature_full_load",
                    "l0_churn_status_daily": "l0_churn_status_daily",
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "dm42_promotion_prepaid": "dm42_promotion_prepaid",
                    "dm43_promotion_prepaid": "dm43_promotion_prepaid",
                    "dm01_fin_top_up": "dm01_fin_top_up",
                    "dm15_mobile_usage_aggr_prepaid": "dm15_mobile_usage_aggr_prepaid",
                    "dm07_sub_clnt_info": "dm07_sub_clnt_info",
                    "prepaid_no_activity_daily": "prepaid_no_activity_daily",
                },
                outputs="unused_memory_reporting_kpis_input",
                name="create_input_data_for_reporting_kpis",
                tags=["create_input_data_for_reporting_kpis"],
            ),
            node(
                partial(
                    node_reporting_kpis,
                    date_from=start_date,
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    arpu_days_agg_periods=[7, 30],
                    drop_update_table=True,
                ),
                inputs={
                    "reporting_kpis_input": "reporting_kpis_input",
                    "unused_memory_reporting_kpis_input": "unused_memory_reporting_kpis_input",
                },
                outputs="unused_memory_reporting_kpis",
                name="reporting_kpis",
                tags=["reporting_kpis"],
            ),
            # ################
            # # Plot not use
            # node(
            #     node_daily_kpis_by_group_report,
            #     inputs={"reporting_kpis": "reporting_kpis",},
            #     outputs="daily_kpis_by_group_report",
            #     name="daily_kpis_by_group_report",
            #     tags=["daily_kpis_by_group_report"],
            # ),
            # node(
            #     node_plot_daily_kpis_by_group_report,
            #     inputs={"daily_kpis_by_group_report": "daily_kpis_by_group_report",},
            #     outputs="plot_daily_kpis_by_group_report",
            #     name="plot_daily_kpis_by_group_report",
            #     tags=["plot_daily_kpis_by_group_report"],
            # ),
            # ##########################
            #
            node(
                partial(
                    create_use_case_view_report,
                    day_list=mock_report_running_date_list,
                    aggregate_period=[7, 15, 30],
                    dormant_days_agg_periods=[5, 7, 14, 30, 60, 90],
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-30),
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                inputs={
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                    "cvm_prepaid_customer_groups": "cvm_prepaid_customer_groups",
                    "campaign_response_input_table": "campaign_response_input_table",
                    "reporting_kpis": "reporting_kpis",
                    "reporting_kpis_input": "reporting_kpis_input",
                    "unused_memory_campaign_response_input_table": "unused_memory_campaign_response_input_table",
                    "unused_memory_reporting_kpis_input": "unused_memory_reporting_kpis_input",
                    "unused_memory_reporting_kpis": "unused_memory_reporting_kpis",
                },
                outputs="unused_memory_use_case_view_report_table",
                name="use_case_view_report_table",
                tags=["use_case_view_report_table",],
            ),
            node(
                partial(store_historical_usecase_view_report),
                inputs={
                    "use_case_view_report_table": "use_case_view_report_table",
                    "unused_memory_use_case_view_report_table": "unused_memory_use_case_view_report_table",
                },
                outputs="unused_memory_historical_use_case_view_report_table",
                name="historical_use_case_view_report_table",
                tags=["historical_use_case_view_report_table",],
            ),
            node(
                partial(
                    create_distinct_aggregate_campaign_feature,
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-50),
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                },
                outputs="unused_memory_distinct_aggregate_campaign_feature_tbl",
            ),
            node(
                partial(
                    create_general_marketing_performance_report,
                    aggregate_period=[7, 30],
                    dormant_days_agg_periods=[5, 7, 14, 30, 45, 60, 90],
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-50),
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                inputs={
                    "reporting_kpis": "reporting_kpis",
                    "distinct_aggregate_campaign_feature_tbl": "distinct_aggregate_campaign_feature_tbl",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "prepaid_no_activity_daily": "prepaid_no_activity_daily",
                    "unused_memory_distinct_aggregate_campaign_feature_tbl": "unused_memory_distinct_aggregate_campaign_feature_tbl",
                    "unused_memory_reporting_kpis": "unused_memory_reporting_kpis",
                },
                outputs="unused_memory_general_marketing_performance_report_tbl",
                name="create_general_marketing_performance_report",
                tags=["create_general_marketing_performance_report"],
            ),
            node(
            # This node took 2.5 Hours
                partial(
                    create_campaign_view_report_input,
                    # -50 day because reporting kpi future data need
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-50),
                    # until present day we ran
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=True,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l0_campaign_history_master_active": "campaign_history_master_active",
                    "use_case_campaign_mapping": "use_case_campaign_mapping",
                    "reporting_kpis": "reporting_kpis",
                },
                outputs="unused_memory_campaign_view_report_input_tbl",
                name="create_campaign_view_report_input",
                tags=["create_campaign_view_report_input","create_campaign_view_report"],
            ),
            node(
                # This node took 35 Mins to run
                partial(
                    create_aggregate_campaign_view_features,
                    # -50 day because reporting kpi future data need
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-50),
                    # until present day we ran
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    aggregate_period=[7, 30],
                    drop_update_table=True,
                ),
                inputs={"campaign_view_report_input": "campaign_view_report_input_tbl",},
                outputs="unused_memory_aggregate_campaign_view_features_tbl",
                name="create_aggregate_campaign_view_features",
                tags=["create_aggregate_campaign_view_features","create_campaign_view_report"],
            ),
            node(
                partial(
                    create_campaign_view_report,
                    # -50 day because reporting kpi future data need
                    date_from=datetime.strptime(mock_report_running_date, "%Y-%m-%d")
                    + timedelta(days=-50),
                    # until present day we ran
                    date_to=datetime.strptime(mock_report_running_date, "%Y-%m-%d"),
                    drop_update_table=False,
                ),
                inputs={
                    "aggregate_campaign_view_features_tbl": "aggregate_campaign_view_features_tbl",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                },
                outputs="unused_memory_campaign_view_report_tbl",
                name="create_campaign_view_report",
                tags=["create_campaign_view_report"],
            ),
        ],
        tags=["churn_ard_report"],
    )
