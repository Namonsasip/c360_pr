from functools import partial

from kedro.pipeline import Pipeline, node

from du.experiment.group_manage_nodes import (
    create_prepaid_test_groups,
    create_sanity_check_for_random_test_group,
    create_postpaid_test_groups,
    update_du_control_group,
)

partition_date_str = "20200930"


def create_du_test_group_pipeline() -> Pipeline:
    return Pipeline(
        [
            # Do not run this node, only run this node when initializing new test groups
            # node(
            #     partial(
            #         create_prepaid_test_groups,
            #         sampling_rate=[0.975, 0.025],
            #         test_group_name=["Default", "GCG"],
            #         test_group_flag=["N", "Y"],
            #         partition_date_str=partition_date_str,
            #     ),
            #     inputs={
            #         "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
            #         "cvm_sandbox_gcg": "cvm_sandbox_gcg",
            #     },
            #     outputs="l0_gcg_pre_" + partition_date_str,
            #     name="create_prepaid_test_groups",
            #     tags=["create_prepaid_test_groups"],
            # ),
            node(
                partial(
                    create_postpaid_test_groups,
                    sampling_rate=[0.975, 0.025],
                    test_group_name=["Default", "GCG"],
                    test_group_flag=["N", "Y"],
                    partition_date_str=partition_date_str,
                ),
                inputs={
                    "l0_customer_profile_profile_customer_profile_post_current_full_load": "l0_customer_profile_profile_customer_profile_post_current_full_load",
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                },
                outputs="l0_gcg_post_" + partition_date_str,
                name="create_postpaid_test_groups",
                tags=["create_postpaid_test_groups"],
            ),
            node(
                partial(
                    create_sanity_check_for_random_test_group,
                    group_name_column="group_name",
                    group_flag_column="group_flag",
                    csv_file_path="data/tmp/sanity_check_test_groups_pre_GCG_"
                    + partition_date_str
                    + ".csv",
                ),
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "l3_usage_postpaid_prepaid_monthly": "l3_usage_postpaid_prepaid_monthly",
                    "df_test_group": "l0_gcg_pre_" + partition_date_str,
                },
                outputs="l5_sanity_checking_gcg_pre_" + partition_date_str,
                name="sanity_checking_test_group_pre",
                tags=["sanity_checking_test_group"],
            ),
            node(
                partial(
                    create_sanity_check_for_random_test_group,
                    group_name_column="group_name",
                    group_flag_column="group_flag",
                    csv_file_path="data/tmp/sanity_check_test_groups_post_GCG_"
                    + partition_date_str
                    + ".csv",
                ),
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "l3_usage_postpaid_prepaid_monthly": "l3_usage_postpaid_prepaid_monthly",
                    "df_test_group": "l0_gcg_post_" + partition_date_str,
                },
                outputs="l5_sanity_checking_gcg_post_" + partition_date_str,
                name="sanity_checking_test_group_post",
                tags=["sanity_checking_test_group"],
            ),
            # node(
            #     partial(
            #         create_prepaid_test_groups,
            #         sampling_rate=[
            #             0.389,
            #             0.022,
            #             0.086,
            #             0.003,
            #             0.086,
            #             0.003,
            #             0.389,
            #             0.022,
            #         ],
            #         test_group_name=[
            #             "ATL_TG",
            #             "ATL_CG",
            #             "BTL1_TG",
            #             "BTL1_CG",
            #             "BTL2_TG",
            #             "BTL2_CG",
            #             "BTL3_TG",
            #             "BTL3_CG",
            #         ],
            #         test_group_flag=[
            #             "ATL_TG",
            #             "ATL_CG",
            #             "BTL1_TG",
            #             "BTL1_CG",
            #             "BTL2_TG",
            #             "BTL2_CG",
            #             "BTL3_TG",
            #             "BTL3_CG",
            #         ],
            #         partition_date_str=partition_date_str,
            #     ),
            #     inputs={
            #         "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
            #     },
            #     outputs="l0_du_pre_experiment3_" + partition_date_str,
            #     name="create_du_prepaid_test_groups",
            #     tags=["create_prepaid_test_groups"],
            # ),
            # node(
            #     partial(
            #         create_sanity_check_for_random_test_group,
            #         group_name_column="group_name",
            #         group_flag_column="group_flag",
            #         csv_file_path="data/tmp/sanity_check_du_pre_experiment3_"
            #         + partition_date_str
            #         + ".csv",
            #     ),
            #     inputs={
            #         "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
            #         "l3_usage_postpaid_prepaid_monthly": "l3_usage_postpaid_prepaid_monthly",
            #         "df_test_group": "l0_du_pre_experiment3_" + partition_date_str,
            #     },
            #     outputs="l5_sanity_du_pre_experiment3_" + partition_date_str,
            #     name="l5_sanity_du_pre_experiment3",
            #     tags=["sanity_checking_test_group"],
            # ),
        ]
    )
