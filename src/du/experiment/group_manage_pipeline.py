from functools import partial

from kedro.pipeline import Pipeline, node

from du.experiment.group_manage_nodes import  (create_prepaid_test_groups,
create_sanity_check_for_random_test_group
                                               )

def create_du_test_group_pipeline() -> Pipeline:
    return Pipeline(
        [
            #Do not run this node, only run this node when initializing new test groups
            node(
                partial(
                    create_prepaid_test_groups,
                    sampling_rate=[0.975, 0.025],
                    test_group_name=["Default", "GCG"],
                    test_group_flag=["N", "Y"],
                ),
                inputs={
                    "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
                },
                outputs="l0_gcg_20200701",
                name="create_prepaid_test_groups",
                tags=["create_prepaid_test_groups"],
            ),
            node(
                partial(
                    create_sanity_check_for_random_test_group,
                    group_name_column="group_name",
                    group_flag_column="group_flag",
                    csv_file_path="data/tmp/sanity_check_test_groups_GCG_20200701.csv",
                ),
                inputs={
                    "l3_customer_profile_include_1mo_non_active": "l3_customer_profile_include_1mo_non_active",
                    "l3_usage_postpaid_prepaid_monthly":"l3_usage_postpaid_prepaid_monthly",
                    "df_test_group":"l0_gcg_20200701",
                },
                outputs="l5_sanity_checking_gcg_20200701",
                name="sanity_checking_test_group",
                tags=["sanity_checking_test_group"],
            ),
        ]
    )


def update_du_test_group_pipeline() -> Pipeline:
    return Pipeline([])
