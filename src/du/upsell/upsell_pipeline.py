from functools import partial
from kedro.pipeline import Pipeline, node

from du.scoring.scoring_nodes import du_join_preference
from du.upsell.experiment4_nodes import create_btl_experiment_score_distribution
from du.upsell.upsell_nodes import (
    apply_data_upsell_rules,
    generate_daily_eligible_list,
    create_target_list_file,
    create_weekly_low_score_upsell_list,
    create_weekly_low_score_target_list_file,
)
from du.experiment.group_manage_nodes import update_du_control_group
import datetime


def create_du_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    update_du_control_group,
                    sampling_rate=[
                        0.389,
                        0.022,
                        0.086,
                        0.003,
                        0.086,
                        0.003,
                        0.389,
                        0.022,
                    ],
                    test_group_name=[
                        "ATL_TG",
                        "ATL_CG",
                        "BTL1_TG",
                        "BTL1_CG",
                        "BTL2_TG",
                        "BTL2_CG",
                        "BTL3_TG",
                        "BTL3_CG",
                    ],
                    test_group_flag=[
                        "ATL_TG",
                        "ATL_CG",
                        "BTL1_TG",
                        "BTL1_CG",
                        "BTL2_TG",
                        "BTL2_CG",
                        "BTL3_TG",
                        "BTL3_CG",
                    ],
                ),
                inputs={
                    "l0_du_pre_experiment3_groups": "l0_du_pre_experiment3_groups",
                    "l0_customer_profile_profile_customer_profile_pre_current_full_load": "l0_customer_profile_profile_customer_profile_pre_current_full_load",
                },
                outputs="unused_memory_update_groups",
                name="update_du_control_group",
                tags=["update_du_control_group"],
            ),
            node(
                du_join_preference,
                inputs={
                    "l5_du_scored": "l5_du_scored",
                    "mapping_for_model_training": "mapping_for_model_training",
                    "l0_product_pru_m_ontop_master_for_weekly_full_load": "l0_product_pru_m_ontop_master_for_weekly_full_load",
                    "l5_du_scoring_master": "l5_du_scoring_master",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                },
                outputs="unused_memory_dataset_4",
                name="l5_du_join_preference",
                tags=["du_join_preference"],
            ),
            node(
                apply_data_upsell_rules,
                inputs={
                    "du_campaign_offer_map_model": "params:du_campaign_offer_map_model",
                    "l5_du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "unused_memory_dataset_4": "unused_memory_dataset_4",
                    "du_campaign_offer_atl_target": "params:du_campaign_offer_atl_target",
                    "du_campaign_offer_btl1_target": "params:du_campaign_offer_btl1_target",
                    "du_campaign_offer_btl2_target": "params:du_campaign_offer_btl2_target",
                    "du_campaign_offer_btl3_target": "params:du_campaign_offer_btl3_target",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code",
                },
                outputs="unused_optimal_upsell",
                name="optimal_upsell",
                tags=["optimal_upsell"],
            ),
            node(
                generate_daily_eligible_list,
                inputs={
                    "du_campaign_offer_atl_target": "params:du_campaign_offer_atl_target",
                    "du_campaign_offer_btl1_target": "params:du_campaign_offer_btl1_target",
                    "du_campaign_offer_btl2_target": "params:du_campaign_offer_btl2_target",
                    "du_campaign_offer_btl3_target": "params:du_campaign_offer_btl3_target",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "l0_du_pre_experiment3_groups": "l0_du_pre_experiment3_groups",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "unused_optimal_upsell": "unused_optimal_upsell",
                },
                outputs="unused_optimal_upsell_2",
                name="generate_daily_eligible_list",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    create_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                ),
                inputs={
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                    "unused_optimal_upsell_2": "unused_optimal_upsell_2",
                },
                outputs="unused_memory_blacklist",
                name="create_target_list_file",
                tags=["create_target_list_file"],
            ),
            # node(
            #     create_btl_experiment_score_distribution,
            #     inputs={
            #         "l4_revenue_prepaid_pru_f_usage_multi_features_sum": "l4_revenue_prepaid_pru_f_usage_multi_features_sum",
            #         "l5_du_scored": "l5_du_scored",
            #     },
            #     outputs="l5_experiment4_eligible_upsell",
            #     name="l5_experiment4_eligible_upsell",
            #     tags=["l5_experiment4_eligible_upsell"],
            # ),
        ],
        tags=["experiment4_eligible_upsell"],
    )


def create_du_weekly_low_score_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                create_weekly_low_score_upsell_list,
                inputs={
                    "du_campaign_offer_atl_target_low_score": "params:du_campaign_offer_atl_target_low_score",
                    "du_campaign_offer_btl3_target_low_score": "params:du_campaign_offer_btl3_target_low_score",
                    "du_control_campaign_child_code_low_score": "params:du_control_campaign_child_code_low_score",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "l0_du_pre_experiment3_groups": "l0_du_pre_experiment3_groups",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                },
                outputs="unused_weekly_low_score_list",
                name="create_weekly_low_score_upsell_list",
                tags=["create_weekly_low_score_upsell_list"],
            ),
            node(
                partial(
                    create_weekly_low_score_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                ),
                inputs={
                    "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
                    "unused_weekly_low_score_list": "unused_weekly_low_score_list",
                },
                outputs="unused_memory",
                name="create_weekly_low_score_target_list_file",
                tags=["create_weekly_low_score_target_list_file"],
            ),
        ]
    )
