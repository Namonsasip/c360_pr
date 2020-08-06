from functools import partial
from kedro.pipeline import Pipeline, node

from du.upsell.experiment4_nodes import create_btl_experiment_score_distribution
from du.upsell.upsell_nodes import apply_data_upsell_rules,generate_daily_eligible_list,create_target_list_file


def create_du_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [
            # node(
            #     apply_data_upsell_rules,
            #     inputs={
            #         "du_campaign_offer_map_model": "params:du_campaign_offer_map_model",
            #         "l5_du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
            #     },
            #     outputs="unused_optimal_upsell",
            #     name="optimal_upsell",
            #     tags=["optimal_upsell"],
            # ),
            # node(
            #     generate_daily_eligible_list,
            #     inputs={
            #         "du_campaign_offer_atl_target": "params:du_campaign_offer_atl_target",
            #         "du_campaign_offer_btl1_target": "params:du_campaign_offer_btl1_target",
            #         "du_campaign_offer_btl2_target": "params:du_campaign_offer_btl2_target",
            #         "du_campaign_offer_btl3_target": "params:du_campaign_offer_btl3_target",
            #         "du_control_campaign_child_code": "params:du_control_campaign_child_code",
            #         "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
            #         "l0_du_pre_experiment3_groups":"l0_du_pre_experiment3_groups",
            #     },
            #     outputs="unused_optimal_upsell_2",
            #     name="generate_daily_eligible_list",
            #     tags=["generate_daily_eligible_list"],
            # ),
            node(
                partial(
                    create_target_list_file,
                    list_date=None,
                ),
                inputs={
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
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
