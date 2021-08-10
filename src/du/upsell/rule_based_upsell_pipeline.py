from functools import partial
from kedro.pipeline import Pipeline, node
from du.upsell.rule_based_upsell_bau_nodes import create_rule_based_daily_upsell_bau
from du.upsell.rule_based_upsell_new_experiment_nodes import (
    create_rule_based_daily_upsell_new_experiment,
)


def create_du_rule_based_upsell_pipeline(mode: str) -> Pipeline:
    if mode == "Production":
        delta_table_schema = "prod_dataupsell"
        # Since Current production doesn't have any suffix so we leave it blank
        suffix = ""
    elif mode == "Development":
        delta_table_schema = "dev_dataupsell"
        suffix = "_dev"
    return Pipeline(
        [
            node(
                partial(
                    create_rule_based_daily_upsell_bau,
                    delta_table_schema=delta_table_schema,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list"
                    + suffix,
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference"
                    + suffix,
                },
                outputs="unused_rulebase_1",
                name="generate_daily_rule_based_upsell_bau",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    create_rule_based_daily_upsell_new_experiment,
                    delta_table_schema=delta_table_schema,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list"
                    + suffix,
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference"
                    + suffix,
                    "du_rule_based_offer_params": "params:du_rule_based_offer_params",
                },
                outputs="unused_rulebase_2",
                name="generate_daily_rule_based_new_experiment",
                tags=["generate_daily_eligible_list"],
            ),
        ]
    )
