from functools import partial
from kedro.pipeline import Pipeline, node
from du.upsell.rule_based_upsell_bau_nodes import create_rule_based_daily_upsell_bau
from du.upsell.rule_based_upsell_new_experiment_nodes import (
    create_rule_based_daily_upsell_new_experiment,
)

PROD_SCHEMA_NAME = "prod_dataupsell"
DEV_SCHEMA_NAME = "dev_dataupsell"


def create_du_rule_based_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_rule_based_daily_upsell_bau,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                },
                outputs="unused_rulebase_1",
                name="generate_daily_rule_based_upsell_bau",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    create_rule_based_daily_upsell_new_experiment,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                    "du_rule_based_offer_params": "params:du_rule_based_offer_params",
                },
                outputs="unused_rulebase_2",
                name="generate_daily_rule_based_new_experiment",
                tags=["generate_daily_eligible_list"],
            ),
        ]
    )


def create_du_rule_based_upsell_pipeline_dev() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_rule_based_daily_upsell_bau,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                },
                outputs="unused_rulebase_1",
                name="generate_daily_rule_based_upsell_bau",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    create_rule_based_daily_upsell_new_experiment,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l4_data_ontop_package_preference": "l4_data_ontop_package_preference",
                    "du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                    "du_rule_based_offer_params": "params:du_rule_based_offer_params",
                },
                outputs="unused_rulebase_2",
                name="generate_daily_rule_based_new_experiment",
                tags=["generate_daily_eligible_list"],
            ),
        ]
    )