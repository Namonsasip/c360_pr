from functools import partial
from kedro.pipeline import Pipeline, node

from du.upsell.target_list_generator_daily_nodes import (create_target_list_file)
from du.upsell.optimal_offer_creation_nodes import create_dataupsell_optimal_offer
from du.upsell.upsell_rules_reference_nodes import (
    generate_daily_eligible_list_reference,
)
from du.upsell.upsell_rules_bau_nodes import generate_daily_eligible_list_bau
from du.upsell.upsell_rules_new_experiment_nodes import (
    generate_daily_eligible_list_new_experiment,
)
import datetime

PROD_SCHEMA_NAME = "prod_dataupsell"
DEV_SCHEMA_NAME = "dev_dataupsell"
PROD_TARGET_LIST_PATH = "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/PCM/"
DEV_TARGET_LIST_PATH = "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/DEV/"


def create_du_upsell_pipeline_dev() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_dataupsell_optimal_offer,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "du_campaign_offer_new_experiment": "params:du_campaign_offer_new_experiment",
                    "du_campaign_offer_bau": "params:du_campaign_offer_bau",
                    "du_campaign_offer_reference": "params:du_campaign_offer_reference",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code",
                },
                outputs="unused_optimal_upsell",
                name="optimal_upsell",
                tags=["optimal_upsell"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_reference,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_reference": "params:du_campaign_offer_reference",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_reference",
                    "unused_optimal_upsell": "unused_optimal_upsell",
                },
                outputs="unused_optimal_upsell_2",
                name="generate_daily_eligible_list_reference",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_bau,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_bau": "params:du_campaign_offer_bau",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_bau",
                    "unused_optimal_upsell": "unused_optimal_upsell_2",
                },
                outputs="unused_optimal_upsell_3",
                name="generate_daily_eligible_list_bau",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_new_experiment,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_new_experiment": "params:du_campaign_offer_new_experiment",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_new_experiment",
                    "unused_optimal_upsell": "unused_optimal_upsell_3",
                },
                outputs="unused_optimal_upsell_4",
                name="generate_daily_eligible_list_new_experiment",
                tags=["generate_daily_eligible_list"],
            ),
        ]
    )


def create_du_target_list_pipeline_dev() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                    target_list_path=DEV_TARGET_LIST_PATH,
                ),
                inputs={
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                },
                outputs="unused_memory_blacklist",
                name="create_target_list_file",
                tags=["create_target_list_file"],
            ),
        ]
    )


def create_du_target_list_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                    target_list_path=PROD_TARGET_LIST_PATH,
                ),
                inputs={
                    "l5_du_offer_daily_eligible_list": "l5_du_offer_daily_eligible_list",
                },
                outputs="unused_memory_blacklist",
                name="create_target_list_file",
                tags=["create_target_list_file"],
            ),
        ]
    )


def create_du_upsell_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    create_dataupsell_optimal_offer,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_with_package_preference": "l5_du_offer_score_with_package_preference",
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "du_campaign_offer_new_experiment": "params:du_campaign_offer_new_experiment",
                    "du_campaign_offer_bau": "params:du_campaign_offer_bau",
                    "du_campaign_offer_reference": "params:du_campaign_offer_reference",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code",
                },
                outputs="unused_optimal_upsell",
                name="optimal_upsell",
                tags=["optimal_upsell"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_reference,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_reference": "params:du_campaign_offer_reference",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_reference",
                    "unused_optimal_upsell": "unused_optimal_upsell",
                },
                outputs="unused_optimal_upsell_2",
                name="generate_daily_eligible_list_reference",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_bau,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_bau": "params:du_campaign_offer_bau",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_bau",
                    "unused_optimal_upsell": "unused_optimal_upsell_2",
                },
                outputs="unused_optimal_upsell_3",
                name="generate_daily_eligible_list_bau",
                tags=["generate_daily_eligible_list"],
            ),
            node(
                partial(
                    generate_daily_eligible_list_new_experiment,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "l5_du_offer_blacklist": "l5_du_offer_blacklist",
                    "du_campaign_offer_new_experiment": "params:du_campaign_offer_new_experiment",
                    "du_control_campaign_child_code": "params:du_control_campaign_child_code_new_experiment",
                    "unused_optimal_upsell": "unused_optimal_upsell_3",
                },
                outputs="unused_optimal_upsell_4",
                name="generate_daily_eligible_list_new_experiment",
                tags=["generate_daily_eligible_list"],
            ),
        ],
        tags=["experiment4_eligible_upsell"],
    )