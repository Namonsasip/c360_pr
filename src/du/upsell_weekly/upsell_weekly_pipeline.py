from functools import partial
from kedro.pipeline import Pipeline, node

from du.upsell_weekly.upsell_weekly_bau_nodes import generate_weekly_eligible_list_bau
from du.upsell_weekly.upsell_weekly_new_experiment_nodes import (
    generate_weekly_eligible_list_new_experiment,
)
from du.upsell_weekly.upsell_weekly_reference_nodes import (
    generate_weekly_eligible_list_reference,
)
from du.upsell_weekly.file_generation_nodes import (
    create_weekly_low_score_target_list_file,
)

import datetime

PROD_SCHEMA_NAME = "prod_dataupsell"
DEV_SCHEMA_NAME = "dev_dataupsell"
PROD_TARGET_LIST_PATH = "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/PCM/"
DEV_TARGET_LIST_PATH = "/dbfs/mnt/cvm02/cvm_output/MCK/DATAUP/DEV/"


def create_du_weekly_low_score_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    generate_weekly_eligible_list_reference,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_reference": "params:du_campaign_child_code_low_score_reference",
                    "du_control_campaign_child_code_low_score_reference": "params:du_control_campaign_child_code_low_score_reference",
                    "unused_optimal_upsell": "l5_du_scored",
                },
                outputs="unused_optimal_upsell_1",
                name="generate_weekly_eligible_list_reference",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    generate_weekly_eligible_list_bau,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_bau": "params:du_campaign_child_code_low_score_bau",
                    "du_control_campaign_child_code_low_score_bau": "params:du_control_campaign_child_code_low_score_bau",
                    "unused_optimal_upsell": "unused_optimal_upsell_1",
                },
                outputs="unused_optimal_upsell_2",
                name="generate_weekly_eligible_list_bau",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    generate_weekly_eligible_list_new_experiment,
                    schema_name=PROD_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_new_experiment": "params:du_campaign_child_code_low_score_new_experiment",
                    "du_control_campaign_child_code_low_score_new_experiment": "params:du_control_campaign_child_code_low_score_new_experiment",
                    "unused_optimal_upsell": "unused_optimal_upsell_2",
                },
                outputs="unused_optimal_upsell_3",
                name="generate_weekly_eligible_list_new_experiment",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    create_weekly_low_score_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                    target_list_path=PROD_TARGET_LIST_PATH,
                ),
                inputs={
                    "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list",
                    "unused_weekly_low_score_list": "unused_optimal_upsell_3",
                },
                outputs="unused_memory",
                name="create_weekly_low_score_target_list_file",
                tags=["create_weekly_low_score_target_list_file"],
            ),
        ]
    )


def create_du_weekly_low_score_pipeline_dev() -> Pipeline:
    return Pipeline(
        [
            node(
                partial(
                    generate_weekly_eligible_list_reference,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_reference": "params:du_campaign_child_code_low_score_reference",
                    "du_control_campaign_child_code_low_score_reference": "params:du_control_campaign_child_code_low_score_reference",
                    "unused_optimal_upsell": "l5_du_scored",
                },
                outputs="unused_optimal_upsell_1",
                name="generate_weekly_eligible_list_reference",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    generate_weekly_eligible_list_bau,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_bau": "params:du_campaign_child_code_low_score_bau",
                    "du_control_campaign_child_code_low_score_bau": "params:du_control_campaign_child_code_low_score_bau",
                    "unused_optimal_upsell": "unused_optimal_upsell_1",
                },
                outputs="unused_optimal_upsell_2",
                name="generate_weekly_eligible_list_bau",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    generate_weekly_eligible_list_new_experiment,
                    schema_name=DEV_SCHEMA_NAME,
                    prod_schema_name=PROD_SCHEMA_NAME,
                    dev_schema_name=DEV_SCHEMA_NAME,
                ),
                inputs={
                    "l0_campaign_tracking_contact_list_pre_full_load": "l0_campaign_tracking_contact_list_pre_full_load",
                    "l5_du_offer_score_optimal_offer": "l5_du_offer_score_optimal_offer",
                    "data_upsell_usecase_control_group_2021": "data_upsell_usecase_control_group_2021",
                    "du_campaign_child_code_low_score_new_experiment": "params:du_campaign_child_code_low_score_new_experiment",
                    "du_control_campaign_child_code_low_score_new_experiment": "params:du_control_campaign_child_code_low_score_new_experiment",
                    "unused_optimal_upsell": "unused_optimal_upsell_2",
                },
                outputs="unused_optimal_upsell_3",
                name="generate_weekly_eligible_list_new_experiment",
                tags=["generate_weekly_eligible_list"],
            ),
            node(
                partial(
                    create_weekly_low_score_target_list_file,
                    list_date=datetime.datetime.now()
                    + datetime.timedelta(hours=7)
                    + datetime.timedelta(days=1),
                    target_list_path=DEV_TARGET_LIST_PATH,
                ),
                inputs={
                    "l5_du_offer_weekly_low_score_list": "l5_du_offer_weekly_low_score_list_dev",
                    "unused_weekly_low_score_list": "unused_optimal_upsell_3",
                },
                outputs="unused_memory",
                name="create_weekly_low_score_target_list_file",
                tags=["create_weekly_low_score_target_list_file"],
            ),
        ]
    )
