# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import os
from datetime import datetime
from distutils.dir_util import copy_tree
from typing import Any, Dict, Tuple

import pandas
import pytz
from cvm.src.treatments.deploy_treatments import deploy_contact, prepare_campaigns_table
from cvm.src.treatments.treatment_translation import package_translation
from cvm.src.treatments.treatments_build import (
    get_treatments_propositions,
    treatments_featurize,
    update_history_with_treatments_propositions,
)
from pyspark.sql import DataFrame


def create_treatments_features(
    propensities: DataFrame,
    prediction_sample: DataFrame,
    microsegments: DataFrame,
    recent_profile: DataFrame,
    main_packs: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Prepare table with users and features needed for treatments generation

    Args:
        propensities: scores created by models.
        prediction_sample: table with features needed.
        microsegments: table with microsegments.
        recent_profile: table with users' national ids, only last date.
        main_packs: table describing prepaid main packages.
        parameters: parameters defined in parameters.yml.
    """
    return treatments_featurize(
        propensities,
        prediction_sample,
        microsegments,
        recent_profile,
        main_packs,
        parameters,
    )


def produce_treatments(
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
    treatments_features: DataFrame,
    users: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """  Generates treatments and updated treatments history.

    Args:
        users: table with users and dates to create targets for, used to map to old sub
            id.
        treatments_features: features used for treatments, output of treatment
            featurizer.
        parameters: parameters defined in parameters.yml.
        treatments_history: table with history of treatments.
    """
    treatments_propositions = get_treatments_propositions(
        parameters, treatments_history, treatments_features, users
    )
    treatments_propositions = treatments_propositions.cache()
    treatments_history = update_history_with_treatments_propositions(
        treatments_propositions, treatments_history, parameters
    )
    return treatments_propositions.toPandas(), treatments_history


def produce_treatments_translated(
    treatments_history: DataFrame,
    parameters: Dict[str, Any],
    treatments_features: DataFrame,
    users: DataFrame,
    package_preference: DataFrame,
    offer_mapping: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """  Generates treatments and updated treatments history.

    Args:
        users: table with users and dates to create targets for, used to map to old sub
            id.
        treatments_features: features used for treatments, output of treatment
            featurizer.
        parameters: parameters defined in parameters.yml.
        treatments_history: table with history of treatments.
        package_preference: product of package preference with propensity table.
        offer_mapping: offer ATL to BTL mapping.
    """
    log = logging.getLogger(__name__)
    treatments_propositions = get_treatments_propositions(
        parameters, treatments_history, treatments_features, users
    )
    treatments_propositions = treatments_propositions.cache()
    log.info(
        "Treatments assigned before translation: {}".format(
            treatments_propositions.count()
        )
    )
    if parameters["treatment_output"]["skip_pref_pack_translation"] == "yes":
        treatments_propositions_translated = treatments_propositions
        log.info("Translation skipped")
    else:
        treatments_propositions_translated = package_translation(
            treatments_propositions,
            package_preference,
            offer_mapping,
            parameters,
        )
        log.info(
            "Treatments assigned after translation: {}".format(
                treatments_propositions_translated.count()
            )
        )
    treatments_history = update_history_with_treatments_propositions(
        treatments_propositions_translated, treatments_history, parameters
    )
    return treatments_propositions_translated.toPandas(), treatments_history


def deploy_treatments(
    treatments_chosen: pandas.DataFrame, parameters: Dict[str, Any],
):
    """ Send the treatments to the campaign team.

    Args:
        treatments_chosen: List of users and campaigns chosen for all use cases.:
        parameters: parameters defined in parameters.yml.
    """

    skip_node = parameters["treatment_output"]["skip_sending"] == "yes"
    if not skip_node:
        use_cases = parameters["targets"]
        for use_case in use_cases:
            campaign_table_prepared = prepare_campaigns_table(
                treatments_chosen, use_case
            )
            deploy_contact(campaign_table_prepared, parameters, use_case)
    else:
        logging.info("Sending treatments skipped")
    # return True to tell kedro that this node has to happen before copying the logs
    return True


def copy_logs_to_dbfs(parameters: Dict[str, Any], treatments_deployed: bool):
    """ Databricks is unable to write logs to dbfs, because random writes are not
    supported. Because of that logs are saved in logs/ folder first and then this node
    copies the logs to persistent dbfs path.

    Args:
        treatments_deployed: parameters used to tell kedro that this node has to
            happen after treatments deployment.
        parameters: parameters defined in parameters.yml. Used to define path of logs.
    """
    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))
    output_path_template = parameters["logs_path"]["output_path_template"]
    output_path_date_format = parameters["logs_path"]["output_path_date_format"]
    output_path_date = created_date.strftime(output_path_date_format)
    output_path = output_path_template.format(output_path_date)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    logging.getLogger(__name__).info("Saving logs to {}".format(output_path))
    copy_tree("logs/", output_path)
