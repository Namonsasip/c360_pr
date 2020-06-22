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
from cvm.src.features.microsegments import (
    add_microsegment_features,
    add_volatility_scores,
    define_microsegments,
)
from cvm.src.treatments.deploy_treatments import deploy_contact, prepare_campaigns_table
from cvm.src.treatments.treatments_build import (
    get_treatments_propositions,
    treatments_featurize,
    update_history_with_treatments_propositions,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from pyspark.sql.functions import when, col
from pyspark.sql import Window


def prepare_microsegments(
        raw_features: DataFrame,
        reve: DataFrame,
        parameters: Dict[str, Any],
        reduce_cols: bool = True,
) -> DataFrame:
    """ Add microsegments columns.

    Args:
        reduce_cols: should columns be reduced only to key columns, microsegment and
            microsegments.
        raw_features: Table with users to add microsegments to and pre - preprocessing
            features.
        reve: Table with monthly revenue. Assumes using l3 profile table.
        parameters: parameters defined in parameters.yml.
    """

    vol = add_volatility_scores(raw_features, reve, parameters)
    micro_features = add_microsegment_features(raw_features, parameters).join(
        vol, "subscription_identifier"
    )
    return define_microsegments(micro_features, parameters, reduce_cols)


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


def package_translation(
        df_contact: DataFrame,
        df_map_id: DataFrame,
        df_package: DataFrame,
        df_mapping: DataFrame,
        parameters: Dict[str, Any],
) -> DataFrame:
    """  Overwrite existing campaign_code with eligible package_preference

    Args:
        df_contact: treatment_chosen generated for send campaign.
        df_map_id: customer profile contain analytic_id and crm_sub_id.
        df_package: product of package preference with propensity table.
        df_mapping: offer ATL to BTL mapping.
        parameters: parameters defined in parameters.yml.

    Returns:
        df_out: result treatment table with package_preference offer overwritten.

    """
    # Get latest data from customer profile and prepare data
    date_filter = df_map_id.selectExpr("MAX(ddate)").collect()[0][0]
    df_map_id = df_map_id.filter("ddate == '{}'".format(date_filter)).selectExpr("analytic_id",
                                                                                 "activation_date as register_date",
                                                                                 "crm_sub_id as old_subscription_identifier")
    df_package = df_package.withColumnRenamed("activation_date", "register_date")

    # Join table
    df_join = df_contact.join(df_map_id, ['old_subscription_identifier'], 'left_outer')
    df_join = df_join.join(df_package, ["analytic_id", "register_date"], "left_outer")
    df_join = df_join.withColumnRenamed('offer_Price', 'offer_price')

    # Fill the unavailable BTL with ATL
    df_mapping = df_mapping.withColumn('MAID_BTL_PRICE',
                                       func.when(func.col('MAID_BTL_PRICE').isNull(), func.col('MAID_ATL')).otherwise(
                                           func.col('MAID_BTL_PRICE')))
    df_mapping = df_mapping.withColumn('DESC_BTL_PRICE',
                                       func.when(func.col('DESC_BTL_PRICE').isNull(), func.col('du_offer')).otherwise(
                                           func.col('DESC_BTL_PRICE')))
    df_mapping = df_mapping.withColumnRenamed('du_offer', 'offer')

    # Constrain to limit new subs from receiving unlimited data package
    df_join = df_join.filter("(register_date < '2019-02-01') OR offer_package_group != 'Fix UL'")
    # Prepare package preference table with anti down-sell rules
    offer_cond = "(" + " AND ".join(parameters["treatment_package_rec"]["condition"]) + ")"
    df_join = df_join.filter(offer_cond)
    window = Window.partitionBy('analytic_id', 'register_date').orderBy(func.col('prob_calibrated_cmp').desc())
    df_join = df_join.select('*',
                             func.max(func.col('prob_calibrated_cmp')).over(window).alias('max_prob_calibrated_cmp'))
    df_join = df_join.filter("prob_calibrated_cmp == max_prob_calibrated_cmp")

    # Mapping package preference offer with available MAID
    df_join = df_join.join(df_mapping, ['offer'], 'left_outer')
    df_join = df_join.filter("MAID_ATL IS NOT NULL").drop('max_prob_calibrated_cmp')

    # Get package type according to treatment_name
    for rule in parameters["package_btl_mapping"]:
        df_join = df_join.withColumn("offer_map", when(col("treatment_name").startswith(rule),
                                                       func.lit(parameters["package_btl_mapping"][rule])))

    df_out = df_join.withColumn("offer_id", when(col("offer_map") == 'ATL',
                                                 col("MAID_ATL")).when(col("offer_map") == 'BTL_PRICE',
                                                                       col("MAID_BTL_PRICE")))
    df_out = df_out.withColumn('campaign_code',
                               when(col("offer_id").isNotNull(), col("offer_id")).otherwise(col("campaign_code")))

    return df_out


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
