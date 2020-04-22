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
from datetime import datetime
from typing import Any, Dict

import pandas
import pytz

from pyspark.sql import DataFrame


def deploy_contact_ard(
    parameters: Dict[str, Any], df: DataFrame,
):
    """ Copy list from df to the target path for ARD campaign targeting.

    Args:
        parameters: parameters defined in parameters.yml.
        df: DataFrame with treatment per customer.

    """
    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))
    df = df[df["use_case"] == "ard"]
    df["data_date"] = created_date.date()
    df.rename(
        columns={
            parameters["treatment_output"]["key_column"]: "crm_subscription_id",
            parameters["treatment_output"]["treatment_column"]: "dummy01",
        },
        inplace=True,
    )
    df = df[["data_date", "crm_subscription_id", "dummy01"]]
    file_name = parameters["treatment_output"]["output_path_ard"] + "_{}.csv".format(
        created_date.strftime("%Y%m%d%H%M%S")
    )
    df.to_csv(file_name, index=False, header=True, sep="|")
    logging.info("ARD treatments saved to {}".format(file_name))

    return 0


def prepare_campaigns_table(
    treatments_chosen: pandas.DataFrame, use_case: str,
) -> pandas.DataFrame:
    """ Prepares table for saving.

    Args:
        treatments_chosen: List of users and campaigns chosen for all use cases.
        use_case: "churn" or "ard".
    """

    use_case_treatments = treatments_chosen[treatments_chosen["use_case"] == use_case]

    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))
    use_case_treatments["data_date"] = created_date.date()

    use_case_treatments.rename(
        columns={
            "subscription_identifier": "crm_subscription_id",
            "campaign_code": "dummy01",
        },
        inplace=True,
    )
    use_case_treatments = use_case_treatments[
        ["data_date", "crm_subscription_id", "dummy01"]
    ]

    if use_case == "churn":
        use_case_treatments["project_name"] = "CVM_Prepaid_churn_model_V2"
    return use_case_treatments


def deploy_contact(
    table_to_save: pandas.DataFrame, parameters: Dict[str, Any], use_case: str,
):
    """ Saves given table to final output paths.

    Args:
        table_to_save: list of users and treatments in format that is ready to save.
        parameters: parameters defined in parameters.yml.
        use_case: "churn" or "ard".
    """
    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))

    output_path_prefix = parameters["treatment_output"][use_case]["output_path_prefix"]
    output_path_suffix_format = parameters["treatment_output"][use_case][
        "output_path_suffix_format"
    ]
    output_path_suffix = created_date.strftime(output_path_suffix_format)

    output_path = "{}{}.csv".format(output_path_prefix, output_path_suffix)
    table_to_save.to_csv(output_path, index=False, header=True, sep="|")
    logging.info("Treatments for {} saved in {}".format(use_case, output_path))
