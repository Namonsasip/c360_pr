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
from datetime import datetime
from typing import Any, Dict

import pytz

from pyspark.sql import DataFrame


def deploy_contact(
    parameters: Dict[str, Any], df: DataFrame,
):
    """ Copy list from df to the target path for campaign targeting

    Args:
        parameters: parameters defined in parameters.yml.
        df: DataFrame with treatment per customer.

    """
    utc_now = pytz.utc.localize(datetime.utcnow())
    created_date = utc_now.astimezone(pytz.timezone("Asia/Bangkok"))
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

    return 0
