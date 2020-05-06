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
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from pyspark.sql import DataFrame


def add_arpus(users_report: DataFrame, reve: DataFrame, min_date: str,) -> DataFrame:
    """ Adds a column with daily revenues for given time range. Imputes the data with
        0 if needed.

    Args:
        users_report: table with users to create report for.
        reve: table with daily arpus.
        min_date: minimum date of report.
    """
    reve_to_pick = ["subscription_identifier", "key_date", "rev_arpu_total_net_rev"]
    reve = (
        prepare_key_columns(reve)
        .select(reve_to_pick)
        .filter("key_date >= '{}'".format(min_date))
    )
    return users_report.join(reve, on="subscription_identifier", how="left").fillna(
        {"rev_arpu_total_net_rev": 0}
    )
