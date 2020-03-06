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

from pyspark.sql import DataFrame


def create_sample_dataset(df: DataFrame, subscription_id_suffix: str) -> DataFrame:
    """ Create dev sample of given table. Dev sample is super small sample. Takes only
    users with certain subscription_identifier suffix.

    Args:
        subscription_id_suffix: suffix to filter subscription_identifier with.
        df: given table.
    Returns:
        Dev sample of table.
    """

    suffix_length = len(subscription_id_suffix)
    df = df.withColumn(
        "subscription_identifier_last_letter",
        df.subscription_identifier.substr(-suffix_length, suffix_length),
    )
    subs_filter = "subscription_identifier_last_letter == '{}'".format(
        subscription_id_suffix
    )

    df = df.filter(subs_filter).drop("subscription_identifier_last_letter")

    return df
