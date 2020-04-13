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
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as func

from cvm.src.targets.churn_targets import add_days


def add_microsegment_features(df: DataFrame, parameters: Dict[str, Any]) -> DataFrame:
    """Create features to create microsegments on.

    Args:
        df: DataFrame with raw (not preprocessed) features.
        parameters: parameters defined in parameters.yml.
    """
    df = df.withColumn(
        "arpu_dynamic",
        df.sum_rev_arpu_total_revenue_monthly_last_month
        / df.sum_rev_arpu_total_revenue_monthly_last_three_month,
    )
    df = df.withColumn(
        "arpu_data_share",
        df.sum_rev_arpu_total_gprs_net_revenue_monthly_last_three_month
        / df.sum_rev_arpu_total_revenue_monthly_last_three_month,
    )
    df = df.withColumn(
        "data_to_voice_consumption",
        df.sum_usg_outgoing_data_volume_daily_last_thirty_day
        / df.sum_usg_outgoing_total_call_duration_daily_last_thirty_day,
    )
    df = df.withColumn(
        "data_weekend_consumption_share",
        df.sum_usg_data_weekend_usage_sum_weekly_last_four_week
        / (
            df.sum_usg_data_weekend_usage_sum_weekly_last_four_week
            + df.sum_usg_data_weekday_usage_sum_weekly_last_four_week
        ),
    )
    df = df.withColumn(
        "voice_weekend_consumption_share",
        df.sum_usg_outgoing_weekend_number_calls_sum_weekly_last_four_week
        / (
            df.sum_usg_outgoing_weekend_number_calls_sum_weekly_last_four_week
            + df.sum_usg_outgoing_weekday_number_calls_sum_weekly_last_four_week
        ),
    )

    key_date = df.select("key_date").first()[0].strftime("%Y-%m-%d")
    date_10days_ago = add_days(key_date, -10)
    df = df.withColumn(
        "action_in_last_10days",
        func.when(df.max_usg_last_action_date_daily_last_ninety_day.isNull(), 0)
        .when(date_10days_ago > df.max_usg_last_action_date_daily_last_ninety_day, 0)
        .otherwise(1),
    )

    df.fillna(parameters["feature_default_values"])

    return df
