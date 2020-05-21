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
from typing import Any, Dict

import pyspark.sql.functions as func
from pyspark.sql import DataFrame, Window


def add_other_sim_card_features(
    df: DataFrame,
    recent_profile: DataFrame,
    main_packs: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Add features for other sim card held by the same national id.

    Args:
        df: table with users.
        recent_profile: table with users' national ids, only last date.
        main_packs: table with main packages details.
        parameters: parameters defined in parameters.yml.
    """
    normal_main_packs = (
        main_packs.filter(
            """promotion_group_tariff not in ('SIM 2 Fly',
             'Net SIM', 'Traveller SIM')"""
        )
        .select("package_id")
        .withColumnRenamed("package_id", "current_package_id")
    )
    number_of_simcards = (
        # keep only normal packages
        recent_profile.join(normal_main_packs, on="current_package_id")
        # calculate number of simcards per national id
        .groupBy("national_id_card")
        .agg(func.count("subscription_identifier").alias("number_of_simcards"))
        .filter("number_of_simcards >= 2 and number_of_simcards <= 4")
        .select(["national_id_card", "number_of_simcards"])
    )
    # calculate statistics
    national_id_card_stats_youngest_card = (
        number_of_simcards.join(recent_profile, on="national_id_card")
        .withColumn(
            "card_age_rn",
            func.row_number.over(
                Window.partitionBy("national_id_card").orderBy(
                    func.col("subscription_tenure").orderBy(
                        func.col("subscription_tenure")
                    )
                )
            ),
        )
        .filter("card_age_rn == 1")
        .selectExpr(
            "national_id_card",
            "norms_net_revenue as revenue_on_youngest_card",
            "youngest_card_tenure",
        )
    )
    national_id_card_stats_oldest_card = (
        number_of_simcards.join(recent_profile, on="national_id_card")
        .withColumn(
            "card_age_rn",
            func.row_number.over(
                Window.partitionBy("national_id_card").orderBy(
                    func.col("subscription_tenure").orderBy(
                        func.col("subscription_tenure").desc()
                    )
                )
            ),
        )
        .filter("card_age_rn == 1")
        .selectExpr(
            "national_id_card",
            "norms_net_revenue as revenue_on_oldest_card",
            "oldest_card_tenure",
        )
    )
    national_id_card_stats = national_id_card_stats_oldest_card.join(
        national_id_card_stats_youngest_card, on="national_id_card"
    )
    # join to given table
    df = df.join(national_id_card_stats, on="national_id_card", how="left")

    return df
