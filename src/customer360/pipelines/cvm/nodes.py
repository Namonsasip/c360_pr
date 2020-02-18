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
from pyspark.sql import DataFrame
import functools


def create_l5_cvm_users_table(
        profile: DataFrame,
        main_packs: DataFrame,
        parameters: Dict[str, Any]
) -> DataFrame:
    """Create l5_cvm_users_table - monthly table of users used for training and
    validating.

    Args:
        profile: monthly customer profiles.
        main_packs: pre-paid main packages description.
        parameters: parameters defined in parameters.yml.
    """

    min_date = parameters["l5_cvm_users_table"]["min_date"]
    users = profile.filter("partition_month >= '{}'".format(min_date))
    users = users.filter(
        "charge_type == 'Pre-paid' AND subscription_status == 'SA'")
    users = users.filter("subscriber_tenure >= 4")
    users = users.filter("norms_net_revenue > 0")

    main_packs = main_packs.filter("promotion_group_tariff not in ('SIM 2 Fly',\
     'SIM NET MARATHON', 'Net SIM', 'Traveller SIM', 'Foreigner SIM')")
    main_packs = main_packs.select('package_id'). \
        withColumnRenamed('package_id', 'current_package_id')
    users = users.join(main_packs, ['current_package_id'], 'inner')
    columns_to_pick = ['partition_month', 'subscription_identifier']
    users = users.select(columns_to_pick)

    return users


def create_l5_cvm_users_sample_table(
        users: DataFrame
) -> DataFrame:
    """Sample long term users to create development sample. Users with at least
    5 months of activity and subscription_identifier ending with 'A' are chosen.

    Args:
        users: Monthly user table.

    Returns:
        Table with subscription_identifiers.
    """

    users_months_count = users.groupby("subscription_identifier").count()
    long_term_users = users_months_count.filter("count == 5").select(
        "subscription_identifier").distinct()
    long_term_users = long_term_users.withColumn(
        "subscription_identifier_last_letter",
        long_term_users.subscription_identifier.substr(-1, 1))
    long_term_users = long_term_users.filter(
        "subscription_identifier_last_letter == 'A'")
    long_term_users = long_term_users.select("subscription_identifier")

    return long_term_users


def create_l5_cvm_ard_monthly_targets(
        users: DataFrame,
        reve: DataFrame,
        parameters: Dict[str, Any]
) -> DataFrame:
    """ Create table with ARPU drop targets.

    Args:
        users: Table with users and dates to create targets for.
        reve: Table with revenue stats.
        parameters: parameters defined in parameters.yml.
    Returns:
        Table with ARD targets.
    """

    def get_ard_targets(
            users: DataFrame,
            reve: DataFrame,
            target_parameters: Dict[str, Any]
    ) -> DataFrame:
        """ Create table with one ARPU drop target.

        Args:
            users: Table with users and dates to create targets for.
            reve: Table with revenue stats.
            target_parameters: parameters for given target.

        Returns:
            Table with single ARPU drop target.
        """

        length = target_parameters["length"]
        drop = target_parameters["drop"]
        target_colname = target_parameters["colname"]

        if length == 1:
            arpu_col = "sum_rev_arpu_total_revenue_monthly_last_month"
        elif length == 3:
            arpu_col = "sum_rev_arpu_total_revenue_monthly_last_three_month"
        else:
            raise Exception("No implementation for length = {0}".format(length))

        # Setup table with dates and arpu
        cols_to_pick = ["subscription_identifier", "start_of_month", arpu_col]
        users = users.withColumnRenamed("partition_month", "start_of_month")
        reve_arpu_only = reve.select(cols_to_pick)
        reve_arpu_only = reve_arpu_only.withColumnRenamed(arpu_col, "reve")
        # Pick only interesting users
        reve_arpu_only = users.join(
            reve_arpu_only,
            ["subscription_identifier", "start_of_month"],
            "left"
        )

        # Setup reve before and after
        reve_arpu_before_after = reve_arpu_only.withColumnRenamed(
            "start_of_month", "start_of_month_before")
        reve_arpu_before_after = reve_arpu_before_after.withColumn(
            "start_of_month",
            func.add_months(reve_arpu_before_after.start_of_month_before,
                            length)
        )
        reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
            "reve", "reve_before")

        reve_arpu_before_after = reve_arpu_before_after.join(
            reve_arpu_only,
            ["start_of_month", "subscription_identifier"]
        )

        reve_arpu_before_after = reve_arpu_before_after.drop("start_of_month")
        reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
            "reve", "reve_after")

        # Add the label
        reve_arpu_before_after = reve_arpu_before_after.withColumn(
            "reve_after_perc",
            reve_arpu_before_after.reve_after / reve_arpu_before_after.reve_before)
        target_col = func.when(
            1 - reve_arpu_before_after.reve_after_perc >= drop,
            "drop").otherwise("no_drop")
        reve_arpu_before_after = reve_arpu_before_after.withColumn("target",
                                                                   target_col)
        reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
            "start_of_month_before", "start_of_month")

        cols_to_select = ["target", "start_of_month", "subscription_identifier"]
        reve_arpu_before_after = reve_arpu_before_after.select(cols_to_select)

        reve_arpu_before_after = reve_arpu_before_after.withColumnRenamed(
            "target", target_colname)

        return reve_arpu_before_after

    local_parameters = parameters["l5_cvm_ard_monthly_targets"]
    ard_target_tables = [get_ard_targets(users, reve, local_parameters[targets])
                         for targets in local_parameters]
    join_targets = lambda df1, df2: df1.join(
        df2,
        ["start_of_month", "subscription_identifier"],
        "full"
    )

    return functools.reduce(join_targets, ard_target_tables)
