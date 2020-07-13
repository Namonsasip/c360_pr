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
from typing import Any, Dict

from cvm.src.utils.incremental_manipulation import filter_latest_date, filter_users
from cvm.src.utils.prepare_key_columns import prepare_key_columns
from cvm.src.utils.utils import get_today
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as func

from src.cvm.src.utils.utils import pick_one_per_subscriber


def create_users_from_cgtg(
    customer_groups: DataFrame, sub_id_mapping: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Creates users table to use during scoring using customer groups
    table.

    Args:
        sub_id_mapping: table with old and new subscription identifiers.
        customer_groups: table with target, control and bau groups.
        parameters: parameters defined in parameters.yml.
    """

    today = get_today(parameters)
    df = (
        customer_groups.filter("target_group == 'TG_2020_CVM_V2'")
        .select("crm_sub_id")
        .distinct()
        .withColumn("key_date", func.lit(today))
        .withColumnRenamed("crm_sub_id", "old_subscription_identifier")
        .join(sub_id_mapping, on="old_subscription_identifier")
    )
    return df


def create_users_from_active_users(
    profile: DataFrame,
    main_packs: DataFrame,
    sub_id_mapping: DataFrame,
    sampling_parameters: Dict[str, Any],
    parameters: Dict[str, Any],
) -> DataFrame:
    """Create l5_cvm_one_day_users_table - one day table of users used for
    training and validating.

    Args:
        sub_id_mapping: table with old and new subscription identifiers.
        profile: monthly customer profiles.
        main_packs: pre-paid main packages description.
        sampling_parameters: sampling parameters defined in parameters.yml.
        parameters: parameters defined in parameters.yml.
    """

    profile = prepare_key_columns(profile)
    date_chosen = sampling_parameters["chosen_date"]
    if date_chosen == "today" or date_chosen == "":
        date_chosen = None
    users = filter_latest_date(profile, parameters, date_chosen)
    users = users.filter(
        "charge_type == 'Pre-paid' \
         AND subscription_status == 'SA' \
         AND subscription_identifier is not null \
         AND subscription_identifier not in ('null', 'NA') \
         AND cust_active_this_month = 'Y'"
    )
    users = users.filter("subscriber_tenure >= 4")

    main_packs = main_packs.filter(
        "promotion_group_tariff not in ('SIM 2 Fly', \
         'Net SIM', 'Traveller SIM')"
    )
    main_packs = (
        main_packs.select("package_id")
        .distinct()
        .withColumnRenamed("package_id", "current_package_id")
    )
    users = users.join(main_packs, ["current_package_id"], "inner")
    columns_to_pick = ["key_date", "subscription_identifier"]
    users = users.select(columns_to_pick).distinct()
    users = users.join(sub_id_mapping, on="subscription_identifier")
    return users


def create_sample_dataset(
    df: DataFrame,
    parameters: Dict[str, Any],
    sampling_parameters: Dict[str, Any],
    using_old_subscription_identifier: bool = False,
) -> DataFrame:
    """ Create sample of given table. Used to limit users and / or pick chosen date from
    data.

    Args:
        using_old_subscription_identifier: if table contains old sub id, make it visible
            in sample
        df: given table.
        sampling_parameters: sampling parameters defined in parameters.yml.
        parameters: parameters defined in parameters.yml.
    Returns:
        Sample of table.
    """
    subscription_id_hash_suffix = sampling_parameters["subscription_id_hash_suffix"]
    max_date = sampling_parameters["chosen_date"]

    sampling_stages = {
        "filter_users": lambda dfx: filter_users(dfx, subscription_id_hash_suffix),
        "take_last_date": lambda dfx: filter_latest_date(dfx, parameters, max_date),
    }

    starting_rows = df.count()
    for stage in sampling_parameters["stages"]:
        df = sampling_stages[stage](df)

    if using_old_subscription_identifier:
        df = df.withColumnRenamed(
            "subscription_identifier", "old_subscription_identifier"
        )

    log = logging.getLogger(__name__)
    if "subscription_identifier" in df.columns:
        df = pick_one_per_subscriber(df)
    elif "old_subscription_identifier" in df.columns:
        df = pick_one_per_subscriber(df, col_name="old_subscription_identifier")
    log.info(f"Sample has {df.count()} rows, down from {starting_rows} rows.")

    return df


def create_sub_id_mapping(l1_profile: DataFrame) -> DataFrame:
    """ Create mapping table from old sub id to new one.

    Args:
        l1_profile: l1 profile table with old and new sub id
    """
    logging.getLogger(__name__).info(
        "Creating `subscription_identifier` replacement dictionary"
    )
    window_latest = Window.partitionBy("old_subscription_identifier").orderBy(
        func.col("event_partition_date").desc()
    )
    return (
        l1_profile.filter("charge_type == 'Pre-paid'")
        .withColumn("date_lp", func.row_number().over(window_latest))
        .filter("date_lp == 1")
        .select(["old_subscription_identifier", "subscription_identifier"])
    )
