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
import pyspark.sql.functions as func
from pyspark.sql import DataFrame, Window


def prepare_geolocation(geolocs: DataFrame) -> DataFrame:
    """ Prepare geolocation data for customer id generation.

    Args:
        geolocs: data with users locations.

    """
    # pick rows and columns needed for geolocation
    date_to_pick = "2020-03-01"
    geoloc_features = [
        "home_weekday_location_id",
        "home_weekend_location_id",
        "work_location_id",
    ]
    geoloc_imsi = [
        "imsi",
    ]
    cols_to_pick = geoloc_features + geoloc_imsi

    imsi_partition = Window.partitionBy("imsi")
    is_duplicate_imsi = func.count("*").over(imsi_partition) > 1

    geoloc = (
        geolocs.filter("start_of_month == '{}'".format(date_to_pick))
        .withColumn("to_drop", is_duplicate_imsi)
        .filter("to_drop")
        .select(cols_to_pick)
    )
    return geoloc


def prepare_profile(profile: DataFrame) -> DataFrame:
    """ Prepare profile data for customer id generation.

    Args:
        profile: users with zipcode, national id card, gender, age

    """
    # pick rows and columns needed for PROFILE
    date_to_pick = "2020-02-01"
    profile_features = [
        "zipcode",
        "prefer_language",
        "age",
        "gender",
        "strong_national_id_card",
        "weak_national_id_card",
    ]
    profile_sub_ids = ["old_subscription_identifier", "subscription_identifier", "imsi"]
    cols_to_pick = profile_sub_ids + profile_features + ["national_id_card"]

    # create weak and strong national_id_cards
    national_id_card_partition = Window.partitionBy("national_id_card")
    national_id_card_count = func.count("*").over(national_id_card_partition)
    is_weak_national_id_card = (national_id_card_count > 4) & (
        national_id_card_count < 100
    )
    is_strong_national_id_card = national_id_card_count <= 4

    profile = (
        profile.filter("start_of_month == '{}'".format(date_to_pick))
        .withColumn(
            "strong_national_id_card",
            func.when(
                is_strong_national_id_card, func.col("national_id_card")
            ).otherwise(func.lit(None)),
        )
        .withColumn(
            "weak_national_id_card",
            func.when(is_weak_national_id_card, func.col("national_id_card")).otherwise(
                func.lit(None)
            ),
        )
        .select(cols_to_pick)
    )
    return profile


def prepare_device(device: DataFrame) -> DataFrame:
    """ Prepare device data for customer id generation.

    Args:
        device: table describing details of device used by customer.
    """
    # pick rows and columns needed for device table
    date_to_pick = "2020-02-01"
    device_features = [
        "handset_imei",
        "dual_sim",
    ]
    device_sub_ids = [
        "subscription_identifier",
    ]
    cols_to_pick = device_sub_ids + device_features
    device = device.filter("event_partition_date == '{}'".format(date_to_pick)).select(
        cols_to_pick
    )
    return device
