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

import pyspark.sql.functions as func
from graphframes import GraphFrame
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


def make_undirected(edges: DataFrame) -> DataFrame:
    """ Make sure the edge table represents undirected graph.

    Args:
        edges: table with `src` and `dst` columns representing graphs columns.
    """
    logging.info("Making the graph edges undirected")
    return (
        edges.selectExpr("src as x", "dst as y", "weight")
        .withColumn(
            "src",
            func.when(func.col("x") <= func.col("y"), func.col("x")).otherwise(
                func.col("y")
            ),
        )
        .withColumn(
            "dst",
            func.when(func.col("x") > func.col("y"), func.col("x")).otherwise(
                func.col("y")
            ),
        )
        .drop(*["x", "y"])
    )


def add_weighted_edges(edges1: DataFrame, edges2: DataFrame) -> DataFrame:
    """ Combines two edges representing tables. Weights are added.

    Args:
        edges1: table with `src` and `dst` columns representing graphs columns.
        edges2: table with `src` and `dst` columns representing graphs columns.

    Returns:

    """
    logging.info("Summing edges")
    return (
        edges1.unionByName(edges2)
        .groupby(["src", "dst"])
        .agg(func.sum("weight").alias("weight"))
    )


def clean_too_many_appearances(
    df: DataFrame, feature: str, too_many: int = 1000
) -> DataFrame:
    """ Replace values with `too_many` or more values of `feature` with null.

    Args:
        df: table to modify.
        feature: name of column to modify.
        too_many: threshold above which values are turned to null.

    Returns:

    """
    feature_partition = Window.partitionBy(feature)
    msisdns_count = func.count("*").over(feature_partition)
    to_clean = msisdns_count >= too_many
    nullify_too_many = func.when(to_clean, func.lit(None)).otherwise(func.col(feature))
    return df.withColumn(feature, nullify_too_many)


def create_edges_for_feature(
    df: DataFrame, feature_name: str, weight: float
) -> DataFrame:
    """ Create table representing edges from table with features.

    Args:
        df: table with features.
        feature_name: feature from `df` to create edges for.
        weight: strength to be assigned to edge.
    """
    df = clean_too_many_appearances(df, feature_name)
    features = df.filter("{} is not null".format(feature_name)).select(
        ["subscription_identifier", feature_name]
    )
    features_left = features.withColumnRenamed(
        "subscription_identifier", "subscription_identifier_left"
    )
    features_right = features.withColumnRenamed(
        "subscription_identifier", "subscription_identifier_right"
    )
    features_edges = (
        features_left.join(features_right, on=feature_name)
        .selectExpr(
            "subscription_identifier_left as src",
            "subscription_identifier_right as dst",
        )
        .withColumn("weight", func.lit(weight))
        .select(["src", "dst", "weight"])
        .filter("src != dst")
        .distinct()
    )
    return make_undirected(features_edges)


def generate_customers(
    vertices: DataFrame, edg: DataFrame, new_col_name: str
) -> DataFrame:
    """ Assign customer id by joining the connected components.

    Args:
        vertices: table with vertices ids.
        edg: table with edges to find connected components for.
        new_col_name: output name for component id.
    """
    return (
        GraphFrame(vertices, edg)
        .connectedComponents()
        .withColumn(new_col_name, func.col("component"))
    )
