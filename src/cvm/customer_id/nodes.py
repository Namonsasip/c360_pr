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
import functools
import logging
from typing import Any, Dict

import pyspark.sql.functions as func
from cvm.src.features.customer_id_features import (
    add_weighted_edges,
    create_edges_for_feature,
    generate_customers,
    prepare_device,
    prepare_geolocation,
    prepare_profile,
)
from pyspark.context import SparkContext
from pyspark.sql import DataFrame


def create_customer_edges(
    geolocs: DataFrame,
    profile: DataFrame,
    device: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """ Prepare table with weighted edges of connection between subscriptions.

    Args:
        geolocs: table with users locations.
        profile: table with users profiles.
        device: table with information about users devices.
        parameters: parameters defined in parameters.yml

    """
    geolocs = prepare_geolocation(geolocs)
    profile = prepare_profile(profile)
    device = prepare_device(device)

    # join tables
    df = profile.join(device, on="subscription_identifier", how="left")
    df = df.join(geolocs, on="imsi", how="left")
    # prepare vertices for graph computation
    weights = parameters["customer_id_weights"]

    # create edges
    feature_edges = []
    for feature_name in weights:
        logging.info("Creating edges for {}".format(feature_name))
        weight = weights[feature_name]
        feature_edges.append(create_edges_for_feature(df, feature_name, weight))
    edges = functools.reduce(add_weighted_edges, feature_edges)
    return edges


def create_customer_ids(
    profile: DataFrame, edges: DataFrame, parameters: Dict[str, Any],
) -> DataFrame:
    """ Creates customer ids by joining connected components defined by edges.

    Args:
        profile: table with users profiles.
        parameters: parameters defined in parameters.yml.
        edges: connections between subscriptions.

    """
    SparkContext.getOrCreate().setCheckpointDir(parameters["checkpoint_path"])
    vertices = (
        prepare_profile(profile)
        .withColumn("id", func.col("subscription_identifier"))
        .select("id")
        .distinct()
    )
    thresholds = parameters["customer_id_thresholds"]
    customers = []
    for col_name in thresholds:
        min_weight = thresholds[col_name]
        edg = edges.filter("weight >= {}".format(min_weight))
        customers.append(
            generate_customers(vertices, edg, col_name).select(
                "subscription_identifier", col_name
            )
        )
    customer_ids = functools.reduce(
        lambda df1, df2: df1.join(df2, on="subscription_identifier"), customers
    )
    return customer_ids
