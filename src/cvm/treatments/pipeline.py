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
from kedro.pipeline import Pipeline, node

from cvm.treatments.nodes import (
    deploy_treatments,
    prepare_microsegments,
    produce_treatments,
)


def generate_treatments(sample_type: str) -> Pipeline:
    """ Creates pipeline defining treatment from propensity scores.

    Args:
        sample_type: "scoring" if list created for scoring, "training" if list created
            for training.

     Returns:
         Kedro pipeline.
     """

    return Pipeline(
        [
            node(
                prepare_microsegments,
                [
                    "features_macrosegments_{}".format(sample_type),
                    "l3_customer_profile_include_1mo_non_active",
                    "parameters",
                ],
                "microsegments_{}".format(sample_type),
                name="create_microsegments_{}".format(sample_type),
            ),
            node(
                produce_treatments,
                [
                    "propensity_scores_{}".format(sample_type),
                    "microsegments_{}".format(sample_type),
                    "microsegments_treatments",
                    "treatments_chosen_history",
                    "parameters",
                    "features_macrosegments_scoring",
                ],
                ["treatments_chosen", "treatments_chosen_history2"],
                name="produce_treatments",
            ),
            node(
                deploy_treatments,
                ["treatments_chosen", "parameters"],
                None,
                name="deploy_treatments",
            ),
        ]
    )
