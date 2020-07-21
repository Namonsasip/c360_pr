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
from cvm.customer_id.nodes import create_customer_edges, create_customer_ids
from kedro.pipeline import Pipeline, node


def create_customer_id() -> Pipeline:
    """ Creates customers ids. """
    return Pipeline(
        [
            node(
                func=create_customer_edges,
                inputs=[
                    "geo_home_work_location_id",
                    "l3_customer_profile_union_monthly_feature",
                    "l1_devices_summary_customer_handset_daily",
                    "parameters",
                ],
                outputs="customer_id_edges",
                name="create_customer_edges",
            ),
            node(
                func=create_customer_ids,
                inputs=[
                    "l3_customer_profile_union_monthly_feature",
                    "customer_id_edges",
                    "parameters",
                ],
                outputs="customer_ids",
                name="create_customer_ids",
            ),
        ]
    )
