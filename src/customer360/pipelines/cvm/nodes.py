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
    users = profile.filter("month_id >= '{}'".format(min_date))
    users = users.filter("charge_type == 'Pre-paid' AND mobile_status == 'SA'")
    users = users.filter("service_month >= 4")
    users = users.filter("norms_net_revenue > 0")

    main_packs = main_packs.filter("promotion_group_tariff not in ('SIM 2 Fly',\
     'SIM NET MARATHON', 'Net SIM', 'Traveller SIM', 'Foreigner SIM')")
    main_packs = main_packs.select('package_id').\
        withColumnRenamed('package_id', 'main_promo_id')
    users = users.join(main_packs, ['main_promo_id'], 'inner')
    columns_to_pick = ['month_id', 'register_date', 'customer_id']
    users = users.select(columns_to_pick)

    return users
