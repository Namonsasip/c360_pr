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

"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test``.
"""


class TestRevenue:
    l3_postpiad_granular_monthly_cols = ['sub_id', 'start_of_month']
    l3_prepiad_granular_monthly_cols = ['access_method_num', 'register_date', 'start_of_month']

    l4_postpaid_granular_monthly_cols = ['sub_id', 'start_of_month']
    l4_prepaid_granular_monthly_cols = ['access_method_num', 'register_date', 'start_of_month']

    postpaid_data = 'l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'
    prepaid_data = 'l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'

    def test_l3_revenue(self, project_context):
        project_context = project_context['ProjectContext']
        postpaid = project_context.catalog.load(self.postpaid_data)
        prepaid = project_context.catalog.load(self.prepaid_data)

        assert postpaid.count() == postpaid.select(self.l3_postpiad_granular_monthly_cols).distinct().count()
        assert postpaid.where("sub_id is null").count() == 0

        assert prepaid.count() == prepaid.select(self.l3_prepiad_granular_monthly_cols).distinct().count()
        assert prepaid.where("access_method_num is null").count() == 0

    def test_l4_revenue(self, project_context):
        project_context = project_context['ProjectContext']
        postpaid = project_context.catalog.load(self.postpaid_data)
        prepaid = project_context.catalog.load(self.prepaid_data)

        assert postpaid.count() == postpaid.select(self.l4_postpaid_granular_monthly_cols).distinct().count()
        assert postpaid.where("sub_id is null").count() == 0

        assert prepaid.count() == prepaid.select(self.l4_prepaid_granular_monthly_cols).distinct().count()
        assert prepaid.where("access_method_num is null").count() == 0
