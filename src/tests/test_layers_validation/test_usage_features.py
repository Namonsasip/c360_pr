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


class TestUsage:
    granular_daily_cols = ['msisdn', 'event_partition_date']
    granular_weekly_cols = ['msisdn', 'start_of_week']

    def test_l1_call_local(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l1_usage_call_relation_sum_daily")
        assert df.count() == df.select(self.granular_daily_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("event_partition_date is null").count() == 0

    def test_l1_call_ir(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l1_usage_call_relation_sum_ir_daily")
        assert df.count() == df.select(self.granular_daily_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("event_partition_date is null").count() == 0

    def test_l1_data_prepaid_prepaid(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l1_usage_data_prepaid_postpaid_daily")
        assert df.count() == df.select(self.granular_daily_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("event_partition_date is null").count() == 0

    def test_l2_call_local(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l2_usage_call_relation_sum_weekly")
        assert df.count() == df.select(self.granular_weekly_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("start_of_week is null").count() == 0

    def test_l2_call_ir(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l2_usage_call_relation_sum_ir_weekly")
        assert df.count() == df.select(self.granular_weekly_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("start_of_week is null").count() == 0

    def test_l2_data(self, project_context):
        project_context = project_context['ProjectContext']
        df = project_context.catalog.load("l2_usage_call_relation_sum_ir_weekly")
        assert df.count() == df.select(self.granular_weekly_cols).distinct().count()
        assert df.where("msisdn is null").count() == 0
        assert df.where("start_of_week is null").count() == 0
