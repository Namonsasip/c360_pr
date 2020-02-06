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

from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F


class TestUnitUsage:

    def test_vas_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        random_list = [random.randint(1, 5) for iTemp in range(0, 121)]

        df = spark.createDataFrame(zip(random_list, my_dates), schema=['number_of_call', 'day_id']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy'))

        # Testing Daily Features here
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_usage_ru_a_vas_postpaid_prepaid_daily'))

        # check event date = 2020-01-01 date the number of calls should be 2
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("usg_vas_total_number_of_call").collect()[0][
                0] == 2

        # check weekly features
        weekly_data = expansion(daily_data,
                                var_project_context.catalog.load('params:l2_usage_ru_a_vas_postpaid_prepaid_weekly'))

        # check start_of_week = 2020-03-30 and features
        assert \
            weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_avg").collect()[0][
                0] > 3
        assert \
            weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_max").collect()[0][
                0] == 5
        assert \
            weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_min").collect()[0][
                0] == 2
        assert \
            weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_sum").collect()[0][
                0] == 10

        # check final features
        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_usage_ru_a_vas_postpaid_prepaid_features')).orderBy(F.col("start_of_week").desc())

        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'min_usg_vas_total_number_of_call_min_weekly_last_week').collect()[0][0] == 1

        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'min_usg_vas_total_number_of_call_min_weekly_last_two_week').collect()[0][0] == 1

        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'min_usg_vas_total_number_of_call_min_weekly_last_four_week').collect()[0][0] == 1
        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'min_usg_vas_total_number_of_call_min_weekly_last_twelve_week').collect()[0][0] == 1
        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'max_usg_vas_total_number_of_call_max_weekly_last_week').collect()[0][0] == 5
        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'max_usg_vas_total_number_of_call_max_weekly_last_two_week').collect()[0][0] == 5
        assert \
            final_features.where("start_of_week = '2020-03-30'").select(
                'max_usg_vas_total_number_of_call_max_weekly_last_four_week').collect()[0][0] == 5

        # AIS DE TO ADD FEATURE HERE


