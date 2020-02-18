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

from datetime import datetime
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window


class TestUnitUsage:

    def test_l3_monthly_customer_profile_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        # Format is <source_column_name>: [<value>, <Type>]
        dummy_customer_profile_data = {
            "access_method_num": ['1', StringType()],
            "register_date": [datetime.strptime("2019-01-01", "%Y-%m-%d"), TimestampType()],
            "birth_date": [datetime.strptime("1990-01-01", "%Y-%m-%d"), TimestampType()],
            "contract_start_month": [datetime.strptime("2019-02-01", "%Y-%m-%d"), TimestampType()],
            "partition_month": [201901, IntegerType()],

            # some other field not yet tested.
            # If it is just a simple column pull/extraction, no need to test it
            "crm_sub_id": [None, StringType()],
            "charge_type": [None, StringType()],
            "mobile_status": [None, StringType()],
            "gender": [None, StringType()],
            "contract_end_month": [None, TimestampType()],
            "mobile_segment": [None, StringType()],
            "network_type": [None, StringType()],
            "trade_cmd_channel_type": [None, StringType()],
            "first_act_region": [None, StringType()],
            "main_promo_name": [None, StringType()],
            "main_promo_id": [None, StringType()],
            "moc_cust_type": [None, StringType()],
            "card_no": [None, StringType()],
            "norms_net_revenue": [None, StringType()],
        }

        # create RDD from values.
        # Make sure to sort the dictionary because order matter during
        # dataframe creation
        rdd_value = []
        for col_name, value in sorted(dummy_customer_profile_data.items()):
            rdd_value.append(value[0])

        rdd = spark.sparkContext.parallelize([rdd_value])

        # create schema
        df_schema = []
        for col_name, value in sorted(dummy_customer_profile_data.items()):
            df_schema.append(StructField(col_name, value[1], True))

        # create dataframe
        df = spark.createDataFrame(rdd, schema=StructType(df_schema))

        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:int_l3_customer_profile_basic_features'))

        # test age calculation
        assert daily_data.select("age").collect()[0][0] == 29

        # AIS DE TO ADD OTHER FEATURES TESTING HERE. Check L1 and L4 features as well
