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

from customer360.pipelines.data_engineering.nodes.revenue_nodes import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window, l4_rolling_ranked_window
from customer360.utilities.re_usable_functions import *
import pandas as pd
import random
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime
from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l1.to_l1_nodes import \
    massive_processing_with_customer

global l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly
l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = [
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202001"],
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202001"],
    [datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'), "TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202001"]

]

global l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly
l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = [
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "test", "3G971", "0", "null", "null",
     datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "10", "0", "0", "1", "0", "0", "0", "100", "2", "267.09", "0", "0",
     "0", "0", "0", "10", "0", "0", "1", "100", "202001"],
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "TEST", "3G971", "0", "null", "null",
     datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "10", "0", "0", "1", "0", "0", "0", "100", "2", "267.09", "0", "0",
     "0", "0", "0", "10", "0", "0", "1", "100", "test"],
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "test", "3G971", "0", "null", "null",
     datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "10", "0", "0", "1", "0", "0", "0", "100", "2", "267.09", "0", "0",
     "0", "0", "0", "10", "0", "0", "1", "100", "202001"]
]

global l0_revenue_prepaid_pru_f_usage_multi_daily
l0_revenue_prepaid_pru_f_usage_multi_daily = [
[datetime.datetime.strptime('2020-01-28', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-31', '%Y-%m-%d'),"test",
 datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"Y","N","N",48.0,"XU","3G882","N","null",
 10.0,15.0,9.0,5.0,4.0,3.0,2.0,2.0,2.14,1.0,2.0,2.0,2.14,1.0,1.0,1.0,1.0,2.14,2.14,1.0,1.0,2.1,0.0,1.0,1.0,2.0,1.0,1.0,
 "3GPre-paid",0.0,"CBS",0.0,0.0,"UDN","20200101"]
]

global customer
customer = [
["1-TEST","test",datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G339","Promotion_ID","25","F","3577","118","SA","Classic","Classic","3G","aWHlgJKyzdZhML+1MsR8zkLsXHK5SUBzt9OMWpVdheZEg9ejPmUEoOqHJqQIIHo0",datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"Pre-paid","null","N","NNNN","N","N",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"20200101"]
]


def set_value(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    rdd1 = spark.sparkContext.parallelize(l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly)
    global df_l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly
    df_l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = spark.createDataFrame(rdd1,
                                                                                       schema=StructType(
                                                                                           [
                                                                                               StructField("mgt_mth_id",
                                                                                                           DateType(),
                                                                                                           True),
                                                                                               StructField("sub_id",
                                                                                                           StringType(),
                                                                                                           True),
                                                                                               StructField(
                                                                                                   "bill_stmnt_id",
                                                                                                   StringType(),
                                                                                                   True),
                                                                                               StructField(
                                                                                                   "total_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_voice_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_vas_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_gprs_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_sms_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_mms_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_others_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_ir_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_idd_net_revenue",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_voice_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_vas_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_gprs_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_sms_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_mms_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_others_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_ir_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_idd_net_tariff_rev_mth",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_voice_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_vas_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_gprs_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_sms_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_mms_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_others_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_ir_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "total_idd_net_tariff_rev_ppu",
                                                                                                   StringType(), True),
                                                                                               StructField(
                                                                                                   "partition_month",
                                                                                                   StringType(),
                                                                                                   True)
                                                                                           ]))

    rdd1 = spark.sparkContext.parallelize(l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly)
    global df_l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly
    df_l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = spark.createDataFrame(rdd1,
                                                                                      schema=StructType(
                                                                                          [StructField("month_id",
                                                                                                       DateType(),
                                                                                                       True),
                                                                                           StructField(
                                                                                               "access_method_num",
                                                                                               StringType(), True),
                                                                                           StructField("package_id",
                                                                                                       StringType(),
                                                                                                       True),
                                                                                           StructField(
                                                                                               "total_amount_mth",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "source_usage:string",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "allocate_group:string",
                                                                                               StringType(), True),
                                                                                           StructField("register_date",
                                                                                                       DateType(),
                                                                                                       True),
                                                                                           StructField(
                                                                                               "total_voice_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_sms_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_mms_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_gprs_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_wifi_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_ir_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_idd_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_others_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_gross_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_net_revenue_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_voice_net_tariff_rev_mth_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_sms_net_tariff_rev_mth_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_mms_net_tariff_rev_mth_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_gprs_net_tariff_rev_mth_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_others_net_tariff_rev_mth_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_voice_net_tariff_rev_ppu_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_sms_net_tariff_rev_ppu_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_mms_net_tariff_rev_ppu_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_gprs_net_tariff_rev_ppu_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "total_others_net_tariff_rev_ppu_af",
                                                                                               StringType(), True),
                                                                                           StructField(
                                                                                               "partition_month:integer",
                                                                                               StringType(), True)
                                                                                           ]))

    rdd1 = spark.sparkContext.parallelize(l0_revenue_prepaid_pru_f_usage_multi_daily)
    global df_l0_revenue_prepaid_pru_f_usage_multi_daily
    df_l0_revenue_prepaid_pru_f_usage_multi_daily = spark.createDataFrame(rdd1,
                                                                          schema=StructType(
                                                                              [StructField("day_id",DateType(), True),
StructField("month_id",DateType(), True),
StructField("access_method_num",StringType(), True),
StructField("register_date",DateType(), True),
StructField("arpu0_yn",StringType(), True),
StructField("usage_yn",StringType(), True),
StructField("data_user_yn",StringType(), True),
StructField("service_months",DoubleType(), True),
StructField("first_activate_region",StringType(), True),
StructField("package_id",StringType(), True),
StructField("has_ontop_gprs_yn",StringType(), True),
StructField("gprs_ontop_package_name",StringType(), True),
StructField("total_net_tariff_gprs_revenue",DoubleType(), True),
StructField("total_net_tariff_gprs_package",DoubleType(), True),
StructField("total_net_tariff_gprs_exc_package",DoubleType(), True),
StructField("total_vol_gprs",DoubleType(), True),
StructField("total_call",DoubleType(), True),
StructField("total_minute",DoubleType(), True),
StructField("total_call_exc_free_no",DoubleType(), True),
StructField("total_minute_exc_free_no",DoubleType(), True),
StructField("total_net_tariff_revenue",DoubleType(), True),
StructField("total_net_tariff_revenue_reward",DoubleType(), True),
StructField("total_call_basic_exc_free_pack",DoubleType(), True),
StructField("total_minute_basic_exc_free_pack",DoubleType(), True),
StructField("total_net_tariff_revenue_basic",DoubleType(), True),
StructField("total_call_basic_free_pack",DoubleType(), True),
StructField("total_min_basic_free_pack",DoubleType(), True),
StructField("total_call_monthly",DoubleType(), True),
StructField("total_net_tariff_revenue_monthly",DoubleType(), True),
StructField("total_net_tariff_revenue_exc_reward",DoubleType(), True),
StructField("total_net_tariff_basic_non_intra",DoubleType(), True),
StructField("total_net_tariff_basic_intra",DoubleType(), True),
StructField("total_minute_basic_exc_free_pack_intra",DoubleType(), True),
StructField("total_minute_basic_exc_free_pack_non_intra",DoubleType(), True),
StructField("total_minute_basic_free_pack_intra",DoubleType(), True),
StructField("total_minute_basic_free_pack_non_intra",DoubleType(), True),
StructField("total_call_basic_exc_free_pack_intra",DoubleType(), True),
StructField("total_call_basic_exc_free_pack_non_intra",DoubleType(), True),
StructField("total_call_basic_free_pack_intra",DoubleType(), True),
StructField("total_call_basic_free_pack_non_intra",DoubleType(), True),
StructField("network_type",StringType(), True),
StructField("total_vol_gprs_4g",DoubleType(), True),
StructField("billing_system",StringType(), True),
StructField("total_vol_gprs_package",DoubleType(), True),
StructField("total_vol_gprs_exc_pack",DoubleType(), True),
StructField("first_activate_province",StringType(), True),
StructField("partition_date",StringType(), True),

                                                                               ]))

    rdd1 = spark.sparkContext.parallelize(customer)
    global customer_pro
    customer_pro = spark.createDataFrame(rdd1, schema=StructType([StructField("subscription_identifier",StringType(), True),
StructField("access_method_num",StringType(), True),
StructField("register_date",DateType(), True),
StructField("zipcode",StringType(), True),
StructField("prefer_language",StringType(), True),
StructField("company_size",StringType(), True),
StructField("corporate_flag",StringType(), True),
StructField("prefer_language_eng",StringType(), True),
StructField("prefer_language_thai",StringType(), True),
StructField("prefer_language_other",StringType(), True),
StructField("current_package_id",StringType(), True),
StructField("current_package_name",StringType(), True),
StructField("age",StringType(), True),
StructField("gender",StringType(), True),
StructField("subscriber_tenure_day",StringType(), True),
StructField("subscriber_tenure_month",StringType(), True),
StructField("subscription_status",StringType(), True),
StructField("customer_segment",StringType(), True),
StructField("serenade_status",StringType(), True),
StructField("network_type",StringType(), True),
StructField("national_id_card",StringType(), True),
StructField("partition_date",DateType(), True),
StructField("charge_type",StringType(), True),
StructField("billing_account_no",StringType(), True),
StructField("suppress_sms",StringType(), True),
StructField("supp_cntn_all",StringType(), True),
StructField("vip_flag",StringType(), True),
StructField("royal_family_flag",StringType(), True),
StructField("start_of_week",DateType(), True),
StructField("start_of_month",DateType(), True),
StructField("event_partition_date",StringType(), True),

    ]))


class TestUnitRevenue:

    def test_l1_revenue_prepaid_pru_f_usage_multi_daily(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_revenue_prepaid_pru_f_usage_multi_daily = massive_processing_with_customer(df_l0_revenue_prepaid_pru_f_usage_multi_daily,
            customer_pro,var_project_context.catalog.load('params:l1_revenue_prepaid_pru_f_usage_multi_daily'))

        joindata = df_l0_revenue_prepaid_pru_f_usage_multi_daily.join(customer_pro,["access_method_num"],"left")
        temp = df_l0_revenue_prepaid_pru_f_usage_multi_daily.withColumn("total_vol_gprs_2g_3g",F.col("total_vol_gprs") - F.col("total_vol_gprs_4g"))
        test = node_from_config(temp,var_project_context.catalog.load('params:l1_revenue_prepaid_pru_f_usage_multi_daily'))

        test.show()
        l1_revenue_prepaid_pru_f_usage_multi_daily.show()
        exit(2)
