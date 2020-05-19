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

from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l3.to_l3_nodes import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window, l4_rolling_ranked_window
from customer360.utilities.re_usable_functions import *
import pandas as pd
import random
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime
from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l1.to_l1_nodes import \
    massive_processing_with_customer
from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l2.to_l2_nodes import build_revenue_l2_layer

global l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly
l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = [
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "1-TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202001"],
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "1-TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202001"],
    [datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'), "1-TEST", "Bill_tag", "111", "1", "10", "100", "0", "0", "0", "0", "0",
     "1", "10", "100", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "202002"]

]

global l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly
l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = [
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "test", "3G971", "1", "1", "1",
     datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
     "1", "1", "1", "1", "1", "1", "1", "1", "202001"],
    [datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "test", "3G971", "1", "1", "1",
     datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'), "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
     "1", "1", "1", "1", "1", "1", "1", "1", "202001"],
     [datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'), "test", "3G971", "1", "1", "1",
      datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'), "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1",
      "1", "1", "1", "1", "1", "1", "1", "1", "202002"]
]

global l0_revenue_prepaid_pru_f_usage_multi_daily
l0_revenue_prepaid_pru_f_usage_multi_daily = [
[datetime.datetime.strptime('2020-01-28', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-31', '%Y-%m-%d'),"test",
 datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"Y","N","N",48.0,"XU","3G882","N","null",
 10.0,15.0,9.0,5.0,4.0,3.0,2.0,2.0,2.14,1.0,2.0,2.0,2.14,1.0,1.0,1.0,1.0,2.14,2.14,1.0,1.0,2.1,0.0,1.0,1.0,2.0,1.0,1.0,
 "3GPre-paid",0.0,"CBS",0.0,0.0,"UDN","20200101",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d')]
]

global customer
customer = [
["1-TEST","test",datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G339","Promotion_ID","25","F","3577","118","SA","Classic","Classic","3G","aWHlgJKyzdZhML+1MsR8zkLsXHK5SUBzt9OMWpVdheZEg9ejPmUEoOqHJqQIIHo0",datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"Pre-paid","null","N","NNNN","N","N",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"20200101"],
["1-TEST","test",datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G339","Promotion_ID","25","F","3577","118","SA","Classic","Classic","3G","aWHlgJKyzdZhML+1MsR8zkLsXHK5SUBzt9OMWpVdheZEg9ejPmUEoOqHJqQIIHo0",datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),"Pre-paid","null","N","NNNN","N","N",datetime.datetime.strptime('2020-02-03', '%Y-%m-%d'),datetime.datetime.strptime('2020-02-01', '%Y-%m-%d'),"20200201"]
]

global l1_revenue_prepaid_pru_f_usage_multi_weekly
l1_revenue_prepaid_pru_f_usage_multi_weekly = [
["test",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),"1-TEST","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-27', '%Y-%m-%d')],
["test",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),"1-TEST","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1",datetime.datetime.strptime('2020-01-27', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-01', '%Y-%m-%d'),datetime.datetime.strptime('2020-01-28', '%Y-%m-%d')]
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
StructField("start_of_week",StringType(), True)

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

def set_value_l1(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    rdd1 = spark.sparkContext.parallelize(l1_revenue_prepaid_pru_f_usage_multi_weekly)
    global df_l1_revenue_prepaid_pru_f_usage_multi_weekly
    df_l1_revenue_prepaid_pru_f_usage_multi_weekly = spark.createDataFrame(rdd1,schema=StructType(
                                                                                           [
StructField("access_method_num",StringType(), True),
StructField("start_of_week",DateType(), True),
StructField("subscription_identifier",StringType(), True),
StructField("rev_arpu_total_net_rev",StringType(), True),
StructField("rev_arpu_net_tariff_rev_reward",StringType(), True),
StructField("rev_arpu_net_tariff_rev_exc_reward",StringType(), True),
StructField("rev_arpu_share_of_exc_reward_over_total_rev",StringType(), True),
StructField("rev_arpu_share_of_revenue_reward_over_total_rev",StringType(), True),
StructField("rev_arpu_diff_in_exc_reward_rev_reward",StringType(), True),
StructField("rev_arpu_data_number_of_on_top_pkg",StringType(), True),
StructField("rev_arpu_data_rev",StringType(), True),
StructField("rev_arpu_data_rev_by_on_top_pkg",StringType(), True),
StructField("rev_arpu_data_rev_by_ppu",StringType(), True),
StructField("rev_arpu_data_rev_4g",StringType(), True),
StructField("rev_arpu_share_data_rev_4g",StringType(), True),
StructField("rev_arpu_data_rev_by_on_top_4g",StringType(), True),
StructField("rev_arpu_share_data_rev_by_on_top_pkg_4g",StringType(), True),
StructField("rev_arpu_data_rev_by_ppu_4g",StringType(), True),
StructField("rev_arpu_share_data_rev_by_ppu_4g",StringType(), True),
StructField("rev_arpu_data_rev_2g_3g",StringType(), True),
StructField("rev_arpu_share_data_rev_2g_3g",StringType(), True),
StructField("rev_arpu_data_rev_by_on_top_pkg_2g_3g",StringType(), True),
StructField("rev_arpu_share_data_rev_by_on_top_pkg_2g_3g",StringType(), True),
StructField("rev_arpu_data_rev_by_ppu_2g_3g",StringType(), True),
StructField("rev_arpu_share_data_rev_by_ppu_2g_3g",StringType(), True),
StructField("rev_arpu_data_rev_by_per_unit",StringType(), True),
StructField("rev_arpu_data_rev_per_unit_2g_3g",StringType(), True),
StructField("rev_arpu_data_rev_per_unit_4g",StringType(), True),
StructField("rev_arpu_diff_rev_by_on_top_pkg_ppu",StringType(), True),
StructField("rev_arpu_diff_rev_by_on_top_pkg_ppu_4g",StringType(), True),
StructField("rev_arpu_diff_rev_by_on_top_pkg_ppu_2g_3g",StringType(), True),
StructField("rev_arpu_diff_rev_2g_3g_vs_4g",StringType(), True),
StructField("rev_arpu_diff_rev_per_unit_2g_3g_vs_4g",StringType(), True),
StructField("rev_arpu_voice",StringType(), True),
StructField("rev_arpu_voice_intra_ppu",StringType(), True),
StructField("rev_arpu_share_voice_intra",StringType(), True),
StructField("rev_arpu_voice_non_intra_ppu",StringType(), True),
StructField("rev_arpu_share_voice_non_intra",StringType(), True),
StructField("rev_arpu_voice_per_call",StringType(), True),
StructField("rev_arpu_voice_intra_per_call",StringType(), True),
StructField("rev_arpu_voice_non_intra_per_call",StringType(), True),
StructField("rev_arpu_voice_per_minute",StringType(), True),
StructField("rev_arpu_voice_intra_per_minute",StringType(), True),
StructField("rev_arpu_voice_non_intra_per_minute",StringType(), True),
StructField("rev_arpu_diff_voice_intra_non_intra",StringType(), True),
StructField("rev_arpu_diff_voice_intra_non_intra_per_min",StringType(), True),
StructField("rev_arpu_days_0_rev",StringType(), True),
StructField("rev_arpu_days_data_0_rev",StringType(), True),
StructField("rev_arpu_days_data_ppu_0_rev",StringType(), True),
StructField("rev_arpu_days_4g_data_0_rev",StringType(), True),
StructField("rev_arpu_days_2g_3g_data_0_rev",StringType(), True),
StructField("rev_arpu_days_4g_data_on_top_pkg_0_rev",StringType(), True),
StructField("rev_arpu_days_2g_3g_data_on_top_pkg_0_rev",StringType(), True),
StructField("rev_arpu_days_4g_data_ppu_0_rev",StringType(), True),
StructField("rev_arpu_days_2g_3g_data_ppu_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_intra_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_non_intra_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_per_call_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_intra_per_call_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_non_intra_per_call_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_per_min_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_intra_per_min_0_rev",StringType(), True),
StructField("rev_arpu_days_voice_non_intra_per_min_0_rev",StringType(), True),
StructField("rev_arpu_last_date_on_top_pkg",DateType(), True),
StructField("start_of_month",DateType(), True),
StructField("event_partition_date",DateType(), True),


                                                                                           ]))

class TestUnitRevenue:

    def test_l1_revenue_prepaid_pru_f_usage_multi_daily(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_revenue_prepaid_pru_f_usage_multi_daily = massive_processing_with_customer(df_l0_revenue_prepaid_pru_f_usage_multi_daily,
            customer_pro,var_project_context.catalog.load('params:l1_revenue_prepaid_pru_f_usage_multi_daily'))

        temp = df_l0_revenue_prepaid_pru_f_usage_multi_daily.withColumn("total_vol_gprs_2g_3g", F.col("total_vol_gprs") - F.col("total_vol_gprs_4g"))

        joindata = temp.join(customer_pro,["access_method_num"],"left")

        test = node_from_config(joindata,var_project_context.catalog.load('params:l1_revenue_prepaid_pru_f_usage_multi_daily'))

        test.show()

        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_total_net_rev").collect()[0][0]),
                     2) == 2.14
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_net_tariff_rev_reward").collect()[0][0]),
            2) == 1
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_net_tariff_rev_exc_reward").collect()[0][0]),
                     2) == 2.14
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_share_of_exc_reward_over_total_rev").collect()[0][
                0]), 2) == 1
        assert round(float(test.where("access_method_num = 'test'").select(
            "rev_arpu_share_of_revenue_reward_over_total_rev").collect()[0][0]), 2) == 0.47
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_diff_in_exc_reward_rev_reward").collect()[0][0]),
                     2) == 1.14
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_number_of_on_top_pkg").collect()[0][0]),
            2) == 1
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev").collect()[0][0]),
                     2) == 10
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_on_top_pkg").collect()[0][0]),
                     2) == 15
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_ppu").collect()[0][0]),
                     2) == 9
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_4g").collect()[0][0]),
                     2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_4g").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_on_top_4g").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_4g").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_by_on_top_pkg_4g").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_ppu_4g").collect()[0][0]),
            2) == 0
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_2g_3g").collect()[0][0]),
                     2) == 10
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_2g_3g").collect()[0][0]),
            2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_on_top_pkg_2g_3g").collect()[0][0]),
            2) == 15
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_by_on_top_pkg_2g_3g").collect()[0][0]),
                     2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_ppu_2g_3g").collect()[0][0]),
            2) == 10
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_share_data_rev_by_ppu_2g_3g").collect()[0][0]),
                     2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_by_per_unit").collect()[0][0]),
            2) == 2
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_data_rev_per_unit_2g_3g").collect()[0][0]),
            2) == 2
        assert test.where("access_method_num = 'test'").select("rev_arpu_data_rev_per_unit_4g").collect()[0][0] == None
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_diff_rev_by_on_top_pkg_ppu").collect()[0][0]),
            2) == 6
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_diff_rev_by_on_top_pkg_ppu_4g").collect()[0][0]),
            2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_diff_rev_by_on_top_pkg_ppu_2g_3g").collect()[0][0]),
                     2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_diff_rev_2g_3g_vs_4g").collect()[0][0]),
            2) == 10
        assert test.where("access_method_num = 'test'").select("rev_arpu_diff_rev_per_unit_2g_3g_vs_4g").collect()[0][0] == None
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_voice").collect()[0][0]),
                     2) == 2.14
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_voice_intra_ppu").collect()[0][0]),
                     2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_voice_intra").collect()[0][0]),
            2) == 0.47
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_voice_non_intra_ppu").collect()[0][0]),
                     2) == 2.14
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_share_voice_non_intra").collect()[0][0]),
            2) == 1
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_voice_per_call").collect()[0][0]),
                     2) == 0.54
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_voice_intra_per_call").collect()[0][0]),
            2) == 0.25
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_voice_non_intra_per_call").collect()[0][0]),
            2) == 0.54
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_voice_per_minute").collect()[0][0]),
            2) == 0.71
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_voice_intra_per_minute").collect()[0][0]),
            2) == 0.33
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_voice_non_intra_per_minute").collect()[0][0]),
                     2) == 0.71
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_diff_voice_intra_non_intra").collect()[0][0]),
                     2) == -1.14
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_diff_voice_intra_non_intra_per_min").collect()[0][
                0]), 2) == -0.38
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_days_0_rev").collect()[0][0]),
                     2) == 0
        assert round(float(test.where("access_method_num = 'test'").select("rev_arpu_days_data_0_rev").collect()[0][0]),
                     2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_data_ppu_0_rev").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_4g_data_0_rev").collect()[0][0]),
            2) == 1
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_2g_3g_data_0_rev").collect()[0][0]),
            2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_4g_data_on_top_pkg_0_rev").collect()[0][0]),
            2) == 1
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_2g_3g_data_on_top_pkg_0_rev").collect()[0][0]),
                     2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_4g_data_ppu_0_rev").collect()[0][0]),
            2) == 1
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_2g_3g_data_ppu_0_rev").collect()[0][0]),
                     2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_voice_0_rev").collect()[0][0]), 2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_voice_intra_0_rev").collect()[0][0]),
            2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_non_intra_0_rev").collect()[0][0]),
                     2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_per_call_0_rev").collect()[0][0]),
                     2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_intra_per_call_0_rev").collect()[0][
                0]), 2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_non_intra_per_call_0_rev").collect()[
                0][0]), 2) == 0
        assert round(
            float(test.where("access_method_num = 'test'").select("rev_arpu_days_voice_per_min_0_rev").collect()[0][0]),
            2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_intra_per_min_0_rev").collect()[0][0]),
                     2) == 0
        assert round(float(
            test.where("access_method_num = 'test'").select("rev_arpu_days_voice_non_intra_per_min_0_rev").collect()[0][
                0]), 2) == 0
        assert test.where("access_method_num = 'test'").select("rev_arpu_last_date_on_top_pkg").collect()[0][0] == None

    def test_l2_revenue_prepaid_weekly(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value_l1(project_context)


        l2_revenue_prepaid_weekly = build_revenue_l2_layer(df_l1_revenue_prepaid_pru_f_usage_multi_weekly,var_project_context.catalog.load('params:l2_revenue_prepaid_weekly'))

        l2_revenue_prepaid_weekly.show()

        #SUM

        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_total_net_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_net_tariff_rev_reward_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_net_tariff_rev_exc_reward_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_on_top_pkg_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_ppu_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_4g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_number_of_on_top_pkg_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_on_top_4g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_ppu_4g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_2g_3g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_on_top_pkg_2g_3g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_ppu_2g_3g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_by_per_unit_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_per_unit_2g_3g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_data_rev_per_unit_4g_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_data_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_data_ppu_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_4g_data_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_2g_3g_data_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_4g_data_on_top_pkg_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_2g_3g_data_on_top_pkg_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_4g_data_ppu_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_2g_3g_data_ppu_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_intra_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_non_intra_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_per_call_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_intra_per_call_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_non_intra_per_call_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_per_min_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_intra_per_min_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_days_voice_non_intra_per_min_0_rev_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_intra_ppu_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_non_intra_ppu_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_per_call_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_intra_per_call_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_non_intra_per_call_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_per_minute_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_intra_per_minute_sum").collect()[0][0] == 2
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_voice_non_intra_per_minute_sum").collect()[0][0] == 2

        #max

        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_exc_reward_over_total_rev_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_revenue_reward_over_total_rev_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_4g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_4g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_4g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_2g_3g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_2g_3g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_2g_3g_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_intra_max").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_non_intra_max").collect()[0][0]) == 1

        #avg

        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_exc_reward_over_total_rev_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_revenue_reward_over_total_rev_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_2g_3g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_2g_3g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_2g_3g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_intra_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_non_intra_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_in_exc_reward_rev_reward_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_rev_by_on_top_pkg_ppu_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_rev_by_on_top_pkg_ppu_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_rev_by_on_top_pkg_ppu_2g_3g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_rev_2g_3g_vs_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_rev_per_unit_2g_3g_vs_4g_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_voice_intra_non_intra_avg").collect()[0][0] == 1
        assert \
        l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_diff_voice_intra_non_intra_per_min_avg").collect()[0][0] == 1

        #min

        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_exc_reward_over_total_rev_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_of_revenue_reward_over_total_rev_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_4g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_4g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_4g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_2g_3g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_on_top_pkg_2g_3g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_data_rev_by_ppu_2g_3g_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_intra_min").collect()[0][0]) == 1
        assert \
        float(l2_revenue_prepaid_weekly.where("subscription_identifier = '1-TEST' and start_of_week = '2020-01-27'").select(
            "rev_arpu_share_voice_non_intra_min").collect()[0][0]) == 1

    def test_l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = node_from_config(df_l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly,
                                                                                   var_project_context.catalog.load('params:l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'))

        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.show()

        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_revenue").collect()[0][0]) == 222
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_revenue").collect()[0][0]) == 200
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_vas_net_revenue").collect()[0][0]) == 20
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_revenue").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_revenue").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_revenue").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_ir_net_revenue").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_idd_net_revenue").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_vas_net_tariff_rev_mth").collect()[0][0]) == 20
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_tariff_rev_mth").collect()[0][0]) == 200
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_tariff_rev_mth").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_tariff_rev_mth").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_tariff_rev_mth").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_ir_net_tariff_rev_mth").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_idd_net_tariff_rev_mth").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_vas_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_ir_net_tariff_rev_ppu").collect()[0][0]) == 0
        assert float(
            l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_idd_net_tariff_rev_ppu").collect()[0][0]) == 0

    def test_l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = node_from_config(df_l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly,
                                                                                   var_project_context.catalog.load('params:l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'))

        l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.show()

        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_ir_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_idd_net_revenue").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_tariff_rev_mth").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_gprs_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_mms_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_others_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_ir_net_tariff_rev_ppu").collect()[0][0]) == 2
        assert float(
            l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.where("start_of_month = '2020-01-01'").select(
                "rev_arpu_total_idd_net_tariff_rev_ppu").collect()[0][0]) == 2

    def test_l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_int(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly = node_from_config(
            df_l0_revenue_prepaid_ru_f_sum_revenue_by_service_monthly,
            var_project_context.catalog.load('params:l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'))

        l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.show()
        l4_join = l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly.join(customer_pro,on=['access_method_num', 'start_of_month'])
        l4_join.show()

        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int = l4_rolling_window(l4_join, var_project_context.catalog.load('params:l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_int'))

        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.show()

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2

        ################################# MAX #######################################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2

        ##################################### sum ####################################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2

        #################### avg ##############################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 2

    def test_l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = node_from_config(
            df_l0_revenue_postpaid_ru_f_sum_revenue_by_service_monthly,
            var_project_context.catalog.load('params:l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'))

        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.show()

        l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly = l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.withColumnRenamed("sub_id", "subscription_identifier")

        l4_join = l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly.join(customer_pro,on=['subscription_identifier', 'start_of_month'])
        l4_join.show()

        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int = l4_rolling_window(l4_join, var_project_context.catalog.load('params:l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int'))

        l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.show()

        ##################### MIN ##########################################################################
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 222
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_vas_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0

        ###################### MAX ######################################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 222
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_vas_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0

        ################# SUM ##################################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 222
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_vas_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0

        ################# AVG ##############################################################################

        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 222
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 2
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_vas_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 20
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 200
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0
        assert l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int.where(
            "start_of_month = '2020-02-01' and subscription_identifier = '1-TEST'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 0

    def test_l4_revenue_prepaid_daily_features(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value_l1(project_context)

        l4_revenue_prepaid_daily_features = l4_rolling_window(df_l1_revenue_prepaid_pru_f_usage_multi_weekly,
                                                           var_project_context.catalog.load(
                                                               'params:l4_revenue_prepaid_daily_features'))

        l4_revenue_prepaid_daily_features.show()

        assert l4_revenue_prepaid_daily_features.where("event_partition_date = '2020-01-28'").select("avg_rev_arpu_total_net_rev_daily_last_seven_day").collect()[0][0] == 1
        assert l4_revenue_prepaid_daily_features.where("event_partition_date = '2020-01-28'").select("sum_rev_arpu_total_net_rev_daily_last_seven_day").collect()[0][0] == 1
        assert float(l4_revenue_prepaid_daily_features.where("event_partition_date = '2020-01-28'").select("max_rev_arpu_total_net_rev_daily_last_seven_day").collect()[0][0]) == 1
        assert float(l4_revenue_prepaid_daily_features.where("event_partition_date = '2020-01-28'").select("min_rev_arpu_total_net_rev_daily_last_seven_day").collect()[0][0]) == 1




