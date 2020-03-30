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

from customer360.pipelines.data_engineering.nodes.stream_nodes import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.re_usable_functions import *
import pandas as pd
import random
from pyspark.sql.types import *
from pyspark.sql import functions as F
import datetime



global temp_l0_streaming_ida_mobile_domain_summary_daily
temp_l0_streaming_ida_mobile_domain_summary_daily = [
[datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-07','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-14','%Y-%m-%d'),'tset','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-21','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101'],
[datetime.datetime.strptime('2020-01-28','%Y-%m-%d'),'test','mdh-pa.googleapis.com','1','1','1','2','20200101']
]

global temp_l0_streaming_ida_mobile_search_daily
temp_l0_streaming_ida_mobile_search_daily = [
[datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-07','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-14','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-21','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),'test','google','word_search','1','20200101'],
[datetime.datetime.strptime('2020-01-28','%Y-%m-%d'),'test','google','word_search','1','20200101']
]

global temp_l0_streaming_ru_a_onair_vimmi_usage_daily_old
temp_l0_streaming_ru_a_onair_vimmi_usage_daily_old = [
[datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-07','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-14','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-21','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
[datetime.datetime.strptime('2020-01-28','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"","NA","ethernet","STB","ipbs9210","AIS-STB","Stream","Channel","content_id","","title","","","","","","","","","product_name",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","category_channal","1","epg_title","","N","Y","2",datetime.datetime.strptime('2020-01-30','%Y-%m-%d'),"20200101",datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')]
]

global temp_l0_streaming_ru_a_onair_vimmi_usage_daily_new
temp_l0_streaming_ru_a_onair_vimmi_usage_daily_new = [
[datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-07','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-14','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-21','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),9,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101'],
[datetime.datetime.strptime('2020-01-28','%Y-%m-%d'),18,"user_test","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"1-TEST","FBB","wifi","STB","Device","AIS-STB","Stream","Channel","test_content_123","N","TV_Channel",None,None,None,None,None,None,None,None,None,datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"20200101","Digital TV","1","epg_title","Free","N","Y","1",None,'20200101']]

global temp_l0_streaming_soc_mobile_app_daily
temp_l0_streaming_soc_mobile_app_daily = [
[datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-07','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-14','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-21','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
[datetime.datetime.strptime('2020-01-28','%Y-%m-%d'),'test','Youtube','videoplayers_editors','1','1','2','2','20200101','MOBILE'],
]
global daily_customer_profile
daily_customer_profile =[
["1-TEST","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G537","NULL","null","null","3574","117","SA","Classic","Classic","3G","National_id_card",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"Pre-paid","null",datetime.datetime.strptime('2020-01-06','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
["1-TEST","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G537","NULL","null","null","3574","117","SA","Classic","Classic","3G","National_id_card",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"Pre-paid","null",datetime.datetime.strptime('2020-01-13','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
["1-TEST","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G537","NULL","null","null","3574","117","SA","Classic","Classic","3G","National_id_card",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"Pre-paid","null",datetime.datetime.strptime('2020-01-20','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')],
["1-TEST","test",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"null","THAI","null","N","N","Y","Y","3G537","NULL","null","null","3574","117","SA","Classic","Classic","3G","National_id_card",datetime.datetime.strptime('2020-01-01','%Y-%m-%d'),"Pre-paid","null",datetime.datetime.strptime('2020-01-27','%Y-%m-%d'),datetime.datetime.strptime('2020-01-01','%Y-%m-%d')]
]


def set_value(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    rdd1 = spark.sparkContext.parallelize(temp_l0_streaming_ida_mobile_domain_summary_daily)
    global df_temp_l0_streaming_ida_mobile_domain_summary_daily
    df_temp_l0_streaming_ida_mobile_domain_summary_daily = spark.createDataFrame(rdd1,
                                                                                 schema=StructType(
                                                                                     [StructField("usage_date",
                                                                                                  DateType(), True),
                                                                                      StructField("mobile_no",
                                                                                                  StringType(), True),
                                                                                      StructField("domain",
                                                                                                  StringType(), True),
                                                                                      StructField("trans", StringType(),
                                                                                                  True),
                                                                                      StructField("uplink_traffic",
                                                                                                  StringType(), True),
                                                                                      StructField("downlink_traffic",
                                                                                                  StringType(), True),
                                                                                      StructField("total_traffic",
                                                                                                  StringType(), True),
                                                                                      StructField("partition_date",
                                                                                                  StringType(), True)
                                                                                      ]))

    rdd1 = spark.sparkContext.parallelize(temp_l0_streaming_ida_mobile_search_daily)
    global df_temp_l0_streaming_ida_mobile_search_daily
    df_temp_l0_streaming_ida_mobile_search_daily = spark.createDataFrame(rdd1,
                                                                         schema=StructType(
                                                                             [StructField("usage_date", DateType(),
                                                                                          True),
                                                                              StructField("mobile_no", StringType(),
                                                                                          True),
                                                                              StructField("domain", StringType(), True),
                                                                              StructField("word", StringType(), True),
                                                                              StructField("count_search", StringType(),
                                                                                          True),
                                                                              StructField("partition_date",
                                                                                          StringType(), True)
                                                                              ]))

    rdd1 = spark.sparkContext.parallelize(temp_l0_streaming_ru_a_onair_vimmi_usage_daily_old)
    global df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily_old
    df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily_old = spark.createDataFrame(rdd1,
                                                                              schema=StructType(
                                                                                  [
StructField("day_id",DateType(), True),
StructField("hour_id",IntegerType(), True),
StructField("user_id",StringType(), True),
StructField("access_method_num",StringType(), True),
StructField("register_date",DateType(), True),
StructField("sff_sub_id",StringType(), True),
StructField("network_type",StringType(), True),
StructField("network_group",StringType(), True),
StructField("device_type",StringType(), True),
StructField("device_name",StringType(), True),
StructField("app_id",StringType(), True),
StructField("event_code",StringType(), True),
StructField("content_group",StringType(), True),
StructField("content_id",StringType(), True),
StructField("paid",StringType(), True),
StructField("title",StringType(), True),
StructField("season_title",StringType(), True),
StructField("series_title",StringType(), True),
StructField("genre",StringType(), True),
StructField("rated",StringType(), True),
StructField("provider_id",StringType(), True),
StructField("provider_name",StringType(), True),
StructField("purchase_id",StringType(), True),
StructField("product_id",StringType(), True),
StructField("product_name",StringType(), True),
StructField("start_time",DateType(), True),
StructField("file_date",StringType(), True),
StructField("ctp_category_channel",StringType(), True),
StructField("volume_mb",StringType(), True),
StructField("epg_title",StringType(), True),
StructField("ctp_type_product",StringType(), True),
StructField("ctp_hd_flag",StringType(), True),
StructField("ctp_sd_flag",StringType(), True),
StructField("duration",StringType(), True),
StructField("expiration_date",DateType(), True),
StructField("partition_date",StringType(), True),
StructField("start_of_week",DateType(), True),
StructField("start_of_month",DateType(), True),
StructField("event_partition_date",DateType(), True)
                                                                                   ]))

    rdd1 = spark.sparkContext.parallelize(temp_l0_streaming_ru_a_onair_vimmi_usage_daily_new)
    global df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily
    df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily = spark.createDataFrame(rdd1,
                                                                              schema=StructType(
                                                                                  [StructField("day_id",DateType(), True),
StructField("hour_id",IntegerType(), True),
StructField("user_id",StringType(), True),
StructField("access_method_num",StringType(), True),
StructField("register_date",DateType(), True),
StructField("sff_sub_id",StringType(), True),
StructField("network_type",StringType(), True),
StructField("network_group",StringType(), True),
StructField("device_type",StringType(), True),
StructField("device_name",StringType(), True),
StructField("app_id",StringType(), True),
StructField("event_code",StringType(), True),
StructField("content_group",StringType(), True),
StructField("content_id",StringType(), True),
StructField("paid",StringType(), True),
StructField("title",StringType(), True),
StructField("season_title",StringType(), True),
StructField("series_title",StringType(), True),
StructField("genre",StringType(), True),
StructField("rated",StringType(), True),
StructField("provider_id",StringType(), True),
StructField("provider_name",StringType(), True),
StructField("purchase_id",StringType(), True),
StructField("product_id",StringType(), True),
StructField("product_name",StringType(), True),
StructField("start_time",DateType(), True),
StructField("file_date",StringType(), True),
StructField("ctp_category_channel",StringType(), True),
StructField("volume_mb",StringType(), True),
StructField("epg_title",StringType(), True),
StructField("ctp_type_product",StringType(), True),
StructField("ctp_hd_flag",StringType(), True),
StructField("ctp_sd_flag",StringType(), True),
StructField("duration",StringType(), True),
StructField("expiration_date",DateType(), True),
StructField("partition_date",StringType(), True)
                                                                                  ]))

    rdd1 = spark.sparkContext.parallelize(temp_l0_streaming_soc_mobile_app_daily)
    global df_temp_l0_streaming_soc_mobile_app_daily
    df_temp_l0_streaming_soc_mobile_app_daily = spark.createDataFrame(rdd1,
                                                                      schema=StructType(
                                                                          [StructField("usage_date", DateType(), True),
                                                                           StructField("mobile_no", StringType(), True),
                                                                           StructField("application", StringType(),
                                                                                       True),
                                                                           StructField("application_group",
                                                                                       StringType(), True),
                                                                           StructField("duration", StringType(), True),
                                                                           StructField("count_trans", StringType(),
                                                                                       True),
                                                                           StructField("upload_kb", StringType(), True),
                                                                           StructField("download_kb", StringType(),
                                                                                       True),
                                                                           StructField("partition_date", StringType(),
                                                                                       True),
                                                                           StructField("partition_type", StringType(),
                                                                                       True)
                                                                           ]))

    rdd1 = spark.sparkContext.parallelize(daily_customer_profile)
    global customer_pro
    customer_pro = spark.createDataFrame(rdd1,schema=StructType([
        StructField("subscription_identifier", StringType(), True),
        StructField("access_method_num", StringType(), True),
        StructField("register_date", DateType(), True),
        StructField("zipcode", StringType(), True),
        StructField("prefer_language", StringType(), True),
        StructField("company_size", StringType(), True),
        StructField("corporate_flag", StringType(), True),
        StructField("prefer_language_eng", StringType(), True),
        StructField("prefer_language_thai", StringType(), True),
        StructField("prefer_language_other", StringType(), True),
        StructField("current_package_id", StringType(), True),
        StructField("current_package_name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("subscriber_tenure_day", StringType(), True),
        StructField("subscriber_tenure_month", StringType(), True),
        StructField("subscription_status", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("serenade_status", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("national_id_card", StringType(), True),
        StructField("partition_date", DateType(), True),
        StructField("charge_type", StringType(), True),
        StructField("billing_account_no", StringType(), True),
        StructField("start_of_week", DateType(), True),
        StructField("start_of_month", DateType(), True)
    ]))


class TestUnitStream:


    def test_int_l1_streaming_video_service_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_video_service_feature'))

        assert l1int_l1_streaming_video_service_feature.select("mobile_no").count() == 8

    def test_l1_streaming_fav_content_group_by_duration(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_content_type_features'))

        l1_streaming_fav_content_group_by_duration = node_from_config(l1int_l1_streaming_video_service_feature,
                                                                         var_project_context.catalog.load(
                                                                             'params:l1_streaming_fav_content_group_by_duration'))

        l1_streaming_fav_content_group_by_duration.show()

        assert l1_streaming_fav_content_group_by_duration.select("access_method_num").count() == 8

    def test_l1_streaming_fav_video_service_by_download_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_video_service_feature'))

        streaming_fav_video_service_by_download_feature = node_from_config(l1int_l1_streaming_video_service_feature,
                                                                         var_project_context.catalog.load(
                                                                             'params:l1_streaming_fav_video_service_by_download_feature'))

        streaming_fav_video_service_by_download_feature.show()

        assert streaming_fav_video_service_by_download_feature.select("mobile_no").count() == 8

    def test_l1_streaming_2nd_fav_video_service_by_download_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_video_service_feature'))

        l1_streaming_2nd_fav_video_service_by_download_feature = node_from_config(l1int_l1_streaming_video_service_feature,
                                                                         var_project_context.catalog.load(
                                                                             'params:l1_streaming_2nd_fav_video_service_by_download_feature'))

        assert l1_streaming_2nd_fav_video_service_by_download_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_fav_tv_channel_by_duration(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_tv_channel_features'))

        l1_streaming_fav_tv_channel_by_duration = node_from_config(l1int_l1_streaming_video_service_feature,
                                                                         var_project_context.catalog.load(
                                                                             'params:l1_streaming_fav_tv_channel_by_duration'))

        l1_streaming_fav_tv_channel_by_duration.show()


        assert l1_streaming_fav_tv_channel_by_duration.select("access_method_num").count() == 8

    def test_int_l1_streaming_music_service_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_music_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_music_service_feature'))

        int_l1_streaming_music_service_feature.show()

        assert int_l1_streaming_music_service_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_fav_music_service_by_download_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_music_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_music_service_feature'))

        l1_streaming_fav_music_service_by_download_feature = node_from_config(int_l1_streaming_music_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:l1_streaming_fav_music_service_by_download_feature'))

        l1_streaming_fav_music_service_by_download_feature.show()


        assert l1_streaming_fav_music_service_by_download_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_2nd_fav_music_service_by_download_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_music_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_music_service_feature'))

        l1_streaming_fav_music_service_by_download_feature = node_from_config(int_l1_streaming_music_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:l1_streaming_2nd_fav_music_service_by_download_feature'))

        l1_streaming_fav_music_service_by_download_feature.show()

        assert l1_streaming_fav_music_service_by_download_feature.select("mobile_no").rdd.isEmpty() == True

    def test_int_l1_streaming_esport_service_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_esport_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_esport_service_feature'))

        int_l1_streaming_esport_service_feature.show()
        ##Empty cause I don't set the column application_group value 'game'
        assert int_l1_streaming_esport_service_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_fav_esport_service_by_download_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)
        int_l1_streaming_music_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_esport_service_feature'))

        l1_streaming_fav_esport_service_by_download_feature = node_from_config(int_l1_streaming_music_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:l1_streaming_fav_esport_service_by_download_feature'))

        l1_streaming_fav_esport_service_by_download_feature.show()
        ##Empty cause I don't set the column application_group value 'game'
        assert l1_streaming_fav_esport_service_by_download_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_2nd_fav_esport_service_by_download_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_music_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_esport_service_feature'))

        l1_streaming_2nd_fav_esport_service_by_download_feature = node_from_config(int_l1_streaming_music_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:l1_streaming_2nd_fav_esport_service_by_download_feature'))

        l1_streaming_2nd_fav_esport_service_by_download_feature.show()
        ##Empty cause I don't set the column application_group value 'game'
        assert l1_streaming_2nd_fav_esport_service_by_download_feature.select("mobile_no").rdd.isEmpty() == True

    def test_l1_streaming_visit_count_and_download_traffic_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_visit_count_and_download_traffic_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                                                                      var_project_context.catalog.load(
                                                                          'params:l1_streaming_visit_count_and_download_traffic_feature'))

        l1_streaming_visit_count_and_download_traffic_feature.show()


        ############################################ TEST ZONE #########################################################
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_youtube_video").collect()[0][
                0]) == 1
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_facebook_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_linetv_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_ais_play_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_netflix_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_hooq_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_iflix_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_spotify_music").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_joox_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_twitch_esport").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_bigo_esport").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_mixer_esport").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("visit_count_steamtv_esport").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_youtube_video").collect()[
                0][0]) == 2
        assert float(l1_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_facebook_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_linetv_video").collect()[
                0][0]) == 0
        assert float(l1_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_ais_play_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_netflix_video").collect()[
                0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_hooq_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_iflix_video").collect()[
                0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_spotify_music").collect()[
                0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_joox_music").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_twitch_esport").collect()[
                0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_bigo_esport").collect()[
                0][0]) == 0
        assert float(
            l1_streaming_visit_count_and_download_traffic_feature.select("download_kb_traffic_mixer_esport").collect()[
                0][0]) == 0
        assert float(l1_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_steamtv_esport").collect()[0][0]) == 0

        ###############################################################################################################

    def test_l1_streaming_session_duration_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_session_duration_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:l1_streaming_session_duration_feature'))


        ######################### TEST ZONE ###########################################################################
        ######################################### SUM ################################################################
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_youtube_video").collect()[0][0]) == 1
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_facebook_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_linetv_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_ais_play_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_netflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_hooq_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_iflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_spotify_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_joox_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_twitch_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_bigo_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_mixer_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("sum_session_duration_steamtv_esport").collect()[0][0]) == 0

        ######################################### MAX ################################################################
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_youtube_video").collect()[0][0]) == 1
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_facebook_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_linetv_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_ais_play_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_netflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_hooq_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_iflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_spotify_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_joox_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_twitch_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_bigo_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_mixer_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("max_session_duration_steamtv_esport").collect()[0][0]) == 0

        ######################################### MIN ################################################################
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_youtube_video").collect()[0][0]) == 1
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_facebook_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_linetv_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_ais_play_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_netflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_hooq_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_iflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_spotify_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_joox_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_twitch_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_bigo_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_mixer_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("min_session_duration_steamtv_esport").collect()[0][0]) == 0

        ######################################### AVG ################################################################
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_youtube_video").collect()[0][
                0]) == 1
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_facebook_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_linetv_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_ais_play_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_netflix_video").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_hooq_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_iflix_video").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_spotify_music").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_joox_music").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_twitch_esport").collect()[0][
                0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_bigo_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_mixer_esport").collect()[0][0]) == 0
        assert float(
            l1_streaming_session_duration_feature.select("avg_duration_per_session_steamtv_esport").collect()[0][
                0]) == 0


    ####################################################################################################################
    def test_int_l2_streaming_content_type_features(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_content_type_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:int_l2_streaming_content_type_features'))

        int_l2_streaming_content_type_features.show()

        assert int_l2_streaming_content_type_features.select("access_method_num").count() == 4

    def test_l2_streaming_fav_content_group_by_volume(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_content_type_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_content_type_features'))

        l2_streaming_fav_content_group_by_volume = node_from_config(int_l2_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:l2_streaming_fav_content_group_by_volume'))

        l2_streaming_fav_content_group_by_volume.show()

        assert l2_streaming_fav_content_group_by_volume.select("access_method_num").count() == 4

    def test_l2_streaming_fav_content_group_by_duration(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_content_type_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_content_type_features'))

        l2_streaming_fav_content_group_by_duration = node_from_config(int_l2_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:l2_streaming_fav_content_group_by_duration'))

        l2_streaming_fav_content_group_by_duration.show()

        assert l2_streaming_fav_content_group_by_duration.select("access_method_num").count() == 4

    ####################################################################################################################
    def test_int_l2_streaming_tv_channel_features(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_tv_channel_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:int_l2_streaming_tv_channel_features'))

        int_l2_streaming_content_type_features.show()

        assert int_l2_streaming_content_type_features.select("access_method_num").count() == 4

    def test_l2_streaming_fav_tv_channel_by_volume(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_tv_channel_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_tv_channel_features'))

        l2_streaming_fav_tv_channel_by_volume = node_from_config(int_l2_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:l2_streaming_fav_tv_channel_by_volume'))

        l2_streaming_fav_tv_channel_by_volume.show()

        assert l2_streaming_fav_tv_channel_by_volume.select("access_method_num").count() == 4

    def test_l2_streaming_fav_tv_channel_by_duration(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_content_type_features = l1_massive_processing(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_content_type_features'))

        int_l2_streaming_content_type_features = l2_massive_processing(int_l1_streaming_content_type_features,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_tv_channel_features'))

        l2_streaming_fav_tv_channel_by_duration = node_from_config(int_l2_streaming_content_type_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:l2_streaming_fav_tv_channel_by_duration'))

        l2_streaming_fav_tv_channel_by_duration.show()

        assert l2_streaming_fav_tv_channel_by_duration.select("access_method_num").count() == 4

    ####################################################################################################################
    def test_int_l2_streaming_tv_show_features(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l0_streaming_vimmi_table = add_start_of_week_and_month(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily)

        int_l2_streaming_tv_show_features = node_from_config(int_l0_streaming_vimmi_table,
                        var_project_context.catalog.load('params:int_l2_streaming_tv_show_features'))

        int_l2_streaming_tv_show_features.show()

        assert int_l2_streaming_tv_show_features.select("access_method_num").rdd.isEmpty() == True

    def test_l2_streaming_fav_tv_show_by_episode_watched(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l0_streaming_vimmi_table = add_start_of_week_and_month(df_temp_l0_streaming_ru_a_onair_vimmi_usage_daily)

        int_l2_streaming_tv_show_features = node_from_config(int_l0_streaming_vimmi_table,
                                                             var_project_context.catalog.load(
                                                                 'params:int_l2_streaming_tv_show_features'))


        l2_streaming_fav_tv_show_by_episode_watched = node_from_config(int_l2_streaming_tv_show_features,
                                                                         var_project_context.catalog.load(
                                                                             'params:l2_streaming_fav_tv_show_by_episode_watched'))

        l2_streaming_fav_tv_show_by_episode_watched.show()

        assert l2_streaming_fav_tv_show_by_episode_watched.select("access_method_num").rdd.isEmpty() == True

    ####################################################################################################################
    def test_int_l2_streaming_service_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:int_l1_streaming_video_service_feature'))

        int_l2_streaming_video_service_feature = l2_massive_processing(int_l1_streaming_video_service_feature,
                                                                  var_project_context.catalog.load(
                                                                      'params:int_l2_streaming_video_service_feature'),customer_pro)

        int_l2_streaming_video_service_feature.show()

        assert int_l2_streaming_video_service_feature.select("access_method_num").count() ==  4

    def test_l2_streaming_fav_service_by_download_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l1_streaming_video_service_feature'))

        int_l2_streaming_video_service_feature = l2_massive_processing(int_l1_streaming_video_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_video_service_feature'),
                                                                       customer_pro)

        l2_streaming_fav_service_by_download_feature = node_from_config(int_l2_streaming_video_service_feature,
                                                                  var_project_context.catalog.load(
                                                                      'params:l2_streaming_fav_service_by_download_feature'))

        l2_streaming_fav_service_by_download_feature.show()

        assert l2_streaming_fav_service_by_download_feature.select("access_method_num").count() == 4

    def test_l2_streaming_2nd_fav_service_by_download_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l1_streaming_video_service_feature'))

        int_l2_streaming_video_service_feature = l2_massive_processing(int_l1_streaming_video_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_video_service_feature'),
                                                                       customer_pro)

        l2_streaming_2nd_fav_service_by_download_feature = node_from_config(int_l2_streaming_video_service_feature,
                                                                  var_project_context.catalog.load(
                                                                      'params:l2_streaming_2nd_fav_service_by_download_feature'))

        l2_streaming_2nd_fav_service_by_download_feature.show()

        assert l2_streaming_2nd_fav_service_by_download_feature.select("access_method_num").rdd.isEmpty() == True

    def test_l2_streaming_fav_service_by_visit_count_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        int_l1_streaming_video_service_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l1_streaming_video_service_feature'))

        int_l2_streaming_video_service_feature = l2_massive_processing(int_l1_streaming_video_service_feature,
                                                                       var_project_context.catalog.load(
                                                                           'params:int_l2_streaming_video_service_feature'),
                                                                       customer_pro)

        l2_streaming_fav_service_by_visit_count_feature = node_from_config(int_l2_streaming_video_service_feature,
                                                                  var_project_context.catalog.load(
                                                                      'params:l2_streaming_fav_service_by_visit_count_feature'))

        l2_streaming_fav_service_by_visit_count_feature.show()

        assert l2_streaming_fav_service_by_visit_count_feature.select("access_method_num").count() == 4

    def test_l2_streaming_visit_count_and_download_traffic_feature(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_visit_count_and_download_traffic_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                                                                       var_project_context.catalog.load(
                                                                           'params:l1_streaming_visit_count_and_download_traffic_feature'))

        l2_streaming_visit_count_and_download_traffic_feature = l2_massive_processing_with_expansion(l1_streaming_visit_count_and_download_traffic_feature,
                                                                  var_project_context.catalog.load(
                                                                      'params:l2_streaming_visit_count_and_download_traffic_feature'),customer_pro)

        l2_streaming_visit_count_and_download_traffic_feature.show()

        ####################### TEST ZONE #############################################################################

        ## 2 cause my dataset have 2 row per week
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_youtube_video_sum").collect()[0][
                0]) == 2
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_facebook_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_linetv_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_ais_play_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_netflix_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_hooq_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_iflix_video_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_spotify_music_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_joox_music_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_twitch_esport_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_bigo_esport_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_mixer_esport_sum").collect()[0][
                0]) == 0
        assert float(
            l2_streaming_visit_count_and_download_traffic_feature.select("visit_count_steamtv_esport_sum").collect()[0][
                0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_youtube_video_sum").collect()[0][0]) == 4
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_facebook_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_linetv_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_ais_play_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_netflix_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_hooq_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_iflix_video_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_spotify_music_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_joox_music_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_twitch_esport_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_bigo_esport_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_mixer_esport_sum").collect()[0][0]) == 0
        assert float(l2_streaming_visit_count_and_download_traffic_feature.select(
            "download_kb_traffic_steamtv_esport_sum").collect()[0][0]) == 0

    def test_int_l2_streaming_sum_per_day(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_visit_count_and_download_traffic_feature = l1_massive_processing(
            df_temp_l0_streaming_soc_mobile_app_daily,
            var_project_context.catalog.load(
                'params:l1_streaming_visit_count_and_download_traffic_feature'))

        int_l2_streaming_sum_per_day = l2_massive_processing_with_expansion(
            l1_streaming_visit_count_and_download_traffic_feature,
            var_project_context.catalog.load(
                'params:int_l2_streaming_sum_per_day'), customer_pro)

        int_l2_streaming_sum_per_day.show()

        ####################### TEST ZONE #############################################################################

        assert float(int_l2_streaming_sum_per_day.select("visit_count_youtube_video_sum").collect()[0][0]) == 1
        assert float(int_l2_streaming_sum_per_day.select("visit_count_facebook_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_linetv_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_ais_play_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_netflix_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_hooq_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_iflix_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_spotify_music_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_joox_music_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_twitch_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_bigo_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_mixer_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("visit_count_steamtv_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_youtube_video_sum").collect()[0][0]) == 2
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_facebook_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_linetv_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_ais_play_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_netflix_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_hooq_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_iflix_video_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_spotify_music_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_joox_music_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_twitch_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_bigo_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_mixer_esport_sum").collect()[0][0]) == 0
        assert float(int_l2_streaming_sum_per_day.select("download_kb_traffic_steamtv_esport_sum").collect()[0][0]) == 0

    def test_int_l2_streaming_ranked_of_day_per_week(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_visit_count_and_download_traffic_feature = l1_massive_processing(
            df_temp_l0_streaming_soc_mobile_app_daily,
            var_project_context.catalog.load(
                'params:l1_streaming_visit_count_and_download_traffic_feature'))

        int_l2_streaming_sum_per_day = l2_massive_processing_with_expansion(
            l1_streaming_visit_count_and_download_traffic_feature,
            var_project_context.catalog.load(
                'params:int_l2_streaming_sum_per_day'), customer_pro)

        int_l2_streaming_ranked_of_day_per_week = node_from_config(
            int_l2_streaming_sum_per_day,
            var_project_context.catalog.load(
                'params:int_l2_streaming_ranked_of_day_per_week'))

        int_l2_streaming_ranked_of_day_per_week.show()

        ############################## TEST ZONE #####################################################################
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_youtube_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_facebook_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_linetv_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_ais_play_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_netflix_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_hooq_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_iflix_video_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_spotify_music_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_joox_music_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_twitch_esport_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_bigo_esport_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_mixer_esport_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_steamtv_esport_sum").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("youtube_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("facebook_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("linetv_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("ais_play_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("netflix_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("hooq_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("iflix_video_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("spotify_music_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("joox_music_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("twitch_esport_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("bigo_esport_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("mixer_esport_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(int_l2_streaming_ranked_of_day_per_week.select("steamtv_esport_by_download_rank").where(
            "day_of_week = '1' AND start_of_week = '2020-01-06'").collect()[0][0]) == 1

        ############################################ RANK 2 ###########################################################

        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_youtube_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_facebook_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_linetv_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_ais_play_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_netflix_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_hooq_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_iflix_video_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_spotify_music_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_joox_music_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_twitch_esport_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_bigo_esport_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_mixer_esport_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("download_kb_traffic_steamtv_esport_sum").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(int_l2_streaming_ranked_of_day_per_week.select("youtube_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("facebook_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("linetv_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("ais_play_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("netflix_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("hooq_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("iflix_video_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("spotify_music_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("joox_music_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("twitch_esport_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("bigo_esport_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("mixer_esport_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(int_l2_streaming_ranked_of_day_per_week.select("steamtv_esport_by_download_rank").where(
            "day_of_week = '2' AND start_of_week = '2020-01-06'").collect()[0][0]) == 2


    def test_l2_streaming_session_duration_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        l1_streaming_session_duration_feature = l1_massive_processing(df_temp_l0_streaming_soc_mobile_app_daily,
                        var_project_context.catalog.load('params:l1_streaming_session_duration_feature'))

        l2_streaming_session_duration_feature = l2_massive_processing(l1_streaming_session_duration_feature,
                                                                      var_project_context.catalog.load('params:l2_streaming_session_duration_feature'))

        l2_streaming_session_duration_feature.show()


        ############################################# SUM ##############################################################
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_youtube_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 2
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_facebook_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_linetv_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_ais_play_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_netflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_hooq_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_iflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_spotify_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_joox_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_twitch_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_bigo_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_mixer_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("sum_session_duration_steamtv_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0

        ################################################ MAX ##########################################################
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_youtube_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_facebook_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_linetv_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_ais_play_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_netflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_hooq_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_iflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_spotify_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_joox_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_twitch_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_bigo_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_mixer_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("max_session_duration_steamtv_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0

        ###################################### MIN #####################################################################
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_youtube_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 1
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_facebook_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_linetv_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_ais_play_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_netflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_hooq_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_iflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_spotify_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_joox_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_twitch_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_bigo_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_mixer_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("min_session_duration_steamtv_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0

        ############################ AVG    total use /(week) ##########################################################
        assert round(float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_youtube_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]),2) == round((2/7),2)
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_facebook_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_linetv_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_ais_play_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_netflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_hooq_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_iflix_video").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_spotify_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_joox_music").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_twitch_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_bigo_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_mixer_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0
        assert float(l2_streaming_session_duration_feature.select("avg_session_duration_per_day_steamtv_esport").where(
            "start_of_week = '2020-01-06'").collect()[0][0]) == 0

        ###############################################################################################################

    def Test(self,project_context):
        var_project_context = project_context['ProjectContext']