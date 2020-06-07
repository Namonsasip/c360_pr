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

import pandas as pd
from pyspark.shell import spark
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l3.to_l3_nodes import *
import datetime
import random
from datetime import datetime
from pyspark.sql.types import *
from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *
from customer360.utilities.re_usable_functions import add_start_of_week_and_month


# def generate_category(days, values_list):
#     column = []
#     for iTemp in range(0, days):
#         rand = random.randint(0, len(values_list) - 1)
#         column.append(values_list[rand])
#     return column
#
#
# def generate_int(days, min_num, max_num):
#     return [random.randint(min_num, max_num) for iTemp in range(0, days)]
#
#
# def date_diff(min_date, max_date):
#     return (max_date - min_date).days + 1  # inclusive
#
#
# # Global Variables
# min_date = datetime.date(2020, 1, 1)
# max_date = datetime.date(2020, 1, 6)
# days = date_diff(min_date, max_date)  # range from min_date to max_date
# random.seed(6)


class TestUnitProfile:

    def test_l1_monthly_customer_profile_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        dummy_list_post = [

            ['1-11V9TZKD', 'bhhBUf2FkJmf4utZ+SfeQU0', datetime.strptime('2019-10-03', "%Y-%m-%d"), '90110', 'THAI',
             'null', 'R', 'N', 'THAI', 'null', 'null', '30', 'M', '20200217', '4', 'Disconnect - Ported', 'MS_CLASSIC',
             '3G', 'bR2n03zvqAUZukNXe0lWVm8mmwzykvdWNv7yYV3VptMqz9gcLmrfJvhzpl7GuABk', 'Post-paid', '31900028907947'],
            ['1-XY3DTW6', 'Frxz4RgCfgxWDHZOdwAy7fxO', datetime.strptime('2019-04-24', "%Y-%m-%d"), '10160', 'THAI',
             'null', 'R', 'N', 'THAI', '7FB6E9FA304CC05E05400144FF8A004', 'VAI_U Zeed 250B 20GB UL SWifi', '22', 'F',
             '20200217', '10', 'Active', 'MS_CLASSIC', '3G',
             '3Q6l8oEyLu9daz3sGPRuLSJ7j5ITpJQaA..v3wqlsmey0HJW3uR7pZRGkfYwXONc', 'Post-paid', '31900026583487'],
            ['1-ZQF8N0V', 'ZnbsNiTRR.g9ck1kQ+D0f0Kb', datetime.strptime('2017-01-17', "%Y-%m-%d"), '32160', 'THAI',
             'null', 'R', 'N', 'THAI', '00A596A179591CF4E05400144FF8A004', '4G_HotDeal MAXX_449 7GB UL SWifi', '45',
             'M', '20200217', '37', 'Active', 'MS_CLASSIC', '3G',
             'kpU7DWm8elxgBDKCRUgIH8HukL.QEeztWw2k83CFbvjqeoLptxUsachHsyEfEUIP', 'Post-paid', '31900027705799'],
            ['1-L0AIQ9W', 'P5qkX1YCJhKvzJODDPFjG4Ct', datetime.strptime('2017-02-21', "%Y-%m-%d"), '33170', 'THAI',
             'null', 'R', 'N', 'THAI', '3137FEAC36334C37B9440203D3844469', '3G_Buffet Plus_450B 2Mbps UL SWifi_EX',
             '41', 'F', '20200217', '36', 'Active', 'MS_CLASSIC', '3G',
             '0PIiuXy5CGmhDTeRbHcYnznQh+ahXIdzyT3b2I5wgztBCe54v89lWmED32cK0w.P', 'Post-paid', '31700012693834'],
            ['1-S4WLM14', 'eb+cCdwYGhWuSBzOpjNQPxqd', datetime.strptime('2018-06-07', "%Y-%m-%d"), '72170', 'THAI',
             'null', 'R', 'N', 'THAI', 'null', 'null', '40', 'M', '20200217', '20', 'Terminate', 'MS_CLASSIC', '3G',
             'mJ5zOb0Z0Jrbj79FyiFeMaeH.127k5iXgcth+5468CpPixMefPdCHiexWkIZEAga', 'Post-paid', '31800021250473'],

        ]

        rdd1 = spark.sparkContext.parallelize(dummy_list_post)
        df_post = spark.createDataFrame(rdd1,
                                        schema=StructType([
                                            StructField("subscription_identifier", StringType(), True),
                                            StructField("mobile_no", StringType(), True),
                                            StructField("register_date", DateType(), True),
                                            StructField("zipcode", StringType(), True),
                                            StructField("prefer_language", StringType(), True),
                                            StructField("corp_account_size", StringType(), True),
                                            StructField("cust_type", StringType(), True),
                                            StructField("rsme_flag", StringType(), True),
                                            StructField("prefer_languagev", StringType(), True),
                                            StructField("current_promotion_id_ma", StringType(), True),
                                            StructField("current_promotion_title_ma", StringType(), True),
                                            StructField("ma_age", StringType(), True),
                                            StructField("ma_gender_code", StringType(), True),
                                            StructField("partition_date", StringType(), True),
                                            StructField("service_month", StringType(), True),
                                            StructField("mobile_status", StringType(), True),
                                            StructField("mobile_segment", StringType(), True),
                                            StructField("network_type", StringType(), True),
                                            StructField("card_id", StringType(), True),
                                            StructField("charge_type", StringType(), True),
                                            StructField("account_no", StringType(), True),
                                        ]))

        dummy_list_pre = [

            ['1-8NLQZOO', '4+9fgSSwdj3cdI7mbQCFHohDmAIlEKovIDSR00IYwcnw+wBL1273646DyA0auqxz',
             datetime.strptime('2003-01-22', "%Y-%m-%d"), '32000', 'THAI', 'null', 'R', '3G058',
             'Ѵ[AIS1/0.35 OTH1Top up 100B/30D]', '71', 'M', '20200301', '205', 'SA', 'Classic', '3G',
             '82IoATU6O1Gber0Lo6k4bSMpBVNDVmIASDNmeaca39afQ115pDTYTzxSpcCXkAGx', 'Pre-paid', 'null'],
            ['1-11IF7VVR', '4HLpSqu7CPckXE1hadmF2KripqJnT.EqpGytxdIMQASa8caT+2dSfBvEJZUU5F4.',
             datetime.strptime('2019-09-20', "%Y-%m-%d"), '80160', 'THAI', 'null', 'null', '3G975',
             'Port in Prepaid[1st min0.99B next1.4st/sec]', '45', 'M', '20200301', '5', 'SA', 'Classic', '3G',
             'NIeLVzTCS4tdbMbg37djZqf.r+U+y4otUJH+0QCkqNyByEgM+QmvFAusniS5iniw', 'Pre-paid', 'null'],
            ['1-U7N8Y-830', '4JArWvAOuFvpI6D3eu.XFm8cuA.fOgIq0fpmTCsrRUE+GwAqYC8MbbZg2skFp546',
             datetime.strptime('2019-11-25', "%Y-%m-%d"), 'null', 'THAI', 'null', 'null', '3GB08',
             'THE ONE NEW CallAllNW1point4StperSec', 'null', 'null', '20200301', '3', 'SA', 'Classic', '3G',
             'fDYxcFKN3nHgAJTv5R5KL9U5bIX.tL8KZVDglAQLefbh6lie60Xx0TWYJkKzLXtk', 'Pre-paid', 'null'],
            ['1-4PMI4-1', '4elgqC18vTAUhXXpCYrfIWky062ZOAeRp+HSySw.Sr8XA.8MKkb2HAHg3OBqM5wl',
             datetime.strptime('2007-10-12', "%Y-%m-%d"), '15000', 'THAI', 'null', 'null', '3G279',
             '[All Network 0.99Baht/min ExVAT]', '47', 'F', '20200301', '148', 'SA', 'Classic', '3G',
             'q734dXHqthY0jsd0FiPeguYtxFAg3BkHil4HlORI79tVlEF3Eng3pBvFT4dtb5pl', 'Pre-paid', 'null'],
            ['1-EOMF2VT', '4gAOsMo8USFgTLUZJMNcaQfct+coVlGARgjyls+Xxy.b8NWttfKWlbtMzO4Fj901',
             datetime.strptime('2015-02-22', "%Y-%m-%d"), 'null', 'ENGLISH', 'null', 'null', '3G518',
             '3G Buffet Day199_NoMF[AIS05-17:0B, Oth2/0.60]ExVAT', 'null', 'null', '20200301', '60', 'SA', 'Classic',
             '3G', 'iPmCoqzPREJvDoCJLPST0tipdRT.JaBqZUZYLYjxDxwwSjl4.CvL99nfHSoP+JE7', 'Pre-paid', 'null'],

        ]

        rdd2 = spark.sparkContext.parallelize(dummy_list_pre)
        df_pre = spark.createDataFrame(rdd2,
                                       schema=StructType([
                                           StructField("subscription_identifier", StringType(), True),
                                           StructField("mobile_no", StringType(), True),
                                           StructField("register_date", DateType(), True),
                                           StructField("zipcode", StringType(), True),
                                           StructField("prefer_language", StringType(), True),
                                           StructField("company_size", StringType(), True),
                                           StructField("cust_type", StringType(), True),
                                           StructField("package_id", StringType(), True),
                                           StructField("promotion_name", StringType(), True),
                                           StructField("age", StringType(), True),
                                           StructField("gender", StringType(), True),
                                           StructField("partition_date", StringType(), True),
                                           StructField("service_month", StringType(), True),
                                           StructField("mobile_status", StringType(), True),
                                           StructField("mobile_segment", StringType(), True),
                                           StructField("root_network_type", StringType(), True),
                                           StructField("card_no", StringType(), True),
                                           StructField("charge_type", StringType(), True),
                                           StructField("billing_account_no", StringType(), True),
                                       ]))

        dummy_list_non_b = [

        ]
        rdd3 = spark.sparkContext.parallelize(dummy_list_non_b)
        df_non_b = spark.createDataFrame(rdd3,
                                         schema=StructType([
                                             StructField("subscription_id", StringType(), True),
                                             StructField("mobile_no", StringType(), True),
                                             StructField("register_date", DateType(), True),
                                             StructField("zipcode", StringType(), True),
                                             StructField("prefer_language", StringType(), True),
                                             StructField("corp_account_size", StringType(), True),
                                             StructField("cust_type", StringType(), True),
                                             StructField("prefer_languagev", StringType(), True),
                                             StructField("current_promotion_id_ma", StringType(), True),
                                             StructField("current_promotion_title_ma", StringType(), True),
                                             StructField("ma_age", StringType(), True),
                                             StructField("ma_gender_code", StringType(), True),
                                             StructField("partition_date", StringType(), True),
                                             StructField("service_month", StringType(), True),
                                             StructField("mobile_status", StringType(), True),
                                             StructField("mobile_segment", StringType(), True),
                                             StructField("network_type", StringType(), True),
                                             StructField("card_id", StringType(), True),
                                             StructField("charge_type", StringType(), True),
                                             StructField("account_no", StringType(), True),
                                             StructField("rsme_flag", StringType(), True),
                                         ]))
        #
        # print('-------------------- post-----------------------------')
        # df_post.show()
        # print('-------------------- pre-----------------------------')
        # df_pre.show()
        # print('-------------------- non mobile -----------------------------')
        # df_non_b.show()

        l1_data = union_daily_cust_profile(df_pre,
                                           df_post,
                                           df_non_b,
                                           var_project_context.catalog.load(
                                               'params:l1_customer_profile_union_daily_feature'))

        l1_total_data = add_start_of_week_and_month(l1_data, var_project_context.catalog.load(
            'params:customer_profile_partition_col'))

        l1_total_data.show()
        exit(2)



    def test_l3_customer_profile_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")

        dummy_list_journey = [
            ['SuTSF9eqzsYsFxBuVVu1Hm51gsKzmegs4So1P31PFcBPwHfDs710pU0Ms+HCI6PX',
             datetime.strptime('2019-06-11', "%Y-%m-%d"), '1-YZ6CJ2F', 'Post-paid', '201909', 'Active', '1981-12',
             'Female', datetime.strptime('2020-06-30', "%Y-%m-%d"), datetime.strptime('2019-06-30', "%Y-%m-%d"),
             'Classic', '3G', 'RETAIL & IT CHAIN', 'NL', '4G_HotDeal MAXX_549 12GB UL SWifi', '11', 'RESIDENTIAL_MOC',
             '1', 'Y', '31900027220037', '549'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201909', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Buffet450Dis50_250 1Mbps UL SWifi', '22',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '342.56'],
            ['SuTSF9eqzsYsFxBuVVu1Hm51gsKzmegs4So1P31PFcBPwHfDs710pU0Ms+HCI6PX',
             datetime.strptime('2019-06-11', "%Y-%m-%d"), '1-YZ6CJ2F', 'Post-paid', '201908', 'Active', '1981-12',
             'Female', datetime.strptime('2020-06-30', "%Y-%m-%d"), datetime.strptime('2019-06-30', "%Y-%m-%d"),
             'Classic', '3G', 'RETAIL & IT CHAIN', 'NL', '4G_HotDeal MAXX_549 12GB UL SWifi', '11', 'RESIDENTIAL_MOC',
             '1', 'Y', '31900027220037', '549'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201908', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Buffet450Dis50_250 1Mbps UL SWifi', '22',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '381.47'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '201912', 'SA', '1995-08', 'M',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'CB', 'SIM2Fly 3G Non-Stop', '3G905', 'null', '2', 'Y',
             'null', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201912', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Save 1099_DC549 UL40GB VDOCloudSWifi', '33',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '570.42'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '201912', 'SA', 'null', 'null',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'NL', 'SUPER SOCIAL SIM V3', '3GA97', 'null', '1', 'Y',
             'null', '46.06'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '201911', 'SA', '1995-08', 'Male',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'CB', 'SIM2Fly 3G Non-Stop', '3G905', 'null', '2', 'Y',
             'null', '342.9'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201911', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Buffet_450B 100Min 1Mbps UL SWifi', '44',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '684.09'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), 'NA_6198973790', 'Pre-paid', '201911', 'SA', 'null', 'null',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'NL', 'SUPER SOCIAL SIM V3', '3GA97', 'null', '1', 'Y',
             'null', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201910', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Buffet_450B 100Min 1Mbps UL SWifi', '44',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '383.1'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '202001', 'SA', 'null', 'null',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'NL', 'SUPER SOCIAL SIM V3', '3GA97', 'null', '1', 'Y',
             'null', '7.15'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '202001', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Save 1099_DC549 UL40GB VDOCloudSWifi', '33',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '549'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '202001', 'SS', '1995-08', 'Male',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'CB', 'SIM2Fly 3G Non-Stop', '3G905', 'null', '2', 'Y',
             'null', '0'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '202001', 'SA', 'null', 'null',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'NL', 'SUPER SOCIAL SIM V3', '3GA97', 'null', '1', 'Y',
             'null', '7.15'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '202001', 'SS', '1995-08', 'Male',
             'null', 'null', 'Classic', '3GPre-paid', 'null', 'CB', 'SIM2Fly 3G Non-Stop', '3G905', 'null', '2', 'Y',
             'null', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '202001', 'Active', '1995-08',
             'Male', 'null', 'null', 'Classic', '3G', 'null', 'CB', '3G_Save 1099_DC549 UL40GB VDOCloudSWifi', '33',
             'RESIDENTIAL_MOC', '2', 'Y', '31800023504177', '549'],

        ]
        rdd4 = spark.sparkContext.parallelize(dummy_list_journey)
        df_journey = spark.createDataFrame(rdd4,
                                           schema=StructType([
                                               StructField("access_method_num", StringType(), True),
                                               StructField("register_date", DateType(), True),
                                               StructField("crm_sub_id", StringType(), True),
                                               StructField("charge_type", StringType(), True),
                                               StructField("partition_month", StringType(), True),
                                               StructField("mobile_status", StringType(), True),
                                               StructField("birth_date", StringType(), True),
                                               StructField("gender", StringType(), True),
                                               StructField("contract_end_month", StringType(), True),
                                               StructField("contract_start_month", StringType(), True),
                                               StructField("mobile_segment", StringType(), True),
                                               StructField("network_type", StringType(), True),
                                               StructField("order_channel_new", StringType(), True),
                                               StructField("first_act_region", StringType(), True),
                                               StructField("main_promo_name", StringType(), True),
                                               StructField("main_promo_id", StringType(), True),
                                               StructField("moc_cust_type", StringType(), True),
                                               StructField("card_no", StringType(), True),
                                               StructField("cust_active_this_month", StringType(), True),
                                               StructField("ba_id", StringType(), True),
                                               StructField("norms_net_revenue", StringType(), True),
                                           ]))

        # df_journey.show()

        korn2 = node_from_config(df_journey, var_project_context.catalog.load(
            'params:int_l3_customer_profile_basic_features'))

        korn2.show()
        korn3 = add_last_month_inactive_user(korn2)

    def test_l4_customer_profile_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        dummy_list_journey = [
            ['SuTSF9eqzsYsFxBuVVu1Hm51gsKzmegs4So1P31PFcBPwHfDs710pU0Ms+HCI6PX',
             datetime.strptime('2019-06-11', "%Y-%m-%d"), '1-YZ6CJ2F', 'Post-paid', '201909', 'Active', '549'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201909', 'Active', '342.56'],
            ['SuTSF9eqzsYsFxBuVVu1Hm51gsKzmegs4So1P31PFcBPwHfDs710pU0Ms+HCI6PX',
             datetime.strptime('2019-06-11', "%Y-%m-%d"), '1-YZ6CJ2F', 'Post-paid', '201908', 'Active', '549'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201908', 'Active', '381.47'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '201912', 'SA', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201912', 'Active', '570.42'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '201912', 'SA', '46.06'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '201911', 'SA', '342.9'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201911', 'Active', '684.09'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), 'NA_6198973790', 'Pre-paid', '201911', 'SA', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '201910', 'Active', '383.1'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '202001', 'SA', '7.15'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '202001', 'Active', '549'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '202001', 'SS', '0'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx',
             datetime.strptime('2019-11-30', "%Y-%m-%d"), '1-U97N7-240', 'Pre-paid', '202001', 'SA', '7.15'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH',
             datetime.strptime('2019-11-13', "%Y-%m-%d"), '1-U0IH6-496', 'Pre-paid', '202001', 'SS', '0'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1',
             datetime.strptime('2010-09-22', "%Y-%m-%d"), '1-7ASCIXN', 'Post-paid', '202001', 'Active', '549'],
        ]

        rdd5 = spark.sparkContext.parallelize(dummy_list_journey)
        df_journey = spark.createDataFrame(rdd5,
                                           schema=StructType([
                                               StructField("access_method_num", StringType(), True),
                                               StructField("register_date", DateType(), True),
                                               StructField("crm_sub_id", StringType(), True),
                                               StructField("charge_type", StringType(), True),
                                               StructField("partition_month", StringType(), True),
                                               StructField("mobile_status", StringType(), True),
                                               StructField("norms_net_revenue", StringType(), True),
                                           ]))
        korn4 = node_from_config(df_journey, var_project_context.catalog.load(
            'params:l4_customer_profile_ltv_to_date'))

        korn4.show()
        print('555555555555555555555555555555555555555555555555555555555')
        exit(2)
