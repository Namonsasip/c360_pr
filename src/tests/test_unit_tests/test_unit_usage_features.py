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
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l2.to_l2_nodes import build_usage_l2_layer
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql.types import *
from pyspark.sql import functions as F
import datetime
from customer360.pipelines.data_engineering.nodes.usage_nodes.to_l1 import merge_all_dataset_to_one_table


def generate_category(days, values_list):
    column = []
    for iTemp in range(0, days):
        rand = random.randint(0, len(values_list) - 1)
        column.append(values_list[rand])
    return column


def generate_int(days, min_num, max_num):
    return [random.randint(min_num, max_num) for iTemp in range(0, days)]


def generate_day_id(min_dt, max_dt):
    my_dates_list = pd.date_range(min_dt, max_dt).tolist()
    day_id = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
    return day_id


def date_diff(min_date, max_date):
    return (max_date - min_date).days + 1  # inclusive


def check_null(df):
    if df.first() is None:
        list_type = []
        for f in df.schema.fields:
            list_type = str(f.dataType)
        if list_type != "DateType":  # D is DateType
            temp = 0
            return temp
        elif list_type == "DateType":
            temp = None
            return temp
    else:
        temp = df.first()[0]
        return temp

def out_going(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    random.seed(100)
    print(
        "***********************L0 usage_outgoing_call_relation_sum_daily generation*******************************")
    # Below section is to create dummy data.
    min_dt = '2020-01-01'
    max_dt = '2020-04-01'
    doubled_days = days * 2
    call_start_dt = generate_day_id(min_dt, max_dt)
    call_start_dt.extend(call_start_dt)  # two records per day
    call_start_dt.sort()

    # print(day_id)
    # _10min_totalcall = generate_int(doubled_days, 300, 600)
    # _15min_totalcall = generate_int(doubled_days,600,900)
    # _20min_totalcall = generate_int(doubled_days, 900, 1200)
    # _30min_totalcall = generate_int(doubled_days, 1200, 1800)
    # _morethan30_totalcall = generate_int(doubled_days, 1800, 2000)

    # l0_tol1_test
    service_type_ = generate_category(doubled_days, ['VOICE', 'SMS'])
    idd_flag = generate_category(doubled_days, ['Y', 'N'])
    call_type = generate_category(doubled_days, ['MO', 'MT'])
    # total_successful_call = generate_int(doubled_days, [0, 1])
    called_network_type = generate_category(doubled_days,
                                            ['TRUE', 'DTAC', '3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN',
                                             'AWN',
                                             'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post',
                                             'AWNINT'])

    # L1_test
    total_call = generate_int(doubled_days, 0, 2000)
    total_durations = generate_int(doubled_days, 0, 5)
    call_start_hr = generate_int(doubled_days, 0, 23)

    day_id = generate_day_id(min_dt, max_dt)
    day_id.extend(day_id)  # two records per day
    day_id.sort()
    hour_id = generate_int(doubled_days, 0, 23)
    total_successful_call = generate_int(doubled_days, 0, 30)

    for i in range(1, len(call_start_hr)):
        if ((i + 1) % 2 == 0):
            while (call_start_hr[i] == call_start_hr[i - 1]):
                new_int = generate_int(1, 0, 23)[0]
                print("i = " + str(i))
                print("new_int = " + str(new_int))
                print("hour_id[i] = " + str(call_start_hr[i]))
                print("hour_id[i-1] = " + str(call_start_hr[i - 1]))
                call_start_hr[i] = new_int
    print(call_start_hr)

    df_usage_outgoing_call_relation_sum_daily = spark.createDataFrame(
        zip(day_id, hour_id, total_call, total_successful_call, called_network_type, call_type, service_type_,
            idd_flag),
        schema=['day_id', 'hour_id', 'total_durations', 'total_successful_call', 'called_network_type', 'call_type',
                'service_type', 'idd_flag']) \
        .withColumn("access_method_num", F.lit(1)) \
        .withColumn("caller_no", F.lit(2)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))

    df_usage_outgoing_call_relation_sum_daily = df_usage_outgoing_call_relation_sum_daily \
        .withColumn("weekday", F.date_format(df_usage_outgoing_call_relation_sum_daily.day_id, 'EEEE'))

    df_usage_outgoing_call_relation_sum_daily = df_usage_outgoing_call_relation_sum_daily.where("call_type = 'MO'")

    return df_usage_outgoing_call_relation_sum_daily

def incoming(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    print(
        "***********************L0 usage_incoming_call_relation_sum_daily generation*******************************")
    # Below section is to create dummy data.
    min_dt = '2020-01-01'
    max_dt = '2020-04-01'
    doubled_days = days * 2
    day_id = generate_day_id(min_dt, max_dt)
    day_id.extend(day_id)  # two records per day
    day_id.sort()
    # print(day_id)
    call_type = generate_category(doubled_days, ['MT', 'MO'])
    service_type = generate_category(doubled_days, ['VOICE', 'SMS'])
    idd_flag = generate_category(doubled_days, ['Y', 'N'])
    total_durations = generate_int(doubled_days, 0, 1000)
    total_successful_call = generate_int(doubled_days, 0, 5)
    caller_network_type = generate_category(doubled_days, ['3GPost-paid', '3GPre-paid', 'AIS',
                                                           'InternalAWN', 'AWN', 'Fixed Line-AWN',
                                                           'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT',
                                                           'DTAC', 'TRUE'])

    rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
    hour_id = generate_int(doubled_days, 0, 23)
    for i in range(1, len(hour_id)):
        if ((i + 1) % 2 == 0):
            while (hour_id[i] == hour_id[i - 1]):
                new_int = generate_int(1, 0, 23)[0]
                print("i = " + str(i))
                print("new_int = " + str(new_int))
                print("hour_id[i] = " + str(hour_id[i]))
                print("hour_id[i-1] = " + str(hour_id[i - 1]))
                hour_id[i] = new_int
    # print(hour_id)
    df_usage_incoming_call = spark.createDataFrame(
        zip(day_id, hour_id, call_type, service_type, idd_flag, total_durations, total_successful_call,
            caller_network_type),
        schema=['day_id', 'hour_id', 'call_type', 'service_type', 'idd_flag', 'total_durations',
                'total_successful_call', 'caller_network_type']) \
        .withColumn("called_no", F.lit(1)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_usage_incoming_call.orderBy("day_id", ascending=True).show()
    # df_usage_incoming_call.orderBy("day_id", ascending=False).show()
    daily_usage_incoming_call = node_from_config(df_usage_incoming_call, var_project_context.catalog.load(
        'params:l1_usage_incoming_call_relation_sum_daily'))

def out_going_ir(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']

    # min_dt = '2020-01-01'
    # max_dt = '2020-04-01'
    # #days = date_diff(min_dt, max_dt)  # range from min_date to max_date
    random.seed(100)
    print("*********************************L1 usage_incoming_sum_ir Starts***************************************")
    # random.seed(100)
    random_dur = [random.randint(1, 5) for iTemp in range(0, days)]
    # random.seed(101)
    random_no_call = [random.randint(1, 5) for iTemp in range(0, days)]
    # service_type = ['VOICE' if random.randint(1, 2) == 1 else 'SMS' for iTemp in range(0, 121)]
    day_id = generate_day_id(min_date, max_date)
    service_type = []
    for iTemp in range(0, 121):
        rand_service = random.randint(1, 3)
        if rand_service == 1:
            service_type.append('VOICE')
        elif rand_service == 2:
            service_type.append('SMS')
        else:
            service_type.append('IR_SMS')
    call_type = ['MO' if random.randint(1, 2) == 1 else 'MT' for iTemp in range(0, 121)]
    df_usg_relation_ir = spark.createDataFrame(zip(call_type, service_type, random_dur, random_no_call, day_id),
                                               schema=['call_type', 'service_type', 'total_durations',
                                                       'total_successful_call', 'day_id']) \
        .withColumn("called_no", F.lit(1)) \
        .withColumn("caller_no", F.lit(2)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_usg_relation_ir.orderBy("day_id", ascending=True).show()
    # df_usg_relation_ir.orderBy("day_id", ascending=False).show()
    daily_usage_outgoing_sum_ir = node_from_config(df_usg_relation_ir, var_project_context.catalog.load(
        'params:l1_usage_outgoing_call_relation_sum_ir_daily'))

    return df_usg_relation_ir

def incoming_ir(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']

    # min_dt = '2020-01-01'
    # max_dt = '2020-04-01'
    # #days = date_diff(min_dt, max_dt)  # range from min_date to max_date
    random.seed(100)
    print("*********************************L1 usage_incoming_sum_ir Starts***************************************")
    # random.seed(100)
    random_dur = [random.randint(1, 5) for iTemp in range(0, days)]
    # random.seed(101)
    random_no_call = [random.randint(1, 5) for iTemp in range(0, days)]
    # service_type = ['VOICE' if random.randint(1, 2) == 1 else 'SMS' for iTemp in range(0, 121)]
    day_id = generate_day_id(min_date, max_date)
    service_type = []
    for iTemp in range(0, 121):
        rand_service = random.randint(1, 3)
        if rand_service == 1:
            service_type.append('VOICE')
        elif rand_service == 2:
            service_type.append('SMS')
        else:
            service_type.append('IR_SMS')
    call_type = ['MO' if random.randint(1, 2) == 1 else 'MT' for iTemp in range(0, 121)]
    df_usg_relation_ir = spark.createDataFrame(zip(call_type, service_type, random_dur, random_no_call, day_id),
                                               schema=['call_type', 'service_type', 'total_durations',
                                                       'total_successful_call', 'day_id']) \
        .withColumn("called_no", F.lit(1)) \
        .withColumn("caller_no", F.lit(2)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_usg_relation_ir.orderBy("day_id", ascending=True).show()
    # df_usg_relation_ir.orderBy("day_id", ascending=False).show()
    daily_usage_incoming_sum_ir = node_from_config(df_usg_relation_ir, var_project_context.catalog.load(
        'params:l1_usage_incoming_call_relation_sum_ir_daily'))

    return df_usg_relation_ir

def ru_a_gprs_cbs(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    print("***********************L0 usage_ru_a_gprs_cbs_usage_daily generation*******************************")
    # Below section is to create dummy data.
    min_dt = '2020-01-01'
    max_dt = '2020-04-01'
    doubled_days = days * 2
    call_start_dt = generate_day_id(min_dt, max_dt)
    call_start_dt.extend(call_start_dt)  # two records per day
    call_start_dt.sort()
    # print(day_id)
    data_upload_amt = generate_int(doubled_days, 1, 5)
    data_download_amt = generate_int(doubled_days, 1, 5)
    cdr_subtype_cd = generate_category(doubled_days, ['ROAMING', 'DOMESTIC'])
    rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
    call_start_hr = generate_int(doubled_days, 0, 23)
    for i in range(1, len(call_start_hr)):
        if ((i + 1) % 2 == 0):
            while (call_start_hr[i] == call_start_hr[i - 1]):
                new_int = generate_int(1, 0, 23)[0]
                print("i = " + str(i))
                print("new_int = " + str(new_int))
                print("hour_id[i] = " + str(call_start_hr[i]))
                print("hour_id[i-1] = " + str(call_start_hr[i - 1]))
                call_start_hr[i] = new_int
    print(call_start_hr)
    df_usage_ru_a_gprs_cbs_usage = spark.createDataFrame(
        zip(call_start_dt, call_start_hr, data_upload_amt, data_download_amt, cdr_subtype_cd, rat_type),
        schema=['call_start_dt', 'call_start_hr', 'data_upload_amt', 'data_download_amt', 'cdr_subtype_cd',
                'rat_type']) \
        .withColumn("access_method_num", F.lit(1)) \
        .withColumn("call_start_dt", F.to_date('call_start_dt', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_usage_ru_a_gprs_cbs_usage.orderBy("call_start_dt", ascending=True).show()
    # df_usage_ru_a_gprs_cbs_usage.orderBy("call_start_dt", ascending=False).show()
    daily_usage_ru_a_gprs_cbs_usage = node_from_config(df_usage_ru_a_gprs_cbs_usage,
                                                       var_project_context.catalog.load(
                                                           'params:l1_usage_ru_a_gprs_cbs_usage_daily'))

    return df_usage_ru_a_gprs_cbs_usage

def ru_vas_post(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    print("***********************L0 usage_ru_a_vas_postpaid_usg_daily generation*******************************")
    # Below section is to create dummy data.
    min_dt = '2020-01-01'
    max_dt = '2020-04-01'
    doubled_days = days * 2
    day_id = generate_day_id(min_dt, max_dt)
    day_id.extend(day_id)  # two records per day
    day_id.sort()
    # print(day_id)
    uplink_volume_kb = generate_int(doubled_days, 1, 5)
    downlink_volume_kb = generate_int(doubled_days, 1, 5)
    call_type_cd = generate_category(doubled_days, [3, 64])
    rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
    hour_id = generate_int(doubled_days, 0, 23)
    for i in range(1, len(hour_id)):
        if ((i + 1) % 2 == 0):

            while (hour_id[i] == hour_id[i - 1]):
                new_int = generate_int(1, 0, 23)[0]
                print("i = " + str(i))
                print("new_int = " + str(new_int))
                print("hour_id[i] = " + str(hour_id[i]))
                print("hour_id[i-1] = " + str(hour_id[i - 1]))
                hour_id[i] = new_int
    # print(hour_id)
    df_usg_ru_vas_post = spark.createDataFrame(
        zip(day_id, hour_id, uplink_volume_kb, downlink_volume_kb, call_type_cd, rat_type),
        schema=['day_id', 'hour_id', 'uplink_volume_kb', 'downlink_volume_kb', 'call_type_cd', 'rat_type']) \
        .withColumn("access_method_num", F.lit(1)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_usg_ru_vas_post.orderBy("day_id", ascending=True).show()
    # df_usg_ru_vas_post.orderBy("day_id", ascending=False).show()
    daily_usg_ru_vas_post = node_from_config(df_usg_ru_vas_post, var_project_context.catalog.load(
        'params:l1_usage_ru_a_vas_postpaid_usg_daily'))

    return df_usg_ru_vas_post

def vas_data(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']

    # Below section is to create dummy data.
    # min_date = datetime.date(2020,1,1)
    # max_date = datetime.date(2020,4,1)
    # days = date_diff(min_date,max_date) #range from min_date to max_date
    random.seed(100)
    my_dates_list = pd.date_range(min_date, max_date).tolist()
    my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
    random_list = [random.randint(1, 5) for iTemp in range(0, days)]
    df_vas_post_pre = spark.createDataFrame(zip(random_list, my_dates), schema=['number_of_call', 'day_id']) \
        .withColumn("access_method_num", F.lit(1)) \
        .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
        .withColumn("partition_date", F.lit('20200101'))
    # df_vas_post_pre.show()
    # Testing Daily Features here
    vas_data = node_from_config(df_vas_post_pre, var_project_context.catalog.load(
        'params:l1_usage_ru_a_vas_postpaid_prepaid_daily'))

    return df_vas_post_pre

def daily_profile(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']
    random.seed(100)
    print("***********************L1 usage_profile generation*******************************")
    # Below section is to create dummy data.
    min_dt = '2020-01-01'
    max_dt = '2020-04-01'
    doubled_days = days * 2
    day_id = generate_day_id(min_dt, max_dt)
    day_id.extend(day_id)  # two records per day
    day_id.sort()
    # print(day_id)
    subscription_identifier = generate_int(doubled_days, 1, 2)
    daily_profile_feature = spark.createDataFrame(
        zip(day_id, subscription_identifier),
        schema=['event_partition_date', 'subscription_identifier']) \
        .withColumn("event_partition_date", F.to_date('event_partition_date', 'dd-MM-yyyy')) \
        .withColumn("access_method_num", F.lit(1))

    return daily_profile_feature

global test_l1_usage_postpaid_prepaid_daily
test_l1_usage_postpaid_prepaid_daily = [
['1-ELB55V1',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-27", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','93239.00','357566.00','1648363.00','1142966.00','3242134.00','0.00','0.00','0.00','0.00','0.00','3242134.00','0.00','0.0','0.0','0.0','0.0','2235058.00','115060.00','2119998.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'5.0','0.0','2235058','115060','2119998','0.0','5.0','0.0','0.0','0.0','0.0','0.0','0.0','3.0','0.0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','5.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','5.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,None,None,None,'1007076.00','106269.00','900807.00',None,None,None,None,None,None,None,None,None,None,None,None,None,None,'1007076','106269','900807',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,'0.0',None,None,'0','0','0',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'3242134.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-J43GCJN',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-27", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.0','0.0','0.0','0.0','0.00','0.00','0.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.0','0.0',None,None,None,'0.0','3.0','0.0','0.0','0.0','0.0','0.0','0.0','2.0','0.0','1.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',None,None,None,None,None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','3.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','3.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,None,None,None,'0.00','0.00','0.00',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,'0.0',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-DXZB-156',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','10977.00','5799.00','0.00','196556.00','213332.00','0.00','0.00','0.00','0.00','0.00','213332.00','0.00','0.0','1.0','0.0','0.0','154126.00','0.00','154126.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.0','161.0','154126','0','154126','1.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','1.0','0.0','0.0','1.0','0.0','1.0','1.0','1.0','1.0','1.0','1.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','161.0','0.0','0.0','0.0','0.0','161.0','0.0','0.0','0.0','161.0','0.0','0.0','0.0','0.0','0.0','161.0','1.0','1.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,None,None,None,'59206.00','0.00','59206.00',None,None,None,None,None,None,None,None,None,None,None,None,None,None,'59206','0','59206',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,'161.0',None,None,'0','0','0',None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'213332.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-FAKEVST',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-06", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','62041035.00','59988136.00','22915129.00','113044239.00','257988539.00','0.00','0.00','0.00','0.00','0.00','257988539.00','0.00','2.0','3.0','294.0','2.0','233697726.00','233697726.00','0.00','0.0','0.0','0.0','0.0','1.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','415.0','233697726','233697726','0','4.0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','4.0','0.0','4.0','4.0','4.0','4.0','4.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','415.0','2.0','0.0','0.0','0.0','299.0','116.0','0.0','0.0','415.0','0.0','0.0','0.0','0.0','0.0','415.0','4.0','2.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','0.0','0.0','0.0','24290813.00','24290813.00','0.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,'0.0','21.0','24290813','24290813','0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','2.0','0.0','2.0','2.0','2.0','2.0','2.0','121.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','0.0','21.0','2.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'257988539.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-FAKEVST',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-13", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','62041035.00','59988136.00','22915129.00','113044239.00','257988539.00','0.00','0.00','0.00','0.00','0.00','257988539.00','0.00','2.0','3.0','294.0','2.0','233697726.00','233697726.00','0.00','0.0','0.0','0.0','0.0','1.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','415.0','233697726','233697726','0','4.0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','4.0','0.0','4.0','4.0','4.0','4.0','4.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','415.0','2.0','0.0','0.0','0.0','299.0','116.0','0.0','0.0','415.0','0.0','0.0','0.0','0.0','0.0','415.0','4.0','2.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','0.0','0.0','0.0','24290813.00','24290813.00','0.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,'0.0','21.0','24290813','24290813','0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','2.0','0.0','2.0','2.0','2.0','2.0','2.0','121.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','0.0','21.0','2.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'257988539.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-FAKEVST',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-20", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','62041035.00','59988136.00','22915129.00','113044239.00','257988539.00','0.00','0.00','0.00','0.00','0.00','257988539.00','0.00','2.0','3.0','294.0','2.0','233697726.00','233697726.00','0.00','0.0','0.0','0.0','0.0','1.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','415.0','233697726','233697726','0','4.0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','4.0','0.0','4.0','4.0','4.0','4.0','4.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','415.0','2.0','0.0','0.0','0.0','299.0','116.0','0.0','0.0','415.0','0.0','0.0','0.0','0.0','0.0','415.0','4.0','2.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','0.0','0.0','0.0','24290813.00','24290813.00','0.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,'0.0','21.0','24290813','24290813','0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','2.0','0.0','2.0','2.0','2.0','2.0','2.0','121.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','0.0','21.0','2.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'257988539.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')],
['1-FAKEVST',None,None,datetime.datetime.strptime("2020-01-01", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-27", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','0.00','62041035.00','59988136.00','22915129.00','113044239.00','257988539.00','0.00','0.00','0.00','0.00','0.00','257988539.00','0.00','2.0','3.0','294.0','2.0','233697726.00','233697726.00','0.00','0.0','0.0','0.0','0.0','1.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','415.0','233697726','233697726','0','4.0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','4.0','0.0','4.0','4.0','4.0','4.0','4.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','415.0','2.0','0.0','0.0','0.0','299.0','116.0','0.0','0.0','415.0','0.0','0.0','0.0','0.0','0.0','415.0','4.0','2.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'2.0','0.0','0.0','0.0','24290813.00','24290813.00','0.00','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),None,'0.0','21.0','24290813','24290813','0','2.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','2.0','0.0','2.0','2.0','2.0','2.0','2.0','121.0','2.0',None,'0','0','0',None,None,None,'0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','21.0','0.0','0.0','0.0','0.0','0.0','21.0','2.0','0.0','0.0','0.0','0.0',datetime.datetime.strptime("2020-01-28", '%Y-%m-%d'),'257988539.00',None,None,datetime.datetime.strptime("2020-01-28", '%Y-%m-%d')]

]

# Global Variables
min_date = datetime.date(2020, 1, 1)
max_date = datetime.date(2020, 4, 1)
days = date_diff(min_date, max_date)  # range from min_date to max_date
random.seed(100)
daily_usg_ru_vas_post = []
daily_usage_incoming_sum_ir = []
daily_usage_outgoing_sum_ir = []
vas_data = []
daily_usage_ru_a_gprs_cbs_usage = []
daily_usage_incoming_call = []
daily_profile_feature = []
usage_outgoing_call_relation_sum_daily = []


class TestUnitUsage:

    def test_vas_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        # min_date = datetime.date(2020,1,1)
        # max_date = datetime.date(2020,4,1)
        # days = date_diff(min_date,max_date) #range from min_date to max_date
        random.seed(100)
        my_dates_list = pd.date_range(min_date, max_date).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        random_list = [random.randint(1, 5) for iTemp in range(0, days)]

        print(
            "*********************************L1 usage_ru_a_vas_postpaid_prepaid_daily Starts***************************************")
        df_vas_post_pre = spark.createDataFrame(zip(random_list, my_dates), schema=['number_of_call', 'day_id']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))
        # df_vas_post_pre.show()
        # Testing Daily Features here
        global vas_data
        vas_data = node_from_config(df_vas_post_pre, var_project_context.catalog.load(
            'params:l1_usage_ru_a_vas_postpaid_prepaid_daily'))
        # vas_data.orderBy('start_of_week').show()
        # check event date = 2020-01-01 date the number of calls should be 2
        assert \
            vas_data.where("event_partition_date = '2020-01-01'").select("usg_vas_total_number_of_call").collect()[0][
                0] == 2
        assert \
            vas_data.where("event_partition_date = '2020-01-01'").select("usg_vas_last_action_dt").collect()[0][
                0] == datetime.date(2020, 1, 1)
        assert \
            vas_data.where("event_partition_date = '2020-01-01'").select("usg_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 1)
        # # check weekly features
        # weekly_data = expansion(daily_data,
        #                         var_project_context.catalog.load('params:l2_usage_ru_a_vas_postpaid_prepaid_weekly'))
        #
        # # check start_of_week = 2020-03-30 and features
        # assert \
        #     weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_avg").collect()[0][
        #         0] > 3
        # assert \
        #     weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_max").collect()[0][
        #         0] == 5
        # assert \
        #     weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_min").collect()[0][
        #         0] == 2
        # assert \
        #     weekly_data.where("start_of_week = '2020-03-30'").select("usg_vas_total_number_of_call_sum").collect()[0][
        #         0] == 10
        #
        # # check final features
        # final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
        #     'params:l4_usage_ru_a_vas_postpaid_prepaid_features')).orderBy(F.col("start_of_week").desc())
        #
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'min_usg_vas_total_number_of_call_min_weekly_last_week').collect()[0][0] == 1
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'min_usg_vas_total_number_of_call_min_weekly_last_two_week').collect()[0][0] == 1
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'min_usg_vas_total_number_of_call_min_weekly_last_four_week').collect()[0][0] == 1
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'min_usg_vas_total_number_of_call_min_weekly_last_twelve_week').collect()[0][0] == 1
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'max_usg_vas_total_number_of_call_max_weekly_last_week').collect()[0][0] == 5
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'max_usg_vas_total_number_of_call_max_weekly_last_two_week').collect()[0][0] == 5
        # assert \
        #     final_features.where("start_of_week = '2020-03-30'").select(
        #         'max_usg_vas_total_number_of_call_max_weekly_last_four_week').collect()[0][0] == 5

        # AIS DE TO ADD FEATURE HERE
        # exit(2)

    def test_incoming_sum_ir_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # min_dt = '2020-01-01'
        # max_dt = '2020-04-01'
        # #days = date_diff(min_dt, max_dt)  # range from min_date to max_date
        random.seed(100)
        print("*********************************L1 usage_incoming_sum_ir Starts***************************************")
        # random.seed(100)
        random_dur = [random.randint(1, 5) for iTemp in range(0, days)]
        # random.seed(101)
        random_no_call = [random.randint(1, 5) for iTemp in range(0, days)]
        # service_type = ['VOICE' if random.randint(1, 2) == 1 else 'SMS' for iTemp in range(0, 121)]
        day_id = generate_day_id(min_date, max_date)
        service_type = []
        for iTemp in range(0, 121):
            rand_service = random.randint(1, 3)
            if rand_service == 1:
                service_type.append('VOICE')
            elif rand_service == 2:
                service_type.append('SMS')
            else:
                service_type.append('IR_SMS')
        call_type = ['MO' if random.randint(1, 2) == 1 else 'MT' for iTemp in range(0, 121)]
        df_usg_relation_ir = spark.createDataFrame(zip(call_type, service_type, random_dur, random_no_call, day_id),
                                                   schema=['call_type', 'service_type', 'total_durations',
                                                           'total_successful_call', 'day_id']) \
            .withColumn("called_no", F.lit(1)) \
            .withColumn("caller_no", F.lit(2)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))
        # df_usg_relation_ir.orderBy("day_id", ascending=True).show()
        # df_usg_relation_ir.orderBy("day_id", ascending=False).show()
        global daily_usage_incoming_sum_ir
        daily_usage_incoming_sum_ir = node_from_config(df_usg_relation_ir, var_project_context.catalog.load(
            'params:l1_usage_incoming_call_relation_sum_ir_daily'))
        # daily_usage_incoming_sum_ir.orderBy("event_partition_date").show()

        # Test for usg_incoming_roaming_call_duration: "sum(case when service_type IN ('VOICE') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-13'").select(
                "usg_incoming_roaming_call_duration").collect()[0][
                0] == 4
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-09'").select(
                "usg_incoming_roaming_call_duration").collect()[0][
                0] == 0
        # Test for usg_incoming_roaming_number_calls: "sum(case when service_type IN ('VOICE') THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-13'").select(
                "usg_incoming_roaming_number_calls").collect()[0][
                0] == 1
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-09'").select(
                "usg_incoming_roaming_number_calls").collect()[0][
                0] == 0
        # Test for usg_incoming_roaming_total_sms: "sum(case when service_type IN ('SMS', 'IR_SMS') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_roaming_total_sms").collect()[0][
                0] == 2
        assert \
            daily_usage_incoming_sum_ir.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_roaming_total_sms").collect()[0][
                0] == 4
        # Test for usg_incoming_roaming_last_sms_date: "max(case when service_type IN ('SMS', 'IR_SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_incoming_roaming_last_sms_date").where("event_partition_date = '2020-04-01'").collect()[0][
                0] is None
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_incoming_roaming_last_sms_date").where("event_partition_date = '2020-03-31'").collect()[0][
                0] == datetime.date(2020, 3, 31)

        # Test for usg_last_sms_date: "max(case when service_type IN ('SMS', 'IR_SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_last_sms_date").where("event_partition_date = '2020-04-01'").collect()[0][
                0] is None
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_last_sms_date").where("event_partition_date = '2020-03-31'").collect()[0][
                0] == datetime.date(2020, 3, 31)
        # Test for usg_last_action_date: "max(date(day_id))"
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_last_action_date").where("event_partition_date = '2020-04-01'").collect()[0][
                0] == datetime.date(2020, 4, 1)
        # Test for usg_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_last_call_date").where("event_partition_date = '2020-04-01'").collect()[0][
                0] == datetime.date(2020, 4, 1)
        # Test for usg_incoming_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_sum_ir.select(
                "usg_incoming_last_call_date").where("event_partition_date = '2020-04-01'").collect()[0][
                0] == datetime.date(2020, 4, 1)
        print("*********************************L1 usage_incoming_sum_ir PASS***************************************")

        print("*********************************L1 usage_outgoing_sum_ir START***************************************")
        global daily_usage_outgoing_sum_ir
        daily_usage_outgoing_sum_ir = node_from_config(df_usg_relation_ir, var_project_context.catalog.load(
            'params:l1_usage_outgoing_call_relation_sum_ir_daily'))

        # daily_usage_outgoing_sum_ir.orderBy("event_partition_date").show()

        # Test for usg_outgoing_roaming_call_duration: "sum(case when service_type IN ('VOICE') THEN total_durations else 0 end)"
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_roaming_call_duration").collect()[0][
                0] == 0
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-03'").select(
                "usg_outgoing_roaming_call_duration").collect()[0][
                0] == 4
        # Test for usg_outgoing_roaming_number_calls: "sum(case when service_type IN ('VOICE') THEN total_successful_call else 0 end)"
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_roaming_number_calls").collect()[0][
                0] == 0
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-03'").select(
                "usg_outgoing_roaming_number_calls").collect()[0][
                0] == 1
        # Test for usg_outgoing_roaming_total_sms: "sum(case when service_type IN ('SMS', 'IR_SMS') THEN total_durations else 0 end)"
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_roaming_total_sms").collect()[0][
                0] == 2
        assert \
            daily_usage_outgoing_sum_ir.where("event_partition_date = '2020-01-03'").select(
                "usg_outgoing_roaming_total_sms").collect()[0][
                0] == 0
        # Test for usg_outgoing_roaming_last_sms_date: "max(case when service_type IN ('SMS', 'IR_SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_outgoing_roaming_last_sms_date").where("event_partition_date = '2020-03-28'").collect()[0][
                0] is None
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_outgoing_roaming_last_sms_date").where("event_partition_date = '2020-03-19'").collect()[0][
                0] == datetime.date(2020, 3, 19)

        # Test for usg_last_sms_date: "max(case when service_type IN ('SMS', 'IR_SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_last_sms_date").where("event_partition_date = '2020-03-28'").collect()[0][
                0] is None
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_last_sms_date").where("event_partition_date = '2020-03-19'").collect()[0][
                0] == datetime.date(2020, 3, 19)
        # Test for usg_last_action_date: "max(date(day_id))"
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_last_action_date").where("event_partition_date = '2020-03-28'").collect()[0][
                0] == datetime.date(2020, 3, 28)
        # Test for usg_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_last_call_date").where("event_partition_date = '2020-03-28'").collect()[0][
                0] == datetime.date(2020, 3, 28)
        # Test for usg_outgoing_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_outgoing_sum_ir.select(
                "usg_outgoing_last_call_date").where("event_partition_date = '2020-03-28'").collect()[0][
                0] == datetime.date(2020, 3, 28)
        print("*********************************L1 usage_outgoing_sum_ir PASS***************************************")
        # exit(2)

    def test_ru_a_vas_post_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        print("***********************L0 usage_ru_a_vas_postpaid_usg_daily generation*******************************")
        # Below section is to create dummy data.
        min_dt = '2020-01-01'
        max_dt = '2020-04-01'
        doubled_days = days * 2
        day_id = generate_day_id(min_dt, max_dt)
        day_id.extend(day_id)  # two records per day
        day_id.sort()
        # print(day_id)
        uplink_volume_kb = generate_int(doubled_days, 1, 5)
        downlink_volume_kb = generate_int(doubled_days, 1, 5)
        call_type_cd = generate_category(doubled_days, [3, 64])
        rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
        hour_id = generate_int(doubled_days, 0, 23)
        for i in range(1, len(hour_id)):
            if ((i + 1) % 2 == 0):

                while (hour_id[i] == hour_id[i - 1]):
                    new_int = generate_int(1, 0, 23)[0]
                    print("i = " + str(i))
                    print("new_int = " + str(new_int))
                    print("hour_id[i] = " + str(hour_id[i]))
                    print("hour_id[i-1] = " + str(hour_id[i - 1]))
                    hour_id[i] = new_int
        # print(hour_id)
        df_usg_ru_vas_post = spark.createDataFrame(
            zip(day_id, hour_id, uplink_volume_kb, downlink_volume_kb, call_type_cd, rat_type),
            schema=['day_id', 'hour_id', 'uplink_volume_kb', 'downlink_volume_kb', 'call_type_cd', 'rat_type']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))
        # df_usg_ru_vas_post.orderBy("day_id", ascending=True).show()
        # df_usg_ru_vas_post.orderBy("day_id", ascending=False).show()
        global daily_usg_ru_vas_post
        daily_usg_ru_vas_post = node_from_config(df_usg_ru_vas_post, var_project_context.catalog.load(
            'params:l1_usage_ru_a_vas_postpaid_usg_daily'))
        # daily_usg_ru_vas_post.orderBy("event_partition_date").show()
        print("***********************L1 usage_ru_a_vas_postpaid_usg_daily START************************************")

        ########################################################################################################
        # Test for usg_outgoing_data_volume: "sum(uplink_volume_kb)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_data_volume").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_outgoing_data_volume_4G: "sum(case when rat_type = '4G' then uplink_volume_kb else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_data_volume_4G").collect()[0][
                0] == 1
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_outgoing_data_volume_4G").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_outgoing_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') then uplink_volume_kb else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_data_volume_2G_3G").collect()[0][
                0] == 4
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_outgoing_data_volume_2G_3G").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_data_volume: "sum(downlink_volume_kb)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_incoming_data_volume").collect()[0][
                0] == 6
        ########################################################################################################
        # Test for usg_incoming_data_volume_4G: "sum(case when rat_type = '4G' then downlink_volume_kb else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_incoming_data_volume_4G").collect()[0][
                0] == 6
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_data_volume_4G").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') then downlink_volume_kb else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_incoming_data_volume_2G_3G").collect()[0][
                0] == 0
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_data_volume_2G_3G").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_data_last_action_date: "max(date(day_id))"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 10)
        ########################################################################################################
        # Test for usg_last_action_date: "max(date(day_id))"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-11'").select(
                "usg_data_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 11)
        ########################################################################################################
        # Test for usg_data_weekend_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                 THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_data_weekend_usage").collect()[0][
                0] == 9  # Sunday
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-03'").select(
                "usg_data_weekend_usage").collect()[0][
                0] == 0  # Friday
        ########################################################################################################
        # Test for usg_data_weekday_usage: "sum(case when date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                     THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-03'").select(
                "usg_data_weekday_usage").collect()[0][
                0] == 10  # Friday
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_data_weekday_usage").collect()[0][
                0] == 0  # Sunday
        ########################################################################################################
        # Test for usg_data_monday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Monday') THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-05'").select(
                "usg_data_monday_usage").collect()[0][
                0] == 0  # Sunday
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_usage").collect()[0][
                0] == 14  # Monday
        ########################################################################################################
        # Test for usg_data_monday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Monday') AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_morning_usage").collect()[0][
                0] == 8  # Monday's morning (hour_id == 9)

        ########################################################################################################
        # Test for usg_data_monday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Monday') AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_afternoon_usage").collect()[0][
                0] == 0  # Monday's morning (hour_id == 9)
        ########################################################################################################
        # Test for usg_data_monday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Monday') AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_evening_usage").collect()[0][
                0] == 0  # Monday's morning (hour_id == 9)
        ########################################################################################################
        # Test for usg_data_monday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Monday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_night_usage").collect()[0][
                0] == 6  # Monday's night (hour_id == 1)
        ########################################################################################################
        # Test for usg_data_tuesday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_usage").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_data_tuesday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_morning_usage").collect()[0][
                0] == 6
        ########################################################################################################
        # Test for usg_data_tuesday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_tuesday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_tuesday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_night_usage").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_data_wednesday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_usage").collect()[0][
                0] == 13
        ########################################################################################################
        # Test for usg_data_wednesday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_morning_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_wednesday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_wednesday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_evening_usage").collect()[0][
                0] == 6
        ########################################################################################################
        # Test for usg_data_wednesday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_thursday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Thursday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_usage").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_data_thursday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Thursday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_thursday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Thursday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_afternoon_usage").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_data_thursday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Thursday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_thursday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Thursday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_night_usage").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_data_friday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Friday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_usage").collect()[0][
                0] == 11
        ########################################################################################################
        # Test for usg_data_friday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Friday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_friday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Friday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_friday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Friday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_friday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Friday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_night_usage").collect()[0][
                0] == 11
        ########################################################################################################
        # Test for usg_data_saturday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-28'").select(
                "usg_data_saturday_usage").collect()[0][
                0] == 13
        ########################################################################################################
        # Test for usg_data_saturday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-28'").select(
                "usg_data_saturday_morning_usage").collect()[0][
                0] == 13
        ########################################################################################################
        # Test for usg_data_saturday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-28'").select(
                "usg_data_saturday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_saturday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-28'").select(
                "usg_data_saturday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_saturday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Saturday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-28'").select(
                "usg_data_saturday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_sunday_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Sunday')
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_data_sunday_usage").collect()[0][
                0] == 12
        ########################################################################################################
        # Test for usg_data_sunday_morning_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Sunday')
        #                                         AND hour_id IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_data_sunday_morning_usage").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_data_sunday_afternoon_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Sunday')
        #                                         AND hour_id IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_data_sunday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_sunday_evening_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Sunday')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_data_sunday_evening_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_sunday_night_usage: "sum(case when date_format(day_id, 'EEEE') IN ('Sunday')
        #                                         AND hour_id IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (uplink_volume_kb + downlink_volume_kb) else 0 end)"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_data_sunday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_total_data_volume: "sum(uplink_volume_kb + downlink_volume_kb)"

        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_total_data_volume").collect()[0][
                0] == 12
        ########################################################################################################
        # Test for usg_total_data_last_action_date: "max(date(day_id))"
        assert \
            daily_usg_ru_vas_post.where("event_partition_date = '2020-03-29'").select(
                "usg_total_data_last_action_date").collect()[0][
                0] == datetime.date(2020, 3, 29)
        ########################################################################################################
        print("********************************L1 usg_ru_vas_post PASS****************************************")
        # exit(2)

    def test_ru_a_gprs_cbs_usage_features(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        print("***********************L0 usage_ru_a_gprs_cbs_usage_daily generation*******************************")
        # Below section is to create dummy data.
        min_dt = '2020-01-01'
        max_dt = '2020-04-01'
        doubled_days = days * 2
        call_start_dt = generate_day_id(min_dt, max_dt)
        call_start_dt.extend(call_start_dt)  # two records per day
        call_start_dt.sort()
        # print(day_id)
        data_upload_amt = generate_int(doubled_days, 1, 5)
        data_download_amt = generate_int(doubled_days, 1, 5)
        cdr_subtype_cd = generate_category(doubled_days, ['ROAMING', 'DOMESTIC'])
        rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
        call_start_hr = generate_int(doubled_days, 0, 23)
        for i in range(1, len(call_start_hr)):
            if ((i + 1) % 2 == 0):
                while (call_start_hr[i] == call_start_hr[i - 1]):
                    new_int = generate_int(1, 0, 23)[0]
                    print("i = " + str(i))
                    print("new_int = " + str(new_int))
                    print("hour_id[i] = " + str(call_start_hr[i]))
                    print("hour_id[i-1] = " + str(call_start_hr[i - 1]))
                    call_start_hr[i] = new_int
        print(call_start_hr)
        df_usage_ru_a_gprs_cbs_usage = spark.createDataFrame(
            zip(call_start_dt, call_start_hr, data_upload_amt, data_download_amt, cdr_subtype_cd, rat_type),
            schema=['call_start_dt', 'call_start_hr', 'data_upload_amt', 'data_download_amt', 'cdr_subtype_cd',
                    'rat_type']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("call_start_dt", F.to_date('call_start_dt', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))
        # df_usage_ru_a_gprs_cbs_usage.orderBy("call_start_dt", ascending=True).show()
        # df_usage_ru_a_gprs_cbs_usage.orderBy("call_start_dt", ascending=False).show()
        global daily_usage_ru_a_gprs_cbs_usage
        daily_usage_ru_a_gprs_cbs_usage = node_from_config(df_usage_ru_a_gprs_cbs_usage,
                                                           var_project_context.catalog.load(
                                                               'params:l1_usage_ru_a_gprs_cbs_usage_daily'))
        print("***********************L1 usage_ru_a_gprs_cbs_usage_daily START************************************")
        ########################################################################################################
        # Test for usg_outgoing_data_volume: "sum(data_upload_amt)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_data_volume").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_outgoing_data_volume_4G: "sum(case when rat_type = '4G' then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_data_volume_4G").collect()[0][
                0] == 0  # today used 3G
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_outgoing_data_volume_4G").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_outgoing_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_outgoing_data_volume_2G_3G").collect()[0][
                0] == 10
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_data_volume_2G_3G").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_outgoing_roaming_data_volume: "sum(case when cdr_subtype_cd = 'ROAMING' then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_outgoing_roaming_data_volume").collect()[0][
                0] == 5
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_outgoing_roaming_data_volume").collect()[0][
                0] == 0
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_roaming_data_volume").collect()[0][
                0] == 4
        ########################################################################################################
        # Test for usg_outgoing_roaming_data_volume_4G: "sum(case when cdr_subtype_cd = 'ROAMING' AND rat_type = '4G'
        #                                                 then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_outgoing_roaming_data_volume_4G").collect()[0][
                0] == 0
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-02'").select(
                "usg_outgoing_roaming_data_volume_4G").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_outgoing_roaming_data_volume_2G_3G: "sum(case when cdr_subtype_cd = 'ROAMING' AND rat_type IN ('2G', '3G')
        #                                                 then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-02'").select(
                "usg_outgoing_roaming_data_volume_2G_3G").collect()[0][
                0] == 0
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_roaming_data_volume_2G_3G").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_outgoing_local_data_volume: "sum(case when cdr_subtype_cd = 'DOMESTIC' then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_outgoing_local_data_volume").collect()[0][
                0] == 5
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_outgoing_local_data_volume").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_outgoing_local_data_volume_4G: "sum(case when cdr_subtype_cd = 'DOMESTIC' AND rat_type = '4G'
        #                                                 then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_outgoing_local_data_volume_4G").collect()[0][
                0] == 4
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_local_data_volume_4G").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_outgoing_local_data_volume_2G_3G: "sum(case when cdr_subtype_cd = 'DOMESTIC' AND rat_type IN ('2G', '3G')
        #                                                 then data_upload_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_local_data_volume_2G_3G").collect()[0][
                0] == 2
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_outgoing_local_data_volume_2G_3G").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_incoming_data_volume: "sum(data_download_amt)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_data_volume").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_incoming_data_volume_4G: "sum(case when rat_type = '4G' then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_data_volume_4G").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_incoming_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_data_volume_2G_3G").collect()[0][
                0] == 1
        ########################################################################################################
        # Test for usg_incoming_roaming_data_volume: "sum(case when cdr_subtype_cd = 'ROAMING' then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_roaming_data_volume").collect()[0][
                0] == 6
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_roaming_data_volume").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_incoming_roaming_data_volume_4G: "sum(case when rat_type = '4G' and cdr_subtype_cd = 'ROAMING'
        #                                               then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_roaming_data_volume_4G").collect()[0][
                0] == 5
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_roaming_data_volume_4G").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_roaming_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') and cdr_subtype_cd = 'ROAMING'
        #                                               then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_roaming_data_volume_2G_3G").collect()[0][
                0] == 2
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_roaming_data_volume_2G_3G").collect()[0][
                0] == 1
        ########################################################################################################
        # Test for usg_incoming_local_data_volume: "sum(case when cdr_subtype_cd = 'DOMESTIC' then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_local_data_volume").collect()[0][
                0] == 3
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_local_data_volume").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_incoming_local_data_volume_4G: "sum(case when rat_type = '4G' and cdr_subtype_cd = 'DOMESTIC'
        #                                               then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_local_data_volume_4G").collect()[0][
                0] == 0
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_local_data_volume_4G").collect()[0][
                0] == 1
        ########################################################################################################
        # Test for usg_incoming_local_data_volume_2G_3G: "sum(case when rat_type IN ('2G', '3G') and cdr_subtype_cd = 'DOMESTIC'
        #                                               then data_download_amt else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_local_data_volume_2G_3G").collect()[0][
                0] == 7
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_local_data_volume_2G_3G").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_data_last_action_date: "max(date(call_start_dt))"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 8)
        ########################################################################################################
        # Test for usg_last_action_date: "max(date(call_start_dt))"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 8)
        ########################################################################################################
        # Test for usg_data_weekend_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday', 'Sunday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 8)
        ########################################################################################################
        # Test for usg_data_weekday_usage: "sum(case when date_format(call_start_dt, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-03'").select(
                "usg_data_weekday_usage").collect()[0][
                0] == 13
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_weekday_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_monday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Monday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_usage").collect()[0][
                0] == 14  # Monday
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_monday_usage").collect()[0][
                0] == 0  # Sunday
        ########################################################################################################
        # Test for usg_data_monday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Monday')
        #                                         AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_morning_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_monday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Monday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_monday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Monday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_evening_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_monday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Monday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-06'").select(
                "usg_data_monday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_tuesday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Tuesday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_usage").collect()[0][
                0] == 9
        ########################################################################################################
        # Test for usg_data_tuesday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Tuesday')
        #                                         AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_morning_usage").collect()[0][
                0] == 4
        ########################################################################################################
        # Test for usg_data_tuesday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Tuesday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_afternoon_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_tuesday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Tuesday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_tuesday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Tuesday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-07'").select(
                "usg_data_tuesday_night_usage").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_data_wednesday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Wednesday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_usage").collect()[0][
                0] == 11
        ########################################################################################################
        # Test for usg_data_wednesday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Wednesday')
        #                                             AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                             THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_morning_usage").collect()[0][
                0] == 6
        ########################################################################################################
        # Test for usg_data_wednesday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Wednesday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_afternoon_usage").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_data_wednesday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Wednesday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_wednesday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Wednesday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-08'").select(
                "usg_data_wednesday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_thursday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Thursday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_usage").collect()[0][
                0] == 12
        ########################################################################################################
        # Test for usg_data_thursday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Thursday')
        #                                       AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_thursday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Thursday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_afternoon_usage").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_data_thursday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Thursday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_evening_usage").collect()[0][
                0] == 4
        ########################################################################################################
        # Test for usg_data_thursday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Thursday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-09'").select(
                "usg_data_thursday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_friday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Friday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_usage").collect()[0][
                0] == 9
        ########################################################################################################
        # Test for usg_data_friday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Friday')
        #                                         AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_friday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Friday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_afternoon_usage").collect()[0][
                0] == 6
        ########################################################################################################
        # Test for usg_data_friday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Friday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_evening_usage").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_data_friday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Friday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-10'").select(
                "usg_data_friday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_saturday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_saturday_usage").collect()[0][
                0] == 14
        ########################################################################################################
        # Test for usg_data_saturday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday')
        #                                         AND call_start_hr IN (7, 8, 9, 10, 11, 12)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_saturday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_saturday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_saturday_afternoon_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_saturday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_saturday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_saturday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Saturday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-04'").select(
                "usg_data_saturday_night_usage").collect()[0][
                0] == 7
        ########################################################################################################
        # Test for usg_data_sunday_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Sunday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_data_sunday_usage").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_data_sunday_morning_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Sunday')
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_data_sunday_morning_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_sunday_afternoon_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Sunday')
        #                                         AND call_start_hr IN (13, 14, 15, 16, 17, 18)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_data_sunday_afternoon_usage").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_data_sunday_evening_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Sunday')
        #                                         AND call_start_hr IN (19, 20, 21, 22, 23, 0)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_data_sunday_evening_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_data_sunday_night_usage: "sum(case when date_format(call_start_dt, 'EEEE') IN ('Sunday')
        #                                         AND call_start_hr IN (1, 2, 3, 4, 5, 6)
        #                                         THEN (data_upload_amt + data_download_amt) else 0 end)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_data_sunday_night_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_total_data_volume: "sum(data_upload_amt + data_download_amt)"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_total_data_volume").collect()[0][
                0] == 8
        ########################################################################################################
        # Test for usg_total_data_last_action_date: "max(date(call_start_dt))"
        assert \
            daily_usage_ru_a_gprs_cbs_usage.where("event_partition_date = '2020-01-05'").select(
                "usg_total_data_last_action_date").collect()[0][
                0] == datetime.date(2020, 1, 5)
        ########################################################################################################
        print("***********************L1 usage_ru_a_gprs_cbs_usage_daily PASS************************************")
        # exit(2)

    def test_usage_incoming_call_relation_sum_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        print(
            "***********************L0 usage_incoming_call_relation_sum_daily generation*******************************")
        # Below section is to create dummy data.
        min_dt = '2020-01-01'
        max_dt = '2020-04-01'
        doubled_days = days * 2
        day_id = generate_day_id(min_dt, max_dt)
        day_id.extend(day_id)  # two records per day
        day_id.sort()
        # print(day_id)
        call_type = generate_category(doubled_days, ['MT', 'MO'])
        service_type = generate_category(doubled_days, ['VOICE', 'SMS'])
        idd_flag = generate_category(doubled_days, ['Y', 'N'])
        total_durations = generate_int(doubled_days, 0, 1000)
        total_successful_call = generate_int(doubled_days, 0, 5)
        caller_network_type = generate_category(doubled_days, ['3GPost-paid', '3GPre-paid', 'AIS',
                                                               'InternalAWN', 'AWN', 'Fixed Line-AWN',
                                                               'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT',
                                                               'DTAC', 'TRUE'])

        rat_type = generate_category(doubled_days, ['2G', '3G', '4G'])
        hour_id = generate_int(doubled_days, 0, 23)
        for i in range(1, len(hour_id)):
            if ((i + 1) % 2 == 0):
                while (hour_id[i] == hour_id[i - 1]):
                    new_int = generate_int(1, 0, 23)[0]
                    print("i = " + str(i))
                    print("new_int = " + str(new_int))
                    print("hour_id[i] = " + str(hour_id[i]))
                    print("hour_id[i-1] = " + str(hour_id[i - 1]))
                    hour_id[i] = new_int
        # print(hour_id)
        df_usage_incoming_call = spark.createDataFrame(
            zip(day_id, hour_id, call_type, service_type, idd_flag, total_durations, total_successful_call,
                caller_network_type),
            schema=['day_id', 'hour_id', 'call_type', 'service_type', 'idd_flag', 'total_durations',
                    'total_successful_call', 'caller_network_type']) \
            .withColumn("called_no", F.lit(1)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))
        df_usage_incoming_call.orderBy("day_id", ascending=True).show(50)
        # df_usage_incoming_call.orderBy("day_id", ascending=True).show()
        # df_usage_incoming_call.orderBy("day_id", ascending=False).show()
        global daily_usage_incoming_call
        daily_usage_incoming_call = node_from_config(df_usage_incoming_call, var_project_context.catalog.load(
            'params:l1_usage_incoming_call_relation_sum_daily'))
        print(
            "***********************L1 usage_incoming_call_relation_sum_daily START************************************")
        # NOTE: incoming uses call_type IN 'MT'
        ########################################################################################################
        # Test for usg_incoming_total_call_duration: "sum(case when service_type IN ('VOICE') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_total_call_duration").collect()[0][
                0] == 88
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_total_call_duration").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_local_call_duration: "sum(case when service_type IN ('VOICE')
        #                                            AND idd_flag = 'N' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_local_call_duration").collect()[0][
                0] == 314
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_local_call_duration").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_number_calls: "sum(case when service_type IN ('VOICE')
        #                                      THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_number_calls").collect()[0][
                0] == 0
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_number_calls").collect()[0][
                0] == 1
        ########################################################################################################
        # Test for usg_incoming_local_number_calls: "sum(case when service_type IN ('VOICE')
        #                                         AND idd_flag = 'N' THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_local_number_calls").collect()[0][
                0] == 0
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_local_number_calls").collect()[0][
                0] == 1
        ########################################################################################################
        # Test for usg_incoming_ais_local_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                                 AND caller_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                                 THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_ais_local_calls_duration").collect()[0][
                0] == 88
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_ais_local_calls_duration").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_ais_local_number_calls: "sum(case when service_type IN ('VOICE')
        #                                                 AND caller_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                                 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_ais_local_number_calls").collect()[0][
                0] == 1
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_ais_local_number_calls").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_outgoing_offnet_local_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                                 AND caller_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                                 THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_outgoing_offnet_local_calls_duration").collect()[0][
                0] == 0
        ##################################################################
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-01'").select(
                "usg_outgoing_offnet_local_calls_duration").collect()[0][
                0] == 506
        ########################################################################################################
        # Test for usg_incoming_offnet_local_number_calls: "sum(case when service_type IN ('VOICE')
        #                                                 AND caller_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                                 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_offnet_local_number_calls").collect()[0][
                0] == 0
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_offnet_local_number_calls").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_total_sms: "sum(case when service_type IN ('SMS') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_total_sms").collect()[0][
                0] == 645
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-01'").select(
                "usg_incoming_total_sms").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_local_sms: "sum(case when service_type IN ('SMS')
        #                                  AND idd_flag = 'N' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_local_sms").collect()[0][
                0] == 645
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_local_sms").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_local_ais_sms: "sum(case when service_type IN ('SMS')
        #                                      AND caller_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                      AND idd_flag = 'N' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_local_ais_sms").collect()[0][
                0] == 645
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_local_ais_sms").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_number_calls_upto_5_mins: "sum(case when service_type IN ('VOICE')
        #                                                 AND total_durations <= 300 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_number_calls_upto_5_mins").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_incoming_number_calls_upto_10_mins: "sum(case when service_type IN ('VOICE')
        #                                                 AND total_durations <= 600 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_number_calls_upto_10_mins").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_incoming_number_calls_upto_15_mins: "sum(case when service_type IN ('VOICE')
        #                                                 AND total_durations <= 900 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_number_calls_upto_15_mins").collect()[0][
                0] == 2  # total_duration = 252
        ########################################################################################################
        # Test for usg_incoming_number_calls_upto_20_mins: "sum(case when service_type IN ('VOICE')
        #                                                 AND total_durations <= 1200 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_number_calls_upto_20_mins").collect()[0][
                0] == 2  # total_duration = 252
        ########################################################################################################
        # Test for usg_incoming_number_calls_upto_30_mins: "sum(case when service_type IN ('VOICE')
        #                                                  AND total_durations <= 1800 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_number_calls_upto_30_mins").collect()[0][
                0] == 2  # total_duration = 252
        ########################################################################################################
        # Test for usg_incoming_number_calls_over_30_mins: "sum(case when service_type IN ('VOICE')
        #                                                  AND total_durations > 1800 THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-28'").select(
                "usg_incoming_number_calls_over_30_mins").collect()[0][
                0] == 0  # total_duration = 252
        ########################################################################################################
        # Test for usg_incoming_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_last_call_date").collect()[0][
                0] == datetime.date(2020, 1, 5)
        ########################################################################################################
        # Test for usg_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_last_call_date").collect()[0][
                0] == datetime.date(2020, 1, 5)
        ########################################################################################################
        # Test for usg_last_action_date: "max(date(day_id))"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-04-01'").select(
                "usg_last_action_date").collect()[0][
                0] == datetime.date(2020, 4, 1)
        ########################################################################################################
        # Test for usg_incoming_last_sms_date: "max(case when service_type IN ('SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_last_sms_date").collect()[0][
                0] == datetime.date(2020, 1, 4)
        ########################################################################################################
        # Test for usg_last_sms_date: "max(case when service_type IN ('SMS') THEN date(day_id) else null end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-04'").select(
                "usg_last_sms_date").collect()[0][
                0] == datetime.date(2020, 1, 4)
        ########################################################################################################
        # Test for usg_incoming_night_time_call: "sum(case when service_type IN ('VOICE')
        #                                        AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_night_time_call").collect()[0][
                0] == 4
        ########################################################################################################
        # Test for usg_incoming_morning_time_call: "sum(case when service_type IN ('VOICE')
        #                                        AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_morning_time_call").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_afternoon_time_call: "sum(case when service_type IN ('VOICE')
        #                                        AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-04-01'").select(
                "usg_incoming_afternoon_time_call").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_evening_time_call: "sum(case when service_type IN ('VOICE')
        #                                        AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_evening_time_call").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_incoming_weekday_number_calls: "sum(case when service_type IN ('VOICE')
        #                                         AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                         THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_weekday_number_calls").collect()[0][
                0] == 4
        ########################################################################################################
        # Test for usg_incoming_weekend_number_calls: "sum(case when service_type IN ('VOICE')
        #                                             AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                             THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_weekend_number_calls").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_incoming_weekday_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                         AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                         THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_weekday_calls_duration").collect()[0][
                0] == 449
        ########################################################################################################
        # Test for usg_incoming_weekend_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                             AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                             THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_weekend_calls_duration").collect()[0][
                0] == 390
        ########################################################################################################
        # Test for usg_incoming_night_time_number_sms: "sum(case when service_type IN ('SMS')
        #                                             AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_night_time_number_sms").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_morning_time_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-31'").select(
                "usg_incoming_morning_time_number_sms").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_afternoon_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-08'").select(
                "usg_incoming_afternoon_number_sms").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_incoming_evening_number_sms: "sum(case when service_type IN ('SMS')
        #                                         AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_evening_number_sms").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_incoming_weekday_number_sms: "sum(case when service_type IN ('SMS')
        #                                         AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday') THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-06'").select(
                "usg_incoming_weekday_number_sms").collect()[0][
                0] == 3
        ########################################################################################################
        # Test for usg_incoming_weekend_number_sms: "sum(case when service_type IN ('SMS')
        #                                         AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday') THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-04'").select(
                "usg_incoming_weekend_number_sms").collect()[0][
                0] == 5
        ########################################################################################################
        # Test for usg_incoming_monday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-20'").select(
                "usg_incoming_monday_voice_usage").collect()[0][
                0] == 792
        ########################################################################################################
        # Test for usg_incoming_monday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-20'").select(
                "usg_incoming_monday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_monday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-20'").select(
                "usg_incoming_monday_afternoon_voice_usage").collect()[0][
                0] == 792
        ########################################################################################################
        # Test for usg_incoming_monday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-20'").select(
                "usg_incoming_monday_evening_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_monday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-20'").select(
                "usg_incoming_monday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_tuesday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_tuesday_voice_usage").collect()[0][
                0] == 729
        ########################################################################################################
        # Test for usg_incoming_tuesday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_tuesday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_tuesday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_tuesday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_tuesday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_tuesday_evening_voice_usage").collect()[0][
                0] == 729
        ########################################################################################################
        # Test for usg_incoming_tuesday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-07'").select(
                "usg_incoming_tuesday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_wednesday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Wednesday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_wednesday_voice_usage").collect()[0][
                0] == 449
        ########################################################################################################
        # Test for usg_incoming_wednesday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_wednesday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_wednesday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_wednesday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_wednesday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_wednesday_evening_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_wednesday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Wednesday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-15'").select(
                "usg_incoming_wednesday_night_voice_usage").collect()[0][
                0] == 449
        ########################################################################################################
        # Test for usg_incoming_thursday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Thursday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_thursday_voice_usage").collect()[0][
                0] == 255
        ########################################################################################################
        # Test for usg_incoming_thursday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Thursday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_thursday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_thursday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Thursday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_thursday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_thursday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Thursday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_thursday_evening_voice_usage").collect()[0][
                0] == 255
        ########################################################################################################
        # Test for usg_incoming_thursday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Thursday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-23'").select(
                "usg_incoming_thursday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_friday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_friday_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_friday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_friday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_friday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_friday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_friday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_friday_evening_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_friday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-03'").select(
                "usg_incoming_friday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_saturday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_saturday_voice_usage").collect()[0][
                0] == 390
        ########################################################################################################
        # Test for usg_incoming_saturday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_saturday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_saturday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_saturday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_saturday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_saturday_evening_voice_usage").collect()[0][
                0] == 390
        ########################################################################################################
        # Test for usg_incoming_saturday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-25'").select(
                "usg_incoming_saturday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_sunday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday') THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_sunday_voice_usage").collect()[0][
                0] == 88
        ########################################################################################################
        # Test for usg_incoming_sunday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_sunday_morning_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_sunday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_sunday_afternoon_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_sunday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_sunday_evening_voice_usage").collect()[0][
                0] == 88
        ########################################################################################################
        # Test for usg_incoming_sunday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-05'").select(
                "usg_incoming_sunday_night_voice_usage").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for # check called_network_type DTAC/true value
        #         usg_incoming_dtac_number_calls: "sum(case when service_type IN ('VOICE')
        #                                          AND caller_network_type = 'DTAC' THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-01'").select(
                "usg_incoming_dtac_number_calls").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_dtac_call_duration: "sum(case when service_type IN ('VOICE')
        #                                          AND caller_network_type = 'DTAC' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-01-01'").select(
                "usg_incoming_dtac_call_duration").collect()[0][
                0] == 506
        ########################################################################################################
        # Test for usg_incoming_true_number_calls: "sum(case when service_type IN ('VOICE')
        #                                          AND caller_network_type = 'TRUE' THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_true_number_calls").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_true_call_duration: "sum(case when service_type IN ('VOICE')
        #                                          AND caller_network_type = 'TRUE' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_true_call_duration").collect()[0][
                0] == 0
        ########################################################################################################
        # Test for usg_incoming_dtac_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND caller_network_type = 'DTAC' THEN total_successful_call else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-31'").select(
                "usg_incoming_dtac_number_sms").collect()[0][
                0] == 2
        ########################################################################################################
        # Test for usg_incoming_true_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND caller_network_type = 'TRUE' THEN total_durations else 0 end)"
        assert \
            daily_usage_incoming_call.where("event_partition_date = '2020-03-23'").select(
                "usg_incoming_true_number_sms").collect()[0][
                0] == 426
        ########################################################################################################

        # exit(2)

    def test_l1_usage_outgoing_call_relation_sum_daily(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        random.seed(100)
        print(
            "***********************L0 usage_outgoing_call_relation_sum_daily generation*******************************")
        # Below section is to create dummy data.
        min_dt = '2020-01-01'
        max_dt = '2020-04-01'
        doubled_days = days * 2
        call_start_dt = generate_day_id(min_dt, max_dt)
        call_start_dt.extend(call_start_dt)  # two records per day
        call_start_dt.sort()

        # print(day_id)
        # _10min_totalcall = generate_int(doubled_days, 300, 600)
        # _15min_totalcall = generate_int(doubled_days,600,900)
        # _20min_totalcall = generate_int(doubled_days, 900, 1200)
        # _30min_totalcall = generate_int(doubled_days, 1200, 1800)
        # _morethan30_totalcall = generate_int(doubled_days, 1800, 2000)

        # l0_tol1_test
        service_type_ = generate_category(doubled_days, ['VOICE', 'SMS'])
        idd_flag = generate_category(doubled_days, ['Y', 'N'])
        call_type = generate_category(doubled_days, ['MO', 'MT'])
        # total_successful_call = generate_int(doubled_days, [0, 1])
        called_network_type = generate_category(doubled_days,
                                                ['TRUE', 'DTAC', '3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN',
                                                 'AWN',
                                                 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post',
                                                 'AWNINT'])

        # L1_test
        total_call = generate_int(doubled_days, 0, 2000)
        total_durations = generate_int(doubled_days, 0, 5)
        call_start_hr = generate_int(doubled_days, 0, 23)

        day_id = generate_day_id(min_dt, max_dt)
        day_id.extend(day_id)  # two records per day
        day_id.sort()
        hour_id = generate_int(doubled_days, 0, 23)
        total_successful_call = generate_int(doubled_days, 0, 30)

        for i in range(1, len(call_start_hr)):
            if ((i + 1) % 2 == 0):
                while (call_start_hr[i] == call_start_hr[i - 1]):
                    new_int = generate_int(1, 0, 23)[0]
                    print("i = " + str(i))
                    print("new_int = " + str(new_int))
                    print("hour_id[i] = " + str(call_start_hr[i]))
                    print("hour_id[i-1] = " + str(call_start_hr[i - 1]))
                    call_start_hr[i] = new_int
        print(call_start_hr)

        df_usage_outgoing_call_relation_sum_daily = spark.createDataFrame(
            zip(day_id, hour_id, total_call, total_successful_call, called_network_type, call_type, service_type_,
                idd_flag),
            schema=['day_id', 'hour_id', 'total_durations', 'total_successful_call', 'called_network_type', 'call_type',
                    'service_type', 'idd_flag']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("caller_no", F.lit(2)) \
            .withColumn("day_id", F.to_date('day_id', 'dd-MM-yyyy')) \
            .withColumn("partition_date", F.lit('20200101'))

        df_usage_outgoing_call_relation_sum_daily = df_usage_outgoing_call_relation_sum_daily \
            .withColumn("weekday", F.date_format(df_usage_outgoing_call_relation_sum_daily.day_id, 'EEEE'))

        df_usage_outgoing_call_relation_sum_daily = df_usage_outgoing_call_relation_sum_daily.where("call_type = 'MO'")
        global usage_outgoing_call_relation_sum_daily
        usage_outgoing_call_relation_sum_daily = node_from_config(df_usage_outgoing_call_relation_sum_daily,
                                                                  var_project_context.catalog.load(
                                                                      'params:l1_usage_outgoing_call_relation_sum_daily'))

        df_usage_outgoing_call_relation_sum_daily.where("day_id = '2020-01-07'").select("day_id", "hour_id", "weekday",
                                                                                        "total_durations").show()

        # print("###############################################################")
        #
        # df_usage_outgoing_call_relation_sum_daily.show(100)
        #
        # print("###############################################################")
        #
        # #usage_outgoing_call_relation_sum_daily.show(100)
        #
        # print("###############################################################")
        #
        # ################################################################################
        # #usg_outgoing_total_call_duration: "sum(case when service_type IN ('VOICE') THEN total_durations else 0 end)"
        # #
        # #usg_outgoing_total_call_duration
        sum_usg_outgoing_total_call_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE'").groupBy("day_id").agg(F.sum("total_durations").alias("total_durations"))
        # sum_usg_outgoing_total_call_duration = sum_usg_outgoing_total_call_duration.where("service_type = 'VOICE'").agg(F.sum("total_durations"))

        print("###############################################################")
        print("###############################################################")
        print("###############################################################")
        print("###############################################################")

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_total_call_duration").collect()[0][0] == check_null(sum_usg_outgoing_total_call_duration. \
                where("day_id = '2020-01-07'").select(
                "total_durations"))
        # or\
        # (usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
        # "usg_outgoing_total_call_duration").collect()[0][0] == 0)

        ###############################################################################
        # usg_outgoing_local_call_duration: "sum(case when service_type IN ('VOICE')
        #                                  AND idd_flag = 'N' THEN total_durations else 0 end)"
        # usg_outgoing_local_call_duration
        sum_usg_outgoing_local_call_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND idd_flag = 'N'").groupBy("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))
        # sum_usg_outgoing_local_call_duration = sum_usg_outgoing_local_call_duration.where("service_type = 'VOICE' AND idd_flag = 'N'").agg(F.sum("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_local_call_duration").collect()[0][0] == check_null(sum_usg_outgoing_local_call_duration. \
                where("day_id = '2020-01-07'").select(
                "total_durations"))
        # or\
        # (usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
        # "usg_outgoing_total_call_duration").collect()[0][0] == 0)

        ###############################################################################
        # usg_outgoing_number_calls: "sum(case when service_type IN ('VOICE') THEN total_successful_call else 0 end)"
        #
        # usg_outgoing_number_calls

        sum_usg_outgoing_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE'").groupBy("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls").collect()[0][0] == check_null(sum_usg_outgoing_number_calls \
                .where("day_id = '2020-01-07'").select(
                "total_successful_call"))
        # or\
        # (usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
        # "usg_outgoing_total_call_duration").collect()[0][0] == 0)

        ###############################################################################
        # usg_outgoing_local_number_calls: "sum(case when service_type IN ('VOICE')
        #                                         AND idd_flag = 'N' THEN total_successful_call else 0 end)"
        # usg_outgoing_local_number_calls
        sum_usg_outgoing_local_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND idd_flag = 'N'").groupBy("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_local_number_calls").collect()[0][0] == check_null(sum_usg_outgoing_local_number_calls \
                .where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ###############################################################################
        # usg_outgoing_ais_local_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                                 AND called_network_type IN ('3GPost-paid', '3GPre-paid',
        #                                                 'AIS','InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local',
        #                                                 'AWNFIX','3GHybrid-Post', 'AWNINT')
        #                                                 THEN total_durations else 0 end)"
        # # usg_outgoing_local_number_calls
        sum_usg_outgoing_ais_local_calls_duration = df_usage_outgoing_call_relation_sum_daily \
            .where(" (service_type = 'VOICE') AND  (called_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', "
                   "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
            .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        #
        # sum_usg_outgoing_ais_local_calls_duration.show()
        #
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_ais_local_calls_duration").collect()[0][0] == check_null(
                sum_usg_outgoing_ais_local_calls_duration. \
                    where("day_id = '2020-01-07'").select("total_durations"))
        # or \
        # (usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select
        #  ("usg_outgoing_ais_local_calls_duration").collect()[0][0] == 0)

        ################################################################################################################
        ## sum_usg_outgoing_ais_local_number_calls = df_usage_outgoing_call_relation_sum_daily \
        #                                             .where(" (service_type = 'VOICE') AND  (called_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', "
        #                                                    "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
        #                                             .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        ##

        sum_usg_outgoing_ais_local_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where(" (service_type = 'VOICE') AND  (called_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', "
                   "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_ais_local_number_calls").collect()[0][0] == check_null(
                sum_usg_outgoing_ais_local_number_calls. \
                    where("day_id = '2020-01-07'").select("total_successful_call")) \
 \
            ################################################################################################################
        # sum_usg_outgoing_offnet_local_calls_duration = df_usage_outgoing_call_relation_sum_daily \
        #                                              .where(" (service_type = 'VOICE') AND  (called_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', "
        #                                                      "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
        #                                               .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        sum_usg_outgoing_offnet_local_calls_duration = df_usage_outgoing_call_relation_sum_daily \
            .where(" (service_type = 'VOICE') AND  (called_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', "
                   "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
            .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_offnet_local_calls_duration").collect()[0][0] == check_null(
                sum_usg_outgoing_offnet_local_calls_duration. \
                    where("day_id = '2020-01-07'").select("total_durations"))

        ################################################################################################################
        # usg_outgoing_offnet_local_number_calls: "sum(case when service_type IN ('VOICE')
        #                                         AND called_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                                         THEN total_successful_call else 0 end)"

        sum_usg_outgoing_offnet_local_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where(" (service_type = 'VOICE') AND  (called_network_type NOT IN ('3GPost-paid', '3GPre-paid', 'AIS', "
                   "'InternalAWN', 'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT'))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_offnet_local_number_calls").collect()[0][0] == check_null(
                sum_usg_outgoing_offnet_local_number_calls. \
                    where("day_id = '2020-01-07'").select("total_successful_call"))

        ################################################################################################################
        # usg_outgoing_total_sms: "sum(case when service_type IN ('SMS') THEN total_durations else 0 end)"
        #
        #
        #

        sum_usg_outgoing_total_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('SMS'))") \
            .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_total_sms").collect()[0][0] == check_null(sum_usg_outgoing_total_sms. \
                where("day_id = '2020-01-07'").select(
                "total_durations"))

        ###############################################################################################################
        # usg_outgoing_local_sms: "sum(case when service_type IN ('SMS')
        #                        AND idd_flag = 'N' THEN total_durations else 0 end)"

        sum_usg_outgoing_local_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('SMS')) AND (idd_flag = 'N')") \
            .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_local_sms").collect()[0][0] == check_null(sum_usg_outgoing_local_sms. \
                where("day_id = '2020-01-07'").select(
                "total_durations"))

        ###############################################################################################################
        # usg_outgoing_local_ais_sms: "sum(case when service_type IN ('SMS')
        #                             AND called_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN',
        #                             'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')
        #                             AND idd_flag = 'N' THEN total_durations else 0 end)"

        sum_usg_outgoing_local_ais_sms = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "(service_type IN ('SMS')) AND (called_network_type IN ('3GPost-paid', '3GPre-paid', 'AIS', 'InternalAWN', "
            "'AWN', 'Fixed Line-AWN', 'AIS Local', 'AWNFIX', '3GHybrid-Post', 'AWNINT')) "
            "AND (idd_flag = 'N')") \
            .groupby("day_id").agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_local_ais_sms").collect()[0][0] == check_null(sum_usg_outgoing_local_ais_sms. \
                where("day_id = '2020-01-07'").select(
                "total_durations"))

        ###############################################################################################################
        # usg_outgoing_number_calls_upto_5_mins: "sum(case when service_type IN ('VOICE')
        #                                       AND total_durations <= 300 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_upto_5_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations <= 300") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_upto_5_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_upto_5_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ################################################################################################################
        # usg_outgoing_number_calls_upto_10_mins: "sum(case when service_type IN ('VOICE')
        #                                        AND total_durations <= 600 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_upto_10_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations <= 600") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_upto_10_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_upto_10_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        ################################################################################################################
        # usg_outgoing_number_calls_upto_15_mins: "sum(case when service_type IN ('VOICE')
        #                                        AND total_durations <= 900 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_upto_15_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations <= 900") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_upto_15_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_upto_15_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        ################################################################################################################
        # usg_outgoing_number_calls_upto_20_mins: "sum(case when service_type IN ('VOICE')
        #                                        AND total_durations <= 1200 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_upto_20_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations <= 1200") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_upto_20_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_upto_20_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        # usg_outgoing_number_calls_upto_30_mins: "sum(case when service_type IN ('VOICE')
        #                                        AND total_durations <= 1800 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_upto_30_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations <= 1800") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_upto_30_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_upto_30_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_number_calls_over_30_mins: "sum(case when service_type IN ('VOICE')
        #                                        AND total_durations <= 1800 THEN total_successful_call else 0 end)"

        sum_usg_outgoing_number_calls_over_30_mins = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND total_durations > 1800") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_number_calls_over_30_mins").collect()[0][0] == check_null(
                sum_usg_outgoing_number_calls_over_30_mins
                    .where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        # usg_outgoing_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"

        sum_usg_outgoing_last_call_date = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE')") \
            .groupby("day_id").agg(F.max("day_id").alias("date"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_last_call_date").collect()[0][0] == check_null(sum_usg_outgoing_last_call_date
                .where("day_id = '2020-01-07'").select(
                "date"))

        ###############################################################################################################
        # usg_last_call_date: "max(case when service_type IN ('VOICE') THEN date(day_id) else null end)"

        sum_usg_last_call_date = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE')") \
            .groupby("day_id").agg(F.max("day_id").alias("date"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_last_call_date").collect()[0][0] == \
            check_null(sum_usg_outgoing_last_call_date.where("day_id = '2020-01-07'").select("date"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_last_sms_date: "max(case when service_type IN ('SMS') THEN date(day_id) else null end)"

        sum_usg_outgoing_last_sms_date = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS')") \
            .groupby("day_id").agg(F.max("day_id").alias("date"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_last_sms_date").collect()[0][0] == \
            check_null(sum_usg_outgoing_last_sms_date.where("day_id = '2020-01-07'").select("date"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_last_sms_date: "max(case when service_type IN ('SMS') THEN date(day_id) else null end)"

        sum_usg_last_sms_date = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS')") \
            .groupby("day_id").agg(F.max("day_id").alias("date"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_last_sms_date").collect()[0][0] == \
            check_null(sum_usg_last_sms_date.where("day_id = '2020-01-07'").select("date"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_last_action_date: "max(date(day_id))"

        sum_usg_last_action_date = df_usage_outgoing_call_relation_sum_daily \
            .groupby("day_id").agg(F.max("day_id").alias("date"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_last_action_date").collect()[0][0] == \
            check_null(sum_usg_last_action_date.where("day_id = '2020-01-07'").select("date"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_night_time_number_calls: "sum(case when service_type IN ('VOICE')
        #                                    AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_successful_call else 0 end)"

        sum_usg_outgoing_night_time_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE') AND (hour_id IN (1, 2, 3, 4, 5, 6))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_night_time_number_calls").collect()[0][0] == \
            check_null(
                sum_usg_outgoing_night_time_number_calls.where("day_id = '2020-01-07'").select("total_successful_call"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_morning_time_number_calls: "sum(case when service_type IN ('VOICE')
        #                                 AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_successful_call else 0 end)"

        sum_usg_outgoing_morning_time_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE') AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_morning_time_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_morning_time_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_afternoon_number_calls: "sum(case when service_type IN ('VOICE')
        #                                          AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_successful_call else 0 end)"

        sum_usg_outgoing_afternoon_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE') AND (hour_id IN (13, 14, 15, 16, 17, 18))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_afternoon_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_afternoon_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_evening_number_calls: "sum(case when service_type IN ('VOICE')
        #                                 AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_successful_call else 0 end)"

        sum_usg_outgoing_evening_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('VOICE') AND (hour_id IN (19, 20, 21, 22, 23, 0))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_evening_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_evening_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ##############################################################################################################
        # usg_outgoing_weekday_number_calls: "sum(case when service_type IN ('VOICE')
        #                                AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                THEN total_successful_call else 0 end)"

        # day_of_month = df_usage_outgoing_call_relation_sum_daily.
        sum_usg_outgoing_weekday_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (weekday IN ('Monday', 'Tuesday', 'Wednesday','Thursday','Friday'))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekday_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekday_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_weekend_number_calls: "sum(case when service_type IN ('VOICE')
        #                                         AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                         THEN total_successful_call else 0 end)"

        sum_usg_outgoing_weekend_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekend_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekend_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_weekday_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                         AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                         THEN total_durations else 0 end)"

        sum_usg_outgoing_weekday_calls_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekday_calls_duration").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekday_calls_duration.where("day_id = '2020-01-07'").select(
                "total_durations"))

        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_weekend_calls_duration: "sum(case when service_type IN ('VOICE')
        #                                             AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                             THEN total_durations else 0 end)"

        sum_usg_outgoing_weekend_calls_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekend_calls_duration").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekend_calls_duration.where("day_id = '2020-01-07'").select(
                "total_durations"))
        # ##############################################################################################################
        # usg_outgoing_night_time_number_sms: "sum(case when service_type IN ('SMS')
        #                                    AND hour_id IN (1, 2, 3, 4, 5, 6) THEN total_successful_call else 0 end)"
        #
        sum_usg_outgoing_night_time_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS') AND (hour_id IN (1, 2, 3, 4, 5, 6))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_night_time_number_sms").collect()[0][0] == \
            check_null(
                sum_usg_outgoing_night_time_number_sms.where("day_id = '2020-01-07'").select("total_successful_call"))

        ##############################################################################################################
        ##############################################################################################################
        # usg_outgoing_morning_time_number_sms: "sum(case when service_type IN ('SMS')
        #                                 AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_successful_call else 0 end)"
        #
        sum_usg_outgoing_morning_time_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS') AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_morning_time_number_sms").collect()[0][0] == \
            check_null(
                sum_usg_outgoing_morning_time_number_sms.where("day_id = '2020-01-07'").select("total_successful_call"))

        ##############################################################################################################
        ##############################################################################################################
        # usg_outgoing_afternoon_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND hour_id IN (13, 14, 15, 16, 17, 18) THEN total_successful_call else 0 end)"
        #
        sum_usg_outgoing_afternoon_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS') AND (hour_id IN (13, 14, 15, 16, 17, 18))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_afternoon_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_afternoon_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ###############################################################################################################
        ###############################################################################################################
        # usg_outgoing_evening_number_sms: "sum(case when service_type IN ('SMS')
        #                                 AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_successful_call else 0 end)"

        sum_usg_outgoing_evening_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type IN ('SMS') AND (hour_id IN (19, 20, 21, 22, 23, 0))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_evening_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_evening_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ##############################################################################################################
        # usg_outgoing_weekday_number_sms: "sum(case when service_type IN ('SMS')
        #                                AND date_format(day_id, 'EEEE') NOT IN ('Saturday', 'Sunday')
        #                                THEN total_successful_call else 0 end)"

        sum_usg_outgoing_weekday_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'SMS' AND (weekday IN ('Monday', 'Tuesday', 'Wednesday','Thursday','Friday'))") \
            .groupby("day_id").agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekday_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekday_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_weekend_number_sms: "sum(case when service_type IN ('SMS')
        #                                         AND date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday')
        #                                         THEN total_successful_call else 0 end)"

        sum_usg_outgoing_weekend_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'SMS' AND (date_format(day_id, 'EEEE') IN ('Saturday', 'Sunday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_weekend_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_weekend_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_monday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Monday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_monday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Monday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_monday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_monday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_monday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_monday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Monday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_monday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_monday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_monday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_monday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Monday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_monday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_monday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_monday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_monday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Monday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_monday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_monday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_monday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_monday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Monday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_monday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_monday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_tuesday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Tuesday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_tuesday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Tuesday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_tuesday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_tuesday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_tuesday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_tuesday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Tuesday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_tuesday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_tuesday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_tuesday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_tuesday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Tuesday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_tuesday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_tuesday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_tuesday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_tuesday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Tuesday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_tuesday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_tuesday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_tuesday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_tuesday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Tuesday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_tuesday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_tuesday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_wednesday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Tuesday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_wednesday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Wednesday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_wednesday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_wednesday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_wednesday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_wednesday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Wednesday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_wednesday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_wednesday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_wednesday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_wednesday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Wednesday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_wednesday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_wednesday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_wednesday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_wednesday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Wednesday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_wednesday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_wednesday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_wednesday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_wednesday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Wednesday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_wednesday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_wednesday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_thursday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Tuesday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_thursday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Thursday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_thursday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_thursday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_thursday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Monday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_thursday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Thursday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_thursday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_thursday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_thursday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_thursday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Thursday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_thursday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_thursday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_thursday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_thursday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Thursday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_thursday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_thursday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_thursday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Tuesday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_thursday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Thursday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_thursday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_thursday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_friday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Friday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_friday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Friday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_friday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_friday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_friday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_friday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Friday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_friday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_friday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_friday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_friday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Friday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_friday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_friday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_friday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_friday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Friday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_friday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_friday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_friday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Friday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_friday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Friday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_friday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_friday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_saturday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Saturday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_saturday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_saturday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_saturday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_saturday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_saturday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Saturday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_saturday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_saturday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_saturday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_saturday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_saturday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_saturday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_saturday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_saturday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_saturday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_saturday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_saturday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Saturday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_saturday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Saturday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_saturday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_saturday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_sunday_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                  AND date_format(day_id, 'EEEE') IN ('Sunday') THEN total_durations else 0 end)"
        #
        sum_usg_outgoing_sunday_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Sunday'))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_sunday_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_sunday_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_sunday_morning_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_sunday_morning_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("(service_type IN ('VOICE')) AND (weekday IN ('Sunday')) AND (hour_id IN (7, 8, 9, 10, 11, 12))") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_sunday_morning_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_sunday_morning_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ###############################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_sunday_afternoon_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_sunday_afternoon_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Sunday'))"
                   "AND hour_id IN (13, 14, 15, 16, 17, 18)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_sunday_afternoon_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_sunday_afternoon_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_sunday_evening_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (19, 20, 21, 22, 23, 0) THEN total_durations else 0 end)"

        sum_usg_outgoing_sunday_evening_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Sunday'))"
                   "AND hour_id IN (19, 20, 21, 22, 23, 0)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_sunday_evening_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_sunday_evening_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_sunday_night_voice_usage: "sum(case when service_type IN ('VOICE')
        #                                           AND date_format(day_id, 'EEEE') IN ('Sunday')
        #                                           AND hour_id IN (7, 8, 9, 10, 11, 12) THEN total_durations else 0 end)"

        sum_usg_outgoing_sunday_night_voice_usage = df_usage_outgoing_call_relation_sum_daily \
            .where(
            "service_type = 'VOICE' AND (date_format(day_id, 'EEEE') IN ('Sunday')) AND hour_id IN (1, 2, 3, 4, 5, 6)") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))

        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_sunday_night_voice_usage").collect()[0][0] == \
            check_null(sum_usg_outgoing_sunday_night_voice_usage.where("day_id = '2020-01-07'").select(
                "total_durations"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_number_calls: "sum(case when service_type IN ('VOICE')
        #                                          AND called_network_type = 'DTAC' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND called_network_type = 'DTAC'") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_call_duration: "sum(case when service_type IN ('VOICE')
        #                                          AND called_network_type = 'DTAC' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_call_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND called_network_type = 'DTAC'") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_call_duration").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_call_duration.where("day_id = '2020-01-07'").select(
                "total_durations"))

        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND called_network_type = 'DTAC' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'SMS' AND called_network_type = 'DTAC'") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_number_calls: "sum(case when service_type IN ('VOICE')
        #                                          AND called_network_type = 'TRUE' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_number_calls = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND called_network_type = 'TRUE'") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_number_calls").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_number_calls.where("day_id = '2020-01-07'").select(
                "total_successful_call"))
        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_call_duration: "sum(case when service_type IN ('VOICE')
        #                                          AND called_network_type = 'TRUE' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_call_duration = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'VOICE' AND called_network_type = 'TRUE'") \
            .groupby("day_id") \
            .agg(F.sum("total_durations").alias("total_durations"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_call_duration").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_call_duration.where("day_id = '2020-01-07'").select(
                "total_durations"))

        ################################################################################################################
        # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#
        # usg_outgoing_dtac_number_sms: "sum(case when service_type IN ('SMS')
        #                                          AND called_network_type = 'TRUE' THEN total_successful_call else 0 end)"

        sum_usg_outgoing_dtac_number_sms = df_usage_outgoing_call_relation_sum_daily \
            .where("service_type = 'SMS' AND called_network_type = 'TRUE'") \
            .groupby("day_id") \
            .agg(F.sum("total_successful_call").alias("total_successful_call"))
        assert \
            usage_outgoing_call_relation_sum_daily.where("event_partition_date = '2020-01-07'").select(
                "usg_outgoing_dtac_number_sms").collect()[0][0] == \
            check_null(sum_usg_outgoing_dtac_number_sms.where("day_id = '2020-01-07'").select(
                "total_successful_call"))

    def test_daily_profile_for_usg(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        random.seed(100)
        print("***********************L1 usage_profile generation*******************************")
        # Below section is to create dummy data.
        min_dt = '2020-01-01'
        max_dt = '2020-04-01'
        doubled_days = days * 2
        day_id = generate_day_id(min_dt, max_dt)
        day_id.extend(day_id)  # two records per day
        day_id.sort()
        # print(day_id)
        subscription_identifier = generate_int(doubled_days, 1, 2)

        global daily_profile_feature
        daily_profile_feature = spark.createDataFrame(
            zip(day_id, subscription_identifier),
            schema=['event_partition_date', 'subscription_identifier']) \
            .withColumn("event_partition_date", F.to_date('event_partition_date', 'dd-MM-yyyy')) \
            .withColumn("access_method_num", F.lit(1))

        daily_profile_feature.orderBy("event_partition_date", ascending=True).show(50)
        exit(2)

    def test_l2_usage(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType([StructField("subscription_identifier",StringType(),True),
                                                              StructField("call_start_dt",DateType(),True),
                                                              StructField("day_id",DateType(),True),
                                                              StructField("start_of_month",DateType(),True),
                                                              StructField("start_of_week",DateType(),True),
                                                              StructField("usg_data_friday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_friday_evening_usage",StringType(),True),
                                                              StructField("usg_data_friday_morning_usage",StringType(),True),
                                                              StructField("usg_data_friday_night_usage",StringType(),True),
                                                              StructField("usg_data_friday_usage",StringType(),True),
                                                              StructField("usg_data_last_action_date",DateType(),True),
                                                              StructField("usg_data_monday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_monday_evening_usage",StringType(),True),
                                                              StructField("usg_data_monday_morning_usage",StringType(),True),
                                                              StructField("usg_data_monday_night_usage",StringType(),True),
                                                              StructField("usg_data_monday_usage",StringType(),True),
                                                              StructField("usg_data_saturday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_saturday_evening_usage",StringType(),True),
                                                              StructField("usg_data_saturday_morning_usage",StringType(),True),
                                                              StructField("usg_data_saturday_night_usage",StringType(),True),
                                                              StructField("usg_data_saturday_usage",StringType(),True),
                                                              StructField("usg_data_sunday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_sunday_evening_usage",StringType(),True),
                                                              StructField("usg_data_sunday_morning_usage",StringType(),True),
                                                              StructField("usg_data_sunday_night_usage",StringType(),True),
                                                              StructField("usg_data_sunday_usage",StringType(),True),
                                                              StructField("usg_data_thursday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_thursday_evening_usage",StringType(),True),
                                                              StructField("usg_data_thursday_morning_usage",StringType(),True),
                                                              StructField("usg_data_thursday_night_usage",StringType(),True),
                                                              StructField("usg_data_thursday_usage",StringType(),True),
                                                              StructField("usg_data_tuesday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_tuesday_evening_usage",StringType(),True),
                                                              StructField("usg_data_tuesday_morning_usage",StringType(),True),
                                                              StructField("usg_data_tuesday_night_usage",StringType(),True),
                                                              StructField("usg_data_tuesday_usage",StringType(),True),
                                                              StructField("usg_data_wednesday_afternoon_usage",StringType(),True),
                                                              StructField("usg_data_wednesday_evening_usage",StringType(),True),
                                                              StructField("usg_data_wednesday_morning_usage",StringType(),True),
                                                              StructField("usg_data_wednesday_night_usage",StringType(),True),
                                                              StructField("usg_data_wednesday_usage",StringType(),True),
                                                              StructField("usg_data_weekday_usage",StringType(),True),
                                                              StructField("usg_data_weekend_usage",StringType(),True),
                                                              StructField("usg_incoming_afternoon_number_sms",StringType(),True),
                                                              StructField("usg_incoming_afternoon_time_call",StringType(),True),
                                                              StructField("usg_incoming_ais_local_calls_duration",StringType(),True),
                                                              StructField("usg_incoming_ais_local_number_calls",StringType(),True),
                                                              StructField("usg_incoming_data_volume",StringType(),True),
                                                              StructField("usg_incoming_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_incoming_data_volume_4G",StringType(),True),
                                                              StructField("usg_incoming_dtac_call_duration",StringType(),True),
                                                              StructField("usg_incoming_dtac_number_calls",StringType(),True),
                                                              StructField("usg_incoming_dtac_number_sms",StringType(),True),
                                                              StructField("usg_incoming_evening_number_sms",StringType(),True),
                                                              StructField("usg_incoming_evening_time_call",StringType(),True),
                                                              StructField("usg_incoming_friday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_friday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_friday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_friday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_friday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_last_call_date",DateType(),True),
                                                              StructField("usg_incoming_last_sms_date",DateType(),True),
                                                              StructField("usg_incoming_local_ais_sms",StringType(),True),
                                                              StructField("usg_incoming_local_call_duration",StringType(),True),
                                                              StructField("usg_incoming_local_data_volume",StringType(),True),
                                                              StructField("usg_incoming_local_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_incoming_local_data_volume_4G",StringType(),True),
                                                              StructField("usg_incoming_local_number_calls",StringType(),True),
                                                              StructField("usg_incoming_local_sms",StringType(),True),
                                                              StructField("usg_incoming_monday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_monday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_monday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_monday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_monday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_morning_time_call",StringType(),True),
                                                              StructField("usg_incoming_morning_time_number_sms",StringType(),True),
                                                              StructField("usg_incoming_night_time_call",StringType(),True),
                                                              StructField("usg_incoming_night_time_number_sms",StringType(),True),
                                                              StructField("usg_incoming_number_calls",StringType(),True),
                                                              StructField("usg_incoming_number_calls_over_30_mins",StringType(),True),
                                                              StructField("usg_incoming_number_calls_upto_10_mins",StringType(),True),
                                                              StructField("usg_incoming_number_calls_upto_15_mins",StringType(),True),
                                                              StructField("usg_incoming_number_calls_upto_20_mins",StringType(),True),
                                                              StructField("usg_incoming_number_calls_upto_30_mins",StringType(),True),
                                                              StructField("usg_incoming_number_calls_upto_5_mins",StringType(),True),
                                                              StructField("usg_incoming_offnet_local_number_calls",StringType(),True),
                                                              StructField("usg_incoming_roaming_call_duration",StringType(),True),
                                                              StructField("usg_incoming_roaming_data_volume",StringType(),True),
                                                              StructField("usg_incoming_roaming_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_incoming_roaming_data_volume_4G",StringType(),True),
                                                              StructField("usg_incoming_roaming_last_sms_date",DateType(),True),
                                                              StructField("usg_incoming_roaming_number_calls",StringType(),True),
                                                              StructField("usg_incoming_roaming_total_sms",StringType(),True),
                                                              StructField("usg_incoming_saturday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_saturday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_saturday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_saturday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_saturday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_sunday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_sunday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_sunday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_sunday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_sunday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_thursday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_thursday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_thursday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_thursday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_thursday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_total_call_duration",StringType(),True),
                                                              StructField("usg_incoming_total_sms",StringType(),True),
                                                              StructField("usg_incoming_true_call_duration",StringType(),True),
                                                              StructField("usg_incoming_true_number_calls",StringType(),True),
                                                              StructField("usg_incoming_true_number_sms",StringType(),True),
                                                              StructField("usg_incoming_tuesday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_tuesday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_tuesday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_tuesday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_tuesday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_wednesday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_wednesday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_wednesday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_wednesday_night_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_wednesday_voice_usage",StringType(),True),
                                                              StructField("usg_incoming_weekday_calls_duration",StringType(),True),
                                                              StructField("usg_incoming_weekday_number_calls",StringType(),True),
                                                              StructField("usg_incoming_weekday_number_sms",StringType(),True),
                                                              StructField("usg_incoming_weekend_calls_duration",StringType(),True),
                                                              StructField("usg_incoming_weekend_number_calls",StringType(),True),
                                                              StructField("usg_incoming_weekend_number_sms",StringType(),True),
                                                              StructField("usg_last_action_date",DateType(),True),
                                                              StructField("usg_last_call_date",DateType(),True),
                                                              StructField("usg_last_sms_date",DateType(),True),
                                                              StructField("usg_outgoing_afternoon_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_afternoon_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_ais_local_calls_duration",StringType(),True),
                                                              StructField("usg_outgoing_ais_local_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_data_volume",StringType(),True),
                                                              StructField("usg_outgoing_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_outgoing_data_volume_4G",StringType(),True),
                                                              StructField("usg_outgoing_dtac_call_duration",StringType(),True),
                                                              StructField("usg_outgoing_dtac_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_dtac_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_evening_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_evening_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_friday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_friday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_friday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_friday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_friday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_last_call_date",DateType(),True),
                                                              StructField("usg_outgoing_last_sms_date",DateType(),True),
                                                              StructField("usg_outgoing_local_ais_sms",StringType(),True),
                                                              StructField("usg_outgoing_local_call_duration",StringType(),True),
                                                              StructField("usg_outgoing_local_data_volume",StringType(),True),
                                                              StructField("usg_outgoing_local_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_outgoing_local_data_volume_4G",StringType(),True),
                                                              StructField("usg_outgoing_local_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_local_sms",StringType(),True),
                                                              StructField("usg_outgoing_monday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_monday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_monday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_monday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_monday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_morning_time_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_morning_time_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_night_time_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_night_time_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_over_30_mins",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_upto_10_mins",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_upto_15_mins",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_upto_20_mins",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_upto_30_mins",StringType(),True),
                                                              StructField("usg_outgoing_number_calls_upto_5_mins",StringType(),True),
                                                              StructField("usg_outgoing_offnet_local_calls_duration",StringType(),True),
                                                              StructField("usg_outgoing_offnet_local_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_roaming_call_duration",StringType(),True),
                                                              StructField("usg_outgoing_roaming_data_volume",StringType(),True),
                                                              StructField("usg_outgoing_roaming_data_volume_2G_3G",StringType(),True),
                                                              StructField("usg_outgoing_roaming_data_volume_4G",StringType(),True),
                                                              StructField("usg_outgoing_roaming_last_sms_date",DateType(),True),
                                                              StructField("usg_outgoing_roaming_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_roaming_total_sms",StringType(),True),
                                                              StructField("usg_outgoing_saturday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_saturday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_saturday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_saturday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_saturday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_sunday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_sunday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_sunday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_sunday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_sunday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_thursday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_thursday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_thursday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_thursday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_thursday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_total_call_duration",StringType(),True),
                                                              StructField("usg_outgoing_total_sms",StringType(),True),
                                                              StructField("usg_outgoing_true_call_duration",StringType(),True),
                                                              StructField("usg_outgoing_true_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_true_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_tuesday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_tuesday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_tuesday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_tuesday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_tuesday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_wednesday_afternoon_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_wednesday_evening_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_wednesday_morning_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_wednesday_night_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_wednesday_voice_usage",StringType(),True),
                                                              StructField("usg_outgoing_weekday_calls_duration",StringType(),True),
                                                              StructField("usg_outgoing_weekday_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_weekday_number_sms",StringType(),True),
                                                              StructField("usg_outgoing_weekend_calls_duration",StringType(),True),
                                                              StructField("usg_outgoing_weekend_number_calls",StringType(),True),
                                                              StructField("usg_outgoing_weekend_number_sms",StringType(),True),
                                                              StructField("usg_total_data_last_action_date",DateType(),True),
                                                              StructField("usg_total_data_volume",StringType(),True),
                                                              StructField("usg_vas_last_action_dt",DateType(),True),
                                                              StructField("usg_vas_total_number_of_call",StringType(),True),
                                                              StructField("event_partition_date",DateType(),True),
                                                              ]))

        # #################################### Sum Test ###########################################
        #
        #
        # df_l1_agg_sum = df_l1_test.groupby("subscription_identifier","start_of_week").agg(F.sum("usg_incoming_afternoon_number_sms").alias("usg_incoming_afternoon_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_afternoon_time_call").alias("usg_incoming_afternoon_time_call_sum"),
        #                                                                                 F.sum("usg_incoming_ais_local_calls_duration").alias("usg_incoming_ais_local_calls_duration_sum"),
        #                                                                                 F.sum("usg_incoming_ais_local_number_calls").alias("usg_incoming_ais_local_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_dtac_call_duration").alias("usg_incoming_dtac_call_duration_sum"),
        #                                                                                 F.sum("usg_incoming_dtac_number_calls").alias("usg_incoming_dtac_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_dtac_number_sms").alias("usg_incoming_dtac_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_evening_number_sms").alias("usg_incoming_evening_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_evening_time_call").alias("usg_incoming_evening_time_call_sum"),
        #                                                                                 F.sum("usg_incoming_friday_afternoon_voice_usage").alias("usg_incoming_friday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_friday_evening_voice_usage").alias("usg_incoming_friday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_friday_morning_voice_usage").alias("usg_incoming_friday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_friday_night_voice_usage").alias("usg_incoming_friday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_friday_voice_usage").alias("usg_incoming_friday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_local_ais_sms").alias("usg_incoming_local_ais_sms_sum"),
        #                                                                                 F.sum("usg_incoming_local_call_duration").alias("usg_incoming_local_call_duration_sum"),
        #                                                                                 F.sum("usg_incoming_local_number_calls").alias("usg_incoming_local_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_local_sms").alias("usg_incoming_local_sms_sum"),
        #                                                                                 F.sum("usg_incoming_monday_afternoon_voice_usage").alias("usg_incoming_monday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_monday_evening_voice_usage").alias("usg_incoming_monday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_monday_morning_voice_usage").alias("usg_incoming_monday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_monday_night_voice_usage").alias("usg_incoming_monday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_monday_voice_usage").alias("usg_incoming_monday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_morning_time_call").alias("usg_incoming_morning_time_call_sum"),
        #                                                                                 F.sum("usg_incoming_morning_time_number_sms").alias("usg_incoming_morning_time_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_night_time_call").alias("usg_incoming_night_time_call_sum"),
        #                                                                                 F.sum("usg_incoming_night_time_number_sms").alias("usg_incoming_night_time_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls").alias("usg_incoming_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_over_30_mins").alias("usg_incoming_number_calls_over_30_mins_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_upto_10_mins").alias("usg_incoming_number_calls_upto_10_mins_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_upto_15_mins").alias("usg_incoming_number_calls_upto_15_mins_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_upto_20_mins").alias("usg_incoming_number_calls_upto_20_mins_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_upto_30_mins").alias("usg_incoming_number_calls_upto_30_mins_sum"),
        #                                                                                 F.sum("usg_incoming_number_calls_upto_5_mins").alias("usg_incoming_number_calls_upto_5_mins_sum"),
        #                                                                                 F.sum("usg_incoming_offnet_local_number_calls").alias("usg_incoming_offnet_local_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_saturday_afternoon_voice_usage").alias("usg_incoming_saturday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_saturday_evening_voice_usage").alias("usg_incoming_saturday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_saturday_morning_voice_usage").alias("usg_incoming_saturday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_saturday_night_voice_usage").alias("usg_incoming_saturday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_saturday_voice_usage").alias("usg_incoming_saturday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_sunday_afternoon_voice_usage").alias("usg_incoming_sunday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_sunday_evening_voice_usage").alias("usg_incoming_sunday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_sunday_morning_voice_usage").alias("usg_incoming_sunday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_sunday_night_voice_usage").alias("usg_incoming_sunday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_sunday_voice_usage").alias("usg_incoming_sunday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_thursday_afternoon_voice_usage").alias("usg_incoming_thursday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_thursday_evening_voice_usage").alias("usg_incoming_thursday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_thursday_morning_voice_usage").alias("usg_incoming_thursday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_thursday_night_voice_usage").alias("usg_incoming_thursday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_thursday_voice_usage").alias("usg_incoming_thursday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_total_call_duration").alias("usg_incoming_total_call_duration_sum"),
        #                                                                                 F.sum("usg_incoming_total_sms").alias("usg_incoming_total_sms_sum"),
        #                                                                                 F.sum("usg_incoming_true_call_duration").alias("usg_incoming_true_call_duration_sum"),
        #                                                                                 F.sum("usg_incoming_true_number_calls").alias("usg_incoming_true_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_true_number_sms").alias("usg_incoming_true_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_tuesday_afternoon_voice_usage").alias("usg_incoming_tuesday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_tuesday_evening_voice_usage").alias("usg_incoming_tuesday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_tuesday_morning_voice_usage").alias("usg_incoming_tuesday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_tuesday_night_voice_usage").alias("usg_incoming_tuesday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_tuesday_voice_usage").alias("usg_incoming_tuesday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_wednesday_afternoon_voice_usage").alias("usg_incoming_wednesday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_wednesday_evening_voice_usage").alias("usg_incoming_wednesday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_wednesday_morning_voice_usage").alias("usg_incoming_wednesday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_wednesday_night_voice_usage").alias("usg_incoming_wednesday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_wednesday_voice_usage").alias("usg_incoming_wednesday_voice_usage_sum"),
        #                                                                                 F.sum("usg_incoming_weekday_calls_duration").alias("usg_incoming_weekday_calls_duration_sum"),
        #                                                                                 F.sum("usg_incoming_weekday_number_calls").alias("usg_incoming_weekday_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_weekday_number_sms").alias("usg_incoming_weekday_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_weekend_calls_duration").alias("usg_incoming_weekend_calls_duration_sum"),
        #                                                                                 F.sum("usg_incoming_weekend_number_calls").alias("usg_incoming_weekend_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_weekend_number_sms").alias("usg_incoming_weekend_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_afternoon_number_calls").alias("usg_outgoing_afternoon_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_afternoon_number_sms").alias("usg_outgoing_afternoon_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_ais_local_calls_duration").alias("usg_outgoing_ais_local_calls_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_ais_local_number_calls").alias("usg_outgoing_ais_local_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_dtac_call_duration").alias("usg_outgoing_dtac_call_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_dtac_number_calls").alias("usg_outgoing_dtac_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_dtac_number_sms").alias("usg_outgoing_dtac_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_evening_number_calls").alias("usg_outgoing_evening_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_evening_number_sms").alias("usg_outgoing_evening_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_friday_afternoon_voice_usage").alias("usg_outgoing_friday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_friday_evening_voice_usage").alias("usg_outgoing_friday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_friday_morning_voice_usage").alias("usg_outgoing_friday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_friday_night_voice_usage").alias("usg_outgoing_friday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_friday_voice_usage").alias("usg_outgoing_friday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_local_ais_sms").alias("usg_outgoing_local_ais_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_local_call_duration").alias("usg_outgoing_local_call_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_local_number_calls").alias("usg_outgoing_local_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_local_sms").alias("usg_outgoing_local_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_monday_afternoon_voice_usage").alias("usg_outgoing_monday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_monday_evening_voice_usage").alias("usg_outgoing_monday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_monday_morning_voice_usage").alias("usg_outgoing_monday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_monday_night_voice_usage").alias("usg_outgoing_monday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_monday_voice_usage").alias("usg_outgoing_monday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_morning_time_number_calls").alias("usg_outgoing_morning_time_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_morning_time_number_sms").alias("usg_outgoing_morning_time_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_night_time_number_calls").alias("usg_outgoing_night_time_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_night_time_number_sms").alias("usg_outgoing_night_time_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls").alias("usg_outgoing_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_over_30_mins").alias("usg_outgoing_number_calls_over_30_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_upto_10_mins").alias("usg_outgoing_number_calls_upto_10_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_upto_15_mins").alias("usg_outgoing_number_calls_upto_15_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_upto_20_mins").alias("usg_outgoing_number_calls_upto_20_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_upto_30_mins").alias("usg_outgoing_number_calls_upto_30_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_number_calls_upto_5_mins").alias("usg_outgoing_number_calls_upto_5_mins_sum"),
        #                                                                                 F.sum("usg_outgoing_offnet_local_calls_duration").alias("usg_outgoing_offnet_local_calls_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_offnet_local_number_calls").alias("usg_outgoing_offnet_local_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_saturday_afternoon_voice_usage").alias("usg_outgoing_saturday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_saturday_evening_voice_usage").alias("usg_outgoing_saturday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_saturday_morning_voice_usage").alias("usg_outgoing_saturday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_saturday_night_voice_usage").alias("usg_outgoing_saturday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_saturday_voice_usage").alias("usg_outgoing_saturday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_sunday_afternoon_voice_usage").alias("usg_outgoing_sunday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_sunday_evening_voice_usage").alias("usg_outgoing_sunday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_sunday_morning_voice_usage").alias("usg_outgoing_sunday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_sunday_night_voice_usage").alias("usg_outgoing_sunday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_sunday_voice_usage").alias("usg_outgoing_sunday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_thursday_afternoon_voice_usage").alias("usg_outgoing_thursday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_thursday_evening_voice_usage").alias("usg_outgoing_thursday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_thursday_morning_voice_usage").alias("usg_outgoing_thursday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_thursday_night_voice_usage").alias("usg_outgoing_thursday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_thursday_voice_usage").alias("usg_outgoing_thursday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_total_call_duration").alias("usg_outgoing_total_call_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_total_sms").alias("usg_outgoing_total_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_true_call_duration").alias("usg_outgoing_true_call_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_true_number_calls").alias("usg_outgoing_true_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_true_number_sms").alias("usg_outgoing_true_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_tuesday_afternoon_voice_usage").alias("usg_outgoing_tuesday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_tuesday_evening_voice_usage").alias("usg_outgoing_tuesday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_tuesday_morning_voice_usage").alias("usg_outgoing_tuesday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_tuesday_night_voice_usage").alias("usg_outgoing_tuesday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_tuesday_voice_usage").alias("usg_outgoing_tuesday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_wednesday_afternoon_voice_usage").alias("usg_outgoing_wednesday_afternoon_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_wednesday_evening_voice_usage").alias("usg_outgoing_wednesday_evening_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_wednesday_morning_voice_usage").alias("usg_outgoing_wednesday_morning_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_wednesday_night_voice_usage").alias("usg_outgoing_wednesday_night_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_wednesday_voice_usage").alias("usg_outgoing_wednesday_voice_usage_sum"),
        #                                                                                 F.sum("usg_outgoing_weekday_calls_duration").alias("usg_outgoing_weekday_calls_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_weekday_number_calls").alias("usg_outgoing_weekday_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_weekday_number_sms").alias("usg_outgoing_weekday_number_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_weekend_calls_duration").alias("usg_outgoing_weekend_calls_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_weekend_number_calls").alias("usg_outgoing_weekend_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_weekend_number_sms").alias("usg_outgoing_weekend_number_sms_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_call_duration").alias("usg_incoming_roaming_call_duration_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_number_calls").alias("usg_incoming_roaming_number_calls_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_total_sms").alias("usg_incoming_roaming_total_sms_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_call_duration").alias("usg_outgoing_roaming_call_duration_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_number_calls").alias("usg_outgoing_roaming_number_calls_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_total_sms").alias("usg_outgoing_roaming_total_sms_sum"),
        #                                                                                 F.sum("usg_data_friday_usage").alias("usg_data_friday_usage_sum"),
        #                                                                                 F.sum("usg_data_monday_usage").alias("usg_data_monday_usage_sum"),
        #                                                                                 F.sum("usg_data_saturday_usage").alias("usg_data_saturday_usage_sum"),
        #                                                                                 F.sum("usg_data_sunday_usage").alias("usg_data_sunday_usage_sum"),
        #                                                                                 F.sum("usg_data_thursday_usage").alias("usg_data_thursday_usage_sum"),
        #                                                                                 F.sum("usg_data_tuesday_usage").alias("usg_data_tuesday_usage_sum"),
        #                                                                                 F.sum("usg_data_wednesday_usage").alias("usg_data_wednesday_usage_sum"),
        #                                                                                 F.sum("usg_incoming_data_volume").alias("usg_incoming_data_volume_sum"),
        #                                                                                 F.sum("usg_incoming_data_volume_2G_3G").alias("usg_incoming_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_incoming_data_volume_4G").alias("usg_incoming_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_incoming_local_data_volume").alias("usg_incoming_local_data_volume_sum"),
        #                                                                                 F.sum("usg_incoming_local_data_volume_2G_3G").alias("usg_incoming_local_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_incoming_local_data_volume_4G").alias("usg_incoming_local_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_data_volume").alias("usg_incoming_roaming_data_volume_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_data_volume_2G_3G").alias("usg_incoming_roaming_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_incoming_roaming_data_volume_4G").alias("usg_incoming_roaming_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_outgoing_data_volume").alias("usg_outgoing_data_volume_sum"),
        #                                                                                 F.sum("usg_outgoing_data_volume_2G_3G").alias("usg_outgoing_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_outgoing_data_volume_4G").alias("usg_outgoing_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_outgoing_local_data_volume").alias("usg_outgoing_local_data_volume_sum"),
        #                                                                                 F.sum("usg_outgoing_local_data_volume_2G_3G").alias("usg_outgoing_local_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_outgoing_local_data_volume_4G").alias("usg_outgoing_local_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_data_volume").alias("usg_outgoing_roaming_data_volume_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_data_volume_2G_3G").alias("usg_outgoing_roaming_data_volume_2G_3G_sum"),
        #                                                                                 F.sum("usg_outgoing_roaming_data_volume_4G").alias("usg_outgoing_roaming_data_volume_4G_sum"),
        #                                                                                 F.sum("usg_total_data_volume").alias("usg_total_data_volume_sum"),
        #                                                                                 F.sum("usg_data_monday_afternoon_usage").alias("usg_data_monday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_tuesday_afternoon_usage").alias("usg_data_tuesday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_wednesday_afternoon_usage").alias("usg_data_wednesday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_thursday_afternoon_usage").alias("usg_data_thursday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_friday_afternoon_usage").alias("usg_data_friday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_saturday_afternoon_usage").alias("usg_data_saturday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_sunday_afternoon_usage").alias("usg_data_sunday_afternoon_usage_sum"),
        #                                                                                 F.sum("usg_data_monday_morning_usage").alias("usg_data_monday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_tuesday_morning_usage").alias("usg_data_tuesday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_wednesday_morning_usage").alias("usg_data_wednesday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_thursday_morning_usage").alias("usg_data_thursday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_friday_morning_usage").alias("usg_data_friday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_saturday_morning_usage").alias("usg_data_saturday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_sunday_morning_usage").alias("usg_data_sunday_morning_usage_sum"),
        #                                                                                 F.sum("usg_data_monday_evening_usage").alias("usg_data_monday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_tuesday_evening_usage").alias("usg_data_tuesday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_wednesday_evening_usage").alias("usg_data_wednesday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_thursday_evening_usage").alias("usg_data_thursday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_friday_evening_usage").alias("usg_data_friday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_saturday_evening_usage").alias("usg_data_saturday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_sunday_evening_usage").alias("usg_data_sunday_evening_usage_sum"),
        #                                                                                 F.sum("usg_data_monday_night_usage").alias("usg_data_monday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_tuesday_night_usage").alias("usg_data_tuesday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_wednesday_night_usage").alias("usg_data_wednesday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_thursday_night_usage").alias("usg_data_thursday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_friday_night_usage").alias("usg_data_friday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_saturday_night_usage").alias("usg_data_saturday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_sunday_night_usage").alias("usg_data_sunday_night_usage_sum"),
        #                                                                                 F.sum("usg_data_weekend_usage").alias("usg_data_weekend_usage_sum"),
        #                                                                                 F.sum("usg_data_weekday_usage").alias("usg_data_weekday_usage_sum"),
        #                                                                                 F.sum("usg_vas_total_number_of_call").alias("usg_vas_total_number_of_call_sum"))

        # df_l1_agg_sum.select("subscription_identifier", "start_of_week").show()


        sum_l2 = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))


        ########################################### TEST ZONE ######################################################

        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_time_call_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_time_call_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_time_call_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_time_call_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_friday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_friday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_friday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_friday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_friday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_friday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_friday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_friday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_friday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_friday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_ais_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_ais_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_monday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_monday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_monday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_monday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_monday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_monday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_monday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_monday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_monday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_monday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_call_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_call_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_call_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_call_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_over_30_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_over_30_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_10_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_10_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_15_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_15_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_20_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_20_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_30_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_30_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_5_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_5_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_offnet_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_offnet_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_saturday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_saturday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_saturday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_saturday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_saturday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_saturday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_saturday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_saturday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_saturday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_saturday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_sunday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_sunday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_sunday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_sunday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_sunday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_sunday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_sunday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_sunday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_sunday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_sunday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_thursday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_thursday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_thursday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_thursday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_thursday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_thursday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_thursday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_thursday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_thursday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_thursday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_tuesday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_tuesday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_tuesday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_tuesday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_tuesday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_tuesday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_tuesday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_tuesday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_tuesday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_tuesday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_wednesday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_wednesday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_wednesday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_wednesday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_wednesday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_wednesday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_wednesday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_wednesday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_wednesday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_wednesday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekday_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekday_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekday_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekday_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekday_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekday_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekend_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekend_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekend_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekend_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_weekend_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_weekend_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_friday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_friday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_friday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_friday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_friday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_friday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_friday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_friday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_friday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_friday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_ais_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_ais_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_monday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_monday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_monday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_monday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_monday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_monday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_monday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_monday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_monday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_monday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_over_30_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_over_30_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_10_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_10_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_15_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_15_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_20_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_20_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_30_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_30_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_5_mins_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_5_mins_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_saturday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_saturday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_saturday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_saturday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_saturday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_saturday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_saturday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_saturday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_saturday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_saturday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_sunday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_sunday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_sunday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_sunday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_sunday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_sunday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_sunday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_sunday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_sunday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_sunday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_thursday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_thursday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_thursday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_thursday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_thursday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_thursday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_thursday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_thursday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_thursday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_thursday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_tuesday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_tuesday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_tuesday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_tuesday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_tuesday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_tuesday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_tuesday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_tuesday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_tuesday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_tuesday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_wednesday_afternoon_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_wednesday_afternoon_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_wednesday_evening_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_wednesday_evening_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_wednesday_morning_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_wednesday_morning_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_wednesday_night_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_wednesday_night_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_wednesday_voice_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_wednesday_voice_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekday_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekday_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekday_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekday_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekday_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekday_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekend_calls_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekend_calls_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekend_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekend_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_weekend_number_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_weekend_number_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_total_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_total_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_call_duration_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_call_duration_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_number_calls_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_number_calls_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_total_sms_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_total_sms_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_friday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_friday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_monday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_monday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_saturday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_saturday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_sunday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_sunday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_thursday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_thursday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_tuesday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_tuesday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_wednesday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_wednesday_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_2G_3G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_2G_3G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_4G_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_4G_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_total_data_volume_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_total_data_volume_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_monday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_monday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_tuesday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_tuesday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_wednesday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_wednesday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_thursday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_thursday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_friday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_friday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_saturday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_saturday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_sunday_afternoon_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_sunday_afternoon_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_monday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_monday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_tuesday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_tuesday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_wednesday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_wednesday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_thursday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_thursday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_friday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_friday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_saturday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_saturday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_sunday_morning_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_sunday_morning_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_monday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_monday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_tuesday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_tuesday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_wednesday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_wednesday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_thursday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_thursday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_friday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_friday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_saturday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_saturday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_sunday_evening_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_sunday_evening_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_monday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_monday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_tuesday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_tuesday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_wednesday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_wednesday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_thursday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_thursday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_friday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_friday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_saturday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_saturday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_sunday_night_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_sunday_night_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_weekend_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_weekend_usage_sum"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_weekday_usage_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_weekday_usage_sum"))
        #
        # exit(2)
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_vas_total_number_of_call_sum").collect()[0][0] == check_null(
        #     df_l1_agg_sum.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_vas_total_number_of_call_sum"))

        #################################### Sum avg ###########################################

        # df_l1_agg = df_l1_test.groupby("subscription_identifier","start_of_week").agg(F.max("usg_last_sms_date").alias("usg_last_sms_date_max"),
        #                                                                                 F.max("usg_incoming_last_sms_date").alias("usg_incoming_last_sms_date_max"),
        #                                                                                 F.max("usg_outgoing_roaming_last_sms_date").alias("usg_outgoing_roaming_last_sms_date_max"),
        #                                                                                 F.max("usg_incoming_roaming_last_sms_date").alias("usg_incoming_roaming_last_sms_date_max"),
        #                                                                                 F.max("usg_last_action_date").alias("usg_last_action_date_max"),
        #                                                                                 F.max("usg_incoming_afternoon_number_sms").alias("usg_incoming_afternoon_number_sms_max"),
        #                                                                                 F.max("usg_incoming_afternoon_time_call").alias("usg_incoming_afternoon_time_call_max"),
        #                                                                                 F.max("usg_incoming_last_call_date").alias("usg_incoming_last_call_date_max"),
        #                                                                                 F.max("usg_last_call_date").alias("usg_last_call_date_max"),
        #                                                                                 F.max("usg_outgoing_last_call_date").alias("usg_outgoing_last_call_date_max"),
        #                                                                                 F.max("usg_outgoing_last_sms_date").alias("usg_outgoing_last_sms_date_max"),
        #                                                                                 F.max("usg_incoming_ais_local_calls_duration").alias("usg_incoming_ais_local_calls_duration_max"),
        #                                                                                 F.max("usg_incoming_ais_local_number_calls").alias("usg_incoming_ais_local_number_calls_max"),
        #                                                                                 F.max("usg_incoming_dtac_call_duration").alias("usg_incoming_dtac_call_duration_max"),
        #                                                                                 F.max("usg_incoming_dtac_number_calls").alias("usg_incoming_dtac_number_calls_max"),
        #                                                                                 F.max("usg_incoming_dtac_number_sms").alias("usg_incoming_dtac_number_sms_max"),
        #                                                                                 F.max("usg_incoming_evening_number_sms").alias("usg_incoming_evening_number_sms_max"),
        #                                                                                 F.max("usg_incoming_evening_time_call").alias("usg_incoming_evening_time_call_max"),
        #                                                                                 F.max("usg_incoming_local_ais_sms").alias("usg_incoming_local_ais_sms_max"),
        #                                                                                 F.max("usg_incoming_local_call_duration").alias("usg_incoming_local_call_duration_max"),
        #                                                                                 F.max("usg_incoming_local_number_calls").alias("usg_incoming_local_number_calls_max"),
        #                                                                                 F.max("usg_incoming_local_sms").alias("usg_incoming_local_sms_max"),
        #                                                                                 F.max("usg_incoming_morning_time_call").alias("usg_incoming_morning_time_call_max"),
        #                                                                                 F.max("usg_incoming_morning_time_number_sms").alias("usg_incoming_morning_time_number_sms_max"),
        #                                                                                 F.max("usg_incoming_night_time_call").alias("usg_incoming_night_time_call_max"),
        #                                                                                 F.max("usg_incoming_night_time_number_sms").alias("usg_incoming_night_time_number_sms_max"),
        #                                                                                 F.max("usg_incoming_number_calls").alias("usg_incoming_number_calls_max"),
        #                                                                                 F.max("usg_incoming_number_calls_over_30_mins").alias("usg_incoming_number_calls_over_30_mins_max"),
        #                                                                                 F.max("usg_incoming_number_calls_upto_10_mins").alias("usg_incoming_number_calls_upto_10_mins_max"),
        #                                                                                 F.max("usg_incoming_number_calls_upto_15_mins").alias("usg_incoming_number_calls_upto_15_mins_max"),
        #                                                                                 F.max("usg_incoming_number_calls_upto_20_mins").alias("usg_incoming_number_calls_upto_20_mins_max"),
        #                                                                                 F.max("usg_incoming_number_calls_upto_30_mins").alias("usg_incoming_number_calls_upto_30_mins_max"),
        #                                                                                 F.max("usg_incoming_number_calls_upto_5_mins").alias("usg_incoming_number_calls_upto_5_mins_max"),
        #                                                                                 F.max("usg_incoming_offnet_local_number_calls").alias("usg_incoming_offnet_local_number_calls_max"),
        #                                                                                 F.max("usg_incoming_total_call_duration").alias("usg_incoming_total_call_duration_max"),
        #                                                                                 F.max("usg_incoming_total_sms").alias("usg_incoming_total_sms_max"),
        #                                                                                 F.max("usg_incoming_true_call_duration").alias("usg_incoming_true_call_duration_max"),
        #                                                                                 F.max("usg_incoming_true_number_calls").alias("usg_incoming_true_number_calls_max"),
        #                                                                                 F.max("usg_incoming_true_number_sms").alias("usg_incoming_true_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_afternoon_number_calls").alias("usg_outgoing_afternoon_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_afternoon_number_sms").alias("usg_outgoing_afternoon_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_ais_local_calls_duration").alias("usg_outgoing_ais_local_calls_duration_max"),
        #                                                                                 F.max("usg_outgoing_ais_local_number_calls").alias("usg_outgoing_ais_local_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_dtac_call_duration").alias("usg_outgoing_dtac_call_duration_max"),
        #                                                                                 F.max("usg_outgoing_dtac_number_calls").alias("usg_outgoing_dtac_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_dtac_number_sms").alias("usg_outgoing_dtac_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_evening_number_calls").alias("usg_outgoing_evening_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_evening_number_sms").alias("usg_outgoing_evening_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_local_ais_sms").alias("usg_outgoing_local_ais_sms_max"),
        #                                                                                 F.max("usg_outgoing_local_call_duration").alias("usg_outgoing_local_call_duration_max"),
        #                                                                                 F.max("usg_outgoing_local_number_calls").alias("usg_outgoing_local_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_local_sms").alias("usg_outgoing_local_sms_max"),
        #                                                                                 F.max("usg_outgoing_morning_time_number_calls").alias("usg_outgoing_morning_time_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_morning_time_number_sms").alias("usg_outgoing_morning_time_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_night_time_number_calls").alias("usg_outgoing_night_time_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_night_time_number_sms").alias("usg_outgoing_night_time_number_sms_max"),
        #                                                                                 F.max("usg_outgoing_number_calls").alias("usg_outgoing_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_over_30_mins").alias("usg_outgoing_number_calls_over_30_mins_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_upto_10_mins").alias("usg_outgoing_number_calls_upto_10_mins_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_upto_15_mins").alias("usg_outgoing_number_calls_upto_15_mins_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_upto_20_mins").alias("usg_outgoing_number_calls_upto_20_mins_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_upto_30_mins").alias("usg_outgoing_number_calls_upto_30_mins_max"),
        #                                                                                 F.max("usg_outgoing_number_calls_upto_5_mins").alias("usg_outgoing_number_calls_upto_5_mins_max"),
        #                                                                                 F.max("usg_outgoing_offnet_local_calls_duration").alias("usg_outgoing_offnet_local_calls_duration_max"),
        #                                                                                 F.max("usg_outgoing_offnet_local_number_calls").alias("usg_outgoing_offnet_local_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_total_call_duration").alias("usg_outgoing_total_call_duration_max"),
        #                                                                                 F.max("usg_outgoing_total_sms").alias("usg_outgoing_total_sms_max"),
        #                                                                                 F.max("usg_outgoing_true_call_duration").alias("usg_outgoing_true_call_duration_max"),
        #                                                                                 F.max("usg_outgoing_true_number_calls").alias("usg_outgoing_true_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_true_number_sms").alias("usg_outgoing_true_number_sms_max"),
        #                                                                                 F.max("usg_incoming_roaming_call_duration").alias("usg_incoming_roaming_call_duration_max"),
        #                                                                                 F.max("usg_incoming_roaming_number_calls").alias("usg_incoming_roaming_number_calls_max"),
        #                                                                                 F.max("usg_incoming_roaming_total_sms").alias("usg_incoming_roaming_total_sms_max"),
        #                                                                                 F.max("usg_outgoing_roaming_call_duration").alias("usg_outgoing_roaming_call_duration_max"),
        #                                                                                 F.max("usg_outgoing_roaming_number_calls").alias("usg_outgoing_roaming_number_calls_max"),
        #                                                                                 F.max("usg_outgoing_roaming_total_sms").alias("usg_outgoing_roaming_total_sms_max"),
        #                                                                                 F.max("usg_total_data_last_action_date").alias("usg_total_data_last_action_date_max"),
        #                                                                                 F.max("usg_data_last_action_date").alias("usg_data_last_action_date_max"),
        #                                                                                 F.max("usg_incoming_data_volume").alias("usg_incoming_data_volume_max"),
        #                                                                                 F.max("usg_incoming_data_volume_2G_3G").alias("usg_incoming_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_incoming_data_volume_4G").alias("usg_incoming_data_volume_4G_max"),
        #                                                                                 F.max("usg_incoming_local_data_volume").alias("usg_incoming_local_data_volume_max"),
        #                                                                                 F.max("usg_incoming_local_data_volume_2G_3G").alias("usg_incoming_local_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_incoming_local_data_volume_4G").alias("usg_incoming_local_data_volume_4G_max"),
        #                                                                                 F.max("usg_incoming_roaming_data_volume").alias("usg_incoming_roaming_data_volume_max"),
        #                                                                                 F.max("usg_incoming_roaming_data_volume_2G_3G").alias("usg_incoming_roaming_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_incoming_roaming_data_volume_4G").alias("usg_incoming_roaming_data_volume_4G_max"),
        #                                                                                 F.max("usg_outgoing_data_volume").alias("usg_outgoing_data_volume_max"),
        #                                                                                 F.max("usg_outgoing_data_volume_2G_3G").alias("usg_outgoing_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_outgoing_data_volume_4G").alias("usg_outgoing_data_volume_4G_max"),
        #                                                                                 F.max("usg_outgoing_local_data_volume").alias("usg_outgoing_local_data_volume_max"),
        #                                                                                 F.max("usg_outgoing_local_data_volume_2G_3G").alias("usg_outgoing_local_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_outgoing_local_data_volume_4G").alias("usg_outgoing_local_data_volume_4G_max"),
        #                                                                                 F.max("usg_outgoing_roaming_data_volume").alias("usg_outgoing_roaming_data_volume_max"),
        #                                                                                 F.max("usg_outgoing_roaming_data_volume_2G_3G").alias("usg_outgoing_roaming_data_volume_2G_3G_max"),
        #                                                                                 F.max("usg_outgoing_roaming_data_volume_4G").alias("usg_outgoing_roaming_data_volume_4G_max"),
        #                                                                                 F.max("usg_total_data_volume").alias("usg_total_data_volume_max"),
        #                                                                                 F.max("usg_vas_total_number_of_call").alias("usg_vas_total_number_of_call_max"),
        #                                                                                 F.max("usg_vas_last_action_dt").alias("usg_vas_last_action_dt_max"))

        ################################## Test zone AVG #############################################################

        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_last_sms_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_last_sms_date_max"))
        #
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_last_sms_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_last_sms_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_last_sms_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_last_sms_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_last_sms_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_last_sms_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_last_action_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_last_action_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_time_call_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_time_call_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_last_call_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_last_call_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_last_call_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_last_call_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_last_call_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_last_call_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_last_sms_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_last_sms_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_calls_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_calls_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_time_call_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_time_call_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_ais_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_ais_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_call_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_call_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_call_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_call_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_over_30_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_over_30_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_10_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_10_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_15_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_15_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_20_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_20_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_30_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_30_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_5_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_5_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_offnet_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_offnet_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_calls_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_calls_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_ais_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_ais_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_over_30_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_over_30_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_10_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_10_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_15_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_15_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_20_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_20_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_30_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_30_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_5_mins_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_5_mins_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_calls_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_calls_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_total_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_total_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_call_duration_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_call_duration_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_number_calls_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_number_calls_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_total_sms_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_total_sms_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_total_data_last_action_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_total_data_last_action_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_data_last_action_date_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_data_last_action_date_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_2G_3G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_2G_3G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_4G_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_4G_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_total_data_volume_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_total_data_volume_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_vas_total_number_of_call_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_vas_total_number_of_call_max"))
        # assert sum_l2.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_vas_last_action_dt_max").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_vas_last_action_dt_max"))

    def test_l2_usage_min(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType(
                                               [StructField("subscription_identifier", StringType(), True),
                                                StructField("call_start_dt", DateType(), True),
                                                StructField("day_id", DateType(), True),
                                                StructField("start_of_month", DateType(), True),
                                                StructField("start_of_week", DateType(), True),
                                                StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_friday_evening_usage", StringType(), True),
                                                StructField("usg_data_friday_morning_usage", StringType(), True),
                                                StructField("usg_data_friday_night_usage", StringType(), True),
                                                StructField("usg_data_friday_usage", StringType(), True),
                                                StructField("usg_data_last_action_date", DateType(), True),
                                                StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_monday_evening_usage", StringType(), True),
                                                StructField("usg_data_monday_morning_usage", StringType(), True),
                                                StructField("usg_data_monday_night_usage", StringType(), True),
                                                StructField("usg_data_monday_usage", StringType(), True),
                                                StructField("usg_data_saturday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                StructField("usg_data_saturday_night_usage", StringType(), True),
                                                StructField("usg_data_saturday_usage", StringType(), True),
                                                StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                StructField("usg_data_sunday_night_usage", StringType(), True),
                                                StructField("usg_data_sunday_usage", StringType(), True),
                                                StructField("usg_data_thursday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                StructField("usg_data_thursday_night_usage", StringType(), True),
                                                StructField("usg_data_thursday_usage", StringType(), True),
                                                StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                StructField("usg_data_tuesday_usage", StringType(), True),
                                                StructField("usg_data_wednesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                StructField("usg_data_wednesday_usage", StringType(), True),
                                                StructField("usg_data_weekday_usage", StringType(), True),
                                                StructField("usg_data_weekend_usage", StringType(), True),
                                                StructField("usg_incoming_afternoon_number_sms", StringType(), True),
                                                StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_ais_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_data_volume", StringType(), True),
                                                StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_time_call", StringType(), True),
                                                StructField("usg_incoming_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_last_call_date", DateType(), True),
                                                StructField("usg_incoming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                StructField("usg_incoming_local_call_duration", StringType(), True),
                                                StructField("usg_incoming_local_data_volume", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_local_sms", StringType(), True),
                                                StructField("usg_incoming_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_morning_time_call", StringType(), True),
                                                StructField("usg_incoming_morning_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_night_time_call", StringType(), True),
                                                StructField("usg_incoming_night_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_number_calls", StringType(), True),
                                                StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_call_duration", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_roaming_number_calls", StringType(), True),
                                                StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                StructField("usg_incoming_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_total_call_duration", StringType(), True),
                                                StructField("usg_incoming_total_sms", StringType(), True),
                                                StructField("usg_incoming_true_call_duration", StringType(), True),
                                                StructField("usg_incoming_true_number_calls", StringType(), True),
                                                StructField("usg_incoming_true_number_sms", StringType(), True),
                                                StructField("usg_incoming_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_incoming_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_weekday_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekday_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                StructField("usg_incoming_weekend_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekend_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                StructField("usg_last_action_date", DateType(), True),
                                                StructField("usg_last_call_date", DateType(), True),
                                                StructField("usg_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_afternoon_number_calls", StringType(), True),
                                                StructField("usg_outgoing_afternoon_number_sms", StringType(), True),
                                                StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_data_volume", StringType(), True),
                                                StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                StructField("usg_outgoing_evening_number_calls", StringType(), True),
                                                StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                StructField("usg_outgoing_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_last_call_date", DateType(), True),
                                                StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_local_sms", StringType(), True),
                                                StructField("usg_outgoing_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_morning_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_calls", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_number_calls", StringType(), True),
                                                StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_call_duration", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_roaming_number_calls", StringType(), True),
                                                StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                StructField("usg_outgoing_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                StructField("usg_outgoing_total_sms", StringType(), True),
                                                StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                StructField("usg_outgoing_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_outgoing_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_weekday_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                StructField("usg_outgoing_weekend_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                StructField("usg_total_data_last_action_date", DateType(), True),
                                                StructField("usg_total_data_volume", StringType(), True),
                                                StructField("usg_vas_last_action_dt", DateType(), True),
                                                StructField("usg_vas_total_number_of_call", StringType(), True),
                                                StructField("event_partition_date", DateType(), True),
                                                ]))

        l2_agg = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

        ########################## TEST min #######################################################################
        df_l1_agg = df_l1_test.groupby("subscription_identifier", "start_of_week").agg(F.min("usg_incoming_afternoon_number_sms").alias("usg_incoming_afternoon_number_sms_min"),
                                                                               F.min("usg_incoming_afternoon_time_call").alias("usg_incoming_afternoon_time_call_min"),
                                                                               F.min("usg_incoming_ais_local_calls_duration").alias("usg_incoming_ais_local_calls_duration_min"),
                                                                               F.min("usg_incoming_ais_local_number_calls").alias("usg_incoming_ais_local_number_calls_min"),
                                                                               F.min("usg_incoming_dtac_call_duration").alias("usg_incoming_dtac_call_duration_min"),
                                                                               F.min("usg_incoming_dtac_number_calls").alias("usg_incoming_dtac_number_calls_min"),
                                                                               F.min("usg_incoming_dtac_number_sms").alias("usg_incoming_dtac_number_sms_min"),
                                                                               F.min("usg_incoming_evening_number_sms").alias("usg_incoming_evening_number_sms_min"),
                                                                               F.min("usg_incoming_evening_time_call").alias("usg_incoming_evening_time_call_min"),
                                                                               F.min("usg_incoming_local_ais_sms").alias("usg_incoming_local_ais_sms_min"),
                                                                               F.min("usg_incoming_local_call_duration").alias("usg_incoming_local_call_duration_min"),
                                                                               F.min("usg_incoming_local_number_calls").alias("usg_incoming_local_number_calls_min"),
                                                                               F.min("usg_incoming_local_sms").alias("usg_incoming_local_sms_min"),
                                                                               F.min("usg_incoming_morning_time_call").alias("usg_incoming_morning_time_call_min"),
                                                                               F.min("usg_incoming_morning_time_number_sms").alias("usg_incoming_morning_time_number_sms_min"),
                                                                               F.min("usg_incoming_night_time_call").alias("usg_incoming_night_time_call_min"),
                                                                               F.min("usg_incoming_night_time_number_sms").alias("usg_incoming_night_time_number_sms_min"),
                                                                               F.min("usg_incoming_number_calls").alias("usg_incoming_number_calls_min"),
                                                                               F.min("usg_incoming_number_calls_over_30_mins").alias("usg_incoming_number_calls_over_30_mins_min"),
                                                                               F.min("usg_incoming_number_calls_upto_10_mins").alias("usg_incoming_number_calls_upto_10_mins_min"),
                                                                               F.min("usg_incoming_number_calls_upto_15_mins").alias("usg_incoming_number_calls_upto_15_mins_min"),
                                                                               F.min("usg_incoming_number_calls_upto_20_mins").alias("usg_incoming_number_calls_upto_20_mins_min"),
                                                                               F.min("usg_incoming_number_calls_upto_30_mins").alias("usg_incoming_number_calls_upto_30_mins_min"),
                                                                               F.min("usg_incoming_number_calls_upto_5_mins").alias("usg_incoming_number_calls_upto_5_mins_min"),
                                                                               F.min("usg_incoming_offnet_local_number_calls").alias("usg_incoming_offnet_local_number_calls_min"),
                                                                               F.min("usg_incoming_total_call_duration").alias("usg_incoming_total_call_duration_min"),
                                                                               F.min("usg_incoming_total_sms").alias("usg_incoming_total_sms_min"),
                                                                               F.min("usg_incoming_true_call_duration").alias("usg_incoming_true_call_duration_min"),
                                                                               F.min("usg_incoming_true_number_calls").alias("usg_incoming_true_number_calls_min"),
                                                                               F.min("usg_incoming_true_number_sms").alias("usg_incoming_true_number_sms_min"),
                                                                               F.min("usg_outgoing_afternoon_number_calls").alias("usg_outgoing_afternoon_number_calls_min"),
                                                                               F.min("usg_outgoing_afternoon_number_sms").alias("usg_outgoing_afternoon_number_sms_min"),
                                                                               F.min("usg_outgoing_ais_local_calls_duration").alias("usg_outgoing_ais_local_calls_duration_min"),
                                                                               F.min("usg_outgoing_ais_local_number_calls").alias("usg_outgoing_ais_local_number_calls_min"),
                                                                               F.min("usg_outgoing_dtac_call_duration").alias("usg_outgoing_dtac_call_duration_min"),
                                                                               F.min("usg_outgoing_dtac_number_calls").alias("usg_outgoing_dtac_number_calls_min"),
                                                                               F.min("usg_outgoing_dtac_number_sms").alias("usg_outgoing_dtac_number_sms_min"),
                                                                               F.min("usg_outgoing_evening_number_calls").alias("usg_outgoing_evening_number_calls_min"),
                                                                               F.min("usg_outgoing_evening_number_sms").alias("usg_outgoing_evening_number_sms_min"),
                                                                               F.min("usg_outgoing_local_ais_sms").alias("usg_outgoing_local_ais_sms_min"),
                                                                               F.min("usg_outgoing_local_call_duration").alias("usg_outgoing_local_call_duration_min"),
                                                                               F.min("usg_outgoing_local_number_calls").alias("usg_outgoing_local_number_calls_min"),
                                                                               F.min("usg_outgoing_local_sms").alias("usg_outgoing_local_sms_min"),
                                                                               F.min("usg_outgoing_morning_time_number_calls").alias("usg_outgoing_morning_time_number_calls_min"),
                                                                               F.min("usg_outgoing_morning_time_number_sms").alias("usg_outgoing_morning_time_number_sms_min"),
                                                                               F.min("usg_outgoing_night_time_number_calls").alias("usg_outgoing_night_time_number_calls_min"),
                                                                               F.min("usg_outgoing_night_time_number_sms").alias("usg_outgoing_night_time_number_sms_min"),
                                                                               F.min("usg_outgoing_number_calls").alias("usg_outgoing_number_calls_min"),
                                                                               F.min("usg_outgoing_number_calls_over_30_mins").alias("usg_outgoing_number_calls_over_30_mins_min"),
                                                                               F.min("usg_outgoing_number_calls_upto_10_mins").alias("usg_outgoing_number_calls_upto_10_mins_min"),
                                                                               F.min("usg_outgoing_number_calls_upto_15_mins").alias("usg_outgoing_number_calls_upto_15_mins_min"),
                                                                               F.min("usg_outgoing_number_calls_upto_20_mins").alias("usg_outgoing_number_calls_upto_20_mins_min"),
                                                                               F.min("usg_outgoing_number_calls_upto_30_mins").alias("usg_outgoing_number_calls_upto_30_mins_min"),
                                                                               F.min("usg_outgoing_number_calls_upto_5_mins").alias("usg_outgoing_number_calls_upto_5_mins_min"),
                                                                               F.min("usg_outgoing_offnet_local_calls_duration").alias("usg_outgoing_offnet_local_calls_duration_min"),
                                                                               F.min("usg_outgoing_offnet_local_number_calls").alias("usg_outgoing_offnet_local_number_calls_min"),
                                                                               F.min("usg_outgoing_total_call_duration").alias("usg_outgoing_total_call_duration_min"),
                                                                               F.min("usg_outgoing_total_sms").alias("usg_outgoing_total_sms_min"),
                                                                               F.min("usg_outgoing_true_call_duration").alias("usg_outgoing_true_call_duration_min"),
                                                                               F.min("usg_outgoing_true_number_calls").alias("usg_outgoing_true_number_calls_min"),
                                                                               F.min("usg_outgoing_true_number_sms").alias("usg_outgoing_true_number_sms_min"),
                                                                               F.min("usg_incoming_roaming_call_duration").alias("usg_incoming_roaming_call_duration_min"),
                                                                               F.min("usg_incoming_roaming_number_calls").alias("usg_incoming_roaming_number_calls_min"),
                                                                               F.min("usg_incoming_roaming_total_sms").alias("usg_incoming_roaming_total_sms_min"),
                                                                               F.min("usg_outgoing_roaming_call_duration").alias("usg_outgoing_roaming_call_duration_min"),
                                                                               F.min("usg_outgoing_roaming_number_calls").alias("usg_outgoing_roaming_number_calls_min"),
                                                                               F.min("usg_outgoing_roaming_total_sms").alias("usg_outgoing_roaming_total_sms_min"),
                                                                               F.min("usg_incoming_data_volume").alias("usg_incoming_data_volume_min"),
                                                                               F.min("usg_incoming_data_volume_2G_3G").alias("usg_incoming_data_volume_2G_3G_min"),
                                                                               F.min("usg_incoming_data_volume_4G").alias("usg_incoming_data_volume_4G_min"),
                                                                               F.min("usg_incoming_local_data_volume").alias("usg_incoming_local_data_volume_min"),
                                                                               F.min("usg_incoming_local_data_volume_2G_3G").alias("usg_incoming_local_data_volume_2G_3G_min"),
                                                                               F.min("usg_incoming_local_data_volume_4G").alias("usg_incoming_local_data_volume_4G_min"),
                                                                               F.min("usg_incoming_roaming_data_volume").alias("usg_incoming_roaming_data_volume_min"),
                                                                               F.min("usg_incoming_roaming_data_volume_2G_3G").alias("usg_incoming_roaming_data_volume_2G_3G_min"),
                                                                               F.min("usg_incoming_roaming_data_volume_4G").alias("usg_incoming_roaming_data_volume_4G_min"),
                                                                               F.min("usg_outgoing_data_volume").alias("usg_outgoing_data_volume_min"),
                                                                               F.min("usg_outgoing_data_volume_2G_3G").alias("usg_outgoing_data_volume_2G_3G_min"),
                                                                               F.min("usg_outgoing_data_volume_4G").alias("usg_outgoing_data_volume_4G_min"),
                                                                               F.min("usg_outgoing_local_data_volume").alias("usg_outgoing_local_data_volume_min"),
                                                                               F.min("usg_outgoing_local_data_volume_2G_3G").alias("usg_outgoing_local_data_volume_2G_3G_min"),
                                                                               F.min("usg_outgoing_local_data_volume_4G").alias("usg_outgoing_local_data_volume_4G_min"),
                                                                               F.min("usg_outgoing_roaming_data_volume").alias("usg_outgoing_roaming_data_volume_min"),
                                                                               F.min("usg_outgoing_roaming_data_volume_2G_3G").alias("usg_outgoing_roaming_data_volume_2G_3G_min"),
                                                                               F.min("usg_outgoing_roaming_data_volume_4G").alias("usg_outgoing_roaming_data_volume_4G_min"),
                                                                               F.min("usg_total_data_volume").alias("usg_total_data_volume_min"),
                                                                               F.min("usg_vas_total_number_of_call").alias("usg_vas_total_number_of_call_min")
                                                                               )

        ################################################## TEST Zone ##################################################

        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_afternoon_time_call_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_afternoon_time_call_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_calls_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_calls_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_ais_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_ais_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_dtac_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_dtac_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_evening_time_call_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_evening_time_call_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_ais_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_ais_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_call_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_call_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_morning_time_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_morning_time_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_call_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_call_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_night_time_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_night_time_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_over_30_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_over_30_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_10_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_10_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_15_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_15_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_20_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_20_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_30_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_30_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_number_calls_upto_5_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_number_calls_upto_5_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_offnet_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_offnet_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_total_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_total_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_true_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_true_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_afternoon_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_afternoon_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_calls_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_calls_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_ais_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_ais_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_dtac_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_dtac_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_evening_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_evening_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_ais_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_ais_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_morning_time_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_morning_time_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_night_time_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_night_time_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_over_30_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_over_30_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_10_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_10_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_15_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_15_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_20_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_20_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_30_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_30_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_number_calls_upto_5_mins_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_number_calls_upto_5_mins_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_calls_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_calls_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_offnet_local_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_offnet_local_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_total_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_total_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_true_number_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_true_number_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_total_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_total_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_call_duration_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_call_duration_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_number_calls_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_number_calls_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_total_sms_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_total_sms_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_local_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_local_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_incoming_roaming_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_incoming_roaming_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_local_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_local_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_2G_3G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_2G_3G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_outgoing_roaming_data_volume_4G_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_outgoing_roaming_data_volume_4G_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_total_data_volume_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_total_data_volume_min"))
        # assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #     "usg_vas_total_number_of_call_min").collect()[0][0] == check_null(
        #     df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
        #         "usg_vas_total_number_of_call_min"))

    def test_l2_usage_avg(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType(
                                               [StructField("subscription_identifier", StringType(), True),
                                                StructField("call_start_dt", DateType(), True),
                                                StructField("day_id", DateType(), True),
                                                StructField("start_of_month", DateType(), True),
                                                StructField("start_of_week", DateType(), True),
                                                StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_friday_evening_usage", StringType(), True),
                                                StructField("usg_data_friday_morning_usage", StringType(), True),
                                                StructField("usg_data_friday_night_usage", StringType(), True),
                                                StructField("usg_data_friday_usage", StringType(), True),
                                                StructField("usg_data_last_action_date", DateType(), True),
                                                StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_monday_evening_usage", StringType(), True),
                                                StructField("usg_data_monday_morning_usage", StringType(), True),
                                                StructField("usg_data_monday_night_usage", StringType(), True),
                                                StructField("usg_data_monday_usage", StringType(), True),
                                                StructField("usg_data_saturday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                StructField("usg_data_saturday_night_usage", StringType(), True),
                                                StructField("usg_data_saturday_usage", StringType(), True),
                                                StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                StructField("usg_data_sunday_night_usage", StringType(), True),
                                                StructField("usg_data_sunday_usage", StringType(), True),
                                                StructField("usg_data_thursday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                StructField("usg_data_thursday_night_usage", StringType(), True),
                                                StructField("usg_data_thursday_usage", StringType(), True),
                                                StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                StructField("usg_data_tuesday_usage", StringType(), True),
                                                StructField("usg_data_wednesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                StructField("usg_data_wednesday_usage", StringType(), True),
                                                StructField("usg_data_weekday_usage", StringType(), True),
                                                StructField("usg_data_weekend_usage", StringType(), True),
                                                StructField("usg_incoming_afternoon_number_sms", StringType(), True),
                                                StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_ais_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_data_volume", StringType(), True),
                                                StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_time_call", StringType(), True),
                                                StructField("usg_incoming_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_last_call_date", DateType(), True),
                                                StructField("usg_incoming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                StructField("usg_incoming_local_call_duration", StringType(), True),
                                                StructField("usg_incoming_local_data_volume", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_local_sms", StringType(), True),
                                                StructField("usg_incoming_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_morning_time_call", StringType(), True),
                                                StructField("usg_incoming_morning_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_night_time_call", StringType(), True),
                                                StructField("usg_incoming_night_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_number_calls", StringType(), True),
                                                StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_call_duration", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_roaming_number_calls", StringType(), True),
                                                StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                StructField("usg_incoming_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_total_call_duration", StringType(), True),
                                                StructField("usg_incoming_total_sms", StringType(), True),
                                                StructField("usg_incoming_true_call_duration", StringType(), True),
                                                StructField("usg_incoming_true_number_calls", StringType(), True),
                                                StructField("usg_incoming_true_number_sms", StringType(), True),
                                                StructField("usg_incoming_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_incoming_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_weekday_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekday_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                StructField("usg_incoming_weekend_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekend_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                StructField("usg_last_action_date", DateType(), True),
                                                StructField("usg_last_call_date", DateType(), True),
                                                StructField("usg_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_afternoon_number_calls", StringType(), True),
                                                StructField("usg_outgoing_afternoon_number_sms", StringType(), True),
                                                StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_data_volume", StringType(), True),
                                                StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                StructField("usg_outgoing_evening_number_calls", StringType(), True),
                                                StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                StructField("usg_outgoing_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_last_call_date", DateType(), True),
                                                StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_local_sms", StringType(), True),
                                                StructField("usg_outgoing_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_morning_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_calls", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_number_calls", StringType(), True),
                                                StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_call_duration", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_roaming_number_calls", StringType(), True),
                                                StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                StructField("usg_outgoing_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                StructField("usg_outgoing_total_sms", StringType(), True),
                                                StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                StructField("usg_outgoing_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_outgoing_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_weekday_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                StructField("usg_outgoing_weekend_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                StructField("usg_total_data_last_action_date", DateType(), True),
                                                StructField("usg_total_data_volume", StringType(), True),
                                                StructField("usg_vas_last_action_dt", DateType(), True),
                                                StructField("usg_vas_total_number_of_call", StringType(), True),
                                                StructField("event_partition_date", DateType(), True),
                                                ]))


        l2_agg = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

        ########################## TEST min #######################################################################

        df_l1_agg = df_l1_test.groupby("subscription_identifier", "start_of_week").agg(F.avg("usg_incoming_ais_local_calls_duration").alias("usg_incoming_ais_local_calls_duration_avg"),
                                                                                        F.avg("usg_incoming_ais_local_number_calls").alias("usg_incoming_ais_local_number_calls_avg"),
                                                                                        F.avg("usg_incoming_dtac_call_duration").alias("usg_incoming_dtac_call_duration_avg"),
                                                                                        F.avg("usg_incoming_dtac_number_calls").alias("usg_incoming_dtac_number_calls_avg"),
                                                                                        F.avg("usg_incoming_dtac_number_sms").alias("usg_incoming_dtac_number_sms_avg"),
                                                                                        F.avg("usg_incoming_evening_number_sms").alias("usg_incoming_evening_number_sms_avg"),
                                                                                        F.avg("usg_incoming_evening_time_call").alias("usg_incoming_evening_time_call_avg"),
                                                                                        F.avg("usg_incoming_local_ais_sms").alias("usg_incoming_local_ais_sms_avg"),
                                                                                        F.avg("usg_incoming_local_call_duration").alias("usg_incoming_local_call_duration_avg"),
                                                                                        F.avg("usg_incoming_local_number_calls").alias("usg_incoming_local_number_calls_avg"),
                                                                                        F.avg("usg_incoming_local_sms").alias("usg_incoming_local_sms_avg"),
                                                                                        F.avg("usg_incoming_morning_time_call").alias("usg_incoming_morning_time_call_avg"),
                                                                                        F.avg("usg_incoming_morning_time_number_sms").alias("usg_incoming_morning_time_number_sms_avg"),
                                                                                        F.avg("usg_incoming_night_time_call").alias("usg_incoming_night_time_call_avg"),
                                                                                        F.avg("usg_incoming_night_time_number_sms").alias("usg_incoming_night_time_number_sms_avg"),
                                                                                        F.avg("usg_incoming_number_calls").alias("usg_incoming_number_calls_avg"),
                                                                                        F.avg("usg_incoming_number_calls_over_30_mins").alias("usg_incoming_number_calls_over_30_mins_avg"),
                                                                                        F.avg("usg_incoming_number_calls_upto_10_mins").alias("usg_incoming_number_calls_upto_10_mins_avg"),
                                                                                        F.avg("usg_incoming_number_calls_upto_15_mins").alias("usg_incoming_number_calls_upto_15_mins_avg"),
                                                                                        F.avg("usg_incoming_number_calls_upto_20_mins").alias("usg_incoming_number_calls_upto_20_mins_avg"),
                                                                                        F.avg("usg_incoming_number_calls_upto_30_mins").alias("usg_incoming_number_calls_upto_30_mins_avg"),
                                                                                        F.avg("usg_incoming_number_calls_upto_5_mins").alias("usg_incoming_number_calls_upto_5_mins_avg"),
                                                                                        F.avg("usg_incoming_offnet_local_number_calls").alias("usg_incoming_offnet_local_number_calls_avg"),
                                                                                        F.avg("usg_incoming_total_call_duration").alias("usg_incoming_total_call_duration_avg"),
                                                                                        F.avg("usg_incoming_total_sms").alias("usg_incoming_total_sms_avg"),
                                                                                        F.avg("usg_incoming_true_call_duration").alias("usg_incoming_true_call_duration_avg"),
                                                                                        F.avg("usg_incoming_true_number_calls").alias("usg_incoming_true_number_calls_avg"),
                                                                                        F.avg("usg_incoming_true_number_sms").alias("usg_incoming_true_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_afternoon_number_calls").alias("usg_outgoing_afternoon_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_afternoon_number_sms").alias("usg_outgoing_afternoon_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_ais_local_calls_duration").alias("usg_outgoing_ais_local_calls_duration_avg"),
                                                                                        F.avg("usg_outgoing_ais_local_number_calls").alias("usg_outgoing_ais_local_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_dtac_call_duration").alias("usg_outgoing_dtac_call_duration_avg"),
                                                                                        F.avg("usg_outgoing_dtac_number_calls").alias("usg_outgoing_dtac_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_dtac_number_sms").alias("usg_outgoing_dtac_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_evening_number_calls").alias("usg_outgoing_evening_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_evening_number_sms").alias("usg_outgoing_evening_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_local_ais_sms").alias("usg_outgoing_local_ais_sms_avg"),
                                                                                        F.avg("usg_outgoing_local_call_duration").alias("usg_outgoing_local_call_duration_avg"),
                                                                                        F.avg("usg_outgoing_local_number_calls").alias("usg_outgoing_local_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_local_sms").alias("usg_outgoing_local_sms_avg"),
                                                                                        F.avg("usg_outgoing_morning_time_number_calls").alias("usg_outgoing_morning_time_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_morning_time_number_sms").alias("usg_outgoing_morning_time_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_night_time_number_calls").alias("usg_outgoing_night_time_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_night_time_number_sms").alias("usg_outgoing_night_time_number_sms_avg"),
                                                                                        F.avg("usg_outgoing_number_calls").alias("usg_outgoing_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_over_30_mins").alias("usg_outgoing_number_calls_over_30_mins_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_upto_10_mins").alias("usg_outgoing_number_calls_upto_10_mins_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_upto_15_mins").alias("usg_outgoing_number_calls_upto_15_mins_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_upto_20_mins").alias("usg_outgoing_number_calls_upto_20_mins_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_upto_30_mins").alias("usg_outgoing_number_calls_upto_30_mins_avg"),
                                                                                        F.avg("usg_outgoing_number_calls_upto_5_mins").alias("usg_outgoing_number_calls_upto_5_mins_avg"),
                                                                                        F.avg("usg_outgoing_offnet_local_calls_duration").alias("usg_outgoing_offnet_local_calls_duration_avg"),
                                                                                        F.avg("usg_outgoing_offnet_local_number_calls").alias("usg_outgoing_offnet_local_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_total_call_duration").alias("usg_outgoing_total_call_duration_avg"),
                                                                                        F.avg("usg_outgoing_total_sms").alias("usg_outgoing_total_sms_avg"),
                                                                                        F.avg("usg_outgoing_true_call_duration").alias("usg_outgoing_true_call_duration_avg"),
                                                                                        F.avg("usg_outgoing_true_number_calls").alias("usg_outgoing_true_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_true_number_sms").alias("usg_outgoing_true_number_sms_avg"),
                                                                                        F.avg("usg_incoming_roaming_call_duration").alias("usg_incoming_roaming_call_duration_avg"),
                                                                                        F.avg("usg_incoming_roaming_number_calls").alias("usg_incoming_roaming_number_calls_avg"),
                                                                                        F.avg("usg_incoming_roaming_total_sms").alias("usg_incoming_roaming_total_sms_avg"),
                                                                                        F.avg("usg_outgoing_roaming_call_duration").alias("usg_outgoing_roaming_call_duration_avg"),
                                                                                        F.avg("usg_outgoing_roaming_number_calls").alias("usg_outgoing_roaming_number_calls_avg"),
                                                                                        F.avg("usg_outgoing_roaming_total_sms").alias("usg_outgoing_roaming_total_sms_avg"),
                                                                                        F.avg("usg_incoming_data_volume").alias("usg_incoming_data_volume_avg"),
                                                                                        F.avg("usg_incoming_data_volume_2G_3G").alias("usg_incoming_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_incoming_data_volume_4G").alias("usg_incoming_data_volume_4G_avg"),
                                                                                        F.avg("usg_incoming_local_data_volume").alias("usg_incoming_local_data_volume_avg"),
                                                                                        F.avg("usg_incoming_local_data_volume_2G_3G").alias("usg_incoming_local_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_incoming_local_data_volume_4G").alias("usg_incoming_local_data_volume_4G_avg"),
                                                                                        F.avg("usg_incoming_roaming_data_volume").alias("usg_incoming_roaming_data_volume_avg"),
                                                                                        F.avg("usg_incoming_roaming_data_volume_2G_3G").alias("usg_incoming_roaming_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_incoming_roaming_data_volume_4G").alias("usg_incoming_roaming_data_volume_4G_avg"),
                                                                                        F.avg("usg_outgoing_data_volume").alias("usg_outgoing_data_volume_avg"),
                                                                                        F.avg("usg_outgoing_data_volume_2G_3G").alias("usg_outgoing_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_outgoing_data_volume_4G").alias("usg_outgoing_data_volume_4G_avg"),
                                                                                        F.avg("usg_outgoing_local_data_volume").alias("usg_outgoing_local_data_volume_avg"),
                                                                                        F.avg("usg_outgoing_local_data_volume_2G_3G").alias("usg_outgoing_local_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_outgoing_local_data_volume_4G").alias("usg_outgoing_local_data_volume_4G_avg"),
                                                                                        F.avg("usg_outgoing_roaming_data_volume").alias("usg_outgoing_roaming_data_volume_avg"),
                                                                                        F.avg("usg_outgoing_roaming_data_volume_2G_3G").alias("usg_outgoing_roaming_data_volume_2G_3G_avg"),
                                                                                        F.avg("usg_outgoing_roaming_data_volume_4G").alias("usg_outgoing_roaming_data_volume_4G_avg"),
                                                                                        F.avg("usg_total_data_volume").alias("usg_total_data_volume_avg"),
                                                                                        F.avg("usg_vas_total_number_of_call").alias("usg_vas_total_number_of_call_avg")
                                                                                       )


        ############################################ TEST ZONE ######################################################

        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_ais_local_calls_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_ais_local_calls_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_ais_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_ais_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_dtac_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_dtac_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_dtac_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_dtac_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_dtac_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_dtac_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_evening_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_evening_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_evening_time_call_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_evening_time_call_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_ais_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_ais_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_morning_time_call_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_morning_time_call_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_morning_time_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_morning_time_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_night_time_call_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_night_time_call_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_night_time_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_night_time_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_over_30_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_over_30_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_upto_10_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_upto_10_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_upto_15_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_upto_15_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_upto_20_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_upto_20_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_upto_30_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_upto_30_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_number_calls_upto_5_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_number_calls_upto_5_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_offnet_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_offnet_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_total_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_total_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_total_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_total_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_true_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_true_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_true_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_true_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_true_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_true_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_afternoon_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_afternoon_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_afternoon_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_afternoon_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_ais_local_calls_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_ais_local_calls_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_ais_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_ais_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_dtac_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_dtac_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_dtac_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_dtac_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_dtac_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_dtac_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_evening_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_evening_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_evening_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_evening_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_ais_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_ais_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_morning_time_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_morning_time_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_morning_time_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_morning_time_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_night_time_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_night_time_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_night_time_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_night_time_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_over_30_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_over_30_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_upto_10_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_upto_10_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_upto_15_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_upto_15_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_upto_20_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_upto_20_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_upto_30_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_upto_30_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_number_calls_upto_5_mins_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_number_calls_upto_5_mins_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_offnet_local_calls_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_offnet_local_calls_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_offnet_local_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_offnet_local_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_total_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_total_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_total_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_total_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_true_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_true_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_true_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_true_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_true_number_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_true_number_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_total_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_total_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_call_duration_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_call_duration_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_number_calls_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_number_calls_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_total_sms_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_total_sms_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_local_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_local_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_incoming_roaming_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_incoming_roaming_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_local_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_local_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_data_volume_2G_3G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_data_volume_2G_3G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_outgoing_roaming_data_volume_4G_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_outgoing_roaming_data_volume_4G_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_total_data_volume_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_total_data_volume_avg"))
        assert l2_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "usg_vas_total_number_of_call_avg").collect()[0][0] == check_null(
            df_l1_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "usg_vas_total_number_of_call_avg"))

    def test_l4_weekly_min(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType(
                                               [StructField("subscription_identifier", StringType(), True),
                                                StructField("call_start_dt", DateType(), True),
                                                StructField("day_id", DateType(), True),
                                                StructField("start_of_month", DateType(), True),
                                                StructField("start_of_week", DateType(), True),
                                                StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_friday_evening_usage", StringType(), True),
                                                StructField("usg_data_friday_morning_usage", StringType(), True),
                                                StructField("usg_data_friday_night_usage", StringType(), True),
                                                StructField("usg_data_friday_usage", StringType(), True),
                                                StructField("usg_data_last_action_date", DateType(), True),
                                                StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_monday_evening_usage", StringType(), True),
                                                StructField("usg_data_monday_morning_usage", StringType(), True),
                                                StructField("usg_data_monday_night_usage", StringType(), True),
                                                StructField("usg_data_monday_usage", StringType(), True),
                                                StructField("usg_data_saturday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                StructField("usg_data_saturday_night_usage", StringType(), True),
                                                StructField("usg_data_saturday_usage", StringType(), True),
                                                StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                StructField("usg_data_sunday_night_usage", StringType(), True),
                                                StructField("usg_data_sunday_usage", StringType(), True),
                                                StructField("usg_data_thursday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                StructField("usg_data_thursday_night_usage", StringType(), True),
                                                StructField("usg_data_thursday_usage", StringType(), True),
                                                StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                StructField("usg_data_tuesday_usage", StringType(), True),
                                                StructField("usg_data_wednesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                StructField("usg_data_wednesday_usage", StringType(), True),
                                                StructField("usg_data_weekday_usage", StringType(), True),
                                                StructField("usg_data_weekend_usage", StringType(), True),
                                                StructField("usg_incoming_afternoon_number_sms", StringType(), True),
                                                StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_ais_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_data_volume", StringType(), True),
                                                StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_time_call", StringType(), True),
                                                StructField("usg_incoming_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_last_call_date", DateType(), True),
                                                StructField("usg_incoming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                StructField("usg_incoming_local_call_duration", StringType(), True),
                                                StructField("usg_incoming_local_data_volume", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_local_sms", StringType(), True),
                                                StructField("usg_incoming_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_morning_time_call", StringType(), True),
                                                StructField("usg_incoming_morning_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_night_time_call", StringType(), True),
                                                StructField("usg_incoming_night_time_number_sms", StringType(), True),
                                                StructField("usg_incoming_number_calls", StringType(), True),
                                                StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_call_duration", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_roaming_number_calls", StringType(), True),
                                                StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                StructField("usg_incoming_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_total_call_duration", StringType(), True),
                                                StructField("usg_incoming_total_sms", StringType(), True),
                                                StructField("usg_incoming_true_call_duration", StringType(), True),
                                                StructField("usg_incoming_true_number_calls", StringType(), True),
                                                StructField("usg_incoming_true_number_sms", StringType(), True),
                                                StructField("usg_incoming_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_incoming_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_weekday_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekday_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                StructField("usg_incoming_weekend_calls_duration", StringType(), True),
                                                StructField("usg_incoming_weekend_number_calls", StringType(), True),
                                                StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                StructField("usg_last_action_date", DateType(), True),
                                                StructField("usg_last_call_date", DateType(), True),
                                                StructField("usg_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_afternoon_number_calls", StringType(), True),
                                                StructField("usg_outgoing_afternoon_number_sms", StringType(), True),
                                                StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_data_volume", StringType(), True),
                                                StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                StructField("usg_outgoing_evening_number_calls", StringType(), True),
                                                StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                StructField("usg_outgoing_friday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_last_call_date", DateType(), True),
                                                StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_local_sms", StringType(), True),
                                                StructField("usg_outgoing_monday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_morning_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_calls", StringType(), True),
                                                StructField("usg_outgoing_night_time_number_sms", StringType(), True),
                                                StructField("usg_outgoing_number_calls", StringType(), True),
                                                StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_call_duration", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_roaming_number_calls", StringType(), True),
                                                StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                StructField("usg_outgoing_saturday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_sunday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_thursday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                StructField("usg_outgoing_total_sms", StringType(), True),
                                                StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                StructField("usg_outgoing_tuesday_afternoon_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_outgoing_wednesday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_weekday_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                StructField("usg_outgoing_weekend_calls_duration", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_calls", StringType(), True),
                                                StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                StructField("usg_total_data_last_action_date", DateType(), True),
                                                StructField("usg_total_data_volume", StringType(), True),
                                                StructField("usg_vas_last_action_dt", DateType(), True),
                                                StructField("usg_vas_total_number_of_call", StringType(), True),
                                                StructField("event_partition_date", DateType(), True),
                                                ]))

        print("123456789")

        df_l1_test.select("subscription_identifier","start_of_week","usg_incoming_data_volume","event_partition_date").where("subscription_identifier = '1-FAKEVST'").show()

        print("123456789")

        l2_agg = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

        l2_agg.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume_min").show()

        ############## L2 to L4 ####################################################################################



        l4_agg = l4_rolling_window(l2_agg , var_project_context.catalog.load('params:l4_usage_postpaid_prepaid_weekly_features_min'))


        l4_agg.where("subscription_identifier = '1-FAKEVST'").select("subscription_identifier","start_of_week","min_usg_incoming_data_volume_min_weekly_last_two_week").show()

        ####################### TEST look-back 2 week #################################################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select("min_usg_incoming_data_volume_min_weekly_last_two_week").collect()[0][0] ==  '233697726.00'

        #### test Null because Dataset start_of_week start on 2020-01-06 must be null #################################
        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select("min_usg_incoming_data_volume_min_weekly_last_two_week").collect()[0][0] == None

        ####################### TEST  look-back ################ 1 Week ##############################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "min_usg_incoming_data_volume_min_weekly_last_week").collect()[0][0] == '233697726.00'

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
            "min_usg_incoming_data_volume_min_weekly_last_week").collect()[0][0] == None

    def test_l4_weekly_max(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType(
                                               [StructField("subscription_identifier", StringType(), True),
                                                StructField("call_start_dt", DateType(), True),
                                                StructField("day_id", DateType(), True),
                                                StructField("start_of_month", DateType(), True),
                                                StructField("start_of_week", DateType(), True),
                                                StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_friday_evening_usage", StringType(), True),
                                                StructField("usg_data_friday_morning_usage", StringType(), True),
                                                StructField("usg_data_friday_night_usage", StringType(), True),
                                                StructField("usg_data_friday_usage", StringType(), True),
                                                StructField("usg_data_last_action_date", DateType(), True),
                                                StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_monday_evening_usage", StringType(), True),
                                                StructField("usg_data_monday_morning_usage", StringType(), True),
                                                StructField("usg_data_monday_night_usage", StringType(), True),
                                                StructField("usg_data_monday_usage", StringType(), True),
                                                StructField("usg_data_saturday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                StructField("usg_data_saturday_night_usage", StringType(), True),
                                                StructField("usg_data_saturday_usage", StringType(), True),
                                                StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                StructField("usg_data_sunday_night_usage", StringType(), True),
                                                StructField("usg_data_sunday_usage", StringType(), True),
                                                StructField("usg_data_thursday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                StructField("usg_data_thursday_night_usage", StringType(), True),
                                                StructField("usg_data_thursday_usage", StringType(), True),
                                                StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                StructField("usg_data_tuesday_usage", StringType(), True),
                                                StructField("usg_data_wednesday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                StructField("usg_data_wednesday_usage", StringType(), True),
                                                StructField("usg_data_weekday_usage", StringType(), True),
                                                StructField("usg_data_weekend_usage", StringType(), True),
                                                StructField("usg_incoming_afternoon_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_ais_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_data_volume", StringType(), True),
                                                StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_time_call", StringType(), True),
                                                StructField("usg_incoming_friday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_last_call_date", DateType(), True),
                                                StructField("usg_incoming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                StructField("usg_incoming_local_call_duration", StringType(), True),
                                                StructField("usg_incoming_local_data_volume", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_local_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_incoming_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_local_sms", StringType(), True),
                                                StructField("usg_incoming_monday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_morning_time_call", StringType(), True),
                                                StructField("usg_incoming_morning_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_night_time_call", StringType(), True),
                                                StructField("usg_incoming_night_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls", StringType(), True),
                                                StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_call_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_roaming_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                StructField("usg_incoming_saturday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_thursday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_total_call_duration", StringType(), True),
                                                StructField("usg_incoming_total_sms", StringType(), True),
                                                StructField("usg_incoming_true_call_duration", StringType(), True),
                                                StructField("usg_incoming_true_number_calls", StringType(), True),
                                                StructField("usg_incoming_true_number_sms", StringType(), True),
                                                StructField("usg_incoming_tuesday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_incoming_wednesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_night_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                StructField("usg_incoming_weekend_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekend_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                StructField("usg_last_action_date", DateType(), True),
                                                StructField("usg_last_call_date", DateType(), True),
                                                StructField("usg_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_afternoon_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_afternoon_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_data_volume", StringType(), True),
                                                StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                StructField("usg_outgoing_evening_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                StructField("usg_outgoing_friday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_last_call_date", DateType(), True),
                                                StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_local_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_local_sms", StringType(), True),
                                                StructField("usg_outgoing_monday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_morning_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_night_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_night_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls", StringType(), True),
                                                StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_calls_duration",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_call_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_roaming_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                StructField("usg_outgoing_saturday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_thursday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                StructField("usg_outgoing_total_sms", StringType(), True),
                                                StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                StructField("usg_outgoing_tuesday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_outgoing_wednesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_night_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                StructField("usg_outgoing_weekend_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekend_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                StructField("usg_total_data_last_action_date", DateType(), True),
                                                StructField("usg_total_data_volume", StringType(), True),
                                                StructField("usg_vas_last_action_dt", DateType(), True),
                                                StructField("usg_vas_total_number_of_call", StringType(), True),
                                                StructField("event_partition_date", DateType(), True),
                                                ]))

        print("123456789")

        df_l1_test.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume",
                          "event_partition_date").where("subscription_identifier = '1-FAKEVST'").show()

        print("123456789")

        l2_agg = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

        l2_agg.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume_max").show()

        ############## L2 to L4 ####################################################################################

        l4_agg = l4_rolling_window(l2_agg, var_project_context.catalog.load(
            'params:l4_usage_postpaid_prepaid_weekly_features_max'))

        l4_agg.where("subscription_identifier = '1-FAKEVST'").select("subscription_identifier", "start_of_week",
                                                                     "max_usg_incoming_data_volume_max_weekly_last_two_week").show()

        ####################### TEST look-back 2 week #################################################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "max_usg_incoming_data_volume_max_weekly_last_two_week").collect()[0][0] == '233697726.00'

        #### test Null because Dataset start_of_week start on 2020-01-06 must be null #################################
        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
            "max_usg_incoming_data_volume_max_weekly_last_two_week").collect()[0][0] == None

        ####################### TEST  look-back ################ 1 Week ##############################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "max_usg_incoming_data_volume_max_weekly_last_week").collect()[0][0] == '233697726.00'

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
            "max_usg_incoming_data_volume_max_weekly_last_week").collect()[0][0] == None

    def test_l4_weekly_sum(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
        df_l1_test = spark.createDataFrame(rdd1,
                                           schema=StructType(
                                               [StructField("subscription_identifier", StringType(), True),
                                                StructField("call_start_dt", DateType(), True),
                                                StructField("day_id", DateType(), True),
                                                StructField("start_of_month", DateType(), True),
                                                StructField("start_of_week", DateType(), True),
                                                StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_friday_evening_usage", StringType(), True),
                                                StructField("usg_data_friday_morning_usage", StringType(), True),
                                                StructField("usg_data_friday_night_usage", StringType(), True),
                                                StructField("usg_data_friday_usage", StringType(), True),
                                                StructField("usg_data_last_action_date", DateType(), True),
                                                StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_monday_evening_usage", StringType(), True),
                                                StructField("usg_data_monday_morning_usage", StringType(), True),
                                                StructField("usg_data_monday_night_usage", StringType(), True),
                                                StructField("usg_data_monday_usage", StringType(), True),
                                                StructField("usg_data_saturday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                StructField("usg_data_saturday_night_usage", StringType(), True),
                                                StructField("usg_data_saturday_usage", StringType(), True),
                                                StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                StructField("usg_data_sunday_night_usage", StringType(), True),
                                                StructField("usg_data_sunday_usage", StringType(), True),
                                                StructField("usg_data_thursday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                StructField("usg_data_thursday_night_usage", StringType(), True),
                                                StructField("usg_data_thursday_usage", StringType(), True),
                                                StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                StructField("usg_data_tuesday_usage", StringType(), True),
                                                StructField("usg_data_wednesday_afternoon_usage", StringType(),
                                                            True),
                                                StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                StructField("usg_data_wednesday_usage", StringType(), True),
                                                StructField("usg_data_weekday_usage", StringType(), True),
                                                StructField("usg_data_weekend_usage", StringType(), True),
                                                StructField("usg_incoming_afternoon_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_ais_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_data_volume", StringType(), True),
                                                StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                StructField("usg_incoming_evening_time_call", StringType(), True),
                                                StructField("usg_incoming_friday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_last_call_date", DateType(), True),
                                                StructField("usg_incoming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                StructField("usg_incoming_local_call_duration", StringType(), True),
                                                StructField("usg_incoming_local_data_volume", StringType(), True),
                                                StructField("usg_incoming_local_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_local_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_incoming_local_number_calls", StringType(), True),
                                                StructField("usg_incoming_local_sms", StringType(), True),
                                                StructField("usg_incoming_monday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_morning_time_call", StringType(), True),
                                                StructField("usg_incoming_morning_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_night_time_call", StringType(), True),
                                                StructField("usg_incoming_night_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls", StringType(), True),
                                                StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_call_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_incoming_roaming_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                StructField("usg_incoming_saturday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_saturday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_thursday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_thursday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_total_call_duration", StringType(), True),
                                                StructField("usg_incoming_total_sms", StringType(), True),
                                                StructField("usg_incoming_true_call_duration", StringType(), True),
                                                StructField("usg_incoming_true_number_calls", StringType(), True),
                                                StructField("usg_incoming_true_number_sms", StringType(), True),
                                                StructField("usg_incoming_tuesday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_incoming_wednesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_night_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_incoming_wednesday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                StructField("usg_incoming_weekend_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekend_number_calls", StringType(),
                                                            True),
                                                StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                StructField("usg_last_action_date", DateType(), True),
                                                StructField("usg_last_call_date", DateType(), True),
                                                StructField("usg_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_afternoon_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_afternoon_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_ais_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_data_volume", StringType(), True),
                                                StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                StructField("usg_outgoing_evening_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                StructField("usg_outgoing_friday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_last_call_date", DateType(), True),
                                                StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                StructField("usg_outgoing_local_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_local_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                StructField("usg_outgoing_local_sms", StringType(), True),
                                                StructField("usg_outgoing_monday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_morning_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_night_time_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_night_time_number_sms", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls", StringType(), True),
                                                StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_calls_duration",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_call_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_data_volume_4G", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                StructField("usg_outgoing_roaming_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                StructField("usg_outgoing_saturday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_saturday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_thursday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_thursday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                StructField("usg_outgoing_total_sms", StringType(), True),
                                                StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                StructField("usg_outgoing_tuesday_afternoon_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                            StringType(), True),
                                                StructField("usg_outgoing_wednesday_evening_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_morning_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_night_voice_usage",
                                                            StringType(),
                                                            True),
                                                StructField("usg_outgoing_wednesday_voice_usage", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                StructField("usg_outgoing_weekend_calls_duration", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekend_number_calls", StringType(),
                                                            True),
                                                StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                StructField("usg_total_data_last_action_date", DateType(), True),
                                                StructField("usg_total_data_volume", StringType(), True),
                                                StructField("usg_vas_last_action_dt", DateType(), True),
                                                StructField("usg_vas_total_number_of_call", StringType(), True),
                                                StructField("event_partition_date", DateType(), True),
                                                ]))

        print("123456789")

        df_l1_test.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume",
                          "event_partition_date").where("subscription_identifier = '1-FAKEVST'").show()

        print("123456789")

        l2_agg = build_usage_l2_layer(df_l1_test,
                                      var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

        l2_agg.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume_sum").show()

        ############## L2 to L4 ####################################################################################

        l4_agg = l4_rolling_window(l2_agg, var_project_context.catalog.load(
            'params:l4_usage_postpaid_prepaid_weekly_features_sum'))

        l4_agg.where("subscription_identifier = '1-FAKEVST'").select("subscription_identifier", "start_of_week",
                                                                     "sum_usg_incoming_data_volume_sum_weekly_last_two_week").show()

        ####################### TEST look-back 2 week #################################################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_two_week").collect()[0][0] == float(l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_week").collect()[0][0]) + float(l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-20'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_week").collect()[0][0])


        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "sum_usg_vas_total_number_of_call_sum_weekly_last_two_week").collect()[0][0] == None

        #### test Null because Dataset start_of_week start on 2020-01-06 must be null #################################
        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_two_week").collect()[0][0] == None

        ####################### TEST  look-back ################ 1 Week ##############################################

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_week").collect()[0][0] == float(l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_week").collect()[0][0])

        assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
            "sum_usg_incoming_data_volume_sum_weekly_last_week").collect()[0][0] == None

    def test_l4_weekly_avg(self, project_context):
        def test_l4_weekly_sum(self, project_context):
            var_project_context = project_context['ProjectContext']
            spark = project_context['Spark']
            rdd1 = spark.sparkContext.parallelize(test_l1_usage_postpaid_prepaid_daily)
            df_l1_test = spark.createDataFrame(rdd1,
                                               schema=StructType(
                                                   [StructField("subscription_identifier", StringType(), True),
                                                    StructField("call_start_dt", DateType(), True),
                                                    StructField("day_id", DateType(), True),
                                                    StructField("start_of_month", DateType(), True),
                                                    StructField("start_of_week", DateType(), True),
                                                    StructField("usg_data_friday_afternoon_usage", StringType(), True),
                                                    StructField("usg_data_friday_evening_usage", StringType(), True),
                                                    StructField("usg_data_friday_morning_usage", StringType(), True),
                                                    StructField("usg_data_friday_night_usage", StringType(), True),
                                                    StructField("usg_data_friday_usage", StringType(), True),
                                                    StructField("usg_data_last_action_date", DateType(), True),
                                                    StructField("usg_data_monday_afternoon_usage", StringType(), True),
                                                    StructField("usg_data_monday_evening_usage", StringType(), True),
                                                    StructField("usg_data_monday_morning_usage", StringType(), True),
                                                    StructField("usg_data_monday_night_usage", StringType(), True),
                                                    StructField("usg_data_monday_usage", StringType(), True),
                                                    StructField("usg_data_saturday_afternoon_usage", StringType(),
                                                                True),
                                                    StructField("usg_data_saturday_evening_usage", StringType(), True),
                                                    StructField("usg_data_saturday_morning_usage", StringType(), True),
                                                    StructField("usg_data_saturday_night_usage", StringType(), True),
                                                    StructField("usg_data_saturday_usage", StringType(), True),
                                                    StructField("usg_data_sunday_afternoon_usage", StringType(), True),
                                                    StructField("usg_data_sunday_evening_usage", StringType(), True),
                                                    StructField("usg_data_sunday_morning_usage", StringType(), True),
                                                    StructField("usg_data_sunday_night_usage", StringType(), True),
                                                    StructField("usg_data_sunday_usage", StringType(), True),
                                                    StructField("usg_data_thursday_afternoon_usage", StringType(),
                                                                True),
                                                    StructField("usg_data_thursday_evening_usage", StringType(), True),
                                                    StructField("usg_data_thursday_morning_usage", StringType(), True),
                                                    StructField("usg_data_thursday_night_usage", StringType(), True),
                                                    StructField("usg_data_thursday_usage", StringType(), True),
                                                    StructField("usg_data_tuesday_afternoon_usage", StringType(), True),
                                                    StructField("usg_data_tuesday_evening_usage", StringType(), True),
                                                    StructField("usg_data_tuesday_morning_usage", StringType(), True),
                                                    StructField("usg_data_tuesday_night_usage", StringType(), True),
                                                    StructField("usg_data_tuesday_usage", StringType(), True),
                                                    StructField("usg_data_wednesday_afternoon_usage", StringType(),
                                                                True),
                                                    StructField("usg_data_wednesday_evening_usage", StringType(), True),
                                                    StructField("usg_data_wednesday_morning_usage", StringType(), True),
                                                    StructField("usg_data_wednesday_night_usage", StringType(), True),
                                                    StructField("usg_data_wednesday_usage", StringType(), True),
                                                    StructField("usg_data_weekday_usage", StringType(), True),
                                                    StructField("usg_data_weekend_usage", StringType(), True),
                                                    StructField("usg_incoming_afternoon_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_incoming_afternoon_time_call", StringType(), True),
                                                    StructField("usg_incoming_ais_local_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_incoming_ais_local_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_incoming_data_volume", StringType(), True),
                                                    StructField("usg_incoming_data_volume_2G_3G", StringType(), True),
                                                    StructField("usg_incoming_data_volume_4G", StringType(), True),
                                                    StructField("usg_incoming_dtac_call_duration", StringType(), True),
                                                    StructField("usg_incoming_dtac_number_calls", StringType(), True),
                                                    StructField("usg_incoming_dtac_number_sms", StringType(), True),
                                                    StructField("usg_incoming_evening_number_sms", StringType(), True),
                                                    StructField("usg_incoming_evening_time_call", StringType(), True),
                                                    StructField("usg_incoming_friday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_friday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_friday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_friday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_friday_voice_usage", StringType(), True),
                                                    StructField("usg_incoming_last_call_date", DateType(), True),
                                                    StructField("usg_incoming_last_sms_date", DateType(), True),
                                                    StructField("usg_incoming_local_ais_sms", StringType(), True),
                                                    StructField("usg_incoming_local_call_duration", StringType(), True),
                                                    StructField("usg_incoming_local_data_volume", StringType(), True),
                                                    StructField("usg_incoming_local_data_volume_2G_3G", StringType(),
                                                                True),
                                                    StructField("usg_incoming_local_data_volume_4G", StringType(),
                                                                True),
                                                    StructField("usg_incoming_local_number_calls", StringType(), True),
                                                    StructField("usg_incoming_local_sms", StringType(), True),
                                                    StructField("usg_incoming_monday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_monday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_monday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_monday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_monday_voice_usage", StringType(), True),
                                                    StructField("usg_incoming_morning_time_call", StringType(), True),
                                                    StructField("usg_incoming_morning_time_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_incoming_night_time_call", StringType(), True),
                                                    StructField("usg_incoming_night_time_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls", StringType(), True),
                                                    StructField("usg_incoming_number_calls_over_30_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls_upto_10_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls_upto_15_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls_upto_20_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls_upto_30_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_number_calls_upto_5_mins", StringType(),
                                                                True),
                                                    StructField("usg_incoming_offnet_local_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_incoming_roaming_call_duration", StringType(),
                                                                True),
                                                    StructField("usg_incoming_roaming_data_volume", StringType(), True),
                                                    StructField("usg_incoming_roaming_data_volume_2G_3G", StringType(),
                                                                True),
                                                    StructField("usg_incoming_roaming_data_volume_4G", StringType(),
                                                                True),
                                                    StructField("usg_incoming_roaming_last_sms_date", DateType(), True),
                                                    StructField("usg_incoming_roaming_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_incoming_roaming_total_sms", StringType(), True),
                                                    StructField("usg_incoming_saturday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_saturday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_saturday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_saturday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_saturday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_sunday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_sunday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_sunday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_sunday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_sunday_voice_usage", StringType(), True),
                                                    StructField("usg_incoming_thursday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_thursday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_thursday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_thursday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_thursday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_total_call_duration", StringType(), True),
                                                    StructField("usg_incoming_total_sms", StringType(), True),
                                                    StructField("usg_incoming_true_call_duration", StringType(), True),
                                                    StructField("usg_incoming_true_number_calls", StringType(), True),
                                                    StructField("usg_incoming_true_number_sms", StringType(), True),
                                                    StructField("usg_incoming_tuesday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_tuesday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_tuesday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_tuesday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_tuesday_voice_usage", StringType(), True),
                                                    StructField("usg_incoming_wednesday_afternoon_voice_usage",
                                                                StringType(), True),
                                                    StructField("usg_incoming_wednesday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_wednesday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_wednesday_night_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_incoming_wednesday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_incoming_weekday_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_incoming_weekday_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_incoming_weekday_number_sms", StringType(), True),
                                                    StructField("usg_incoming_weekend_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_incoming_weekend_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_incoming_weekend_number_sms", StringType(), True),
                                                    StructField("usg_last_action_date", DateType(), True),
                                                    StructField("usg_last_call_date", DateType(), True),
                                                    StructField("usg_last_sms_date", DateType(), True),
                                                    StructField("usg_outgoing_afternoon_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_afternoon_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_ais_local_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_ais_local_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_data_volume", StringType(), True),
                                                    StructField("usg_outgoing_data_volume_2G_3G", StringType(), True),
                                                    StructField("usg_outgoing_data_volume_4G", StringType(), True),
                                                    StructField("usg_outgoing_dtac_call_duration", StringType(), True),
                                                    StructField("usg_outgoing_dtac_number_calls", StringType(), True),
                                                    StructField("usg_outgoing_dtac_number_sms", StringType(), True),
                                                    StructField("usg_outgoing_evening_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_evening_number_sms", StringType(), True),
                                                    StructField("usg_outgoing_friday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_friday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_friday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_friday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_friday_voice_usage", StringType(), True),
                                                    StructField("usg_outgoing_last_call_date", DateType(), True),
                                                    StructField("usg_outgoing_last_sms_date", DateType(), True),
                                                    StructField("usg_outgoing_local_ais_sms", StringType(), True),
                                                    StructField("usg_outgoing_local_call_duration", StringType(), True),
                                                    StructField("usg_outgoing_local_data_volume", StringType(), True),
                                                    StructField("usg_outgoing_local_data_volume_2G_3G", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_local_data_volume_4G", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_local_number_calls", StringType(), True),
                                                    StructField("usg_outgoing_local_sms", StringType(), True),
                                                    StructField("usg_outgoing_monday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_monday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_monday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_monday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_monday_voice_usage", StringType(), True),
                                                    StructField("usg_outgoing_morning_time_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_morning_time_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_night_time_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_night_time_number_sms", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls", StringType(), True),
                                                    StructField("usg_outgoing_number_calls_over_30_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls_upto_10_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls_upto_15_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls_upto_20_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls_upto_30_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_number_calls_upto_5_mins", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_offnet_local_calls_duration",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_offnet_local_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_roaming_call_duration", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_roaming_data_volume", StringType(), True),
                                                    StructField("usg_outgoing_roaming_data_volume_2G_3G", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_roaming_data_volume_4G", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_roaming_last_sms_date", DateType(), True),
                                                    StructField("usg_outgoing_roaming_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_roaming_total_sms", StringType(), True),
                                                    StructField("usg_outgoing_saturday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_saturday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_saturday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_saturday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_saturday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_sunday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_sunday_evening_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_sunday_morning_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_sunday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_sunday_voice_usage", StringType(), True),
                                                    StructField("usg_outgoing_thursday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_thursday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_thursday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_thursday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_thursday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_total_call_duration", StringType(), True),
                                                    StructField("usg_outgoing_total_sms", StringType(), True),
                                                    StructField("usg_outgoing_true_call_duration", StringType(), True),
                                                    StructField("usg_outgoing_true_number_calls", StringType(), True),
                                                    StructField("usg_outgoing_true_number_sms", StringType(), True),
                                                    StructField("usg_outgoing_tuesday_afternoon_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_tuesday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_tuesday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_tuesday_night_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_tuesday_voice_usage", StringType(), True),
                                                    StructField("usg_outgoing_wednesday_afternoon_voice_usage",
                                                                StringType(), True),
                                                    StructField("usg_outgoing_wednesday_evening_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_wednesday_morning_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_wednesday_night_voice_usage",
                                                                StringType(),
                                                                True),
                                                    StructField("usg_outgoing_wednesday_voice_usage", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_weekday_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_weekday_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_weekday_number_sms", StringType(), True),
                                                    StructField("usg_outgoing_weekend_calls_duration", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_weekend_number_calls", StringType(),
                                                                True),
                                                    StructField("usg_outgoing_weekend_number_sms", StringType(), True),
                                                    StructField("usg_total_data_last_action_date", DateType(), True),
                                                    StructField("usg_total_data_volume", StringType(), True),
                                                    StructField("usg_vas_last_action_dt", DateType(), True),
                                                    StructField("usg_vas_total_number_of_call", StringType(), True),
                                                    StructField("event_partition_date", DateType(), True),
                                                    ]))

            print("123456789")

            df_l1_test.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume",
                              "event_partition_date").where("subscription_identifier = '1-FAKEVST'").show()

            print("123456789")

            l2_agg = build_usage_l2_layer(df_l1_test,
                                          var_project_context.catalog.load('params:l2_usage_postpaid_prepaid_daily'))

            l2_agg.select("subscription_identifier", "start_of_week", "usg_incoming_data_volume_sum").show()

            ############## L2 to L4 ####################################################################################

            l4_agg = l4_rolling_window(l2_agg, var_project_context.catalog.load(
                'params:l4_usage_postpaid_prepaid_weekly_features_avg'))

            l4_agg.where("subscription_identifier = '1-FAKEVST'").select("subscription_identifier", "start_of_week",
                                                                         "avg_usg_incoming_data_volume_avg_weekly_last_two_week").show()

            ####################### TEST look-back 2 week #################################################################

            assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "avg_usg_incoming_data_volume_avg_weekly_last_two_week").collect()[0][0] == (float(
                l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                    "avg_usg_incoming_data_volume_avg_weekly_last_week").collect()[0][0]) + float(
                l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-20'").select(
                    "avg_usg_incoming_data_volume_avg_weekly_last_week").collect()[0][0])) / 2

            #### test Null because Dataset start_of_week start on 2020-01-06 must be null #################################
            assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
                "avg_usg_incoming_data_volume_avg_weekly_last_two_week").collect()[0][0] == None

            ####################### TEST  look-back ################ 1 Week ##############################################

            assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-27'").select(
                "avg_usg_incoming_data_volume_avg_weekly_last_week").collect()[0][0] == float('233697726.0')

            assert l4_agg.where("subscription_identifier = '1-FAKEVST' AND start_of_week = '2020-01-06'").select(
                "avg_usg_incoming_data_volume_avg_weekly_last_week").collect()[0][0] == None