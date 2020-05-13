from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.re_usable_functions import l2_massive_processing_with_expansion
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import timedelta, datetime
from pyspark.sql.types import *
from customer360.utilities.re_usable_functions import *


global ivr_test
ivr_test = [
["test","20201006","1","1","1","1","1","1","1","1","1","1",datetime.strptime('2020-01-06', '%Y-%m-%d'),datetime.strptime('2020-01-01', '%Y-%m-%d'),datetime.strptime('2020-01-06', '%Y-%m-%d')],
["test","20200107","1","1","1","1","1","1","1","1","1","1",datetime.strptime('2020-01-06', '%Y-%m-%d'),datetime.strptime('2020-01-01', '%Y-%m-%d'),datetime.strptime('2020-01-07', '%Y-%m-%d')]
]

global daily_customer_profile
daily_customer_profile = [
    ["1-TEST", "test", datetime.strptime('2020-01-06', '%Y-%m-%d'), "null", "THAI", "null", "N", "N", "Y", "Y",
     "3G537", "NULL", "null", "null", "3574", "117", "SA", "Classic", "Classic", "3G", "National_id_card",
     "20200106", datetime.strptime('2020-01-06', '%Y-%m-%d'), "Pre-paid", "null",
     datetime.strptime('2020-01-06', '%Y-%m-%d'), datetime.strptime('2020-01-01', '%Y-%m-%d')],
["1-TEST", "test", datetime.strptime('2020-01-06', '%Y-%m-%d'), "null", "THAI", "null", "N", "N", "Y", "Y",
     "3G537", "NULL", "null", "null", "3574", "117", "SA", "Classic", "Classic", "3G", "National_id_card",
     "20200107", datetime.strptime('2020-01-07', '%Y-%m-%d'), "Pre-paid", "null",
     datetime.strptime('2020-01-06', '%Y-%m-%d'), datetime.strptime('2020-01-01', '%Y-%m-%d')]
]

def set_value(project_context):
    var_project_context = project_context['ProjectContext']
    spark = project_context['Spark']

    rdd1 = spark.sparkContext.parallelize(ivr_test)
    global df_ivr_log
    df_ivr_log = spark.createDataFrame(rdd1,schema=StructType([
        StructField("mobile_number", StringType(), True),
        StructField("partition_date", StringType(), True),
        StructField("touchpoints_num_of_call_ivr", StringType(), True),
        StructField("touchpoints_num_of_disconnection_by_ivr", StringType(), True),
        StructField("touchpoints_num_of_disconnection_by_customer", StringType(), True),
        StructField("touchpoints_num_of_disconnection_by_transfer_agent", StringType(), True),
        StructField("touchpoints_ivr_moring_calls", StringType(), True),
        StructField("touchpoints_ivr_afternoon_calls", StringType(), True),
        StructField("touchpoints_evening_calls", StringType(), True),
        StructField("touchpoints_night_calls", StringType(), True),
        StructField("touchpoints_distinct_languages_chosen", StringType(), True),
        StructField("touchpoints_unsuccesful_connect_tuxedo", StringType(), True),
        StructField("start_of_week", DateType(), True),
        StructField("start_of_month", DateType(), True),
        StructField("event_partition_date", DateType(), True)
    ]))

    rdd1 = spark.sparkContext.parallelize(daily_customer_profile)
    global customer_pro
    customer_pro = spark.createDataFrame(rdd1, schema=StructType([
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
        StructField("partition_date", StringType(), True),
        StructField("event_partition_date", DateType(), True),
        StructField("charge_type", StringType(), True),
        StructField("billing_account_no", StringType(), True),
        StructField("start_of_week", DateType(), True),
        StructField("start_of_month", DateType(), True)
    ]))

class TestUnitTp:

    def test_to_call_center(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        random_type = ['hYAUKs+f31uJzUZp3iM5F.fO8CgW7107gM602izDqDn.6GjSvNd8lhU6af7ItzfP', '98MkP5bMx1ykVd8RBE3C.XztcSxOJmjcyHAUmXwvCHW2Bb+cKKwh1kqv0.QI4ddw']
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        day_list = [iTemp.date().strftime("%d/%m/%Y %H:%M:%S") for iTemp in my_dates_list]
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]

        start_time = datetime.strptime("01/01/2020 08:35:55", '%d/%m/%Y %H:%M:%S')
        start_time_list = []
        for i in range(len(my_dates)):
            start_time_list.append(start_time)
            start_time = start_time + timedelta(seconds=random.randint(1, 432000))

        caller_list = [random_type[random.randint(0,1)] for iTemp in range(0, len(my_dates))]
        called_list = [random_type[random.randint(0, 1)] for iTemp in range(0, len(my_dates))]

        df = spark.createDataFrame(zip(called_list,caller_list,my_dates,my_dates,my_dates,start_time_list), schema=['called_no','caller_no','temp','register_date', 'recharge_date','day_id']) \
            .withColumn("call_type", F.lit('MO')) \
            .withColumn("called_network_type", F.lit('3GPre-paid')) \
            .withColumn("caller_network_type", F.lit('AIS')) \
            .withColumn("caller_total_net_revenue", F.lit(11.81)) \
            .withColumn("hour_id", F.lit('null')) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("idd_flag", F.lit('N')) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("service_type", F.lit('VOICE')) \
            .withColumn("total_durations", F.lit(1142)) \
            .withColumn("total_minutes", F.lit(23)) \
            .withColumn("total_successful_call", F.lit(7))

        l1_to_call_center = node_from_config(df, var_project_context.catalog.load(
            'params:l1_touchpoints_to_call_center_features'))



        assert \
            l1_to_call_center.where("caller_no = 'hYAUKs+f31uJzUZp3iM5F.fO8CgW7107gM602izDqDn.6GjSvNd8lhU6af7ItzfP'").select(
                "touchpoints_number_of_calls_on_cc").collect()[0][
                0] == 7
        assert \
            l1_to_call_center.where("caller_no = 'hYAUKs+f31uJzUZp3iM5F.fO8CgW7107gM602izDqDn.6GjSvNd8lhU6af7ItzfP'").select(
                "touchpoints_duration_of_calls_on_cc").collect()[0][
                0] == 1142

        l1_to_call_center=l1_to_call_center.withColumn("subscription_identifier", F.lit(1))
        l2_to_call_center = l2_massive_processing_with_expansion(l1_to_call_center, var_project_context.catalog.load(
            'params:l2_touchpoints_to_call_center_features'))

        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_number_of_calls_on_cc_sum").collect()[0][
                0] == 21
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_duration_of_calls_on_cc_sum").collect()[0][
                0] == 3426
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_number_of_calls_on_cc_avg").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_duration_of_calls_on_cc_avg").collect()[0][
                0] == 1142
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_number_of_calls_on_cc_max").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_duration_of_calls_on_cc_max").collect()[0][
                0] == 1142
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_number_of_calls_on_cc_min").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "touchpoints_duration_of_calls_on_cc_min").collect()[0][
                0] == 1142

        l3_to_call_center = expansion(l1_to_call_center, var_project_context.catalog.load(
            'params:l3_touchpoints_to_call_center_features'))

        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_on_cc_sum").collect()[0][
                0] == 91
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_on_cc_sum").collect()[0][
                0] == 14846
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_on_cc_avg").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_on_cc_avg").collect()[0][
                0] == 1142
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_on_cc_max").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_on_cc_max").collect()[0][
                0] == 1142
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_on_cc_min").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_on_cc_min").collect()[0][
                0] == 1142

        l4_to_call_center = l4_rolling_window(l2_to_call_center, var_project_context.catalog.load(
            'params:l4_touchpoints_to_call_center_features'))

        
        # sum_number_of_calls_on_cc_sum_weekly_last_week
        # sum_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21


        # sum_number_of_calls_on_cc_sum_weekly_last_four_week
        # sum_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # sum_duration_of_calls_on_cc_sum_weekly_last_week
        # sum_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # sum_duration_of_calls_on_cc_sum_weekly_last_four_week
        # sum_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # avg_number_of_calls_on_cc_sum_weekly_last_week
        # avg_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # avg_number_of_calls_on_cc_sum_weekly_last_four_week
        # avg_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # avg_duration_of_calls_on_cc_sum_weekly_last_week
        # avg_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # avg_duration_of_calls_on_cc_sum_weekly_last_four_week
        # avg_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # max_number_of_calls_on_cc_sum_weekly_last_week
        # max_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # max_number_of_calls_on_cc_sum_weekly_last_week
        # max_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21



        # max_number_of_calls_on_cc_sum_weekly_last_four_week
        # max_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # max_duration_of_calls_on_cc_sum_weekly_last_week
        # max_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426



        # min_touchpoints_number_of_calls_on_cc_sum_weekly_last_week
        # min_touchpoints_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # min_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week
        # min_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21

        # min_duration_of_calls_on_cc_sum_weekly_last_week
        # min_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # min_duration_of_calls_on_cc_sum_weekly_last_four_week
        # min_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426

        l1_from_call_center = node_from_config(df, var_project_context.catalog.load(
            'params:l1_touchpoints_from_call_center_features'))

        # number_of_calls_from_cc: "sum(total_successful_call)"
        # duration_of_calls_from_cc: "sum(total_durations)"
        assert \
            l1_from_call_center.where(
                "start_of_week = '2020-03-02'").select(
                "touchpoints_number_of_calls_from_cc").collect()[0][
                0] == 7
        assert \
            l1_from_call_center.where(
                "start_of_week = '2020-03-02'").select(
                "touchpoints_duration_of_calls_from_cc").collect()[0][
                0] == 1142

        l1_from_call_center = l1_from_call_center.withColumn("subscription_identifier", F.lit(1))
        l2_from_call_center = l2_massive_processing_with_expansion(l1_from_call_center, var_project_context.catalog.load(
            'params:l2_touchpoints_from_call_center_features'))
        

        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_number_of_calls_from_cc_sum").collect()[0][
                0] == 14
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_duration_of_calls_from_cc_sum").collect()[0][
                0] == 2284
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_number_of_calls_from_cc_avg").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_duration_of_calls_from_cc_avg").collect()[0][
                0] == 1142
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_number_of_calls_from_cc_max").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_duration_of_calls_from_cc_max").collect()[0][
                0] == 1142
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_number_of_calls_from_cc_min").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "touchpoints_duration_of_calls_from_cc_min").collect()[0][
                0] == 1142


        l3_from_call_center = expansion(l1_from_call_center,var_project_context.catalog.load(
                                                                       'params:l3_touchpoints_from_call_center_features'))

        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_from_cc_sum").collect()[0][
                0] == 91
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_from_cc_sum").collect()[0][
                0] == 14846
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_from_cc_avg").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_from_cc_avg").collect()[0][
                0] == 1142
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_from_cc_max").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_from_cc_max").collect()[0][
                0] == 1142
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_number_of_calls_from_cc_min").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "touchpoints_duration_of_calls_from_cc_min").collect()[0][
                0] == 1142

        l4_from_call_center = l4_rolling_window(l2_from_call_center,var_project_context.catalog.load(
                                                                       'params:l4_touchpoints_from_call_center_features'))
        # sum_number_of_calls_from_cc_sum_weekly_last_week
        # sum_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # sum_number_of_calls_from_cc_sum_weekly_last_four_week
        # sum_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # sum_duration_of_calls_from_cc_sum_weekly_last_week
        # sum_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426

        # sum_duration_of_calls_from_cc_sum_weekly_last_four_week
        # sum_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_touchpoints_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # avg_number_of_calls_from_cc_sum_weekly_last_week
        # avg_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # avg_number_of_calls_from_cc_sum_weekly_last_four_week
        # avg_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # avg_duration_of_calls_from_cc_sum_weekly_last_week
        # avg_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # avg_duration_of_calls_from_cc_sum_weekly_last_four_week
        # avg_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_touchpoints_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # max_number_of_calls_from_cc_sum_weekly_last_week
        # max_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # max_number_of_calls_from_cc_sum_weekly_last_four_week
        # max_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21

        # max_duration_of_calls_from_cc_sum_weekly_last_week
        # max_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # max_duration_of_calls_from_cc_sum_weekly_last_four_week
        # max_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_touchpoints_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # min_touchpoints_number_of_calls_from_cc_sum_weekly_last_week
        # min_touchpoints_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # min_touchpoints_number_of_calls_from_cc_sum_weekly_last_four_week
        # min_touchpoints_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # min_duration_of_calls_from_cc_sum_weekly_last_week
        # min_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # min_duration_of_calls_from_cc_sum_weekly_last_four_week
        # min_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_touchpoints_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426

    def l4_data(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        interactionType = ['phone', 'voice','offline','bot','my_ais','email']

        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        part_date = [iTemp.date().strftime("%Y%m%d") for iTemp in my_dates_list]
        interactionType_list = [interactionType[random.randint(0, 5)] for iTemp in range(0, len(my_dates_list))]
        df = spark.createDataFrame(zip(my_dates,my_dates, part_date,interactionType_list), schema=['register_date','temp', 'partition_date','interactiontype']) \
            .withColumn("subscription_identifier", F.lit(1)) \
            .withColumn("topic", F.lit('information')) \
            .withColumn("caseID", F.lit('Uy0xOC0wMDA3NTQ3NDY=')) \
            .withColumn("mobilenumber", F.lit('ZmxkMDRRSmJVS0lLZlRDcG1HTVZlTHpCdXJKUGlNN1RwYzYyNzVBN01sRklSRXRHOGNIMno0bFlyMlFtdk5zdA==')) \
            .withColumn("tt_caseID", F.lit("null"))

        l1_nim_work = node_from_config(df, var_project_context.catalog.load(
            'params:l1_touchpoints_nim_work_features'))
        l1_nim_work = l1_nim_work.withColumn("subscription_identifier", F.lit(1))
        l2_nim_work = l2_massive_processing_with_expansion(l1_nim_work, var_project_context.catalog.load(
            'params:l2_touchpoints_nim_work_features'))
        
        l3_nim_work = expansion(l1_nim_work, var_project_context.catalog.load(
            'params:l3_touchpoints_nim_work_features'))
        
        l4_nim_work = l4_rolling_window(l2_nim_work, var_project_context.catalog.load(
            'params:l4_touchpoints_nim_work_features'))

        return l4_nim_work

    def test_nim_work_sum(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        l4_nim_work = TestUnitTp().l4_data(project_context)
        # sum_num_of_commercial_question_with_cc_sum_weekly_last_week
        # sum_num_of_commercial_question_with_cc_sum_weekly_last_two_week
        # sum_num_of_commercial_question_with_cc_sum_weekly_last_four_week
        # sum_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 6
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 6
        # sum_num_of_claims_with_cc_sum_weekly_last_week
        # sum_num_of_claims_with_cc_sum_weekly_last_two_week
        # sum_num_of_claims_with_cc_sum_weekly_last_four_week
        # sum_num_of_claims_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 6
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 6
        # sum_num_of_consultation_with_cc_sum_weekly_last_week
        # sum_num_of_consultation_with_cc_sum_weekly_last_two_week
        # sum_num_of_consultation_with_cc_sum_weekly_last_four_week
        # sum_num_of_consultation_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 6
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 6
        # sum_num_of_commercial_question_in_store_sum_weekly_last_week
        # sum_num_of_commercial_question_in_store_sum_weekly_last_two_week
        # sum_num_of_commercial_question_in_store_sum_weekly_last_four_week
        # sum_num_of_commercial_question_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_claims_in_store_sum_weekly_last_week
        # sum_num_of_claims_in_store_sum_weekly_last_two_week
        # sum_num_of_claims_in_store_sum_weekly_last_four_week
        # sum_num_of_claims_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_consultation_in_store_sum_weekly_last_week
        # sum_num_of_consultation_in_store_sum_weekly_last_two_week
        # sum_num_of_consultation_in_store_sum_weekly_last_four_week
        # sum_num_of_consultation_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_commercial_question_with_chatbot_sum_weekly_last_week
        # sum_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week
        # sum_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week
        # sum_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_claims_with_chatbot_sum_weekly_last_week
        # sum_num_of_claims_with_chatbot_sum_weekly_last_two_week
        # sum_num_of_claims_with_chatbot_sum_weekly_last_four_week
        # sum_num_of_claims_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_consultation_with_chatbot_sum_weekly_last_week
        # sum_num_of_consultation_with_chatbot_sum_weekly_last_two_week
        # sum_num_of_consultation_with_chatbot_sum_weekly_last_four_week
        # sum_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_chatbot_visit_sum_weekly_last_week
        # sum_num_of_chatbot_visit_sum_weekly_last_two_week
        # sum_num_of_chatbot_visit_sum_weekly_last_four_week
        # sum_num_of_chatbot_visit_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_chatbot_visit_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_chatbot_visit_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_chatbot_visit_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_chatbot_visit_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_days_with_chatbot_sum_weekly_last_week
        # sum_num_of_days_with_chatbot_sum_weekly_last_two_week
        # sum_num_of_days_with_chatbot_sum_weekly_last_four_week
        # sum_num_of_days_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_commercial_question_with_ais_app_sum_weekly_last_week
        # sum_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week
        # sum_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week
        # sum_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_claims_with_ais_app_sum_weekly_last_week
        # sum_num_of_claims_with_ais_app_sum_weekly_last_two_week
        # sum_num_of_claims_with_ais_app_sum_weekly_last_four_week
        # sum_num_of_claims_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_consultation_with_ais_app_sum_weekly_last_week
        # sum_num_of_consultation_with_ais_app_sum_weekly_last_two_week
        # sum_num_of_consultation_with_ais_app_sum_weekly_last_four_week
        # sum_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_days_with_ais_app_sum_weekly_last_week
        # sum_num_of_days_with_ais_app_sum_weekly_last_two_week
        # sum_num_of_days_with_ais_app_sum_weekly_last_four_week
        # sum_num_of_days_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_days_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # sum_num_of_commercial_question_online_sum_weekly_last_week
        # sum_num_of_commercial_question_online_sum_weekly_last_two_week
        # sum_num_of_commercial_question_online_sum_weekly_last_four_week
        # sum_num_of_commercial_question_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_online_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_online_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_commercial_question_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_claims_with_online_sum_weekly_last_week
        # sum_num_of_claims_with_online_sum_weekly_last_two_week
        # sum_num_of_claims_with_online_sum_weekly_last_four_week
        # sum_num_of_claims_with_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_online_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_online_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_claims_with_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
        # sum_num_of_consultation_online_sum_weekly_last_week
        # sum_num_of_consultation_online_sum_weekly_last_two_week
        # sum_num_of_consultation_online_sum_weekly_last_four_week
        # sum_num_of_consultation_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_online_sum_weekly_last_two_week").collect()[0][
                0] == 4
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_online_sum_weekly_last_four_week").collect()[0][
                0] == 5
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "sum_touchpoints_num_of_consultation_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 5
    def test_nim_work_avg(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        l4_nim_work = TestUnitTp().l4_data(project_context)       
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # avg_num_of_claims_with_cc_sum_weekly_last_week
        # avg_num_of_claims_with_cc_sum_weekly_last_two_week
        # avg_num_of_claims_with_cc_sum_weekly_last_four_week
        # avg_num_of_claims_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # avg_num_of_consultation_with_cc_sum_weekly_last_week
        # avg_num_of_consultation_with_cc_sum_weekly_last_two_week
        # avg_num_of_consultation_with_cc_sum_weekly_last_four_week
        # avg_num_of_consultation_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # avg_num_of_commercial_question_in_store_sum_weekly_last_week
        # avg_num_of_commercial_question_in_store_sum_weekly_last_two_week
        # avg_num_of_commercial_question_in_store_sum_weekly_last_four_week
        # avg_num_of_commercial_question_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_claims_in_store_sum_weekly_last_week
        # avg_num_of_claims_in_store_sum_weekly_last_two_week
        # avg_num_of_claims_in_store_sum_weekly_last_four_week
        # avg_num_of_claims_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_consultation_in_store_sum_weekly_last_week
        # avg_num_of_consultation_in_store_sum_weekly_last_two_week
        # avg_num_of_consultation_in_store_sum_weekly_last_four_week
        # avg_num_of_consultation_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_commercial_question_with_chatbot_sum_weekly_last_week
        # avg_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week
        # avg_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week
        # avg_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1.7       
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_claims_with_chatbot_sum_weekly_last_week
        # avg_num_of_claims_with_chatbot_sum_weekly_last_two_week
        # avg_num_of_claims_with_chatbot_sum_weekly_last_four_week
        # avg_num_of_claims_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_consultation_with_chatbot_sum_weekly_last_week
        # avg_num_of_consultation_with_chatbot_sum_weekly_last_two_week
        # avg_num_of_consultation_with_chatbot_sum_weekly_last_four_week
        # avg_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1         
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_chatbot_visit_sum_weekly_last_week
        # avg_num_of_chatbot_visit_sum_weekly_last_two_week
        # avg_num_of_chatbot_visit_sum_weekly_last_four_week
        # avg_num_of_chatbot_visit_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_chatbot_visit_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_chatbot_visit_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_chatbot_visit_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_chatbot_visit_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_days_with_chatbot_sum_weekly_last_week
        # avg_num_of_days_with_chatbot_sum_weekly_last_two_week
        # avg_num_of_days_with_chatbot_sum_weekly_last_four_week
        # avg_num_of_days_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_commercial_question_with_ais_app_sum_weekly_last_week
        # avg_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week
        # avg_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week
        # avg_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_claims_with_ais_app_sum_weekly_last_week
        # avg_num_of_claims_with_ais_app_sum_weekly_last_two_week
        # avg_num_of_claims_with_ais_app_sum_weekly_last_four_week
        # avg_num_of_claims_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_consultation_with_ais_app_sum_weekly_last_week
        # avg_num_of_consultation_with_ais_app_sum_weekly_last_two_week
        # avg_num_of_consultation_with_ais_app_sum_weekly_last_four_week
        # avg_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_days_with_ais_app_sum_weekly_last_week
        # avg_num_of_days_with_ais_app_sum_weekly_last_two_week
        # avg_num_of_days_with_ais_app_sum_weekly_last_four_week
        # avg_num_of_days_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_days_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0.7
        # avg_num_of_commercial_question_online_sum_weekly_last_week
        # avg_num_of_commercial_question_online_sum_weekly_last_two_week
        # avg_num_of_commercial_question_online_sum_weekly_last_four_week
        # avg_num_of_commercial_question_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_online_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_online_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_commercial_question_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_claims_with_online_sum_weekly_last_week
        # avg_num_of_claims_with_online_sum_weekly_last_two_week
        # avg_num_of_claims_with_online_sum_weekly_last_four_week
        # avg_num_of_claims_with_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_online_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_online_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_claims_with_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
        # avg_num_of_consultation_online_sum_weekly_last_week
        # avg_num_of_consultation_online_sum_weekly_last_two_week
        # avg_num_of_consultation_online_sum_weekly_last_four_week
        # avg_num_of_consultation_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_online_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_online_sum_weekly_last_four_week").collect()[0][
                0] == 1.7
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "avg_touchpoints_num_of_consultation_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1.7
    def test_nim_work_min(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        l4_nim_work = TestUnitTp().l4_data(project_context)
        # min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_week
        # min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_two_week
        # min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_four_week
        # min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        
        # min_touchpoints_num_of_claims_with_cc_sum_weekly_last_week
        # min_touchpoints_num_of_claims_with_cc_sum_weekly_last_two_week
        # min_touchpoints_num_of_claims_with_cc_sum_weekly_last_four_week
        # min_touchpoints_num_of_claims_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_week
        # min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_two_week
        # min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_four_week
        # min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_week
        # min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_two_week
        # min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_four_week
        # min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_claims_in_store_sum_weekly_last_week
        # min_touchpoints_num_of_claims_in_store_sum_weekly_last_two_week
        # min_touchpoints_num_of_claims_in_store_sum_weekly_last_four_week
        # min_touchpoints_num_of_claims_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_consultation_in_store_sum_weekly_last_week
        # min_touchpoints_num_of_consultation_in_store_sum_weekly_last_two_week
        # min_touchpoints_num_of_consultation_in_store_sum_weekly_last_four_week
        # min_touchpoints_num_of_consultation_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_week
        # min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week
        # min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week
        # min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_week
        # min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_two_week
        # min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_four_week
        # min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_week
        # min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_two_week
        # min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_four_week
        # min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_chatbot_visit_sum_weekly_last_week
        # min_touchpoints_num_of_chatbot_visit_sum_weekly_last_two_week
        # min_touchpoints_num_of_chatbot_visit_sum_weekly_last_four_week
        # min_touchpoints_num_of_chatbot_visit_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_chatbot_visit_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_chatbot_visit_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_chatbot_visit_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_chatbot_visit_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_week
        # min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_two_week
        # min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_four_week
        # min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_week
        # min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week
        # min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week
        # min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_week
        # min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_two_week
        # min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_four_week
        # min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_week
        # min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_two_week
        # min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_four_week
        # min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_week
        # min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_two_week
        # min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_four_week
        # min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_days_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 0
        # min_touchpoints_num_of_commercial_question_online_sum_weekly_last_week
        # min_touchpoints_num_of_commercial_question_online_sum_weekly_last_two_week
        # min_touchpoints_num_of_commercial_question_online_sum_weekly_last_four_week
        # min_touchpoints_num_of_commercial_question_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_online_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_online_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_commercial_question_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_claims_with_online_sum_weekly_last_week
        # min_touchpoints_num_of_claims_with_online_sum_weekly_last_two_week
        # min_touchpoints_num_of_claims_with_online_sum_weekly_last_four_week
        # min_touchpoints_num_of_claims_with_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_online_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_online_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_claims_with_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # min_touchpoints_num_of_consultation_online_sum_weekly_last_week
        # min_touchpoints_num_of_consultation_online_sum_weekly_last_two_week
        # min_touchpoints_num_of_consultation_online_sum_weekly_last_four_week
        # min_touchpoints_num_of_consultation_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_online_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_online_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "min_touchpoints_num_of_consultation_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
    def test_nim_work_max(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        l4_nim_work = TestUnitTp().l4_data(project_context)
         # max_num_of_commercial_question_with_cc_sum_weekly_last_week
        # max_num_of_commercial_question_with_cc_sum_weekly_last_two_week
        # max_num_of_commercial_question_with_cc_sum_weekly_last_four_week
        # max_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_claims_with_cc_sum_weekly_last_week
        # max_num_of_claims_with_cc_sum_weekly_last_two_week
        # max_num_of_claims_with_cc_sum_weekly_last_four_week
        # max_num_of_claims_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_consultation_with_cc_sum_weekly_last_week
        # max_num_of_consultation_with_cc_sum_weekly_last_two_week
        # max_num_of_consultation_with_cc_sum_weekly_last_four_week
        # max_num_of_consultation_with_cc_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_cc_sum_weekly_last_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_cc_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_cc_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_commercial_question_in_store_sum_weekly_last_week
        # max_num_of_commercial_question_in_store_sum_weekly_last_two_week
        # max_num_of_commercial_question_in_store_sum_weekly_last_four_week
        # max_num_of_commercial_question_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # max_num_of_claims_in_store_sum_weekly_last_week
        # max_num_of_claims_in_store_sum_weekly_last_two_week
        # max_num_of_claims_in_store_sum_weekly_last_four_week
        # max_num_of_claims_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # max_num_of_consultation_in_store_sum_weekly_last_week
        # max_num_of_consultation_in_store_sum_weekly_last_two_week
        # max_num_of_consultation_in_store_sum_weekly_last_four_week
        # max_num_of_consultation_in_store_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_in_store_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_in_store_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_in_store_sum_weekly_last_four_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_in_store_sum_weekly_last_twelve_week").collect()[0][
                0] == 1
        # max_num_of_commercial_question_with_chatbot_sum_weekly_last_week
        # max_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week
        # max_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week
        # max_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_claims_with_chatbot_sum_weekly_last_week
        # max_num_of_claims_with_chatbot_sum_weekly_last_two_week
        # max_num_of_claims_with_chatbot_sum_weekly_last_four_week
        # max_num_of_claims_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_consultation_with_chatbot_sum_weekly_last_week
        # max_num_of_consultation_with_chatbot_sum_weekly_last_two_week
        # max_num_of_consultation_with_chatbot_sum_weekly_last_four_week
        # max_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_chatbot_visit_sum_weekly_last_week
        # max_num_of_chatbot_visit_sum_weekly_last_two_week
        # max_num_of_chatbot_visit_sum_weekly_last_four_week
        # max_num_of_chatbot_visit_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_chatbot_visit_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_chatbot_visit_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_chatbot_visit_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_chatbot_visit_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_days_with_chatbot_sum_weekly_last_week
        # max_num_of_days_with_chatbot_sum_weekly_last_two_week
        # max_num_of_days_with_chatbot_sum_weekly_last_four_week
        # max_num_of_days_with_chatbot_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_chatbot_sum_weekly_last_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_chatbot_sum_weekly_last_two_week").collect()[0][
                0] == 1
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_chatbot_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_chatbot_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_commercial_question_with_ais_app_sum_weekly_last_week
        # max_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week
        # max_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week
        # max_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_claims_with_ais_app_sum_weekly_last_week
        # max_num_of_claims_with_ais_app_sum_weekly_last_two_week
        # max_num_of_claims_with_ais_app_sum_weekly_last_four_week
        # max_num_of_claims_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_consultation_with_ais_app_sum_weekly_last_week
        # max_num_of_consultation_with_ais_app_sum_weekly_last_two_week
        # max_num_of_consultation_with_ais_app_sum_weekly_last_four_week
        # max_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_days_with_ais_app_sum_weekly_last_week
        # max_num_of_days_with_ais_app_sum_weekly_last_two_week
        # max_num_of_days_with_ais_app_sum_weekly_last_four_week
        # max_num_of_days_with_ais_app_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_ais_app_sum_weekly_last_week").collect()[0][
                0] == 0
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_ais_app_sum_weekly_last_two_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_ais_app_sum_weekly_last_four_week").collect()[0][
                0] == 2
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_days_with_ais_app_sum_weekly_last_twelve_week").collect()[0][
                0] == 2
        # max_num_of_commercial_question_online_sum_weekly_last_week
        # max_num_of_commercial_question_online_sum_weekly_last_two_week
        # max_num_of_commercial_question_online_sum_weekly_last_four_week
        # max_num_of_commercial_question_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_online_sum_weekly_last_two_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_online_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_commercial_question_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_claims_with_online_sum_weekly_last_week
        # max_num_of_claims_with_online_sum_weekly_last_two_week
        # max_num_of_claims_with_online_sum_weekly_last_four_week
        # max_num_of_claims_with_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_online_sum_weekly_last_two_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_online_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_claims_with_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
        # max_num_of_consultation_online_sum_weekly_last_week
        # max_num_of_consultation_online_sum_weekly_last_two_week
        # max_num_of_consultation_online_sum_weekly_last_four_week
        # max_num_of_consultation_online_sum_weekly_last_twelve_week
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_online_sum_weekly_last_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_online_sum_weekly_last_two_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_online_sum_weekly_last_four_week").collect()[0][
                0] == 3
        l4_nim_work.where(
                "start_of_week = '2020-01-20'").select(
                "max_touchpoints_num_of_consultation_online_sum_weekly_last_twelve_week").collect()[0][
                0] == 3
    def test_nim_work(self,project_context):
 
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        interactionType = ['phone', 'voice','offline','bot','my_ais','email']

        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        part_date = [iTemp.date().strftime("%Y%m%d") for iTemp in my_dates_list]
        interactionType_list = [interactionType[random.randint(0, 5)] for iTemp in range(0, len(my_dates_list))]
        df = spark.createDataFrame(zip(my_dates,my_dates, part_date,interactionType_list), schema=['register_date','temp', 'partition_date','interactiontype']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("topic", F.lit('information')) \
            .withColumn("caseID", F.lit('Uy0xOC0wMDA3NTQ3NDY=')) \
            .withColumn("mobilenumber", F.lit('ZmxkMDRRSmJVS0lLZlRDcG1HTVZlTHpCdXJKUGlNN1RwYzYyNzVBN01sRklSRXRHOGNIMno0bFlyMlFtdk5zdA==')) \
            .withColumn("tt_caseID", F.lit("null"))

        random.seed(100)
        l1_nim_work = node_from_config(df, var_project_context.catalog.load(
            'params:l1_touchpoints_nim_work_features'))

        # num_of_commercial_question_with_cc
        # num_of_claims_with_cc
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_commercial_question_with_cc").collect()[0][
                0] == 1
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_claims_with_cc").collect()[0][
                0] == 1
        # num_of_consultation_with_cc
        # num_of_commercial_question_in_store
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_consultation_with_cc").collect()[0][
                0] == 1
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_commercial_question_in_store").collect()[0][
                0] == 0
        # num_of_claims_in_store
        # num_of_consultation_in_store
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_claims_in_store").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_consultation_in_store").collect()[0][
                0] == 0
        # num_of_commercial_question_with_chatbot
        # num_of_claims_with_chatbot
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_commercial_question_with_chatbot").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_claims_with_chatbot").collect()[0][
                0] == 0
        # num_of_consultation_with_chatbot
        # num_of_chatbot_visit
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_consultation_with_chatbot").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_chatbot_visit").collect()[0][
                0] == 0
        # num_of_days_with_chatbot
        # num_of_commercial_question_with_ais_app
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_days_with_chatbot").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_commercial_question_with_ais_app").collect()[0][
                0] == 0
        # num_of_claims_with_ais_app
        # num_of_consultation_with_ais_app
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_claims_with_ais_app").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_consultation_with_ais_app").collect()[0][
                0] == 0
        # num_of_days_with_ais_app
        # num_of_commercial_question_online
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_days_with_ais_app").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_commercial_question_online").collect()[0][
                0] == 0
        # num_of_claims_with_online
        # num_of_consultation_online
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_claims_with_online").collect()[0][
                0] == 0
        assert \
            l1_nim_work.where(
                "start_of_week = '2020-02-10'").select(
                "touchpoints_num_of_consultation_online").collect()[0][
                0] == 0
        l1_nim_work = l1_nim_work.withColumn("subscription_identifier", F.lit(1))
        l2_nim_work = l2_massive_processing_with_expansion(l1_nim_work, var_project_context.catalog.load(
            'params:l2_touchpoints_nim_work_features'))
        
        # num_of_commercial_question_with_cc_sum
        # num_of_claims_with_cc_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_commercial_question_with_cc_sum").collect()[0][
                0] == 3
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_claims_with_cc_sum").collect()[0][
                0] == 3
        # num_of_consultation_with_cc_sum
        # num_of_commercial_question_in_store_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_consultation_with_cc_sum").collect()[0][
                0] == 3
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_commercial_question_in_store_sum").collect()[0][
                0] == 2
        # num_of_claims_in_store_sum
        # num_of_consultation_in_store_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_claims_in_store_sum").collect()[0][
                0] == 2
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_consultation_in_store_sum").collect()[0][
                0] == 2
        # num_of_commercial_question_with_chatbot_sum
        # num_of_claims_with_chatbot_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_commercial_question_with_chatbot_sum").collect()[0][
                0] == 1
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_claims_with_chatbot_sum").collect()[0][
                0] == 1
        # num_of_consultation_with_chatbot_sum
        # num_of_chatbot_visit_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_consultation_with_chatbot_sum").collect()[0][
                0] == 1
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_chatbot_visit_sum").collect()[0][
                0] == 1
        # num_of_days_with_chatbot_sum
        # num_of_commercial_question_with_ais_app_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_days_with_chatbot_sum").collect()[0][
                0] == 1
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_commercial_question_with_ais_app_sum").collect()[0][
                0] == 0
        # num_of_claims_with_ais_app_sum
        # num_of_consultation_with_ais_app_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_claims_with_ais_app_sum").collect()[0][
                0] == 0
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_consultation_with_ais_app_sum").collect()[0][
                0] == 0
        # num_of_days_with_ais_app_sum
        # num_of_commercial_question_online_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_days_with_ais_app_sum").collect()[0][
                0] == 0
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_commercial_question_online_sum").collect()[0][
                0] == 1
        # num_of_claims_with_online_sum
        # num_of_consultation_online_sum
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_claims_with_online_sum").collect()[0][
                0] == 1
        assert \
            l2_nim_work.where(
                "start_of_week = '2020-01-27'").select(
                "touchpoints_num_of_consultation_online_sum").collect()[0][
                0] == 1

        l3_nim_work = expansion(l1_nim_work, var_project_context.catalog.load(
            'params:l3_touchpoints_nim_work_features'))
        
        # num_of_commercial_question_with_cc_sum
        # num_of_claims_with_cc_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_commercial_question_with_cc_sum").collect()[0][
                0] == 12
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_claims_with_cc_sum").collect()[0][
                0] == 12
        # num_of_consultation_with_cc_sum
        # num_of_commercial_question_in_store_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_consultation_with_cc_sum").collect()[0][
                0] == 12
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_commercial_question_in_store_sum").collect()[0][
                0] == 6
        # num_of_claims_in_store_sum
        # num_of_consultation_in_store_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_claims_in_store_sum").collect()[0][
                0] == 6
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_consultation_in_store_sum").collect()[0][
                0] == 6
        # num_of_commercial_question_with_chatbot_sum
        # num_of_claims_with_chatbot_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_commercial_question_with_chatbot_sum").collect()[0][
                0] == 5
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_claims_with_chatbot_sum").collect()[0][
                0] == 5
        # num_of_consultation_with_chatbot_sum
        # num_of_chatbot_visit_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_consultation_with_chatbot_sum").collect()[0][
                0] == 5
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_chatbot_visit_sum").collect()[0][
                0] == 5
        # num_of_days_with_chatbot_sum
        # num_of_commercial_question_with_ais_app_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_days_with_chatbot_sum").collect()[0][
                0] == 5
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_commercial_question_with_ais_app_sum").collect()[0][
                0] == 2
        # num_of_claims_with_ais_app_sum
        # num_of_consultation_with_ais_app_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_claims_with_ais_app_sum").collect()[0][
                0] == 2
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_consultation_with_ais_app_sum").collect()[0][
                0] == 2
        # num_of_days_with_ais_app_sum
        # num_of_commercial_question_online_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_days_with_ais_app_sum").collect()[0][
                0] == 2
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_commercial_question_online_sum").collect()[0][
                0] == 6
        # num_of_claims_with_online_sum
        # num_of_consultation_online_sum
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_claims_with_online_sum").collect()[0][
                0] == 6
        assert \
            l3_nim_work.where(
                "start_of_month = '2020-01-01'").select(
                "touchpoints_num_of_consultation_online_sum").collect()[0][
                0] == 6

    def test_ivr_l1_to_l4(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        test= df_ivr_log.withColumnRenamed("mobile_number","access_method_num")
        l1_ivr = test.join(customer_pro,on=["access_method_num","event_partition_date","start_of_week"],how="left")

        l1_ivr.show()

        l2_ivr = l2_massive_processing_with_expansion(l1_ivr,var_project_context.catalog.load(
            'params:l2_touchpoints_ivr_features'))
        l2_ivr.show()

        l1_ivr_start_of_month = test.join(customer_pro, on=["access_method_num", "event_partition_date", "start_of_month"], how="left")
        l3_ivr = expansion(l1_ivr_start_of_month, var_project_context.catalog.load(
            'params:l3_touchpoints_ivr_features'))
        l3_ivr.show()

        l4_ivr = l4_rolling_window(l2_ivr,var_project_context.catalog.load(
            'params:l4_touchpoints_ivr_features'))
        l4_ivr.show()

        exit(2)

    def test_ivr_l2(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        test= df_ivr_log.withColumnRenamed("mobile_number","access_method_num")
        l1_ivr = test.join(customer_pro,on=["access_method_num","event_partition_date","start_of_week"],how="left")

        l2_ivr = l2_massive_processing_with_expansion(l1_ivr,var_project_context.catalog.load(
            'params:l2_touchpoints_ivr_features'))
        l2_ivr.show()

        ##################################### Sum ######################################################################
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_num_of_call_ivr_sum").collect()[0][0]),
                     2) == 2
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_ivr_sum").collect()[0][0]), 2) == 2
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_customer_sum").collect()[0][0]), 2) == 2
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_sum").collect()[0][0]), 2) == 2
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_moring_calls_sum").collect()[0][0]),
                     2) == 2
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_afternoon_calls_sum").collect()[0][0]),
                     2) == 2
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_evening_calls_sum").collect()[0][0]),
            2) == 2
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_night_calls_sum").collect()[0][0]),
            2) == 2
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_distinct_languages_chosen_sum").collect()[
                0][0]), 2) == 2
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_unsuccesful_connect_tuxedo_sum").collect()[
                0][0]), 2) == 2

        ####################################### AVG ###################################################################

        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_num_of_call_ivr_avg").collect()[0][0]),
                     2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_ivr_avg").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_customer_avg").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_avg").collect()[0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_moring_calls_avg").collect()[0][0]),
                     2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_afternoon_calls_avg").collect()[0][0]),
                     2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_evening_calls_avg").collect()[0][0]),
            2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_night_calls_avg").collect()[0][0]),
            2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_distinct_languages_chosen_avg").collect()[
                0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_unsuccesful_connect_tuxedo_avg").collect()[
                0][0]), 2) == 1

        ############################### MAX #########################################################################

        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_num_of_call_ivr_max").collect()[0][0]),
                     2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_ivr_max").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_customer_max").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_max").collect()[0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_moring_calls_max").collect()[0][0]),
                     2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_afternoon_calls_max").collect()[0][0]),
                     2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_evening_calls_max").collect()[0][0]),
            2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_night_calls_max").collect()[0][0]),
            2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_distinct_languages_chosen_max").collect()[
                0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_unsuccesful_connect_tuxedo_max").collect()[
                0][0]), 2) == 1

        ################################## MIN ########################################################################

        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_num_of_call_ivr_min").collect()[0][0]),
                     2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_ivr_min").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_customer_min").collect()[0][0]), 2) == 1
        assert round(float(l2_ivr.where("start_of_week = '2020-01-06'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_min").collect()[0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_moring_calls_min").collect()[0][0]),
                     2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_ivr_afternoon_calls_min").collect()[0][0]),
                     2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_evening_calls_min").collect()[0][0]),
            2) == 1
        assert round(
            float(l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_night_calls_min").collect()[0][0]),
            2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_distinct_languages_chosen_min").collect()[
                0][0]), 2) == 1
        assert round(float(
            l2_ivr.where("start_of_week = '2020-01-06'").select("touchpoints_unsuccesful_connect_tuxedo_min").collect()[
                0][0]), 2) == 1

    def test_ivr_l3(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        set_value(project_context)

        test = df_ivr_log.withColumnRenamed("mobile_number", "access_method_num")
        l1_ivr_start_of_month = test.join(customer_pro,on=["access_method_num", "event_partition_date", "start_of_month"],
                                          how="left")
        l3_ivr = expansion(l1_ivr_start_of_month, var_project_context.catalog.load(
            'params:l3_touchpoints_ivr_features'))
        l3_ivr.show()

        ##################################### Sum ######################################################################
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_num_of_call_ivr_sum").collect()[0][0]),
            2) == 2
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_ivr_sum").collect()[0][0]), 2) == 2
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_customer_sum").collect()[0][0]), 2) == 2
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_sum").collect()[0][0]), 2) == 2
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_moring_calls_sum").collect()[0][0]),
            2) == 2
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_afternoon_calls_sum").collect()[0][
                0]),
            2) == 2
        assert round(
            float(
                l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_evening_calls_sum").collect()[0][0]),
            2) == 2
        assert round(
            float(l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_night_calls_sum").collect()[0][0]),
            2) == 2
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_distinct_languages_chosen_sum").collect()[
                0][0]), 2) == 2
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select(
                "touchpoints_unsuccesful_connect_tuxedo_sum").collect()[
                0][0]), 2) == 2

        ####################################### AVG ###################################################################

        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_num_of_call_ivr_avg").collect()[0][0]),
            2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_ivr_avg").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_customer_avg").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_avg").collect()[0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_moring_calls_avg").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_afternoon_calls_avg").collect()[0][
                0]),
            2) == 1
        assert round(
            float(
                l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_evening_calls_avg").collect()[0][0]),
            2) == 1
        assert round(
            float(l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_night_calls_avg").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_distinct_languages_chosen_avg").collect()[
                0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select(
                "touchpoints_unsuccesful_connect_tuxedo_avg").collect()[
                0][0]), 2) == 1

        ############################### MAX #########################################################################

        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_num_of_call_ivr_max").collect()[0][0]),
            2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_ivr_max").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_customer_max").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_max").collect()[0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_moring_calls_max").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_afternoon_calls_max").collect()[0][
                0]),
            2) == 1
        assert round(
            float(
                l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_evening_calls_max").collect()[0][0]),
            2) == 1
        assert round(
            float(l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_night_calls_max").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_distinct_languages_chosen_max").collect()[
                0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select(
                "touchpoints_unsuccesful_connect_tuxedo_max").collect()[
                0][0]), 2) == 1

        ################################## MIN ########################################################################

        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_num_of_call_ivr_min").collect()[0][0]),
            2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_ivr_min").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_customer_min").collect()[0][0]), 2) == 1
        assert round(float(l3_ivr.where("start_of_month = '2020-01-01'").select(
            "touchpoints_num_of_disconnection_by_transfer_agent_min").collect()[0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_moring_calls_min").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_ivr_afternoon_calls_min").collect()[0][
                0]),
            2) == 1
        assert round(
            float(
                l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_evening_calls_min").collect()[0][0]),
            2) == 1
        assert round(
            float(l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_night_calls_min").collect()[0][0]),
            2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select("touchpoints_distinct_languages_chosen_min").collect()[
                0][0]), 2) == 1
        assert round(float(
            l3_ivr.where("start_of_month = '2020-01-01'").select(
                "touchpoints_unsuccesful_connect_tuxedo_min").collect()[
                0][0]), 2) == 1


