from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.re_usable_functions import l2_massive_processing_with_expansion
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import timedelta, datetime


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

      
        
        
       

#