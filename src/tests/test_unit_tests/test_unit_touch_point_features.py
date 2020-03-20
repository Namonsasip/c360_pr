from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.re_usable_functions import l2_massive_processing_with_expansion
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import timedelta, datetime


class TestUnitTp:

    def test_to_call_center(self,project_context):

        # # D:\save\test\project-samudra\src\tests\test_unit_tests\test_unit_touch_point_features.py::TestUnitTp::test_to_call_center
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
                "number_of_calls_on_cc").collect()[0][
                0] == 7
        assert \
            l1_to_call_center.where("caller_no = 'hYAUKs+f31uJzUZp3iM5F.fO8CgW7107gM602izDqDn.6GjSvNd8lhU6af7ItzfP'").select(
                "duration_of_calls_on_cc").collect()[0][
                0] == 1142

        l1_to_call_center=l1_to_call_center.withColumn("access_method_num", F.lit(1))
        l2_to_call_center = l2_massive_processing_with_expansion(l1_to_call_center, var_project_context.catalog.load(
            'params:l2_touchpoints_to_call_center_features'))

        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "number_of_calls_on_cc_sum").collect()[0][
                0] == 21
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "duration_of_calls_on_cc_sum").collect()[0][
                0] == 3426
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "number_of_calls_on_cc_avg").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "duration_of_calls_on_cc_avg").collect()[0][
                0] == 1142
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "number_of_calls_on_cc_max").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "duration_of_calls_on_cc_max").collect()[0][
                0] == 1142
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "number_of_calls_on_cc_min").collect()[0][
                0] == 7
        assert \
            l2_to_call_center.where(
                "start_of_week = '2019-12-30'").select(
                "duration_of_calls_on_cc_min").collect()[0][
                0] == 1142

        l3_to_call_center = expansion(l1_to_call_center, var_project_context.catalog.load(
            'params:l3_touchpoints_to_call_center_features'))

        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_on_cc_sum").collect()[0][
                0] == 91
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_on_cc_sum").collect()[0][
                0] == 14846
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_on_cc_avg").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_on_cc_avg").collect()[0][
                0] == 1142
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_on_cc_max").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_on_cc_max").collect()[0][
                0] == 1142
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_on_cc_min").collect()[0][
                0] == 7
        assert \
            l3_to_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_on_cc_min").collect()[0][
                0] == 1142

        l4_to_call_center = l4_rolling_window(l2_to_call_center, var_project_context.catalog.load(
            'params:l4_touchpoints_to_call_center_features'))



        # sum_number_of_calls_on_cc_sum_weekly_last_week
        # sum_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21


        # sum_number_of_calls_on_cc_sum_weekly_last_four_week
        # sum_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # sum_duration_of_calls_on_cc_sum_weekly_last_week
        # sum_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # sum_duration_of_calls_on_cc_sum_weekly_last_four_week
        # sum_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # avg_number_of_calls_on_cc_sum_weekly_last_week
        # avg_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # avg_number_of_calls_on_cc_sum_weekly_last_four_week
        # avg_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # avg_duration_of_calls_on_cc_sum_weekly_last_week
        # avg_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # avg_duration_of_calls_on_cc_sum_weekly_last_four_week
        # avg_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # max_number_of_calls_on_cc_sum_weekly_last_week
        # max_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # max_number_of_calls_on_cc_sum_weekly_last_week
        # max_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21



        # max_number_of_calls_on_cc_sum_weekly_last_four_week
        # max_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # max_duration_of_calls_on_cc_sum_weekly_last_week
        # max_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426



        # min_number_of_calls_on_cc_sum_weekly_last_week
        # min_number_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # min_number_of_calls_on_cc_sum_weekly_last_four_week
        # min_number_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21

        # min_duration_of_calls_on_cc_sum_weekly_last_week
        # min_duration_of_calls_on_cc_sum_weekly_last_two_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_on_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_on_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # min_duration_of_calls_on_cc_sum_weekly_last_four_week
        # min_duration_of_calls_on_cc_sum_weekly_last_twelve_week
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_on_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_to_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_on_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426

        l1_from_call_center = node_from_config(df, var_project_context.catalog.load(
            'params:l1_touchpoints_from_call_center_features'))

        # number_of_calls_from_cc: "sum(total_successful_call)"
        # duration_of_calls_from_cc: "sum(total_durations)"
        assert \
            l1_from_call_center.where(
                "start_of_week = '2020-03-02'").select(
                "number_of_calls_from_cc").collect()[0][
                0] == 7
        assert \
            l1_from_call_center.where(
                "start_of_week = '2020-03-02'").select(
                "duration_of_calls_from_cc").collect()[0][
                0] == 1142

        l1_from_call_center = l1_from_call_center.withColumn("access_method_num", F.lit(1))
        l2_from_call_center = l2_massive_processing_with_expansion(l1_from_call_center, var_project_context.catalog.load(
            'params:l2_touchpoints_from_call_center_features'))


        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "number_of_calls_from_cc_sum").collect()[0][
                0] == 21
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "duration_of_calls_from_cc_sum").collect()[0][
                0] == 3426
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "number_of_calls_from_cc_avg").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "duration_of_calls_from_cc_avg").collect()[0][
                0] == 1142
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "number_of_calls_from_cc_max").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "duration_of_calls_from_cc_max").collect()[0][
                0] == 1142
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "number_of_calls_from_cc_min").collect()[0][
                0] == 7
        assert \
            l2_from_call_center.where(
                "start_of_week = '2020-07-27'").select(
                "duration_of_calls_from_cc_min").collect()[0][
                0] == 1142


        l3_from_call_center = expansion(l1_from_call_center,var_project_context.catalog.load(
                                                                       'params:l3_touchpoints_from_call_center_features'))

        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_from_cc_sum").collect()[0][
                0] == 91
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_from_cc_sum").collect()[0][
                0] == 14846
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_from_cc_avg").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_from_cc_avg").collect()[0][
                0] == 1142
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_from_cc_max").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_from_cc_max").collect()[0][
                0] == 1142
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "number_of_calls_from_cc_min").collect()[0][
                0] == 7
        assert \
            l3_from_call_center.where(
                "start_of_month = '2020-05-01'").select(
                "duration_of_calls_from_cc_min").collect()[0][
                0] == 1142

        l4_from_call_center = l4_rolling_window(l2_from_call_center,var_project_context.catalog.load(
                                                                       'params:l4_touchpoints_from_call_center_features'))
        # sum_number_of_calls_from_cc_sum_weekly_last_week
        # sum_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # sum_number_of_calls_from_cc_sum_weekly_last_four_week
        # sum_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # sum_duration_of_calls_from_cc_sum_weekly_last_week
        # sum_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426

        # sum_duration_of_calls_from_cc_sum_weekly_last_four_week
        # sum_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "sum_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # avg_number_of_calls_from_cc_sum_weekly_last_week
        # avg_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # avg_number_of_calls_from_cc_sum_weekly_last_four_week
        # avg_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # avg_duration_of_calls_from_cc_sum_weekly_last_week
        # avg_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # avg_duration_of_calls_from_cc_sum_weekly_last_four_week
        # avg_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "avg_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # max_number_of_calls_from_cc_sum_weekly_last_week
        # max_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # max_number_of_calls_from_cc_sum_weekly_last_four_week
        # max_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21

        # max_duration_of_calls_from_cc_sum_weekly_last_week
        # max_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # max_duration_of_calls_from_cc_sum_weekly_last_four_week
        # max_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "max_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426
        # min_number_of_calls_from_cc_sum_weekly_last_week
        # min_number_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 21
        # min_number_of_calls_from_cc_sum_weekly_last_four_week
        # min_number_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_number_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 21
        # min_duration_of_calls_from_cc_sum_weekly_last_week
        # min_duration_of_calls_from_cc_sum_weekly_last_two_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_from_cc_sum_weekly_last_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_from_cc_sum_weekly_last_two_week").collect()[0][
                0] == 3426
        # min_duration_of_calls_from_cc_sum_weekly_last_four_week
        # min_duration_of_calls_from_cc_sum_weekly_last_twelve_week
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_from_cc_sum_weekly_last_four_week").collect()[0][
                0] == 3426
        assert \
            l4_from_call_center.where(
                "start_of_week = '2020-01-06'").select(
                "min_duration_of_calls_from_cc_sum_weekly_last_twelve_week").collect()[0][
                0] == 3426

