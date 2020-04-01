from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import datetime
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *
from src.customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
from src.customer360.pipelines.data_engineering.nodes.loyalty_nodes.to_l1.to_l1_nodes import *


class TestUnitLoyalty:

    def gen_data(self,project_context):
            # kedro test C:\Users\myPC\Desktop\project-samudra\src\tests\test_unit_tests\test_unit_loyalty_features.py::TestUnitLoyalty::test_gen_data
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        random_type = [545,565,116,124,10,11,17,27,125,30,97,100,100041,2,15,16,26,29,118,119,120,121,122,585,13,28,117,126,127,129,130,131,132,133,134,501,505,14,25,101,123,128,525]
        # print(len(random_type))
        # exit(0)
        msisdn_type = ['ank5bkxpOWxOZStrY2FFaVpzeEd6UXl4MDNkanZnMlR0dGdjaGRyUERDUmI0aEx2S21QMVV3eHVhTGdUS2MxNA==','']
        date1 = '2019-01-01'
        date2 = '2019-04-01'

        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        random_list = [random_type[random.randint(0, 42)] for iTemp in range(0, len(my_dates))]
        msisdn_list = [msisdn_type[random.randint(0, 1)] for iTemp in range(0, len(my_dates))]
        random.seed(100)
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        point_list = [random.randint(1, 10) for iTemp in range(0, len(my_dates))]
        df = spark.createDataFrame(zip(random_list, my_dates, random_list2, my_dates,msisdn_list,point_list),
                                      schema=['category', 'temp', 'face_value','register_date','msisdn','loyalty_points_spend']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("recharge_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("recharge_time", F.lit('2019-08-01T11:25:55.000+0000')) \
            .withColumn("subscription_identifier", F.lit(123)) \
            .withColumn("mobile_no", F.lit("TEc5cUU1dXRnbDFDRCtwMTJtVlhsZ1p3NW5TOUgxcjdaWEhoY2VJMStYOGFWaTFWZUE3bmV1czZBKy5qQlE2Yg==")) \
            .withColumn("project_id", F.lit('ODA4NDY=')) \
            .withColumn("project_type_id", F.lit(1)) \
            .withColumn("project_subtype", F.lit(1)) 
        
        df  = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))

        return df

    def test_number_of_rewards(self,project_context):
        var_project_context = project_context['ProjectContext']
        df0 = TestUnitLoyalty().gen_data(project_context)

        df = node_from_config(df0,var_project_context.catalog.load(
            'params:l1_loyalty_number_of_rewards_daily'))
        # df.show(df.count(),False)
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_total").collect()[0][
                0] == 3
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_travel").collect()[0][
                0] == 1
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_ais_rewards").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_entertainment").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_food_and_drink").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_lifestyle").collect()[0][
                0] == 2
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_rewards_others").collect()[0][
                0] == 0

        df2 = node_from_config(df,var_project_context.catalog.load(
            'params:l2_loyalty_number_of_rewards_weekly'))

        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_total").collect()[0][
                0] == 21
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_travel").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_ais_rewards").collect()[0][
                0] == 1
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_entertainment").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_food_and_drink").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_lifestyle").collect()[0][
                0] == 9
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_rewards_others").collect()[0][
                0] == 3

        df3 = node_from_config(df,var_project_context.catalog.load(
            'params:l3_loyalty_number_of_rewards_monthly'))
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_total").collect()[0][
                0] == 93
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_travel").collect()[0][
                0] == 10
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_ais_rewards").collect()[0][
                0] == 8
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_entertainment").collect()[0][
                0] == 13
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_food_and_drink").collect()[0][
                0] == 15
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_lifestyle").collect()[0][
                0] == 26
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_rewards_others").collect()[0][
                0] == 15

        df4 = l4_rolling_window(df2,var_project_context.catalog.load(
            'params:l4_rolling_window_loyalty_number_of_rewards'))

        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_total_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_total_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_total_weekly_last_four_week").collect()[0][
                0] == 84
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_total_weekly_last_twelve_week").collect()[0][
                0] == 144
            # sum_loyalty_rewards_travel_weekly_last_week
            # sum_loyalty_rewards_travel_weekly_last_two_week
            # sum_loyalty_rewards_travel_weekly_last_four_week
            # sum_loyalty_rewards_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_travel_weekly_last_two_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_travel_weekly_last_four_week").collect()[0][
                0] == 6
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_travel_weekly_last_twelve_week").collect()[0][
                0] == 14
            # sum_loyalty_rewards_ais_rewards_weekly_last_week
            # sum_loyalty_rewards_ais_rewards_weekly_last_two_week
            # sum_loyalty_rewards_ais_rewards_weekly_last_four_week
            # sum_loyalty_rewards_ais_rewards_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_ais_rewards_weekly_last_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 10
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 12    
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] == 19   
            # sum_loyalty_rewards_entertainment_weekly_last_week
            # sum_loyalty_rewards_entertainment_weekly_last_two_week
            # sum_loyalty_rewards_entertainment_weekly_last_four_week
            # sum_loyalty_rewards_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_entertainment_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_entertainment_weekly_last_two_week").collect()[0][
                0] == 8
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_entertainment_weekly_last_four_week").collect()[0][
                0] == 16
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_entertainment_weekly_last_twelve_week").collect()[0][
                0] == 22

            # sum_loyalty_rewards_food_and_drink_weekly_last_week
            # sum_loyalty_rewards_food_and_drink_weekly_last_two_week
            # sum_loyalty_rewards_food_and_drink_weekly_last_four_week
            # sum_loyalty_rewards_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_food_and_drink_weekly_last_week").collect()[0][
                0] == 2
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 8
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] == 20
            # sum_loyalty_rewards_lifestyle_weekly_last_week
            # sum_loyalty_rewards_lifestyle_weekly_last_two_week
            # sum_loyalty_rewards_lifestyle_weekly_last_four_week
            # sum_loyalty_rewards_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_lifestyle_weekly_last_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_lifestyle_weekly_last_two_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_lifestyle_weekly_last_four_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_lifestyle_weekly_last_twelve_week").collect()[0][
                0] == 37
            # sum_loyalty_rewards_others_weekly_last_week
            # sum_loyalty_rewards_others_weekly_last_two_week
            # sum_loyalty_rewards_others_weekly_last_four_week
            # sum_loyalty_rewards_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_others_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_others_weekly_last_two_week").collect()[0][
                0] == 10
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_others_weekly_last_four_week").collect()[0][
                0] == 15
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_rewards_others_weekly_last_twelve_week").collect()[0][
                0] == 25
            # avg_loyalty_rewards_total_weekly_last_week
            # avg_loyalty_rewards_total_weekly_last_two_week
            # avg_loyalty_rewards_total_weekly_last_four_week
            # avg_loyalty_rewards_total_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_total_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_total_weekly_last_two_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_total_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_total_weekly_last_twelve_week").collect()[0][
                0] - 20.5714) < 0.1
            # avg_loyalty_rewards_travel_weekly_last_week
            # avg_loyalty_rewards_travel_weekly_last_two_week
            # avg_loyalty_rewards_travel_weekly_last_four_week
            # avg_loyalty_rewards_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_travel_weekly_last_two_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_travel_weekly_last_four_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_travel_weekly_last_twelve_week").collect()[0][
                0] == 2
            # avg_loyalty_rewards_ais_rewards_weekly_last_week
            # avg_loyalty_rewards_ais_rewards_weekly_last_two_week
            # avg_loyalty_rewards_ais_rewards_weekly_last_four_week
            # avg_loyalty_rewards_ais_rewards_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_ais_rewards_weekly_last_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 3
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] - 2.714) < 0.1
            # avg_loyalty_rewards_entertainment_weekly_last_week
            # avg_loyalty_rewards_entertainment_weekly_last_two_week
            # avg_loyalty_rewards_entertainment_weekly_last_four_week
            # avg_loyalty_rewards_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_entertainment_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_entertainment_weekly_last_two_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_entertainment_weekly_last_four_week").collect()[0][
                0] == 4
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_entertainment_weekly_last_twelve_week").collect()[0][
                0] - 3.142) <0.1
            # avg_loyalty_rewards_food_and_drink_weekly_last_week
            # avg_loyalty_rewards_food_and_drink_weekly_last_two_week
            # avg_loyalty_rewards_food_and_drink_weekly_last_four_week
            # avg_loyalty_rewards_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_food_and_drink_weekly_last_week").collect()[0][
                0] == 2
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 2
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] - 2.857) < 0.1
            # avg_loyalty_rewards_lifestyle_weekly_last_week
            # avg_loyalty_rewards_lifestyle_weekly_last_two_week
            # avg_loyalty_rewards_lifestyle_weekly_last_four_week
            # avg_loyalty_rewards_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_lifestyle_weekly_last_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_lifestyle_weekly_last_two_week").collect()[0][
                0] == 3.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_lifestyle_weekly_last_four_week").collect()[0][
                0] == 5.75
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_lifestyle_weekly_last_twelve_week").collect()[0][
                0] - 5.2857) <0.1
            # avg_loyalty_rewards_others_weekly_last_week
            # avg_loyalty_rewards_others_weekly_last_two_week
            # avg_loyalty_rewards_others_weekly_last_four_week
            # avg_loyalty_rewards_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_others_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_others_weekly_last_two_week").collect()[0][
                0] == 5 
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_others_weekly_last_four_week").collect()[0][
                0] == 3.75
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_rewards_others_weekly_last_twelve_week").collect()[0][
                0] - 3.5714) <0.1

    def test_number_of_points_spend(self,project_context):
        var_project_context = project_context['ProjectContext']
        df0 = TestUnitLoyalty().gen_data(project_context)

        df = node_from_config(df0,var_project_context.catalog.load(
            'params:l1_loyalty_number_of_points_spend_daily'))
            
        
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_total").collect()[0][
                0] == 9
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_travel").collect()[0][
                0] == 3
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_ais_points_spend").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_entertainment").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_food_and_drink").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_lifestyle").collect()[0][
                0] == 6
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_points_spend_others").collect()[0][
                0] == 0

        df2 = node_from_config(df,var_project_context.catalog.load(
            'params:l2_loyalty_number_of_points_spend_weekly'))

        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_total").collect()[0][
                0] == 99
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_travel").collect()[0][
                0] == 5
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_ais_rewards").collect()[0][
                0] == 7
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_entertainment").collect()[0][
                0] == 8
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_food_and_drink").collect()[0][
                0] == 13
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_lifestyle").collect()[0][
                0] == 41
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_points_spend_others").collect()[0][
                0] == 8

        df3 = node_from_config(df,var_project_context.catalog.load(
            'params:l3_loyalty_number_of_points_spend_monthly'))

        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_total").collect()[0][
                0] == 510
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_travel").collect()[0][
                0] == 62
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_ais_rewards").collect()[0][
                0] == 40
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_entertainment").collect()[0][
                0] == 67
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_food_and_drink").collect()[0][
                0] == 97
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_lifestyle").collect()[0][
                0] == 139
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_points_spend_others").collect()[0][
                0] == 73

        df4 = l4_rolling_window(df2,var_project_context.catalog.load(
            'params:l4_rolling_window_loyalty_number_of_points_spend'))

        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_total_weekly_last_week").collect()[0][
                0] == 124
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_total_weekly_last_two_week").collect()[0][
                0] == 244
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_total_weekly_last_four_week").collect()[0][
                0] == 461
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_total_weekly_last_twelve_week").collect()[0][
                0] == 798
            # sum_loyalty_points_spend_travel_weekly_last_week
            # sum_loyalty_points_spend_travel_weekly_last_two_week
            # sum_loyalty_points_spend_travel_weekly_last_four_week
            # sum_loyalty_points_spend_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_travel_weekly_last_two_week").collect()[0][
                0] == 18
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_travel_weekly_last_four_week").collect()[0][
                0] == 29
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_travel_weekly_last_twelve_week").collect()[0][
                0] == 82
            
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_ais_rewards_weekly_last_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 38
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 52
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] == 85
            # sum_loyalty_points_spend_entertainment_weekly_last_week
            # sum_loyalty_points_spend_entertainment_weekly_last_two_week
            # sum_loyalty_points_spend_entertainment_weekly_last_four_week
            # sum_loyalty_points_spend_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_entertainment_weekly_last_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_entertainment_weekly_last_two_week").collect()[0][
                0] == 46
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_entertainment_weekly_last_four_week").collect()[0][
                0] == 81
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_entertainment_weekly_last_twelve_week").collect()[0][
                0] == 117

            # sum_loyalty_points_spend_food_and_drink_weekly_last_week
            # sum_loyalty_points_spend_food_and_drink_weekly_last_two_week
            # sum_loyalty_points_spend_food_and_drink_weekly_last_four_week
            # sum_loyalty_points_spend_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_food_and_drink_weekly_last_week").collect()[0][
                0] == 16
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 24
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 61
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] == 134
            # sum_loyalty_points_spend_lifestyle_weekly_last_week
            # sum_loyalty_points_spend_lifestyle_weekly_last_two_week
            # sum_loyalty_points_spend_lifestyle_weekly_last_four_week
            # sum_loyalty_points_spend_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_lifestyle_weekly_last_week").collect()[0][
                0] == 25
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_lifestyle_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_lifestyle_weekly_last_four_week").collect()[0][
                0] == 119
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_lifestyle_weekly_last_twelve_week").collect()[0][
                0] == 199
            # sum_loyalty_points_spend_others_weekly_last_week
            # sum_loyalty_points_spend_others_weekly_last_two_week
            # sum_loyalty_points_spend_others_weekly_last_four_week
            # sum_loyalty_points_spend_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_others_weekly_last_week").collect()[0][
                0] == 27
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_others_weekly_last_two_week").collect()[0][
                0] == 66
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_others_weekly_last_four_week").collect()[0][
                0] == 86
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_points_spend_others_weekly_last_twelve_week").collect()[0][
                0] == 139
            # avg_loyalty_points_spend_total_weekly_last_week
            # avg_loyalty_points_spend_total_weekly_last_two_week
            # avg_loyalty_points_spend_total_weekly_last_four_week
            # avg_loyalty_points_spend_total_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_total_weekly_last_week").collect()[0][
                0] == 124
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_total_weekly_last_two_week").collect()[0][
                0] == 122
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_total_weekly_last_four_week").collect()[0][
                0] == 115.25
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_total_weekly_last_twelve_week").collect()[0][
                0]  == 114
            # avg_loyalty_points_spend_travel_weekly_last_week
            # avg_loyalty_points_spend_travel_weekly_last_two_week
            # avg_loyalty_points_spend_travel_weekly_last_four_week
            # avg_loyalty_points_spend_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_travel_weekly_last_two_week").collect()[0][
                0] == 9
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_travel_weekly_last_four_week").collect()[0][
                0] == 7.25
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_travel_weekly_last_twelve_week").collect()[0][
                0]  - 11.7142) < 0.01
            
            # -----------------------------------
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_ais_rewards_weekly_last_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 19
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 13
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] - 12.1428) < 0.01
            # avg_loyalty_points_spend_entertainment_weekly_last_week
            # avg_loyalty_points_spend_entertainment_weekly_last_two_week
            # avg_loyalty_points_spend_entertainment_weekly_last_four_week
            # avg_loyalty_points_spend_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_entertainment_weekly_last_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_entertainment_weekly_last_two_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_entertainment_weekly_last_four_week").collect()[0][
                0] == 20.25
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_entertainment_weekly_last_twelve_week").collect()[0][
                0] - 16.7142) <0.01
            # avg_loyalty_points_spend_food_and_drink_weekly_last_week
            # avg_loyalty_points_spend_food_and_drink_weekly_last_two_week
            # avg_loyalty_points_spend_food_and_drink_weekly_last_four_week
            # avg_loyalty_points_spend_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_food_and_drink_weekly_last_week").collect()[0][
                0] == 16
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 12
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 15.25
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] - 19.1428) < 0.01
            # avg_loyalty_points_spend_lifestyle_weekly_last_week
            # avg_loyalty_points_spend_lifestyle_weekly_last_two_week
            # avg_loyalty_points_spend_lifestyle_weekly_last_four_week
            # avg_loyalty_points_spend_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_lifestyle_weekly_last_week").collect()[0][
                0] == 25
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_lifestyle_weekly_last_two_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_lifestyle_weekly_last_four_week").collect()[0][
                0] == 29.75
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_lifestyle_weekly_last_twelve_week").collect()[0][
                0] - 28.4285) <0.01
            # avg_loyalty_points_spend_others_weekly_last_week
            # avg_loyalty_points_spend_others_weekly_last_two_week
            # avg_loyalty_points_spend_others_weekly_last_four_week
            # avg_loyalty_points_spend_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_others_weekly_last_week").collect()[0][
                0] == 27
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_others_weekly_last_two_week").collect()[0][
                0] == 33
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_others_weekly_last_four_week").collect()[0][
                0] == 21.5
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_points_spend_others_weekly_last_twelve_week").collect()[0][
                0] - 19.8571) <0.01


    def test_number_of_services(self,project_context):

        var_project_context = project_context['ProjectContext']
        df0 = TestUnitLoyalty().gen_data(project_context)

        df = node_from_config(df0,var_project_context.catalog.load(
            'params:l1_loyalty_number_of_services_daily'))
               
        # exit(2)
        # loyalty_services_total: "sum(case when mobile_no is not null then 1 else 0 end)"
        # loyalty_services_travel: "sum(case when category in (545,565,116,124) then 1 else 0 end)"
        # loyalty_services_ais_rewards: "sum(case when category in (10,11,17,27,125) then 1 else 0 end)"
        # loyalty_services_entertainment: "sum(case when category in (30,97,100,100041) then 1 else 0 end)"
        # loyalty_services_food_and_drink: "sum(case when category in (12,15,16,26,29,118,119,120,121,122,585) then 1 else 0 end)"
        # loyalty_services_lifestyle: "sum(case when category in (13,28,117,126,127,129,130,131,132,133,134,501,505) then 1 else 0 end)"
        # loyalty_services_others: "sum(case when category in (14,25,101,123,128,525) then 1 else 0 end)"
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_total").collect()[0][
                0] == 3
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_travel").collect()[0][
                0] == 1
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_ais_rewards").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_entertainment").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_food_and_drink").collect()[0][
                0] == 0
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_lifestyle").collect()[0][
                0] == 2
        assert \
            df.where("start_of_week='2019-01-28'").select("loyalty_services_others").collect()[0][
                0] == 0
        
        df2 = node_from_config(df,var_project_context.catalog.load(
            'params:l2_loyalty_number_of_services_weekly'))

        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_total").collect()[0][
                0] == 21
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_travel").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_ais_rewards").collect()[0][
                0] == 1
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_entertainment").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_food_and_drink").collect()[0][
                0] == 2
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_lifestyle").collect()[0][
                0] == 9
        assert \
            df2.where("start_of_week='2019-01-28'").select("loyalty_services_others").collect()[0][
                0] == 3
        

        df3 = node_from_config(df,var_project_context.catalog.load(
            'params:l3_loyalty_number_of_services_monthly'))
        # df3.show(df3.count(),False)
        # exit(8)

        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_total").collect()[0][
                0] == 93
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_travel").collect()[0][
                0] == 10
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_ais_rewards").collect()[0][
                0] == 8
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_entertainment").collect()[0][
                0] == 13
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_food_and_drink").collect()[0][
                0] == 15
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_lifestyle").collect()[0][
                0] == 26
        assert \
            df3.where("start_of_month='2019-01-01'").select("loyalty_services_others").collect()[0][
                0] == 15

        df4 = l4_rolling_window(df2,var_project_context.catalog.load(
            'params:l4_rolling_window_loyalty_number_of_services'))

            # sum_loyalty_services_total_weekly_last_week
            # sum_loyalty_services_total_weekly_last_two_week
            # sum_loyalty_services_total_weekly_last_four_week
            # sum_loyalty_services_total_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_total_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_total_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_total_weekly_last_four_week").collect()[0][
                0] == 84
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_total_weekly_last_twelve_week").collect()[0][
                0] == 144
            # sum_loyalty_services_travel_weekly_last_week
            # sum_loyalty_services_travel_weekly_last_two_week
            # sum_loyalty_services_travel_weekly_last_four_week
            # sum_loyalty_services_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_travel_weekly_last_two_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_travel_weekly_last_four_week").collect()[0][
                0] == 6
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_travel_weekly_last_twelve_week").collect()[0][
                0] == 14
            # sum_loyalty_services_ais_rewards_weekly_last_week
            # sum_loyalty_services_ais_rewards_weekly_last_two_week
            # sum_loyalty_services_ais_rewards_weekly_last_four_week
            # sum_loyalty_services_ais_rewards_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_ais_rewards_weekly_last_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 10
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 12    
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] == 19   
            # sum_loyalty_services_entertainment_weekly_last_week
            # sum_loyalty_services_entertainment_weekly_last_two_week
            # sum_loyalty_services_entertainment_weekly_last_four_week
            # sum_loyalty_services_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_entertainment_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_entertainment_weekly_last_two_week").collect()[0][
                0] == 8
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_entertainment_weekly_last_four_week").collect()[0][
                0] == 16
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_entertainment_weekly_last_twelve_week").collect()[0][
                0] == 22

            # sum_loyalty_services_food_and_drink_weekly_last_week
            # sum_loyalty_services_food_and_drink_weekly_last_two_week
            # sum_loyalty_services_food_and_drink_weekly_last_four_week
            # sum_loyalty_services_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_food_and_drink_weekly_last_week").collect()[0][
                0] == 2
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 8
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] == 20
            # sum_loyalty_services_lifestyle_weekly_last_week
            # sum_loyalty_services_lifestyle_weekly_last_two_week
            # sum_loyalty_services_lifestyle_weekly_last_four_week
            # sum_loyalty_services_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_lifestyle_weekly_last_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_lifestyle_weekly_last_two_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_lifestyle_weekly_last_four_week").collect()[0][
                0] == 23
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_lifestyle_weekly_last_twelve_week").collect()[0][
                0] == 37
            # sum_loyalty_services_others_weekly_last_week
            # sum_loyalty_services_others_weekly_last_two_week
            # sum_loyalty_services_others_weekly_last_four_week
            # sum_loyalty_services_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_others_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_others_weekly_last_two_week").collect()[0][
                0] == 10
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_others_weekly_last_four_week").collect()[0][
                0] == 15
        assert \
            df4.where("start_of_week='2019-02-18'").select("sum_loyalty_services_others_weekly_last_twelve_week").collect()[0][
                0] == 25
            # avg_loyalty_services_total_weekly_last_week
            # avg_loyalty_services_total_weekly_last_two_week
            # avg_loyalty_services_total_weekly_last_four_week
            # avg_loyalty_services_total_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_total_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_total_weekly_last_two_week").collect()[0][
                0] == 21
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_total_weekly_last_four_week").collect()[0][
                0] == 21
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_total_weekly_last_twelve_week").collect()[0][
                0] - 20.5714) < 0.1
            # avg_loyalty_services_travel_weekly_last_week
            # avg_loyalty_services_travel_weekly_last_two_week
            # avg_loyalty_services_travel_weekly_last_four_week
            # avg_loyalty_services_travel_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_travel_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_travel_weekly_last_two_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_travel_weekly_last_four_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_travel_weekly_last_twelve_week").collect()[0][
                0] == 2
            # avg_loyalty_services_ais_rewards_weekly_last_week
            # avg_loyalty_services_ais_rewards_weekly_last_two_week
            # avg_loyalty_services_ais_rewards_weekly_last_four_week
            # avg_loyalty_services_ais_rewards_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_ais_rewards_weekly_last_week").collect()[0][
                0] == 7
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_ais_rewards_weekly_last_two_week").collect()[0][
                0] == 5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_ais_rewards_weekly_last_four_week").collect()[0][
                0] == 3
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_ais_rewards_weekly_last_twelve_week").collect()[0][
                0] - 2.714) < 0.1
            # avg_loyalty_services_entertainment_weekly_last_week
            # avg_loyalty_services_entertainment_weekly_last_two_week
            # avg_loyalty_services_entertainment_weekly_last_four_week
            # avg_loyalty_services_entertainment_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_entertainment_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_entertainment_weekly_last_two_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_entertainment_weekly_last_four_week").collect()[0][
                0] == 4
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_entertainment_weekly_last_twelve_week").collect()[0][
                0] - 3.142) <0.1
            # avg_loyalty_services_food_and_drink_weekly_last_week
            # avg_loyalty_services_food_and_drink_weekly_last_two_week
            # avg_loyalty_services_food_and_drink_weekly_last_four_week
            # avg_loyalty_services_food_and_drink_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_food_and_drink_weekly_last_week").collect()[0][
                0] == 2
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_food_and_drink_weekly_last_two_week").collect()[0][
                0] == 1.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_food_and_drink_weekly_last_four_week").collect()[0][
                0] == 2
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_food_and_drink_weekly_last_twelve_week").collect()[0][
                0] - 2.857) < 0.1
            # avg_loyalty_services_lifestyle_weekly_last_week
            # avg_loyalty_services_lifestyle_weekly_last_two_week
            # avg_loyalty_services_lifestyle_weekly_last_four_week
            # avg_loyalty_services_lifestyle_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_lifestyle_weekly_last_week").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_lifestyle_weekly_last_two_week").collect()[0][
                0] == 3.5
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_lifestyle_weekly_last_four_week").collect()[0][
                0] == 5.75
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_lifestyle_weekly_last_twelve_week").collect()[0][
                0] - 5.2857) <0.1
            # avg_loyalty_services_others_weekly_last_week
            # avg_loyalty_services_others_weekly_last_two_week
            # avg_loyalty_services_others_weekly_last_four_week
            # avg_loyalty_services_others_weekly_last_twelve_week
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_others_weekly_last_week").collect()[0][
                0] == 4
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_others_weekly_last_two_week").collect()[0][
                0] == 5 
        assert \
            df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_others_weekly_last_four_week").collect()[0][
                0] == 3.75
        assert \
            abs(df4.where("start_of_week='2019-02-18'").select("avg_loyalty_services_others_weekly_last_twelve_week").collect()[0][
                0] - 3.5714) <0.1
        