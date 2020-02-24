from datetime import datetime

from pyspark.sql.functions import to_timestamp

from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F, SparkSession
from datetime import timedelta


class TestUnitBilling:
    def test_topup_frequency_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        my_dates.sort()
        random_list = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        start_time = datetime.strptime("01/01/2020 08:35:55", '%d/%m/%Y %H:%M:%S')
        start_time_list = []
        for i in range(len(my_dates)):
            start_time_list.append(start_time)
            start_time = start_time + timedelta(seconds=random.randint(1, 432000))

        df = spark.createDataFrame(zip(random_list, my_dates, start_time_list),
                                   schema=['face_value', 'temp', 'recharge_time']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("recharge_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("event_partition_date", F.to_date('recharge_date', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.recharge_time))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.recharge_time)))
        df = df.orderBy('recharge_time')

        df.show(1000, False)
        intermediate_data = node_from_config(df, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate'))

        intermediate_data.orderBy('recharge_time').show(1000, False)
        assert \
            intermediate_data.where("recharge_time = '2020-10-17 12:08:09'").select("payments_time_diff").collect()[0][
                0] == 4

        weekly_data = node_from_config(intermediate_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly'))

        assert \
            weekly_data.where("start_of_week = '2020-10-12'").select("payments_max_time_diff").collect()[0][
                0] == 4
        assert \
            weekly_data.where("start_of_week = '2020-10-12'").select("payments_min_time_diff").collect()[0][
                0] == 1
        assert \
            weekly_data.where("start_of_week = '2020-10-12'").select("payments_time_diff").collect()[0][
                0] == 6
        assert \
            weekly_data.where("start_of_week = '2020-10-12'").select("payments_time_diff_avg").collect()[0][
                0] == 2

        weekly_data.orderBy('start_of_week').show(1000, False)

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_time_diff_bw_topups'))

        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "sum_payments_time_diff_weekly_last_week").collect()[0][
                0] == 4
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "sum_payments_time_diff_weekly_last_two_week").collect()[0][
                0] == 9
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "sum_payments_time_diff_weekly_last_four_week").collect()[0][
                0] == 16
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "sum_payments_time_diff_weekly_last_twelve_week").collect()[0][
                0] == 46
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "avg_payments_time_diff_avg_weekly_last_week").collect()[0][
                0] == 2
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "avg_payments_time_diff_avg_weekly_last_two_week").collect()[0][
                0] == 2.25
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "avg_payments_time_diff_avg_weekly_last_four_week").collect()[0][
                0] == 2.875
        assert \
            final_features.where("start_of_week = '2020-10-12'").select(
                "avg_payments_time_diff_avg_weekly_last_twelve_week").collect()[0][
                0] == 2.522727272727273

        final_features.show(100, False)

    def test_topup_and_volume_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random_list = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]

        df = spark.createDataFrame(zip(random_list, my_dates), schema=['face_value', 'temp']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("recharge_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("event_partition_date", F.to_date('recharge_date', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))

        # df.show()
        # df.printSchema()
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))
        # daily_data.orderBy('event_partition_date').show()

        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_top_up_and_count_weekly'))
        # weekly_data.orderBy('start_of_week').show()
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_top_up_volume").collect()[0][
                0] == 1000
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_top_ups").collect()[0][
                0] == 3

        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_ups").collect()[0][0] == 21
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_up_volume").collect()[0][0] == 11700
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_top_up_volume_avg").collect()[0][
                    0]) == 1671

        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_feature_top_up_and_count_monthly'))
        # monthly_data.show()
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups").collect()[0][0] == 87
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_volume").collect()[0][0] == 48800
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_volume_avg").collect()[0][
                    0]) == 1682

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_topup_and_volume')).orderBy(F.col("start_of_week").desc())

        # final_features.orderBy('start_of_week').show()
        assert \
            final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_top_ups_weekly_last_twelve_week").collect()[0][0] == 246
        assert \
            final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0] == 131100
        assert \
            final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_top_ups_weekly_last_twelve_week").collect()[0][0] == 20.5
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0]) == 10925

    def test_arpu_roaming_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random_list = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]

        df = spark.createDataFrame(zip(random_list, my_dates), schema=['ir_call_charge_amount', 'temp']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))
        # df.show(100, False)
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_rpu_roaming'))

        # daily_data.orderBy('event_partition_date').show()
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_arpu_roaming").collect()[0][
                0] == 1000
        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_rpu_roaming_weekly'))
        # weekly_data.show(100, False)
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_arpu_roaming").collect()[0][0] == 11700
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_arpu_roaming_avg").collect()[0][
                    0]) == 1671

        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_feature_rpu_roaming_monthly'))
        # monthly_data.show()
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_arpu_roaming").collect()[0][0] == 48800
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_arpu_roaming_avg").collect()[0][
                    0]) == 1682

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_rpu_roaming')).orderBy(F.col("start_of_week").desc())
        # final_features.orderBy('start_of_week').show()

        assert \
            final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_arpu_roaming_weekly_last_twelve_week").collect()[0][0] == 131100
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_arpu_roaming_avg_weekly_last_twelve_week").collect()[0][0]) == 1598

    def test_before_top_up_balance_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random_list = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]

        df = spark.createDataFrame(zip(random_list, my_dates), schema=['pre_bal_amt', 'temp']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))
        # df.orderBy('event_partition_date').show()
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_before_top_up_balance'))
        # daily_data.orderBy('event_partition_date').show(300,False)
        assert \
            int(daily_data.where("event_partition_date = '2020-01-01'").select(
                "payments_before_top_up_balance").collect()[0][0]) == 333
        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_before_top_up_balance_weekly'))
        # weekly_data.orderBy('start_of_week').show()
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_before_top_up_balance").collect()[0][
                    0]) == 557
        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_before_top_up_balance_monthly'))
        # monthly_data.show(100,False)
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_before_top_up_balance").collect()[0][
                    0]) == 560

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_before_top_up_balance')).orderBy(F.col("start_of_week").desc())

        final_features.orderBy('start_of_week').show()
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 6395
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 532

    def test_top_up_channel_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        # recharge_daily_data
        random_type = ['4', 'B1', 'B58', '3', 'B0', '7', '16', 'B43', '1', '5', '53', 'B69', '51', 'B50']
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random_list = [random_type[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        df = spark.createDataFrame(zip(random_list, my_dates, random_list2),
                                   schema=['recharge_type', 'temp', 'face_value']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))

        df.orderBy('event_partition_date').show()

        # df.show()

        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_top_up_channels'))

        daily_data.orderBy('event_partition_date').show(300, False)

        assert \
            int(daily_data.where("event_partition_date='2020-02-01'").select(
                "payments_top_ups_by_bank_atm_cdm").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_cash_card").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_cash_card").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_epin_slip").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_epin_slip").collect()[0][0]) == 0
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_epos").collect()[0][0]) == 1
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_epos").collect()[0][0]) == 100
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_rom").collect()[0][0]) == 2
        assert \
            int(daily_data.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_rom").collect()[0][0]) == 1300

        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_top_up_channels_weekly'))
        # weekly_data.orderBy('start_of_week').show()

        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_bank_atm_cdm").collect()[0][0]) == 1
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][0]) == 900
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_cash_card").collect()[0][0]) == 3
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_cash_card").collect()[0][0]) == 1000
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][0]) == 7
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][0]) == 3800
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_epin_slip").collect()[0][0]) == 2
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_epin_slip").collect()[0][0]) == 1700
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_epos").collect()[0][0]) == 5
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_epos").collect()[0][0]) == 3500
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_rom").collect()[0][0]) == 3
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_rom").collect()[0][0]) == 1700
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_bank_atm_cdm").collect()[0][0]) == int(1 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][0]) == int(900 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_cash_card").collect()[0][0]) == int(3 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_cash_card").collect()[0][0]) == int(1000 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_digital_online_self_service").collect()[0][0]) == int(7 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][0]) == int(3800 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epin_slip").collect()[0][0]) == int(2 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epin_slip").collect()[0][0]) == int(1700 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epos").collect()[0][0]) == int(5 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epos").collect()[0][0]) == int(3500 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_rom").collect()[0][0]) == int(3 / 7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_rom").collect()[0][0]) == int(1700 / 7)

        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_top_up_channels_monthly'))
        # monthly_data.show(100, False)

        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_bank_atm_cdm").collect()[
                    0][
                    0]) == 18
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][
                    0]) == 8800
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_cash_card").collect()[0][
                    0]) == 18
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_cash_card").collect()[
                    0][
                    0]) == 11300
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][
                    0]) == 14
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][
                    0]) == 6300
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_epin_slip").collect()[0][
                    0]) == 4
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_epin_slip").collect()[
                    0][
                    0]) == 2000
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_epos").collect()[0][
                    0]) == 20
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_epos").collect()[0][
                    0]) == 9600
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_rom").collect()[0][
                    0]) == 13
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_rom").collect()[0][
                    0]) == 7500
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_bank_atm_cdm").collect()[
                0][
                0]) == (18 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][
                0]) == (8800 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_cash_card").collect()[0][
                0]) == (18 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_cash_card").collect()[
                0][
                0]) == (11300 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_digital_online_self_service").collect()[0][
                0]) == (14 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][
                0]) == (6300 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_epin_slip").collect()[0][
                0]) == (4 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_epin_slip").collect()[
                0][
                0]) == (2000 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_epos").collect()[0][
                0]) == (20 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_epos").collect()[0][
                0]) == (9600 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_rom").collect()[0][
                0]) == (13 / 29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_rom").collect()[0][
                0]) == (7500 / 29)

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_top_up_channels')).orderBy(F.col("start_of_week").desc())

        # final_features.orderBy('start_of_week').show()
        # final_features.printSchema()
