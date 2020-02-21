from pyspark.sql.functions import to_timestamp

from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F, SparkSession


class TestUnitBilling:

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

        # print('test1234')
        # df.show()
        # df.printSchema()
        # print('test3333')
        # print('L11111111111111111111111111111')
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))
        # print('L11111111111111111111111111111end')
        # daily_data.orderBy('event_partition_date').show()
        # daily_data.printSchema()

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
        # print('monthlydatadebug')
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

        # print('finalfeatures')
        final_features.orderBy('start_of_week').show()
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

        # exit(2)

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
        # print('test1234')
        # df.show(100, False)
        # df.printSchema()
        # print('test3333')
        # print('L11111111111111111111111111111')
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_rpu_roaming'))
        print('L11111111111111111111111111111end')
        daily_data.orderBy('event_partition_date').show()
        # daily_data.printSchema()
        # payments_arpu_roaming: "sum(ir_call_charge_amount)"
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_arpu_roaming").collect()[0][
                0] == 1000
        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_rpu_roaming_weekly'))
        # print('weeklytest')
        # weekly_data.show(100, False)
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_arpu_roaming").collect()[0][0] == 11700
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_arpu_roaming_avg").collect()[0][
                    0]) == 1671
        # payments_arpu_roaming: "sum(payments_arpu_roaming)"
        # payments_arpu_roaming_avg: "avg(payments_arpu_roaming)"
        # exit(2)

        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_feature_rpu_roaming_monthly'))
        # print('monthlydata')
        # monthly_data.show()
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_arpu_roaming").collect()[0][0] == 48800
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_arpu_roaming_avg").collect()[0][
                    0]) == 1682
        # payments_arpu_roaming: "sum(payments_arpu_roaming)"
        # payments_arpu_roaming_avg: "avg(payments_arpu_roaming)"

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_rpu_roaming')).orderBy(F.col("start_of_week").desc())
        # print('finalfeatures')
        # final_features.orderBy('start_of_week').show()

        assert \
            final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_arpu_roaming_weekly_last_twelve_week").collect()[0][0] == 131100
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_arpu_roaming_avg_weekly_last_twelve_week").collect()[0][0]) == 1598
        # feature_list:
        # sum: ["payments_arpu_roaming"]
        # avg: ["payments_arpu_roaming_avg"]

        # exit(2)

    def test_before_top_up_balance_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        # l1_billing_and_payment_before_top_up_balance
        # feature_list:
        # payments_before_top_up_balance: "avg(pre_bal_amt)"
        # l2_billing_and_payment_before_top_up_balance_weekly
        #     feature_list:
        #       payments_before_top_up_balance: "avg(payments_before_top_up_balance)"
        # l3_billing_and_payment_before_top_up_balance_monthly
        # feature_list:
        # payments_before_top_up_balance: "avg(payments_before_top_up_balance)"
        # l4_billing_before_top_up_balance
        # feature_list:
        # sum: ["payments_before_top_up_balance"]
        # avg: ["payments_before_top_up_balance"]

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
        # print('rawdata')
        # df.orderBy('event_partition_date').show()
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_before_top_up_balance'))
        # print('dailydata')
        # daily_data.orderBy('event_partition_date').show(300,False)
        assert \
            int(daily_data.where("event_partition_date = '2020-01-01'").select(
                "payments_before_top_up_balance").collect()[0][0]) == 333
        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_before_top_up_balance_weekly'))
        # print('weeklydata')
        # weekly_data.orderBy('start_of_week').show()
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_before_top_up_balance").collect()[0][
                    0]) == 557
        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_before_top_up_balance_monthly'))
        # print('monthlydata')
        # monthly_data.show(100,False)
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_before_top_up_balance").collect()[0][
                    0]) == 560

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_before_top_up_balance')).orderBy(F.col("start_of_week").desc())
        # print('finalfeature')
        # final_features.orderBy('start_of_week').show()
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "sum_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 6395
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select(
                "avg_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 532
        # exit(2)

    def test_top_up_channel_feature(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # l1_billing_and_payment_top_up_channels
        # feature_list:
        # payments_top_ups_by_bank_atm_cdm: "sum(case when recharge_type in ('4','B6','B3','B1','B4','B5','B7','B8','41','B2','B9','44','B58','B53','B10','B11') then 1 else 0 end)"
        # payments_top_up_vol_by_bank_atm_cdm: "sum(case when recharge_type in ('4','B6','B3','B1','B4','B5','B7','B8','41','B2','B9','44','B58','B53','B10','B11') then face_value else 0 end)"
        # payments_top_ups_by_cash_card: "sum(case when recharge_type in ('3','B0') then 1 else 0 end)"
        # payments_top_up_vol_by_cash_card: "sum(case when recharge_type in ('3','B0') then face_value else 0 end)"
        # payments_top_ups_by_digital_online_self_service: "sum(case when recharge_type in ('6','7','8','32','43','16','33','65','37','55','31','36','63','45','B43','B38','B26','B24','B22','B59','B25','B30','B72','B27','B76','B44','B40','B23','B57','B74','B33','B13','B41','B29','B17','B32','B21','B28','B35','B20','B42','B12','B31','B15','B36','B34') then 1 else 0 end)"
        # payments_top_up_vol_by_digital_online_self_service: "sum(case when recharge_type in ('6','7','8','32','43','16','33','65','37','55','31','36','63','45','B43','B38','B26','B24','B22','B59','B25','B30','B72','B27','B76','B44','B40','B23','B57','B74','B33','B13','B41','B29','B17','B32','B21','B28','B35','B20','B42','B12','B31','B15','B36','B34') then face_value else 0 end)"
        # payments_top_ups_by_epin_slip: "sum(case when recharge_type in ('1') then 1 else 0 end)"
        # payments_top_up_vol_by_epin_slip: "sum(case when recharge_type in ('1') then face_value else 0 end)"
        # payments_top_ups_by_epos: "sum(case when recharge_type in ('5','18','19','46','53','48','B47','B49','B39','B46','B55','B69','B48') then 1 else 0 end)"
        # payments_top_up_vol_by_epos: "sum(case when recharge_type in ('5','18','19','46','53','48','B47','B49','B39','B46','B55','B69','B48') then face_value else 0 end)"
        # payments_top_ups_by_rom: "sum(case when recharge_type in ('B50','51') then 1 else 0 end)"
        # payments_top_up_vol_by_rom: "sum(case when recharge_type in ('B50','51') then face_value else 0 end)"
        #
        # l2_billing_and_payment_top_up_channels_weekly
        # feature_list:
        # payments_top_ups_by_bank_atm_cdm: "sum(payments_top_ups_by_bank_atm_cdm)"
        # payments_top_up_vol_by_bank_atm_cdm: "sum(payments_top_up_vol_by_bank_atm_cdm)"
        # payments_top_ups_by_cash_card: "sum(payments_top_ups_by_cash_card)"
        # payments_top_up_vol_by_cash_card: "sum(payments_top_up_vol_by_cash_card)"
        # payments_top_ups_by_digital_online_self_service: "sum(payments_top_ups_by_digital_online_self_service)"
        # payments_top_up_vol_by_digital_online_self_service: "sum(payments_top_up_vol_by_digital_online_self_service)"
        # payments_top_ups_by_epin_slip: "sum(payments_top_ups_by_epin_slip)"
        # payments_top_up_vol_by_epin_slip: "sum(payments_top_up_vol_by_epin_slip)"
        # payments_top_ups_by_epos: "sum(payments_top_ups_by_epos)"
        # payments_top_up_vol_by_epos: "sum(payments_top_up_vol_by_epos)"
        # payments_top_ups_by_rom: "sum(payments_top_ups_by_rom)"
        # payments_top_up_vol_by_rom: "sum(payments_top_up_vol_by_rom)"
        # payments_top_ups_avg_by_bank_atm_cdm: "avg(payments_top_ups_by_bank_atm_cdm)"
        # payments_top_up_vol_avg_by_bank_atm_cdm: "avg(payments_top_up_vol_by_bank_atm_cdm)"
        # payments_top_ups_avg_by_cash_card: "avg(payments_top_ups_by_cash_card)"
        # payments_top_up_vol_avg_by_cash_card: "avg(payments_top_up_vol_by_cash_card)"
        # payments_top_ups_avg_by_digital_online_self_service: "avg(payments_top_ups_by_digital_online_self_service)"
        # payments_top_up_vol_avg_by_digital_online_self_service: "avg(payments_top_up_vol_by_digital_online_self_service)"
        # payments_top_ups_avg_by_epin_slip: "avg(payments_top_ups_by_epin_slip)"
        # payments_top_up_vol_avg_by_epin_slip: "avg(payments_top_up_vol_by_epin_slip)"
        # payments_top_ups_avg_by_epos: "avg(payments_top_ups_by_epos)"
        # payments_top_up_vol_avg_by_epos: "avg(payments_top_up_vol_by_epos)"
        # payments_top_ups_avg_by_rom: "avg(payments_top_ups_by_rom)"
        # payments_top_up_vol_avg_by_rom: "avg(payments_top_up_vol_by_rom)"
        #
        # l3_billing_and_payment_top_up_channels_monthly
        # feature_list:
        # payments_top_ups_by_bank_atm_cdm: "sum(payments_top_ups_by_bank_atm_cdm)"
        # payments_top_up_vol_by_bank_atm_cdm: "sum(payments_top_up_vol_by_bank_atm_cdm)"
        # payments_top_ups_by_cash_card: "sum(payments_top_ups_by_cash_card)"
        # payments_top_up_vol_by_cash_card: "sum(payments_top_up_vol_by_cash_card)"
        # payments_top_ups_by_digital_online_self_service: "sum(payments_top_ups_by_digital_online_self_service)"
        # payments_top_up_vol_by_digital_online_self_service: "sum(payments_top_up_vol_by_digital_online_self_service)"
        # payments_top_ups_by_epin_slip: "sum(payments_top_ups_by_epin_slip)"
        # payments_top_up_vol_by_epin_slip: "sum(payments_top_up_vol_by_epin_slip)"
        # payments_top_ups_by_epos: "sum(payments_top_ups_by_epos)"
        # payments_top_up_vol_by_epos: "sum(payments_top_up_vol_by_epos)"
        # payments_top_ups_by_rom: "sum(payments_top_ups_by_rom)"
        # payments_top_up_vol_by_rom: "sum(payments_top_up_vol_by_rom)"
        # payments_top_ups_avg_by_bank_atm_cdm: "avg(payments_top_ups_by_bank_atm_cdm)"
        # payments_top_up_vol_avg_by_bank_atm_cdm: "avg(payments_top_up_vol_by_bank_atm_cdm)"
        # payments_top_ups_avg_by_cash_card: "avg(payments_top_ups_by_cash_card)"
        # payments_top_up_vol_avg_by_cash_card: "avg(payments_top_up_vol_by_cash_card)"
        # payments_top_ups_avg_by_digital_online_self_service: "avg(payments_top_ups_by_digital_online_self_service)"
        # payments_top_up_vol_avg_by_digital_online_self_service: "avg(payments_top_up_vol_by_digital_online_self_service)"
        # payments_top_ups_avg_by_epin_slip: "avg(payments_top_ups_by_epin_slip)"
        # payments_top_up_vol_avg_by_epin_slip: "avg(payments_top_up_vol_by_epin_slip)"
        # payments_top_ups_avg_by_epos: "avg(payments_top_ups_by_epos)"
        # payments_top_up_vol_avg_by_epos: "avg(payments_top_up_vol_by_epos)"
        # payments_top_ups_avg_by_rom: "avg(payments_top_ups_by_rom)"
        # payments_top_up_vol_avg_by_rom: "avg(payments_top_up_vol_by_rom)"
        #
        # l4_billing_top_up_channels
        # feature_list:
        # sum: ["payments_top_ups_by_bank_atm_cdm", "payments_top_up_vol_by_bank_atm_cdm",
        #       "payments_top_ups_by_cash_card", "payments_top_up_vol_by_cash_card",
        #       "payments_top_ups_by_digital_online_self_service", "payments_top_up_vol_by_digital_online_self_service",
        #       "payments_top_ups_by_epin_slip",
        #       "payments_top_up_vol_by_epin_slip", "payments_top_ups_by_epos", "payments_top_up_vol_by_epos",
        #       "payments_top_ups_by_rom", "payments_top_up_vol_by_rom"]
        # avg: ["payments_top_ups_avg_by_bank_atm_cdm", "payments_top_up_vol_avg_by_bank_atm_cdm",
        #       "payments_top_ups_avg_by_cash_card", "payments_top_up_vol_avg_by_cash_card",
        #       "payments_top_ups_avg_by_digital_online_self_service",
        #       "payments_top_up_vol_avg_by_digital_online_self_service", "payments_top_ups_avg_by_epin_slip",
        #       "payments_top_up_vol_avg_by_epin_slip", "payments_top_ups_avg_by_epos", "payments_top_up_vol_avg_by_epos",
        #       "payments_top_ups_avg_by_rom", "payments_top_up_vol_avg_by_rom"]

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
        df = spark.createDataFrame(zip(random_list, my_dates,random_list2), schema=['recharge_type', 'temp','face_value']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))
        print('rawdata')
        df.orderBy('event_partition_date').show()

        #
        # print('testtest')
        # df.show()

        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_top_up_channels'))
        print('dailydata')
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
        print('weeklydata')
        weekly_data.orderBy('start_of_week').show()

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
                "payments_top_ups_avg_by_bank_atm_cdm").collect()[0][0]) == int(1/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][0]) == int(900/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_cash_card").collect()[0][0]) == int(3/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_cash_card").collect()[0][0]) == int(1000/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_digital_online_self_service").collect()[0][0]) == int(7/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][0]) == int(3800/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epin_slip").collect()[0][0]) == int(2/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epin_slip").collect()[0][0]) == int(1700/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epos").collect()[0][0]) == int(5/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epos").collect()[0][0]) == int(3500/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_rom").collect()[0][0]) == int(3/7)
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_rom").collect()[0][0]) == int(1700/7)


        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_top_up_channels_monthly'))
        print('monthlydata')
        monthly_data.show(100, False)

        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_bank_atm_cdm").collect()[0][
                    0]) == 18
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_bank_atm_cdm").collect()[0][
                    0]) == 8800
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_cash_card").collect()[0][
                    0]) == 18
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_cash_card").collect()[0][
                    0]) == 11300
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_digital_online_self_service").collect()[0][
                    0]) == 14
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_digital_online_self_service").collect()[0][
                    0]) == 6300
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_by_epin_slip").collect()[0][
                    0]) == 4
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_by_epin_slip").collect()[0][
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
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_bank_atm_cdm").collect()[0][
                    0]) == (18/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][
                    0]) == (8800/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_cash_card").collect()[0][
                    0]) == (18/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_cash_card").collect()[0][
                    0]) == (11300/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_digital_online_self_service").collect()[0][
                    0]) == (14/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][
                    0]) == (6300/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_epin_slip").collect()[0][
                    0]) == (4/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_epin_slip").collect()[0][
                    0]) == (2000/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_epos").collect()[0][
                    0]) == (20/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_epos").collect()[0][
                    0]) == (9600/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg_by_rom").collect()[0][
                    0]) == (13/29)
        assert \
            (monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_vol_avg_by_rom").collect()[0][
                    0]) == (7500/29)


        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_top_up_channels')).orderBy(F.col("start_of_week").desc())
        print('finalfeature')
        final_features.orderBy('start_of_week').show()

        # sum: ["payments_top_ups_by_bank_atm_cdm", "payments_top_up_vol_by_bank_atm_cdm",
        #       "payments_top_ups_by_cash_card", "payments_top_up_vol_by_cash_card",
        #       "payments_top_ups_by_digital_online_self_service", "payments_top_up_vol_by_digital_online_self_service",
        #       "payments_top_ups_by_epin_slip",
        #       "payments_top_up_vol_by_epin_slip", "payments_top_ups_by_epos", "payments_top_up_vol_by_epos",
        #       "payments_top_ups_by_rom", "payments_top_up_vol_by_rom"]
        # avg: ["payments_top_ups_avg_by_bank_atm_cdm", "payments_top_up_vol_avg_by_bank_atm_cdm",
        #       "payments_top_ups_avg_by_cash_card", "payments_top_up_vol_avg_by_cash_card",
        #       "payments_top_ups_avg_by_digital_online_self_service",
        #       "payments_top_up_vol_avg_by_digital_online_self_service", "payments_top_ups_avg_by_epin_slip",
        #       "payments_top_up_vol_avg_by_epin_slip", "payments_top_ups_avg_by_epos", "payments_top_up_vol_avg_by_epos",
        #       "payments_top_ups_avg_by_rom", "payments_top_up_vol_avg_by_rom"]

        # exit(2)
