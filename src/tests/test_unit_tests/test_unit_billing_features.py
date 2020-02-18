from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import datetime


class TestUnitBilling:

    def get_df(self,project_context):
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        random_list = [random.randint(1, 5) for iTemp in range(0, 121)]

        df = spark.createDataFrame(zip(my_dates,my_dates), schema=['register_date', 'recharge_date']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("face_value", F.lit(1)) \
            .withColumn("account_id", F.lit(1)) \
            .withColumn("pre_bal_amt", F.lit(1)) \
            .withColumn("recharge_type", F.lit("B6")) \
            .withColumn("recharge_time", F.lit(datetime.strptime("01/01/2020 08:35:55", '%d/%m/%Y %H:%M:%S'))) \
            .withColumn("register_date", F.to_date('register_date', 'dd-MM-yyyy'))\
            .withColumn("recharge_date", F.to_date('recharge_date', 'dd-MM-yyyy'))

        return df

    def test_topup_and_volume_features(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_topup_volume = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))

        # check event date = 2020-01-01 date, the number of topup should be 1
        assert \
            daily_topup_volume.where("event_partition_date = '2020-01-01'").select("payments_top_ups").collect()[0][
                0] == 1

        # check event date = 2020-01-01 date, top up volume should be 1
        assert \
            daily_topup_volume.where("event_partition_date = '2020-01-01'").select("payments_top_up_volume").collect()[0][
                0] == 1


    def test_arpu_roaming(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        random_list = [random.randint(1, 5) for iTemp in range(0, 121)]

        df = spark.createDataFrame(zip(my_dates,my_dates), schema=['date_id', 'mobile_register_date']) \
            .withColumn("access_method_number", F.lit(1)) \
            .withColumn("crm_sub_id", F.lit(1)) \
            .withColumn("ir_call_charge_amount", F.lit(1)) \
            .withColumn("mobile_register_date", F.to_date('mobile_register_date', 'dd-MM-yyyy'))\
            .withColumn("date_id", F.to_date('date_id', 'dd-MM-yyyy'))

        # Testing Daily Features here
        daily_arpu_roaming = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_rpu_roaming'))


        # check event date = 2020-01-01 date, arpu roaming should be 1
        assert \
            daily_arpu_roaming.where("event_partition_date = '2020-01-01'").select("payments_arpu_roaming").collect()[0][
                0] == 1

    def test_payment_balance_before_topup(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_payament_balance_before_topup = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_before_top_up_balance'))


        # check event date = 2020-01-01 date, arpu roaming should be 1
        assert \
            daily_payament_balance_before_topup.where("event_partition_date = '2020-01-01'").select("payments_before_top_up_balance").collect()[0][
                0] == 1

    def test_most_popular_topup_channel(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_most_popular_topup_channel = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_most_popular_topup_channel'))


        # check event date = 2020-01-01 date, count for total topup from channel
        assert \
            daily_most_popular_topup_channel.where("event_partition_date = '2020-01-01'").select("payments_total_top_up").collect()[0][
                0] == 1

        # check event date = 2020-01-01 date, top up channel
        assert \
            daily_most_popular_topup_channel.where("event_partition_date = '2020-01-01'").select("recharge_type").collect()[0][
                0] == "B6"

    def test_topup_hour_and_day(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_topup_hour_and_day = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_popular_topup_day'))

        # check event date = 2020-01-01 date, topup day
        assert \
            daily_topup_hour_and_day.where("event_partition_date = '2020-01-01'").select("payment_popular_day").collect()[0][
                0] == 4

        # check event date = 2020-01-01 date, topup hour
        assert \
            daily_topup_hour_and_day.where("event_partition_date = '2020-01-01'").select("payment_popular_hour").collect()[0][
                0] == 8

    def test_time_since_last_topup(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_time_since_last_topup = node_from_config(df, var_project_context.catalog.load(
            'params:l1_time_since_last_top_up'))

        # check event date = 2020-01-01 date, recharge time
        assert \
            daily_time_since_last_topup.where("event_partition_date = '2020-01-01'").select("recharge_time").collect()[0][
                0].strftime("%Y-%m-%d %H:%M:%S") == "2020-01-01 08:35:55"

        # check event date = 2020-01-01 date, face value
        assert \
            daily_time_since_last_topup.where("event_partition_date = '2020-01-01'").select("face_value").collect()[0][
                0] == 1

    def test_topup_channels(self, project_context):
        var_project_context = project_context['ProjectContext']

        df = TestUnitBilling().get_df(project_context)

        # Testing Daily Features here
        daily_topup_channels = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_top_up_channels'))

        # check event date = 2020-01-01 date
        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_bank_atm_cdm").collect()[0][
                0] == 1

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_bank_atm_cdm").collect()[0][
                0] == 1

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_cash_card").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_cash_card").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_digital_online_self_service").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_digital_online_self_service").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_epin_slip").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_epin_slip").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_epos").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_epos").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_ups_by_rom").collect()[0][
                0] == 0

        assert \
            daily_topup_channels.where("event_partition_date = '2020-01-01'").select("payments_top_up_vol_by_rom").collect()[0][
                0] == 0



