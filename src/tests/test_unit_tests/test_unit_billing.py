from datetime import datetime

from kedro.pipeline import node
from pyspark.sql.functions import to_timestamp

from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window, l4_rolling_ranked_window
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l2.to_l2_nodes import *
from src.customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l1.to_l1_nodes import *
from src.customer360.pipelines.data_engineering.nodes.billing_nodes.to_l3.to_l3_nodes import *
import pandas as pd
import random
from pyspark.sql import functions as F, SparkSession
from datetime import timedelta



class TestUnitBilling:

    def test_rpu_monthly(self, project_context):
        '''
        l3_billing_and_payment_revenue_per_user_monthly
        l4_billing_rpu
        l4_dynamics_arpu
        '''
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        # df = billing_monthly_data
        #     l3_billing_and_payment_revenue_per_user_monthly:
        #     where_clause: "where start_of_month is not null"
        #     feature_list:
        #     payments_arpu: "sum(norms_net_revenue)"
        #     payments_arpu_gprs: "sum(norms_net_revenue_gprs)"
        #     payments_arpu_vas: "sum(norms_net_revenue_vas)"
        #     payments_arpu_voice: "sum(norms_net_revenue_voice)"
        #
        # granularity: "start_of_month,access_method_num,register_date,subscription_identifier"
        date1 = '2020-01-01'
        date2 = '2020-06-01'
        dummy_date = pd.date_range(date1, date2).tolist()
        # dummy_date = ['2019-01-01', '2019-02-01', '2019-03-01', '2019-04-01', '2019-05-01', '2019-06-01', '2019-07-01',
        #               '2019-08-01', '2019-09-01', '2019-10-01', '2019-11-01', '2019-12-01', '2020-01-01', '2020-02-01',
        #               '2020-03-01', '2020-04-01']
        # dummy_date = pd.to_datetime(dummy_date).tolist()
        dummy_date = [iTemp.date().strftime("%Y-%m-%d") for iTemp in dummy_date]
        random.seed(100)
        random_list = [random.randint(1, 9) * 100 for iTemp in range(0, len(dummy_date))]
        random_list2 = [random.randint(1, 9) * 100 for iTemp in range(0, len(dummy_date))]
        random_list3 = [random.randint(1, 9) * 100 for iTemp in range(0, len(dummy_date))]
        random_list4 = [random.randint(1, 9) * 100 for iTemp in range(0, len(dummy_date))]
        df = spark.createDataFrame(zip(dummy_date, random_list, random_list2, random_list3, random_list4),
                                   schema=['temp', 'norms_net_revenue', 'norms_net_revenue_gprs',
                                           'norms_net_revenue_vas', 'norms_net_revenue_voice']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.to_date('temp', 'yyyy-MM-dd'))))

        print('rawdata')
        df.show(888, False)

        l3_billing_and_payments_monthly_rpu = node_from_config(df, var_project_context.catalog.load(
            'params:l3_billing_and_payment_revenue_per_user_monthly'))
        print('l3monthlyrpu')
        l3_billing_and_payments_monthly_rpu.show(888, False)
        assert \
            l3_billing_and_payments_monthly_rpu.where("start_of_month = '2020-05-01'").select(
                "payments_arpu").collect()[0][
                0] == 13900
        assert \
            l3_billing_and_payments_monthly_rpu.where("start_of_month = '2020-05-01'").select(
                "payments_arpu_gprs").collect()[0][
                0] == 15200
        assert \
            l3_billing_and_payments_monthly_rpu.where("start_of_month = '2020-05-01'").select(
                "payments_arpu_vas").collect()[0][
                0] == 15600
        assert \
            l3_billing_and_payments_monthly_rpu.where("start_of_month = '2020-05-01'").select(
                "payments_arpu_voice").collect()[0][
                0] == 14500

        l4_billing_rolling_window_rpu_intermediate = l4_rolling_window(l3_billing_and_payments_monthly_rpu,
                                                                       var_project_context.catalog.load(
                                                                           'params:l4_billing_rpu'))
        print('l4rpuintermediate')
        l4_billing_rolling_window_rpu_intermediate.show(888, False)
        assert \
            l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_monthly_last_three_month").collect()[0][
                0] == 42200
        assert \
            l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_gprs_monthly_last_three_month").collect()[0][
                0] == 41200
        assert \
            l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_vas_monthly_last_three_month").collect()[0][
                0] == 46100
        assert \
            l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_voice_monthly_last_three_month").collect()[0][
                0] == 46800
        assert \
            int(l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_monthly_last_three_month").collect()[0][
                    0]) == int(14066.6666666666)
        assert \
            int(l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_gprs_monthly_last_three_month").collect()[0][
                    0]) == int(13733.3333333333)
        assert \
            int(l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_vas_monthly_last_three_month").collect()[0][
                    0]) == int(15366.6666666666)
        assert \
            l4_billing_rolling_window_rpu_intermediate.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_voice_monthly_last_three_month").collect()[0][
                0] == 15600

        l4_billing_rolling_window_rpu = node_from_config(l4_billing_rolling_window_rpu_intermediate,
                                                         var_project_context.catalog.load(
                                                             'params:l4_dynamics_arpu'))
        print('l4rpu')
        l4_billing_rolling_window_rpu.show(888, False)


        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").count() == 1

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "access_method_num").collect()[0][
                0] == 1

        assert \
            str(l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "subscription_identifier").collect()[0][
                0] == 123

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_last_month").collect()[0][
                0] == 13600

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_last_three_month").collect()[0][
                0] == 42200

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_gprs_last_month").collect()[0][
                0] == 12800

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_gprs_last_three_month").collect()[0][
                0] == 41200

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_vas_last_month").collect()[0][
                0] == 15700

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_vas_last_three_month").collect()[0][
                0] == 46100

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_voice_last_month").collect()[0][
                0] == 15300

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "sum_payments_arpu_voice_last_three_month").collect()[0][
                0] == 46800

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_last_month").collect()[0][
                0] == 13600

        assert \
            int(l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_last_three_month").collect()[0][
                0]) == int(14066.66667)

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_gprs_last_month").collect()[0][
                0] == 12800

        assert \
            int(l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_gprs_last_three_month").collect()[0][
                0]) == int(13733.33333)

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_vas_last_month").collect()[0][
                0] == 15700

        assert \
            int(l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_vas_last_three_month").collect()[0][
                0]) == int(15366.66667)

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_voice_last_month").collect()[0][
                0] == 15300

        assert \
            l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "avg_payments_arpu_voice_last_three_month").collect()[0][
                0] == 15600

        assert \
            int(l4_billing_rolling_window_rpu.where("start_of_month = '2020-05-01'").select(
                "payments_one_month_over_three_month_dynamics_arpu").collect()[0][
                0]*1000000) == int(0.966824645*1000000)

        # exit(2)

    def test_missed_bill(self, project_context):
        '''
        l3_missed_bills

        l3_overdue_bills

        l4_missed_bills

        l4_overdue_bills
        '''
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        my_dates.sort()
        list = ['1', '2', '3', None]

        random.seed(100)
        random_list = [list[random.randint(0, 3)] for iTemp in range(0, len(my_dates))]
        random_list2 = [random.randint(0, 40) for iTemp in range(0, len(my_dates))]
        random_list3 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        df = spark.createDataFrame(zip(my_dates, random_list, random_list2, random_list3),
                                   schema=['temp', 'bill_seq_no', 'no_of_days', 'bill_stmt_tot_invoiced_amt']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.to_date('temp', 'dd-MM-yyyy'))))

        print('rawdata')
        df.orderBy('temp').show(999, False)

        l3_billing_and_payments_monthly_missed_bills = node_from_config(df, var_project_context.catalog.load(
            'params:l3_missed_bills'))
        print('l3show')
        l3_billing_and_payments_monthly_missed_bills.show(888, False)
        assert \
            l3_billing_and_payments_monthly_missed_bills.where("start_of_month = '2020-01-01'").select(
                "payments_missed_bills").collect()[0][
                0] == 23

        l4_rolling_window_missed_bills = l4_rolling_window(l3_billing_and_payments_monthly_missed_bills,
                                                           var_project_context.catalog.load(
                                                               'params:l4_missed_bills'))

        print('l4rollingmissedbill')
        l4_rolling_window_missed_bills.show(999, False)
        assert \
            l4_rolling_window_missed_bills.where("start_of_month = '2020-03-01'").select(
                "sum_payments_missed_bills_monthly_last_three_month").collect()[0][
                0] == 42
        assert \
            l4_rolling_window_missed_bills.where("start_of_month = '2020-03-01'").select(
                "avg_payments_missed_bills_monthly_last_three_month").collect()[0][
                0] == 21

        l3_billing_and_payments_monthly_overdue_bills = node_from_config(df, var_project_context.catalog.load(
            'params:l3_overdue_bills'))
        print('overduetest')
        l3_billing_and_payments_monthly_overdue_bills.orderBy('start_of_month').show(999, False)

        assert \
            l3_billing_and_payments_monthly_overdue_bills.where("start_of_month = '2020-01-01'").select(
                "payments_over_due_bills").collect()[0][
                0] == 70
        assert \
            l3_billing_and_payments_monthly_overdue_bills.where("start_of_month = '2020-01-01'").select(
                "payments_over_due_bills_1_to_10_days").collect()[0][
                0] == 17
        assert \
            l3_billing_and_payments_monthly_overdue_bills.where("start_of_month = '2020-01-01'").select(
                "payments_over_due_bills_10_to_30_days").collect()[0][
                0] == 32
        assert \
            l3_billing_and_payments_monthly_overdue_bills.where("start_of_month = '2020-01-01'").select(
                "payments_over_due_bills_30_plus_days").collect()[0][
                0] == 21

        l4_rolling_window_overdue_bills = l4_rolling_window(l3_billing_and_payments_monthly_overdue_bills,
                                                            var_project_context.catalog.load(
                                                                'params:l4_overdue_bills'))
        print('l4overduebill')
        l4_rolling_window_overdue_bills.show(888, False)

        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "sum_payments_over_due_bills_monthly_last_three_month").collect()[0][
                0] == 135
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "sum_payments_over_due_bills_1_to_10_days_monthly_last_three_month").collect()[0][
                0] == 27
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "sum_payments_over_due_bills_10_to_30_days_monthly_last_three_month").collect()[0][
                0] == 72
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "sum_payments_over_due_bills_30_plus_days_monthly_last_three_month").collect()[0][
                0] == 36
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "avg_payments_over_due_bills_monthly_last_three_month").collect()[0][
                0] == 67.5
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "avg_payments_over_due_bills_1_to_10_days_monthly_last_three_month").collect()[0][
                0] == 13.5
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "avg_payments_over_due_bills_10_to_30_days_monthly_last_three_month").collect()[0][
                0] == 36
        assert \
            l4_rolling_window_overdue_bills.where("start_of_month = '2020-03-01'").select(
                "avg_payments_over_due_bills_30_plus_days_monthly_last_three_month").collect()[0][
                0] == 18

        l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume = node_from_config(df,
                                                                                                 var_project_context.catalog.load(
                                                                                                     'params:l3_last_overdue_bill_days_ago_and_volume'))

        print('lastoverduevolumn')
        l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume.show(888, False)

        assert \
            l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume.where(
                "start_of_month = '2020-01-01'").select(
                "payments_last_overdue_bill_days_ago").collect()[0][
                0] == 40
        assert \
            l3_billing_and_payments_monthly_last_overdue_bill_days_ago_and_volume.where(
                "start_of_month = '2020-01-01'").select(
                "payments_last_overdue_bill_volume").collect()[0][
                0] == 36700
        # exit(2)

    def test_automatic_flag(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        list = ['PM_13', 'PM_14', '1', '2', 'PM_11']
        dummy_date = ['2019-01-01', '2019-02-01', '2019-03-01', '2019-04-01', '2019-05-01', '2019-06-01', '2019-07-01',
                      '2019-08-01', '2019-09-01', '2019-10-01', '2019-11-01', '2019-12-01', '2020-01-01', '2020-02-01',
                      '2020-03-01', '2020-04-01']
        dummy_date = pd.to_datetime(dummy_date).tolist()
        dummy_date = [iTemp.date().strftime("%d-%m-%Y") for iTemp in dummy_date]
        random.seed(100)
        random_list = [list[random.randint(0, 4)] for iTemp in range(0, len(dummy_date))]
        df = spark.createDataFrame(zip(dummy_date, random_list),
                                   schema=['start_of_month', 'channel_identifier']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        # df = df.withColumn("start_of_month", F.to_date(df.temp,'yyyy-MM-dd'))

        l3_billing_monthly_automated_payments = node_from_config(df, var_project_context.catalog.load(
            'params:l3_automated_flag'))
        print('rawdata')
        df.orderBy('start_of_month').show(999, False)
        print('l3test')
        l3_billing_monthly_automated_payments.orderBy('start_of_month').show(999, False)
        assert \
            l3_billing_monthly_automated_payments.where("start_of_month = '01-01-2019'").select(
                "automated_payment_flag").collect()[0][
                0] == 'Y'
        assert \
            l3_billing_monthly_automated_payments.where("start_of_month = '01-01-2020'").select(
                "automated_payment_flag").collect()[0][
                0] == 'N'
        # exit(2)

        '''
            automated_payment_flag: "case when sum(case when channel_identifier in ('PM_13','PM_14') then 1
                                                 else 0 end) > 0
                                    then 'Y' else 'N' end"
  granularity: "start_of_month,access_method_num,register_date,subscription_identifier"
        '''

    def test_bill_monthly_history(self, project_context):
        """
        l3_bill_volume

        l4_payments_bill_shock

        l4_payments_bill_volume

        l4_dynamics_bill_volume
        """
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        dummy_date = ['2019-01-01', '2019-02-01', '2019-03-01', '2019-04-01', '2019-05-01', '2019-06-01', '2019-07-01',
                      '2019-08-01', '2019-09-01', '2019-10-01', '2019-11-01', '2019-12-01', '2020-01-01', '2020-02-01',
                      '2020-03-01', '2020-04-01']

        dummy_date = pd.to_datetime(dummy_date).tolist()
        dummy_date = [iTemp.date().strftime("%d-%m-%Y") for iTemp in dummy_date]

        random.seed(100)
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(dummy_date))]
        df = spark.createDataFrame(zip(dummy_date, random_list2, random_list2, random_list2),
                                   schema=['temp', 'bill_stmt_tot_net_revenue_amt', 'bill_stmt_tot_invoiced_amt',
                                           'bill_stmt_tot_net_ir_mrkp_amt']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("billing_stmt_period_eff_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("recharge_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.billing_stmt_period_eff_date)))

        l3_billing_and_payments_monthly_bill_volume = node_from_config(df, var_project_context.catalog.load(
            'params:l3_bill_volume'))
        print('l3billvolume')
        l3_billing_and_payments_monthly_bill_volume.orderBy('start_of_month').show(888, False)
        assert \
            l3_billing_and_payments_monthly_bill_volume.where("start_of_month = '2020-01-01'").select(
                "payments_bill_volume").collect()[0][
                0] == 800
        assert \
            l3_billing_and_payments_monthly_bill_volume.where("start_of_month = '2020-01-01'").select(
                "payments_roaming_bill_volume").collect()[0][
                0] == 800

        l4_billing_rolling_window_bill_volume_intermediate = l4_rolling_window(
            l3_billing_and_payments_monthly_bill_volume, var_project_context.catalog.load(
                'params:l4_payments_bill_volume'))

        assert \
            l4_billing_rolling_window_bill_volume_intermediate.where("start_of_month = '2019-04-01'").select(
                "sum_payments_bill_volume_monthly_last_three_month").collect()[0][
                0] == 1900
        assert \
            l4_billing_rolling_window_bill_volume_intermediate.where("start_of_month = '2019-04-01'").select(
                "sum_payments_roaming_bill_volume_monthly_last_three_month").collect()[0][
                0] == 1900
        assert \
            int(l4_billing_rolling_window_bill_volume_intermediate.where("start_of_month = '2019-04-01'").select(
                "avg_payments_bill_volume_monthly_last_three_month").collect()[0][
                    0]) == 633
        assert \
            int(l4_billing_rolling_window_bill_volume_intermediate.where("start_of_month = '2019-04-01'").select(
                "avg_payments_roaming_bill_volume_monthly_last_three_month").collect()[0][
                    0]) == 633

        print('l4billvolume')
        l4_billing_rolling_window_bill_volume_intermediate.show(888, False)

        l4_billing_rolling_window_bill_volume = node_from_config(l4_billing_rolling_window_bill_volume_intermediate,
                                                                 var_project_context.catalog.load(
                                                                     'params:l4_dynamics_bill_volume'))
        print('l4billingrolling')

        l4_billing_rolling_window_bill_volume.show(888, False)

        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "start_of_month").count()) == 1
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "access_method_num").collect()[0][
                    0]) == 1
        assert \
            str(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "subscription_identifier").collect()[0][
                    0]) == 123
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "sum_payments_roaming_bill_volume_last_month").collect()[0][
                    0]) == 800
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "sum_payments_roaming_bill_volume_last_three_month").collect()[0][
                    0]) == 1900
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "avg_payments_roaming_bill_volume_last_month").collect()[0][
                    0]) == 800
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "avg_payments_roaming_bill_volume_last_three_month").collect()[0][
                    0]) == 633
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "sum_payments_bill_volume_last_month").collect()[0][
                    0]) == 800
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "sum_payments_bill_volume_last_three_month").collect()[0][
                    0]) == 1900
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "avg_payments_bill_volume_last_month").collect()[0][
                    0]) == 800
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "avg_payments_bill_volume_last_three_month").collect()[0][
                    0]) == 633
        assert \
            int(l4_billing_rolling_window_bill_volume.where("start_of_month = '2019-04-01'").select(
                "payments_one_month_over_three_month_dynamics_bill_volume").collect()[0][
                    0] * 100000) == int(1.26315789473684 * 100000)

        l4_billing_statement_history_billshock = node_from_config(df, var_project_context.catalog.load(
            'params:l4_payments_bill_shock'))
        print('billshock')
        l4_billing_statement_history_billshock.show(999, False)
        assert \
            l4_billing_statement_history_billshock.where("start_of_month = '2020-01-01'").count() == 1
        assert \
            l4_billing_statement_history_billshock.where("start_of_month = '2020-01-01'").select(
                "access_method_num").collect()[0][
                0] == 1
        assert \
            str(l4_billing_statement_history_billshock.where("start_of_month = '2020-01-01'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'
        assert \
            (l4_billing_statement_history_billshock.where("start_of_month = '2020-01-01'").select(
                "subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (l4_billing_statement_history_billshock.where("start_of_month = '2019-01-01'").select(
                "bill_stmt_tot_net_revenue_amt").collect()[0][
                0]) == 300
        assert \
            (l4_billing_statement_history_billshock.where("start_of_month = '2019-01-01'").select(
                "bill_shock_flag").collect()[0][
                0]) == 'N'
        assert \
            (l4_billing_statement_history_billshock.where("start_of_month = '2019-02-01'").select(
                "bill_shock_flag").collect()[0][
                0]) == 'Y'

        # exit(2)

    def test_last_topup_channel(self, project_context):
        """
        l2_last_topup_channel

        l3_last_topup_channel
        """
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        date1 = '2020-01-01'
        date2 = '2020-04-01'

        dummy_desc = ['Refill Card', 'My AIS App, My AIS Web', 'Refill Card for non Mobile', 'ATM', 'AIS Shop',
                      'Refill on Mobile', 'Partner Online Top Up', 'AIS Auto Top up', 'Cash card',
                      'Recharge via ATM of BBL', 'Recharge via mPay (Agent mCash)', 'Refill on Mobile',
                      'Recharge via ATM of BAAC', 'Biz Reward Platform']

        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        my_dates.sort()
        random_list = [dummy_desc[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        start_time = datetime.strptime("01/01/2020 08:35:55", '%d/%m/%Y %H:%M:%S')
        start_time_list = []
        for i in range(len(my_dates)):
            start_time_list.append(start_time)
            start_time = start_time + timedelta(seconds=random.randint(1, 432000))

        input_df = spark.createDataFrame(zip(random_list, start_time_list),
                                         schema=['recharge_topup_event_type_name', 'recharge_time']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123)) \
            # .withColumn("start_of_week",F.to_date(F.date_trunc('week', start_time_list)))
        input_df = input_df.withColumn("start_of_week", F.to_date(F.date_trunc('week', input_df.recharge_time))) \
            .withColumn("start_of_month", F.to_date(F.date_trunc('month', input_df.recharge_time)))

        print('testdata')
        input_df.orderBy('start_of_week').show(888, False)
        l2_billing_and_payments_weekly_last_top_up_channel = node_from_config(input_df,
                                                                              var_project_context.catalog.load(
                                                                                  'params:l2_last_topup_channel'))

        l2_billing_and_payments_weekly_last_top_up_channel = l2_billing_and_payments_weekly_last_top_up_channel.withColumn("rn", expr(
            "row_number() over(partition by start_of_week,access_method_num,register_date order by recharge_time desc)"))

        l2_billing_and_payments_weekly_last_top_up_channel = l2_billing_and_payments_weekly_last_top_up_channel.filter("rn = 1").drop("rn")
        print('l2test')
        l2_billing_and_payments_weekly_last_top_up_channel.orderBy('start_of_week').show(888, False)
        # payments_last_top_up_channel
        assert \
            l2_billing_and_payments_weekly_last_top_up_channel.where(
                "start_of_week = '2020-01-06'").select(
                "payments_last_top_up_channel").collect()[0][
                0] == 'Refill on Mobile'
        l3_billing_and_payments_monthly_last_top_up_channel = node_from_config(input_df,
                                                                               var_project_context.catalog.load(
                                                                                   'params:l3_last_topup_channel'))

        l3_billing_and_payments_monthly_last_top_up_channel = l3_billing_and_payments_monthly_last_top_up_channel.withColumn("rn", expr(
            "row_number() over(partition by start_of_month,access_method_num,register_date order by register_date desc)"))

        l3_billing_and_payments_monthly_last_top_up_channel = l3_billing_and_payments_monthly_last_top_up_channel.filter("rn = 1").drop("rn")
        print('l3test')
        l3_billing_and_payments_monthly_last_top_up_channel.orderBy('start_of_month').show(999, False)
        assert \
            l3_billing_and_payments_monthly_last_top_up_channel.where(
                "start_of_month = '2020-03-01'").select(
                "payments_last_top_up_channel").collect()[0][
                0] == 'Biz Reward Platform'
        # exit(2)

    def test_popular_channel_feature(self, project_context):
        """
        l1_billing_and_payment_most_popular_topup_channel
        l2_popular_top_up_channel
        l2_most_popular_topup_channel
        l3_popular_topup_channel
        l3_most_popular_topup_channel
        l4_most_popular_topup_channel_initial
        l4_most_popular_topup_channel
        """
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        # Below section is to create dummy data.
        random_type = ['4', 'B1', 'B58', '3', 'B0', '7', '16', 'B43', '1', '5', '53', 'B69', '51', 'B50']
        date1 = '2020-01-01'
        date2 = '2020-04-01'

        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        random_list = [random_type[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        random.seed(100)
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        df = spark.createDataFrame(zip(random_list, my_dates, random_list2),
                                   schema=['recharge_type', 'temp', 'face_value']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("recharge_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))

        print('dfdebug')
        df.show()
        print('dfdebug2')
        l1_billing_and_payments_daily_most_popular_top_up_channel = node_from_config(df,
                                                                                     var_project_context.catalog.load(
                                                                                         'params:l1_billing_and_payment_most_popular_topup_channel'))
        print('dailydebug')
        l1_billing_and_payments_daily_most_popular_top_up_channel.show(999, False)
        assert \
            l1_billing_and_payments_daily_most_popular_top_up_channel.where(
                "event_partition_date = '2020-01-05'").where('recharge_type="B1"').select(
                "payments_total_top_up").collect()[0][
                0] == 2

        dummy_type = ['1', '16', '3', '4', '5', '51', '53', '7', 'B0', 'B1', 'B43', 'B50', 'B58', 'B69']
        dummy_desc = ['Refill Card', 'My AIS App, My AIS Web', 'Refill Card for non Mobile', 'ATM', 'AIS Shop',
                      'Refill on Mobile', 'Partner Online Top Up', 'AIS Auto Top up', 'Cash card',
                      'Recharge via ATM of BBL', 'Recharge via mPay (Agent mCash)', 'Refill on Mobile',
                      'Recharge via ATM of BAAC', 'Biz Reward Platform']
        topup_type = spark.createDataFrame(zip(dummy_type, dummy_desc),
                                           schema=['recharge_topup_event_type_cd', 'recharge_topup_event_type_name'])
        joined_data = top_up_channel_joined_data(l1_billing_and_payments_daily_most_popular_top_up_channel, topup_type)
        print('joineddata')
        joined_data.show(999, False)
        # joined_data.show(999,False)
        l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate = node_from_config(joined_data,
                                                                                                   var_project_context.catalog.load(
                                                                                                       'params:l2_popular_top_up_channel'))
        print('weeklyy')
        l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate.show(999, False)

        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate.where(
                "start_of_week = '2019-12-30'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "payments_total_top_up").collect()[0][
                0] == 3
        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate.where(
                "start_of_week = '2019-12-30'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "rank").collect()[0][
                0] == 2

        l2_billing_and_payments_weekly_most_popular_top_up_channel = node_from_config(
            l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate, var_project_context.catalog.load(
                'params:l2_most_popular_topup_channel'))
        print('mostweekly')
        l2_billing_and_payments_weekly_most_popular_top_up_channel.show(999, False)

        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel.where("start_of_week = '2019-12-30'").select(
                "access_method_num").collect()[0][
                0] == 1
        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel.where(
                "start_of_week = '2019-12-30'").count() == 1
        assert \
            str(l2_billing_and_payments_weekly_most_popular_top_up_channel.where("start_of_week = '2019-12-30'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'
        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel.where("start_of_week = '2019-12-30'").select(
                "payments_total_top_up").collect()[0][
                0] == 4
        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel.where("start_of_week = '2019-12-30'").select(
                "payments_top_up_channel").collect()[0][
                0] == 'Recharge via ATM of BBL'
        assert \
            l2_billing_and_payments_weekly_most_popular_top_up_channel.where("start_of_week = '2019-12-30'").select(
                "subscription_identifier").collect()[0][
                0] == 123

        l3_billing_and_payments_monthly_most_popular_top_up_channel_2 = node_from_config(joined_data,
                                                                                         var_project_context.catalog.load(
                                                                                             'params:l3_popular_topup_channel'))
        print('monthlyy')
        l3_billing_and_payments_monthly_most_popular_top_up_channel_2.show(999, False)
        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel_2.where("start_of_month = '2020-01-01'").where(
                'recharge_topup_event_type_name="Biz Reward Platform"').select(
                "rank").collect()[0][
                0] == 4 or 5
        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel_2.where("start_of_month = '2020-01-01'").where(
                'recharge_topup_event_type_name="Biz Reward Platform"').select(
                "payments_total_top_up").collect()[0][
                0] == 8

        l3_billing_and_payments_monthly_most_popular_top_up_channel = node_from_config(
            l3_billing_and_payments_monthly_most_popular_top_up_channel_2, var_project_context.catalog.load(
                'params:l3_most_popular_topup_channel'))
        print('monthlymost')
        l3_billing_and_payments_monthly_most_popular_top_up_channel.show(999, False)

        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel.where(
                "start_of_month = '2020-01-01'").count() == 1
        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel.where("start_of_month = '2020-01-01'").select(
                "payments_top_up_channel").collect()[0][
                0] == 'Refill on Mobile'
        assert \
            str(l3_billing_and_payments_monthly_most_popular_top_up_channel.where(
                "start_of_month = '2020-01-01'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'

        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel.where("start_of_month = '2020-01-01'").select(
                "payments_total_top_up").collect()[0][
                0] == 15
        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel.where("start_of_month = '2020-01-01'").select(
                "payments_top_up_channel").collect()[0][
                0] == 'Refill on Mobile'
        assert \
            l3_billing_and_payments_monthly_most_popular_top_up_channel.where("start_of_month = '2020-01-01'").select(
                "subscription_identifier").collect()[0][
                0] == 123

        l4_rolling_window_most_popular_topup_channel_1 = l4_rolling_window(
            l2_billing_and_payments_weekly_most_popular_top_up_channel_intermediate, var_project_context.catalog.load(
                'params:l4_most_popular_topup_channel_initial'))
        print('finaldebugini')
        l4_rolling_window_most_popular_topup_channel_1.show(999, False)
        assert \
            l4_rolling_window_most_popular_topup_channel_1.where("start_of_week = '2020-02-03'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "sum_payments_total_top_up_weekly_last_week").collect()[0][
                0] == 5
        assert \
            l4_rolling_window_most_popular_topup_channel_1.where("start_of_week = '2020-02-03'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "sum_payments_total_top_up_weekly_last_two_week").collect()[0][
                0] == 8
        assert \
            l4_rolling_window_most_popular_topup_channel_1.where("start_of_week = '2020-02-03'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "sum_payments_total_top_up_weekly_last_four_week").collect()[0][
                0] == 13
        assert \
            l4_rolling_window_most_popular_topup_channel_1.where("start_of_week = '2020-02-03'").where(
                'recharge_topup_event_type_name="Refill on Mobile"').select(
                "sum_payments_total_top_up_weekly_last_twelve_week").collect()[0][
                0] == 16

        print('beforeerror')
        l4_billing_rolling_window_most_popular_topup_channel = l4_rolling_ranked_window(
            l4_rolling_window_most_popular_topup_channel_1, var_project_context.catalog.load(
                'params:l4_most_popular_topup_channel'))
        print('finalfeaturedebug')
        l4_billing_rolling_window_most_popular_topup_channel.orderBy('start_of_week').show(888, False)
        assert \
            l4_billing_rolling_window_most_popular_topup_channel.where("start_of_week = '2020-02-10'").select(
                "payments_top_up_channel_last_week").collect()[0][
                0] == 'Refill Card for non Mobile'
        assert \
            l4_billing_rolling_window_most_popular_topup_channel.where("start_of_week = '2020-02-10'").select(
                "payments_top_up_channel_last_two_week").collect()[0][
                0] == 'Refill on Mobile'
        assert \
            l4_billing_rolling_window_most_popular_topup_channel.where("start_of_week = '2020-02-10'").select(
                "payments_top_up_channel_last_four_week").collect()[0][
                0] == 'Refill on Mobile'
        assert \
            l4_billing_rolling_window_most_popular_topup_channel.where("start_of_week = '2020-02-10'").select(
                "payments_top_up_channel_last_twelve_week").collect()[0][
                0] == 'Refill on Mobile'
        # exit(2)

    def test_topup_frequency_feature(self, project_context):
        """
        l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate
        l2_billing_and_payment_feature_time_diff_bw_topups_weekly
        l4_billing_time_diff_bw_topups

        l1_time_since_last_top_up
        l2_time_since_last_top_up
        l2_last_three_topup_volume_ranked
        l2_last_three_topup_volume
        l3_time_since_last_top_up
        l3_last_three_topup_volume_ranked
        l3_last_three_topup_volume

        """
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

        df = spark.createDataFrame(zip(random_list, start_time_list, start_time_list),
                                   schema=['face_value', 'recharge_date', 'recharge_time']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("event_partition_date", F.to_date('recharge_date', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', df.recharge_time))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.recharge_time)))
        df = df.orderBy('recharge_time')

        print('showdf')
        df.show(888, False)

        # nothing to test
        l1_billing_and_payments_daily_time_since_last_top_up = node_from_config(df, var_project_context.catalog.load(
            'params:l1_time_since_last_top_up'))
        print('timelasttopup')
        l1_billing_and_payments_daily_time_since_last_top_up.show(999, False)
        assert \
            l1_billing_and_payments_daily_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "access_method_num").collect()[0][
                0] == 1
        assert \
            str(l1_billing_and_payments_daily_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "register_date").collect()[0][
                    0]) == '2019-01-01'
        assert \
            l1_billing_and_payments_daily_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "subscription_identifier").collect()[0][
                0] == 123
        assert \
            l1_billing_and_payments_daily_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "face_value").collect()[0][
                0] == 800

        assert \
            str(l1_billing_and_payments_daily_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "recharge_time").collect()[0][
                    0]) == '2020-01-06 15:38:21'

        l2_billing_and_payments_weekly_time_since_last_top_up = node_from_config(
            l1_billing_and_payments_daily_time_since_last_top_up, var_project_context.catalog.load(
                'params:l2_time_since_last_top_up'))
        print('l2time')
        l2_billing_and_payments_weekly_time_since_last_top_up.orderBy('start_of_week').show(999, False)
        assert \
            l2_billing_and_payments_weekly_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "payments_time_since_last_top_up").collect()[0][
                0] == 2

        assert \
            str(l2_billing_and_payments_weekly_time_since_last_top_up.where("start_of_week = '2020-01-06'").select(
                "payments_recharge_time").collect()[0][
                    0]) == '2020-01-10 09:37:52'

        l2_billing_and_payments_weekly_last_three_topup_volume_1 = node_from_config(
            l1_billing_and_payments_daily_time_since_last_top_up, var_project_context.catalog.load(
                'params:l2_last_three_topup_volume_ranked'))
        print('lastthreeranked')

        l2_billing_and_payments_weekly_last_three_topup_volume_1.orderBy('start_of_week').show(999, False)
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").where(
                'rank=1').select("face_value").collect()[
                0][
                0] == 700
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").where(
                'rank=1').select(
                "subscription_identifier").collect()[
                0][
                0] == 123
        assert \
            str(l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").where(
                'rank=1').select(
                "register_date").collect()[
                    0][
                    0]) == '2019-01-01'
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").where(
                'rank=1').select(
                "access_method_num").collect()[
                0][
                0] == 1
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").where(
                'face_value=700').select("rank").collect()[
                0][
                0] == 1
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume_1.where("start_of_week = '2020-01-06'").count() == 3

        l2_billing_and_payments_weekly_last_three_topup_volume = node_from_config(
            l2_billing_and_payments_weekly_last_three_topup_volume_1, var_project_context.catalog.load(
                'params:l2_last_three_topup_volume'))
        assert \
            l2_billing_and_payments_weekly_last_three_topup_volume.where("start_of_week = '2020-01-06'").select(
                "payment_top_volume").collect()[0][
                0] == 1800
        print('last3')
        l2_billing_and_payments_weekly_last_three_topup_volume.orderBy('start_of_week').show(888, False)

        l3_billing_and_payments_monthly_time_since_last_top_up = node_from_config(
            l1_billing_and_payments_daily_time_since_last_top_up, var_project_context.catalog.load(
                'params:l3_time_since_last_top_up'))
        print('l3time')
        l3_billing_and_payments_monthly_time_since_last_top_up.orderBy('start_of_month').show(999, False)

        assert \
            l3_billing_and_payments_monthly_time_since_last_top_up.where("start_of_month = '2020-01-01'").select(
                "payments_time_since_last_top_up").collect()[0][
                0] == 2

        assert \
            str(l3_billing_and_payments_monthly_time_since_last_top_up.where("start_of_month = '2020-01-01'").select(
                "payments_recharge_time").collect()[0][
                    0]) == '2020-01-30 03:29:41'
        l3_billing_and_payments_monthly_last_three_topup_volume_1 = node_from_config(
            l1_billing_and_payments_daily_time_since_last_top_up, var_project_context.catalog.load(
                'params:l3_last_three_topup_volume_ranked'))
        print('last3threeranked')

        l3_billing_and_payments_monthly_last_three_topup_volume_1.orderBy('start_of_month').show(999, False)
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume_1.where("start_of_month = '2020-01-01'").where(
                'rank=2').select("face_value").collect()[
                0][
                0] == 500
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume_1.where(
                "start_of_month = '2020-01-01'").count() == 15
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume_1.where("start_of_month = '2020-01-01'").where(
                'rank=2').select(
                "access_method_num").collect()[
                0][
                0] == 1
        assert \
            str(l3_billing_and_payments_monthly_last_three_topup_volume_1.where("start_of_month = '2020-01-01'").where(
                'rank=2').select(
                "register_date").collect()[
                    0][
                    0]) == '2019-01-01'
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume_1.where("start_of_month = '2020-01-01'").where(
                'rank=2').select(
                "subscription_identifier").collect()[
                0][
                0] == 123
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume_1.where("start_of_month = '2020-01-01'").where(
                'face_value=500').select(
                "rank").collect()[
                0][
                0] == 2
        l3_billing_and_payments_monthly_last_three_topup_volume = node_from_config(
            l3_billing_and_payments_monthly_last_three_topup_volume_1, var_project_context.catalog.load(
                'params:l3_last_three_topup_volume'))
        assert \
            l3_billing_and_payments_monthly_last_three_topup_volume.where("start_of_month = '2020-01-01'").select(
                "payment_top_volume").collect()[0][
                0] == 1400
        print('last_3')
        l3_billing_and_payments_monthly_last_three_topup_volume.orderBy('start_of_month').show(888, False)

        # exit(2)

        l2_billing_and_payments_weekly_topup_diff_time_intermediate = node_from_config(df,
                                                                                       var_project_context.catalog.load(
                                                                                           'params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly_intermdeiate'))

        print('debuginit')
        l2_billing_and_payments_weekly_topup_diff_time_intermediate.printSchema()
        l2_billing_and_payments_weekly_topup_diff_time_intermediate.orderBy('start_of_week').show(999, False)
        assert \
            l2_billing_and_payments_weekly_topup_diff_time_intermediate.where(
                "event_partition_date = '2020-01-05'").select("access_method_num").collect()[0][
                0] == 1
        assert \
            str(l2_billing_and_payments_weekly_topup_diff_time_intermediate.where(
                "event_partition_date = '2020-01-05'").select("register_date").collect()[0][
                    0]) == '2019-01-01'
        assert \
            l2_billing_and_payments_weekly_topup_diff_time_intermediate.where(
                "event_partition_date = '2020-01-05'").select("payments_time_diff").collect()[0][
                0] == 4
        l2_billing_and_payments_weekly_topup_diff_time_intermediate = l2_billing_and_payments_weekly_topup_diff_time_intermediate.withColumn(
            "subscription_identifier", F.lit(1))
        l2_billing_and_payments_weekly_topup_time_diff = node_from_config(
            l2_billing_and_payments_weekly_topup_diff_time_intermediate, var_project_context.catalog.load(
                'params:l2_billing_and_payment_feature_time_diff_bw_topups_weekly'))

        print('debug12341')
        l2_billing_and_payments_weekly_topup_time_diff.orderBy('start_of_week').show(999, False)
        # weekly_data.where("start_of_week = '2020-10-12'").show()
        assert \
            l2_billing_and_payments_weekly_topup_time_diff.where("start_of_week = '2020-01-06'").select(
                "payments_max_time_diff").collect()[0][
                0] == 4
        assert \
            l2_billing_and_payments_weekly_topup_time_diff.where("start_of_week = '2020-01-06'").select(
                "payments_min_time_diff").collect()[0][
                0] == 0
        assert \
            l2_billing_and_payments_weekly_topup_time_diff.where("start_of_week = '2020-01-06'").select(
                "payments_time_diff").collect()[0][
                0] == 4
        assert \
            l2_billing_and_payments_weekly_topup_time_diff.where("start_of_week = '2020-01-06'").select(
                "payments_time_diff_avg").collect()[0][
                0] == 2

        l2_billing_and_payments_weekly_topup_time_diff.orderBy('start_of_week').show(1000, False)

        l4_billing_rolling_window_time_diff_bw_top_ups_1 = l4_rolling_window(
            l2_billing_and_payments_weekly_topup_time_diff, var_project_context.catalog.load(
                'params:l4_billing_time_diff_bw_topups'))
        print('final feature')
        l4_billing_rolling_window_time_diff_bw_top_ups_1.show(100, False)
        assert \
            l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "sum_payments_time_diff_weekly_last_week").collect()[0][
                0] == 4
        assert \
            l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "sum_payments_time_diff_weekly_last_two_week").collect()[0][
                0] == 10
        assert \
            l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "sum_payments_time_diff_weekly_last_four_week").collect()[0][
                0] == 16
        assert \
            l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "sum_payments_time_diff_weekly_last_twelve_week").collect()[0][
                0] == 24
        assert \
            l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "avg_payments_time_diff_avg_weekly_last_week").collect()[0][
                0] == 1.3333333333333333
        assert \
            (l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "avg_payments_time_diff_avg_weekly_last_two_week").collect()[0][
                0]) == 1.6666666666666665
        assert \
            (l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "avg_payments_time_diff_avg_weekly_last_four_week").collect()[0][
                0]) == 1.4999999999999998
        assert \
            (l4_billing_rolling_window_time_diff_bw_top_ups_1.where("start_of_week = '2020-02-10'").select(
                "avg_payments_time_diff_avg_weekly_last_twelve_week").collect()[0][
                0]) == 2

        # exit(2)

    def test_topup_and_volume_feature(self, project_context):
        """
        l1_billing_and_payment_feature_top_up_and_count
        l2_billing_and_payment_feature_top_up_and_count_weekly
        l3_billing_and_payment_feature_top_up_and_count_monthly
        l4_billing_topup_and_volume


        l4_billing_topup_and_volume_daily_feature

        l4_dynamics_topups_and_volume
        """
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
        l1_billing_and_payments_daily_topup_and_volume = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))
        print('dailydata')
        l1_billing_and_payments_daily_topup_and_volume.orderBy('event_partition_date').show()

        l4_daily_feature_topup_and_volume = l4_rolling_window(l1_billing_and_payments_daily_topup_and_volume,
                                                              var_project_context.catalog.load(
                                                                  'params:l4_billing_topup_and_volume_daily_feature'))

        print('l4_daily_feature_topup_and_volume')
        l4_daily_feature_topup_and_volume.show(999, False)

        assert \
            l4_daily_feature_topup_and_volume.where("event_partition_date = '2020-02-06'").select(
                "sum_payments_top_ups_daily_last_ninety_day").collect()[0][
                0] == 108
        assert \
            l4_daily_feature_topup_and_volume.where("event_partition_date = '2020-02-06'").select(
                "sum_payments_top_up_volume_daily_last_ninety_day").collect()[0][
                0] == 55400
        assert \
            l4_daily_feature_topup_and_volume.where("event_partition_date = '2020-02-06'").select(
                "avg_payments_top_ups_daily_last_ninety_day").collect()[0][
                0] == 3
        assert \
            int(l4_daily_feature_topup_and_volume.where("event_partition_date = '2020-02-06'").select(
                "avg_payments_top_up_volume_daily_last_ninety_day").collect()[0][
                    0]) == 1538

        # exit(2)

        l2_billing_and_payments_weekly_topup_and_volume = node_from_config(
            l1_billing_and_payments_daily_topup_and_volume, var_project_context.catalog.load(
                'params:l2_billing_and_payment_feature_top_up_and_count_weekly'))
        # weekly_data.orderBy('start_of_week').show()
        assert \
            l1_billing_and_payments_daily_topup_and_volume.where("event_partition_date = '2020-01-01'").select(
                "payments_top_up_volume").collect()[0][
                0] == 1000
        assert \
            l1_billing_and_payments_daily_topup_and_volume.where("event_partition_date = '2020-01-01'").select(
                "payments_top_ups").collect()[0][
                0] == 3

        assert \
            l2_billing_and_payments_weekly_topup_and_volume.where("start_of_week='2020-01-06'").select(
                "payments_top_ups").collect()[0][0] == 21
        assert \
            l2_billing_and_payments_weekly_topup_and_volume.where("start_of_week='2020-01-06'").select(
                "payments_top_up_volume").collect()[0][0] == 11700
        assert \
            l2_billing_and_payments_weekly_topup_and_volume.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(l2_billing_and_payments_weekly_topup_and_volume.where("start_of_week='2020-01-06'").select(
                "payments_top_up_volume_avg").collect()[0][
                    0]) == 1671

        l3_billing_and_payments_monthly_topup_and_volume = node_from_config(
            l1_billing_and_payments_daily_topup_and_volume, var_project_context.catalog.load(
                'params:l3_billing_and_payment_feature_top_up_and_count_monthly'))
        # monthly_data.show()
        assert \
            l3_billing_and_payments_monthly_topup_and_volume.where("start_of_month='2020-02-01'").select(
                "payments_top_ups").collect()[0][0] == 87
        assert \
            l3_billing_and_payments_monthly_topup_and_volume.where("start_of_month='2020-02-01'").select(
                "payments_top_up_volume").collect()[0][0] == 48800
        assert \
            l3_billing_and_payments_monthly_topup_and_volume.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(l3_billing_and_payments_monthly_topup_and_volume.where("start_of_month='2020-02-01'").select(
                "payments_top_up_volume_avg").collect()[0][
                    0]) == 1682

        l4_billing_rolling_window_topup_and_volume_intermediate = l4_rolling_window(
            l2_billing_and_payments_weekly_topup_and_volume, var_project_context.catalog.load(
                'params:l4_billing_topup_and_volume')).orderBy(F.col("start_of_week").desc())

        # final_features.orderBy('start_of_week').show()
        assert \
            l4_billing_rolling_window_topup_and_volume_intermediate.where("start_of_week='2020-03-23'").select(
                "sum_payments_top_ups_weekly_last_twelve_week").collect()[0][0] == 246
        assert \
            l4_billing_rolling_window_topup_and_volume_intermediate.where("start_of_week='2020-03-23'").select(
                "sum_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0] == 131100
        assert \
            l4_billing_rolling_window_topup_and_volume_intermediate.where("start_of_week='2020-03-23'").select(
                "avg_payments_top_ups_weekly_last_twelve_week").collect()[0][0] == 20.5
        assert \
            int(l4_billing_rolling_window_topup_and_volume_intermediate.where("start_of_week='2020-03-23'").select(
                "avg_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0]) == 10925

        l4_billing_rolling_window_topup_and_volume = node_from_config(
            l4_billing_rolling_window_topup_and_volume_intermediate, var_project_context.catalog.load(
                'params:l4_dynamics_topups_and_volume'))
        print('l4dynamics')
        l4_billing_rolling_window_topup_and_volume.show(999, False)

        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").count() == 1
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-23'").select(
                "subscription_identifier").collect()[0][0] == 123
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-23'").select(
                "access_method_num").collect()[0][0] == 1
        assert \
            str(l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-23'").select(
                "register_date").collect()[0][0]) == '2019-01-01'

        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_ups_last_week").collect()[0][0] == 21
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_ups_last_two_week").collect()[0][0] == 42
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_ups_last_four_week").collect()[0][0] == 84
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_ups_last_twelve_week").collect()[0][0] == 252
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_up_volume_last_week").collect()[0][0] == 11700
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_up_volume_last_two_week").collect()[0][0] == 22800
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_up_volume_last_four_week").collect()[0][0] == 42900
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "sum_payments_top_up_volume_last_twelve_week").collect()[0][0] == 134800
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_ups_last_week").collect()[0][0] == 21
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_ups_last_two_week").collect()[0][0] == 21
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_ups_last_four_week").collect()[0][0] == 21
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_ups_last_twelve_week").collect()[0][0] == 21

        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_up_volume_last_week").collect()[0][0] == 11700
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_up_volume_last_two_week").collect()[0][0] == 11400
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_up_volume_last_four_week").collect()[0][0] == 10725
        assert \
            int(l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "avg_payments_top_up_volume_last_twelve_week").collect()[0][0]) == 11233
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "payments_one_week_over_two_week_top_up_no").collect()[0][0] == 1
        assert \
            l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "payments_four_week_over_twelve_week_top_up_no").collect()[0][0] == 1
        assert \
            int(l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "payments_one_week_over_two_week_top_up_volume").collect()[0][0] * 1000000) == int(1026315.78947368)
        assert \
            int(l4_billing_rolling_window_topup_and_volume.where("start_of_week='2020-03-30'").select(
                "payments_four_week_over_twelve_week_top_up_volume").collect()[0][0] * 1000000) == int(954747.774480712)

        # exit(2)

    def test_arpu_roaming_feature(self, project_context):
        """
        l1_billing_and_payment_rpu_roaming
        l2_billing_and_payment_feature_rpu_roaming_weekly
        l3_billing_and_payment_feature_rpu_roaming_monthly
        l4_billing_rpu_roaming
        """
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
        l1_billing_and_payments_daily_rpu_roaming = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_rpu_roaming'))

        # daily_data.orderBy('event_partition_date').show()
        assert \
            l1_billing_and_payments_daily_rpu_roaming.where("event_partition_date = '2020-01-01'").select(
                "payments_arpu_roaming").collect()[0][
                0] == 1000
        l2_billing_weekly_rpu_roaming = node_from_config(l1_billing_and_payments_daily_rpu_roaming,
                                                         var_project_context.catalog.load(
                                                             'params:l2_billing_and_payment_feature_rpu_roaming_weekly'))
        # weekly_data.show(100, False)
        assert \
            l2_billing_weekly_rpu_roaming.where("start_of_week='2020-01-06'").select("payments_arpu_roaming").collect()[
                0][0] == 11700
        assert \
            int(l2_billing_weekly_rpu_roaming.where("start_of_week='2020-01-06'").select(
                "payments_arpu_roaming_avg").collect()[0][
                    0]) == 1671

        l3_billing_monthly_rpu_roaming = node_from_config(l1_billing_and_payments_daily_rpu_roaming,
                                                          var_project_context.catalog.load(
                                                              'params:l3_billing_and_payment_feature_rpu_roaming_monthly'))
        # monthly_data.show()
        assert \
            l3_billing_monthly_rpu_roaming.where("start_of_month='2020-02-01'").select(
                "payments_arpu_roaming").collect()[0][0] == 48800
        assert \
            int(l3_billing_monthly_rpu_roaming.where("start_of_month='2020-02-01'").select(
                "payments_arpu_roaming_avg").collect()[0][
                    0]) == 1682

        l4_billing_rolling_window_rpu_roaming = l4_rolling_window(l2_billing_weekly_rpu_roaming,
                                                                  var_project_context.catalog.load(
                                                                      'params:l4_billing_rpu_roaming')).orderBy(
            F.col("start_of_week").desc())
        # final_features.orderBy('start_of_week').show()

        assert \
            l4_billing_rolling_window_rpu_roaming.where("start_of_week='2020-03-23'").select(
                "sum_payments_arpu_roaming_weekly_last_twelve_week").collect()[0][0] == 131100
        assert \
            int(l4_billing_rolling_window_rpu_roaming.where("start_of_week='2020-03-23'").select(
                "avg_payments_arpu_roaming_avg_weekly_last_twelve_week").collect()[0][0]) == 1598

    def test_before_top_up_balance_feature(self, project_context):
        """
        l1_billing_and_payment_before_top_up_balance
        l2_billing_and_payment_before_top_up_balance_weekly
        l3_billing_and_payment_before_top_up_balance_monthly
        l4_billing_before_top_up_balance
        """
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
        l1_billing_and_payments_daily_before_top_up_balance = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_before_top_up_balance'))
        # daily_data.orderBy('event_partition_date').show(300,False)
        assert \
            int(l1_billing_and_payments_daily_before_top_up_balance.where("event_partition_date = '2020-01-01'").select(
                "payments_before_top_up_balance").collect()[0][0]) == 333
        l2_billing_and_payments_weekly_before_top_up_balance = node_from_config(
            l1_billing_and_payments_daily_before_top_up_balance, var_project_context.catalog.load(
                'params:l2_billing_and_payment_before_top_up_balance_weekly'))
        # weekly_data.orderBy('start_of_week').show()
        assert \
            int(l2_billing_and_payments_weekly_before_top_up_balance.where("start_of_week='2020-01-06'").select(
                "payments_before_top_up_balance").collect()[0][
                    0]) == 557
        l3_billing_and_payments_monthly_before_top_up_balance = node_from_config(
            l1_billing_and_payments_daily_before_top_up_balance, var_project_context.catalog.load(
                'params:l3_billing_and_payment_before_top_up_balance_monthly'))
        # monthly_data.show(100,False)
        assert \
            int(l3_billing_and_payments_monthly_before_top_up_balance.where("start_of_month='2020-02-01'").select(
                "payments_before_top_up_balance").collect()[0][
                    0]) == 560

        l4_billing_rolling_window_before_top_up_balance = l4_rolling_window(
            l2_billing_and_payments_weekly_before_top_up_balance, var_project_context.catalog.load(
                'params:l4_billing_before_top_up_balance')).orderBy(F.col("start_of_week").desc())
        print('final')
        l4_billing_rolling_window_before_top_up_balance.orderBy('start_of_week').show()
        assert \
            int(l4_billing_rolling_window_before_top_up_balance.where("start_of_week='2020-03-23'").select(
                "sum_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 6395
        assert \
            int(l4_billing_rolling_window_before_top_up_balance.where("start_of_week='2020-03-23'").select(
                "avg_payments_before_top_up_balance_weekly_last_twelve_week").collect()[0][0]) == 532

    def test_top_up_channel_feature(self, project_context):
        """
        l1_billing_and_payment_top_up_channels
        l2_billing_and_payment_top_up_channels_weekly
        l3_billing_and_payment_top_up_channels_monthly
        l4_billing_top_up_channels
        """
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

        l1_billing_and_payments_daily_top_up_channels = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_top_up_channels'))

        l1_billing_and_payments_daily_top_up_channels.orderBy('event_partition_date').show(300, False)

        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-02-01'").select(
                "payments_top_ups_by_bank_atm_cdm").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_cash_card").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_cash_card").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_epin_slip").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_epin_slip").collect()[0][0]) == 0
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_epos").collect()[0][0]) == 1
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_epos").collect()[0][0]) == 100
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_ups_by_rom").collect()[0][0]) == 2
        assert \
            int(l1_billing_and_payments_daily_top_up_channels.where("event_partition_date='2020-03-23'").select(
                "payments_top_up_vol_by_rom").collect()[0][0]) == 1300

        l2_billing_and_payments_weekly_top_up_channels = node_from_config(l1_billing_and_payments_daily_top_up_channels,
                                                                          var_project_context.catalog.load(
                                                                              'params:l2_billing_and_payment_top_up_channels_weekly'))
        # weekly_data.orderBy('start_of_week').show()

        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_bank_atm_cdm").collect()[0][0]) == 1
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][0]) == 900
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_cash_card").collect()[0][0]) == 3
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_cash_card").collect()[0][0]) == 1000
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][0]) == 7
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][0]) == 3800
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_epin_slip").collect()[0][0]) == 2
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_epin_slip").collect()[0][0]) == 1700
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_epos").collect()[0][0]) == 5
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_epos").collect()[0][0]) == 3500
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_by_rom").collect()[0][0]) == 3
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_by_rom").collect()[0][0]) == 1700
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_bank_atm_cdm").collect()[0][0]) == int(1 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][0]) == int(900 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_cash_card").collect()[0][0]) == int(3 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_cash_card").collect()[0][0]) == int(1000 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_digital_online_self_service").collect()[0][0]) == int(7 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][0]) == int(3800 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epin_slip").collect()[0][0]) == int(2 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epin_slip").collect()[0][0]) == int(1700 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_epos").collect()[0][0]) == int(5 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_epos").collect()[0][0]) == int(3500 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_ups_avg_by_rom").collect()[0][0]) == int(3 / 7)
        assert \
            int(l2_billing_and_payments_weekly_top_up_channels.where("start_of_week='2020-01-06'").select(
                "payments_top_up_vol_avg_by_rom").collect()[0][0]) == int(1700 / 7)

        l3_billing_and_payments_monthly_top_up_channels = node_from_config(
            l1_billing_and_payments_daily_top_up_channels, var_project_context.catalog.load(
                'params:l3_billing_and_payment_top_up_channels_monthly'))
        # monthly_data.show(100, False)

        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_bank_atm_cdm").collect()[
                    0][
                    0]) == 18
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_bank_atm_cdm").collect()[0][
                    0]) == 8800
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_cash_card").collect()[0][
                    0]) == 18
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_cash_card").collect()[
                    0][
                    0]) == 11300
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_digital_online_self_service").collect()[0][
                    0]) == 14
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_digital_online_self_service").collect()[0][
                    0]) == 6300
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_epin_slip").collect()[0][
                    0]) == 4
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_epin_slip").collect()[
                    0][
                    0]) == 2000
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_epos").collect()[0][
                    0]) == 20
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_epos").collect()[0][
                    0]) == 9600
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_by_rom").collect()[0][
                    0]) == 13
        assert \
            int(l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_by_rom").collect()[0][
                    0]) == 7500
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_bank_atm_cdm").collect()[
                0][
                0]) == (18 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_bank_atm_cdm").collect()[0][
                0]) == (8800 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_cash_card").collect()[0][
                0]) == (18 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_cash_card").collect()[
                0][
                0]) == (11300 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_digital_online_self_service").collect()[0][
                0]) == (14 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_digital_online_self_service").collect()[0][
                0]) == (6300 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_epin_slip").collect()[0][
                0]) == (4 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_epin_slip").collect()[
                0][
                0]) == (2000 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_epos").collect()[0][
                0]) == (20 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_epos").collect()[0][
                0]) == (9600 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_ups_avg_by_rom").collect()[0][
                0]) == (13 / 29)
        assert \
            (l3_billing_and_payments_monthly_top_up_channels.where("start_of_month='2020-02-01'").select(
                "payments_top_up_vol_avg_by_rom").collect()[0][
                0]) == (7500 / 29)

        l4_billing_rolling_window_top_up_channels = l4_rolling_window(l2_billing_and_payments_weekly_top_up_channels,
                                                                      var_project_context.catalog.load(
                                                                          'params:l4_billing_top_up_channels')).orderBy(
            F.col("start_of_week").desc())

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_bank_atm_cdm_weekly_last_twelve_week").collect()[0][
                0]) == 20

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_cash_card_weekly_last_twelve_week").collect()[0][
                0]) == 16
        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_digital_online_self_service_weekly_last_twelve_week").collect()[0][
                0]) == 22

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_epin_slip_weekly_last_twelve_week").collect()[0][
                0]) == 5

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_epos_weekly_last_twelve_week").collect()[0][
                0]) == 20

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_ups_by_rom_weekly_last_twelve_week").collect()[0][
                0]) == 16

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_bank_atm_cdm_weekly_last_twelve_week").collect()[0][
                0]) == 12400

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_cash_card_weekly_last_twelve_week").collect()[0][
                0]) == 9300

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_digital_online_self_service_weekly_last_twelve_week").collect()[0][
                0]) == 10600

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_epin_slip_weekly_last_twelve_week").collect()[0][
                0]) == 3400

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_epos_weekly_last_twelve_week").collect()[0][
                0]) == 10700

        assert \
            (l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "sum_payments_top_up_vol_by_rom_weekly_last_twelve_week").collect()[0][
                0]) == 10300

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_bank_atm_cdm_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.651428571 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_cash_card_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.468571429 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_digital_online_self_service_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.662857143 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_epin_slip_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.142857143 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_epos_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.582857143 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_ups_avg_by_rom_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(0.491428571 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_bank_atm_cdm_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(390.8571429 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_cash_card_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(277.1428571 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_digital_online_self_service_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(322.2857143 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_epin_slip_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(97.14285714 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_epos_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(312.5714286 * 1000000)

        assert \
            int(l4_billing_rolling_window_top_up_channels.where("start_of_week='2020-02-03'").select(
                "avg_payments_top_up_vol_avg_by_rom_weekly_last_twelve_week").collect()[0][
                    0] * 1000000) == int(318.2857143 * 1000000)

        print('finalfeature')
        l4_billing_rolling_window_top_up_channels.orderBy('start_of_week').show()
        l4_billing_rolling_window_top_up_channels.printSchema()
        # exit(2)

    def test_popular_topup_day(self, project_context):
        # kedro test D:\save\test\project-samudra\src\tests\test_unit_tests\test_unit_billing.py::TestUnitBilling::test_popular_topup_day
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        random_type = ['4', 'B1', 'B58', '3', 'B0', '7', '16', 'B43', '1', '5', '53', 'B69', '51', 'B50']
        date1 = '2020-01-01'
        date2 = '2020-04-01'
        random.seed(100)
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random_list = [random_type[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        df_pre = spark.createDataFrame(zip(random_list, my_dates, random_list2),
                                       schema=['recharge_type', 'temp', 'face_value']) \
            .withColumn("subscription_identifier", F.lit("MS05RTRSLTIwMw==")) \
            .withColumn("partition_date", F.to_date(F.lit('20190101'), 'yyyyMMdd')) \
            .withColumn("subscription_id", F.lit("MS05RTRSLTIwMw==")) \
            .withColumn("mobile_no", F.lit(
            "eFBIRjk3V0s0bit3ZWdFYU9oVzYxcmx3bmZSQWFYdUFHTW1ucHhxaElhS0JoMTl6TzZTeUI5STk5cHJPSDFtRg==")) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("zipcode", F.lit("MTAxMjA=")) \
            .withColumn("prefer_language", F.lit("RU5HTElTSA==")) \
            .withColumn("company_size", F.lit("Rw==")) \
            .withColumn("rsme_flag", F.lit("Rw==")) \
            .withColumn("corp_account_size", F.lit("Rw==")) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("cust_type", F.lit('B')) \
            .withColumn("prefer_language", F.lit('english')) \
            .withColumn("prefer_language_eng", F.lit('english')) \
            .withColumn("prefer_language_thai", F.lit('thai')) \
            .withColumn("prefer_language_other", F.lit('thai')) \
            .withColumn("package_id", F.lit("M0c5NTQ=")) \
            .withColumn("current_package_id", F.lit("M0c5NTQ=")) \
            .withColumn("current_promotion_id_ma", F.lit("M0c5NTQ=")) \
            .withColumn("promotion_name", F.lit("RWFzeU5ldDY0ayBBbGxOVyAxQm5leHQxU3QgLSBTdG9wTmV0")) \
            .withColumn("current_package_name", F.lit("RWFzeU5ldDY0ayBBbGxOVyAxQm5leHQxU3QgLSBTdG9wTmV0")) \
            .withColumn("current_promotion_title_ma", F.lit("RWFzeU5ldDY0ayBBbGxOVyAxQm5leHQxU3QgLSBTdG9wTmV0")) \
            .withColumn("age", F.lit(23)) \
            .withColumn("ma_age", F.lit(23)) \
            .withColumn("gender", F.lit("TQ==")) \
            .withColumn("ma_gender_code", F.lit("TQ==")) \
            .withColumn("partition_date", F.lit('20200214')) \
            .withColumn("subscriber_tenure_day", F.lit('20200214')) \
            .withColumn("service_month", F.lit(203)) \
            .withColumn("subscriber_tenure_month", F.lit(203)) \
            .withColumn("subscription_status", F.lit("U0E=")) \
            .withColumn("mobile_status", F.lit("U0E=")) \
            .withColumn("mobile_segment", F.lit("Q2xhc3NpYw==")) \
            .withColumn("customer_segment", F.lit("Q2xhc3NpYw==")) \
            .withColumn("serenade_status", F.lit("Q2xhc3NpYw==")) \
            .withColumn("root_network_type", F.lit("M0c=")) \
            .withColumn("network_type", F.lit("M0c=")) \
            .withColumn("national_id_card", F.lit(
            "Y2pHOFUxUlk3Nmd0WVpBTjdONWR0K1h5T085OGRrNnZ5ZDV6WGNaZGxiTlorUnVqUVVmVmhxeFNpRU5obFZBYQ==")) \
            .withColumn("card_id", F.lit(
            "Y2pHOFUxUlk3Nmd0WVpBTjdONWR0K1h5T085OGRrNnZ5ZDV6WGNaZGxiTlorUnVqUVVmVmhxeFNpRU5obFZBYQ==")) \
            .withColumn("charge_type", F.lit('Pre-paid')) \
            .withColumn("Post-paid", F.lit('Pre-paid')) \
            .withColumn("billing_account_no", F.lit("null")) \
            .withColumn("card_no", F.lit(
            "Y2pHOFUxUlk3Nmd0WVpBTjdONWR0K1h5T085OGRrNnZ5ZDV6WGNaZGxiTlorUnVqUVVmVmhxeFNpRU5obFZBYQ==")) \
            .withColumn("account_no", F.lit(
            "Y2pHOFUxUlk3Nmd0WVpBTjdONWR0K1h5T085OGRrNnZ5ZDV6WGNaZGxiTlorUnVqUVVmVmhxeFNpRU5obFZBYQ=="))

        union_data = union_daily_cust_profile(df_pre, df_pre, df_pre, var_project_context.catalog.load(
            'params:l1_customer_profile_union_daily_feature'))

        random_type = ['4', 'B1', 'B58', '3', 'B0', '7', '16', 'B43', '1', '5', '53', 'B69', '51', 'B50']
        date1 = '2019-01-01'
        date2 = '2019-04-01'

        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        random_list = [random_type[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        random.seed(100)
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]
        df_rt = spark.createDataFrame(zip(random_list, my_dates, random_list2),
                                      schema=['recharge_type', 'temp', 'face_value']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("recharge_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("recharge_time", F.lit('2019-08-01T11:25:55.000+0000')) \
            .withColumn("subscription_identifier", F.lit(123))
        df_rt = df_rt.withColumn("start_of_month", F.to_date(F.date_trunc('month', df_rt.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df_rt.event_partition_date)))

        # popular_topup_day = billing_popular_topup_day_hour(df_rt,union_data,var_project_context.catalog.load('params:l1_billing_and_payment_popular_topup_day'))
        popular_topup_day = node_from_config(df_rt, var_project_context.catalog.load(
            'params:l1_billing_and_payment_popular_topup_day'))

        assert \
            (popular_topup_day.where("start_of_month='2019-01-01'").select("access_method_num").collect()[0][
                0]) == 1
        assert \
            (popular_topup_day.where("start_of_month='2019-01-01'").select("register_date").collect()[0][
                0]) == datetime.strptime('2019-1-1', "%Y-%m-%d").date()
        assert \
            (popular_topup_day.where("start_of_month='2019-01-01'").select("subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (popular_topup_day.where("start_of_month='2019-01-01'").select("payment_popular_day").collect()[0][
                0]) == 3
        assert \
            (popular_topup_day.where("start_of_month='2019-01-01'").select("payment_popular_hour").collect()[0][
                0]) == 11
        # l2
        weekly_popular_topup_day1 = node_from_config(popular_topup_day,
                                                     var_project_context.catalog.load('params:l2_popular_topup_day_1'))

        assert \
            (weekly_popular_topup_day1.where("start_of_week='2018-12-31'").select(
                "payment_popular_day_topup_count").collect()[0][
                0]) == 18  # 6*3
        assert \
            (weekly_popular_topup_day1.where("start_of_week='2018-12-31'").select(
                "rank").collect()[0][
                0]) == 1

        weekly_popular_topup_day2 = node_from_config(weekly_popular_topup_day1,
                                                     var_project_context.catalog.load('params:l2_popular_topup_day_2'))

        assert \
            (weekly_popular_topup_day2.where("start_of_week='2018-12-31'").select(
                "access_method_num").collect()[0][
                0]) == 1
        assert \
            (weekly_popular_topup_day2.where("start_of_week='2018-12-31'").select(
                "register_date").collect()[0][
                0]) == datetime.strptime('2019-1-1', "%Y-%m-%d").date()
        assert \
            (weekly_popular_topup_day2.where("start_of_week='2018-12-31'").select(
                "subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (weekly_popular_topup_day2.where("start_of_week='2019-02-18'").select(
                "payment_popular_day").collect()[0][
                0]) == 3

        weekly_topup_hour_1 = node_from_config(popular_topup_day,
                                               var_project_context.catalog.load('params:l2_popular_topup_hour_1'))

        assert \
            (weekly_topup_hour_1.where("start_of_week='2018-12-31'").select(
                "payment_popular_hour_topup_count").collect()[0][
                0]) == 18  # 6*3
        assert \
            (weekly_topup_hour_1.where("start_of_week='2018-12-31'").select(
                "rank").collect()[0][
                0]) == 1
        weekly_topup_hour_2 = node_from_config(weekly_topup_hour_1,
                                               var_project_context.catalog.load('params:l2_popular_topup_hour_2'))

        assert \
            (weekly_topup_hour_2.where("start_of_week='2018-12-31'").select(
                "access_method_num").collect()[0][
                0]) == 1
        assert \
            (weekly_topup_hour_2.where("start_of_week='2018-12-31'").select(
                "register_date").collect()[0][
                0]) == datetime.strptime('2019-1-1', "%Y-%m-%d").date()
        assert \
            (weekly_topup_hour_2.where("start_of_week='2018-12-31'").select(
                "subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (weekly_topup_hour_2.where("start_of_week='2018-12-31'").select(
                "payment_popular_hour").collect()[0][
                0]) == 11
        # l3
        popular_topup_day_ranked = node_from_config(popular_topup_day, var_project_context.catalog.load(
            'params:l3_popular_topup_day_ranked'))

        assert \
            (popular_topup_day_ranked.where("start_of_month='2019-01-01'").select(
                "payment_popular_day_topup_count").collect()[0][
                0]) == 93  # 31*3
        assert \
            (popular_topup_day_ranked.where("start_of_month='2019-01-01'").select(
                "rank").collect()[0][
                0]) == 1

        monthly_popular_topup_day = node_from_config(popular_topup_day_ranked, var_project_context.catalog.load(
            'params:l3_popular_topup_day'))

        assert \
            (monthly_popular_topup_day.where("start_of_month='2019-01-01'").select(
                "access_method_num").collect()[0][
                0]) == 1
        assert \
            (monthly_popular_topup_day.where("start_of_month='2019-01-01'").select(
                "register_date").collect()[0][
                0]) == datetime.strptime('2019-1-1', "%Y-%m-%d").date()
        assert \
            (monthly_popular_topup_day.where("start_of_month='2019-01-01'").select(
                "subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (monthly_popular_topup_day.where("start_of_month='2019-01-01'").select(
                "payment_popular_day").collect()[0][
                0]) == 3

        popular_topup_hour_ranked = node_from_config(popular_topup_day, var_project_context.catalog.load(
            'params:l3_popular_topup_hour_ranked'))

        assert \
            (popular_topup_hour_ranked.where("start_of_month='2019-01-01'").select(
                "payment_popular_hour_topup_count").collect()[0][
                0]) == 93  # 31*3
        assert \
            (popular_topup_hour_ranked.where("start_of_month='2019-01-01'").select(
                "rank").collect()[0][
                0]) == 1

        monthly_popular_topup_hour = node_from_config(popular_topup_hour_ranked,
                                                      var_project_context.catalog.load(
                                                          'params:l3_popular_topup_hour'))

        assert \
            (monthly_popular_topup_hour.where("start_of_month='2019-01-01'").select(
                "access_method_num").collect()[0][
                0]) == 1
        assert \
            (monthly_popular_topup_hour.where("start_of_month='2019-01-01'").select(
                "register_date").collect()[0][
                0]) == datetime.strptime('2019-1-1', "%Y-%m-%d").date()
        assert \
            (monthly_popular_topup_hour.where("start_of_month='2019-01-01'").select(
                "subscription_identifier").collect()[0][
                0]) == 123
        assert \
            (monthly_popular_topup_hour.where("start_of_month='2019-01-01'").select(
                "payment_popular_hour").collect()[0][
                0]) == 11
        # l4
        popular_topup_day_1 = l4_rolling_window(weekly_popular_topup_day1,
                                                var_project_context.catalog.load(
                                                    'params:l4_popular_topup_day_initial'))

        assert \
            (popular_topup_day_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_day_topup_count_weekly_last_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_day_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_day_topup_count_weekly_last_two_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_day_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_day_topup_count_weekly_last_four_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_day_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_day_topup_count_weekly_last_twelve_week").collect()[0][
                0]) == 18

        window_popular_topup_day = l4_rolling_ranked_window(popular_topup_day_1,
                                                            var_project_context.catalog.load(
                                                                'params:l4_popular_topup_day'))

        assert \
            (window_popular_topup_day.where("start_of_week='2019-01-07'").select(
                "payment_popular_topup_day_last_week").collect()[0][
                0]) == 3
        assert \
            (window_popular_topup_day.where("start_of_week='2019-01-07'").select(
                "payment_popular_topup_day_last_two_week").collect()[0][
                0]) == 3
        assert \
            (window_popular_topup_day.where("start_of_week='2019-01-07'").select(
                "payment_popular_topup_day_last_four_week").collect()[0][
                0]) == 3
        assert \
            (window_popular_topup_day.where("start_of_week='2019-01-07'").select(
                "payment_popular_topup_day_last_twelve_week").collect()[0][
                0]) == 3

        popular_topup_hour_1 = l4_rolling_window(weekly_topup_hour_1, var_project_context.catalog.load(
            'params:l4_popular_topup_hour_initial'))

        assert \
            (popular_topup_hour_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_hour_topup_count_weekly_last_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_hour_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_hour_topup_count_weekly_last_two_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_hour_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_hour_topup_count_weekly_last_four_week").collect()[0][
                0]) == 18
        assert \
            (popular_topup_hour_1.where("start_of_week='2019-01-07'").select(
                "sum_payment_popular_hour_topup_count_weekly_last_twelve_week").collect()[0][
                0]) == 18

        window_popular_topup_hour = l4_rolling_ranked_window(popular_topup_hour_1, var_project_context.catalog.load(
            'params:l4_popular_topup_hour'))

        assert \
            (window_popular_topup_hour.where("start_of_week='2019-01-07'").select(
                "payment_popular_hour_last_week").collect()[0][
                0]) == 11
        assert \
            (window_popular_topup_hour.where("start_of_week='2019-01-07'").select(
                "payment_popular_hour_last_two_week").collect()[0][
                0]) == 11
        assert \
            (window_popular_topup_hour.where("start_of_week='2019-01-07'").select(
                "payment_popular_hour_last_four_week").collect()[0][
                0]) == 11
        assert \
            (window_popular_topup_hour.where("start_of_week='2019-01-07'").select(
                "payment_popular_hour_last_twelve_week").collect()[0][
                0]) == 11

    def test_time_diff_bw_topups(self, project_context):
        #kedro test D:\save\test\project-samudra\src\tests\test_unit_tests\test_unit_billing.py::TestUnitBilling::test_time_diff_bw_topups
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        random_type = ['4', 'B1', 'B58', '3', 'B0', '7', '16', 'B43', '1', '5', '53', 'B69', '51', 'B50']
        date1 = '2019-01-01'
        date2 = '2019-04-01'

        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        random_list = [random_type[random.randint(0, 13)] for iTemp in range(0, len(my_dates))]
        random.seed(100)
        random_list2 = [random.randint(1, 10) * 100 for iTemp in range(0, len(my_dates))]

        start_time = datetime.strptime("01/01/2020 08:35:55", '%d/%m/%Y %H:%M:%S')
        start_time_list = []
        for i in range(len(my_dates)):
            start_time_list.append(start_time)
            start_time = start_time + timedelta(seconds=random.randint(1, 432000))

        recharge_date_list = [d.strftime('%Y-%m-%d') for d in start_time_list]

        df_rt = spark.createDataFrame(zip(random_list, my_dates, random_list2,start_time_list,recharge_date_list),
                                      schema=['recharge_type', 'temp', 'face_value','recharge_time','recharge_date']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("charge_type", F.lit('Pre-paid')) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("subscription_identifier", F.lit(123))
        df_rt = df_rt.withColumn("start_of_month", F.to_date(F.date_trunc('month', df_rt.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df_rt.event_partition_date)))
        topup_diff_time_intermediate = node_from_config(df_rt,var_project_context.catalog.load(
                'params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly_intermdeiate'))

        # access_method_num: "access_method_num"
        # register_date: "register_date"
        # payments_time_diff: "datediff(recharge_time,lag(recharge_time,1) over(partition by date_trunc('month',date(recharge_date))

        assert \
            (topup_diff_time_intermediate.where("event_partition_date='2021-05-05'").select(
                "payments_time_diff").collect()[0][
                 0]) == 4
        assert \
            (topup_diff_time_intermediate.where("event_partition_date='2021-05-05'").select(
                "access_method_num").collect()[0][
                 0]) == 1

        # a = node_from_config(df_rt, var_project_context.catalog.load(
        #     'params:l3_billing_and_payments_monthly_topup_time_diff'))
        topup_diff_time_intermediate = topup_diff_time_intermediate.withColumn("access_method_num", F.lit(1)) \
            .withColumn("charge_type", F.lit('Pre-paid')) \
            .withColumn("subscription_identifier", F.lit(123)) \
            .withColumn("event_partition_date",  F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd'))
        topup_diff_time_intermediate.show(273,False)
        diff_bw_topups_monthly = node_from_config(topup_diff_time_intermediate,var_project_context.catalog.load(
                'params:l3_billing_and_payment_feature_time_diff_bw_topups_monthly'))
        # payments_max_time_diff: "max(payments_time_diff)"
        # payments_min_time_diff: "min(payments_time_diff)"
        # payments_time_diff: "sum(payments_time_diff)"
        # payments_time_diff_avg: "avg(payments_time_diff)"

        # null
        # 0
        # 2
        # 3
        # 4
        # 0
        # 5
        # 0
        # 2
        # 1
        # 4
        # 1
        # 5
        # min = 0
        # max = 5
        # sum = 27
        # avg = 2.25

        assert \
            (diff_bw_topups_monthly.where("start_of_month='2020-09-01'").select(
                "payments_max_time_diff").collect()[0][
                 0]) == 5
        assert \
            (diff_bw_topups_monthly.where("start_of_month='2020-09-01'").select(
                "payments_min_time_diff").collect()[0][
                 0]) == 0
        assert \
            (diff_bw_topups_monthly.where("start_of_month='2020-09-01'").select(
                "payments_time_diff").collect()[0][
                 0]) == 27
        assert \
            (diff_bw_topups_monthly.where("start_of_month='2020-09-01'").select(
                "payments_time_diff_avg").collect()[0][
                 0]) == 2.25
        # exit(5)