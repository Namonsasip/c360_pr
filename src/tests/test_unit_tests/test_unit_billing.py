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
            .withColumn("recharge_date",F.to_date('temp','dd-MM-yyyy'))\
            .withColumn("event_partition_date", F.to_date('recharge_date', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'),'yyyy-MM-dd') )\
            .withColumn("subscription_identifier",F.lit(123))
        df = df.withColumn("start_of_month",F.to_date(F.date_trunc('month', df.event_partition_date))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', df.event_partition_date)))

        print('test1234')
        df.show()
        df.printSchema()
        print('test3333')
        print('L11111111111111111111111111111')
        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))
        print('L11111111111111111111111111111end')
        daily_data.orderBy('event_partition_date').show()
        daily_data.printSchema()

        weekly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l2_billing_and_payment_feature_top_up_and_count_weekly'))
        weekly_data.orderBy('start_of_week').show()
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_top_up_volume").collect()[0][
                0] == 1000
        assert \
            daily_data.where("event_partition_date = '2020-01-01'").select("payments_top_ups").collect()[0][
                0] == 3



        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_ups").collect()[0][0] == 21
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_up_volume").collect()[0][0] == 12000
        assert \
            weekly_data.where("start_of_week='2020-01-06'").select("payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(weekly_data.where("start_of_week='2020-01-06'").select("payments_top_up_volume_avg").collect()[0][
                    0]) == 1714

        monthly_data = node_from_config(daily_data, var_project_context.catalog.load(
            'params:l3_billing_and_payment_feature_top_up_and_count_monthly'))
        print('monthlydatadebug')
        monthly_data.show()
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups").collect()[0][0]==87
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_volume").collect()[0][0] == 48700
        assert \
            monthly_data.where("start_of_month='2020-02-01'").select("payments_top_ups_avg").collect()[0][0] == 3
        assert \
            int(monthly_data.where("start_of_month='2020-02-01'").select("payments_top_up_volume_avg").collect()[0][0]) == 1679

        final_features = l4_rolling_window(weekly_data, var_project_context.catalog.load(
            'params:l4_billing_topup_and_volume')).orderBy(F.col("start_of_week").desc())

        print('finalfeatures')
        final_features.orderBy('start_of_week').show()
        assert \
            final_features.where("start_of_week='2020-03-23'").select("sum_payments_top_ups_weekly_last_twelve_week").collect()[0][0]==249
        assert \
            final_features.where("start_of_week='2020-03-23'").select("sum_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0]==132200
        assert \
            final_features.where("start_of_week='2020-03-23'").select("avg_payments_top_ups_weekly_last_twelve_week").collect()[0][0]==20.75
        assert \
            int(final_features.where("start_of_week='2020-03-23'").select("avg_payments_top_up_volume_weekly_last_twelve_week").collect()[0][0])==11016
        # sum: ["payments_top_ups", "payments_top_up_volume"]
        # avg: ["payments_top_ups", "payments_top_up_volume"]
        # exit(2)