from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
import pandas as pd
import random
from pyspark.sql import functions as F


class TestUnitBilling:

    def test_usage_feature(self, project_context):
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

        df = spark.createDataFrame(zip(random_list, my_dates), schema=['face_value', 'recharge_date']) \
            .withColumn("access_method_num", F.lit(1)) \
            .withColumn("recharge_date", F.to_date('recharge_date', 'dd-MM-yyyy')) \
            .withColumn("register_date", F.lit('2019-01-01')) \


        # df.orderBy('recharge_date').show()

        daily_data = node_from_config(df, var_project_context.catalog.load(
            'params:l1_billing_and_payment_feature_top_up_and_count'))

        # daily_data.orderBy('recharge_date').show()
        # daily_data.printSchema()

        assert \
            daily_data.where("recharge_date = '2020-01-01'").select("payments_top_up_volume").collect()[0][
                0] == 1000

        # exit(2)
