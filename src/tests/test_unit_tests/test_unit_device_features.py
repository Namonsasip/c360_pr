from datetime import datetime
from kedro.pipeline import node
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window, l4_rolling_ranked_window
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l2.to_l2_nodes import *
from src.customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l1.to_l1_nodes import *
from src.customer360.pipelines.data_engineering.nodes.device_nodes.to_l3.to_l3_nodes import *
import pandas as pd
import random
from pyspark.sql import functions as F, SparkSession


class TestUnitDevice:

    def test_device_used(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        '''l2_device_most_used:
          where_clause: ""
          feature_list:
            device_used_days: "count(distinct date_id)"
            device_most_used_handset_samsung: "case when handset_brand_name='SAMSUNG' then 'Y' else 'N' end"
            device_most_used_handset_huawei: "case when handset_brand_name='HUAWEI' then 'Y' else 'N' end"
            device_most_used_handset_oppo: "case when handset_brand_name='OPPO' then 'Y' else 'N' end"
            device_most_used_handset_apple: "case when handset_brand_name='APPLE' then 'Y' else 'N' end"
            device_most_used_handset_xiaomi: "case when handset_brand_name='XIAOMI' then 'Y' else 'N' end"
            device_most_used_handset_others: "case when handset_brand_name not in ('SAMSUNG','HUAWEI','OPPO','APPLE','XIAOMI') then 'Y' else 'N' end"
            rank: "row_number() over(partition by start_of_month,start_of_week,mobile_no,register_date
                       order by count(distinct date_id) desc)"
          granularity: "start_of_month,start_of_week,mobile_no,register_date,handset_brand_name"'''

        date1 = '2020-01-01'
        date2 = '2020-06-01'

        device_list = ['SAMSUNG', 'HUAWEI', 'OPPO', 'APPLE', 'XIAOMI', 'other']

        dummy_date = pd.date_range(date1, date2).tolist()
        dummy_date = [iTemp.date().strftime("%Y-%m-%d") for iTemp in dummy_date]
        random.seed(100)
        random_list = [device_list[random.randint(0, 5)] for iTemp in range(0, len(dummy_date))]

        df = spark.createDataFrame(zip(dummy_date, random_list),
                                   schema=['date_id', 'handset_brand_name']) \
            .withColumn("mobile_no", F.lit(1)) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd'))
        df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.to_date('date_id', 'yyyy-MM-dd')))) \
            .withColumn("start_of_week", F.to_date(F.date_trunc('week', F.to_date('date_id', 'yyyy-MM-dd'))))
        print('rawdata')
        df.show(888, False)

        device_most_used_weekly = node_from_config(df,var_project_context.catalog.load('params:l2_device_most_used'))
        print('devicemostused')
        device_most_used_weekly.orderBy('start_of_week').show(999,False)


        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_used_days").collect()[0][
                0] == 2
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_most_used_handset_samsung").collect()[0][
                0] == 'N'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_most_used_handset_huawei").collect()[0][
                0] == 'N'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_most_used_handset_oppo").collect()[0][
                0] =='N'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="APPLE"').select(
                "device_most_used_handset_apple").collect()[0][
                0] =='Y'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_most_used_handset_xiaomi").collect()[0][
                0] =='Y'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
                "device_most_used_handset_others").collect()[0][
                0] =='N'
        assert \
            device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="APPLE"').select(
                "rank").collect()[0][
                0] ==2
        assert \
            device_most_used_weekly.where('handset_brand_name="OPPO"').select(
                "device_most_used_handset_oppo").collect()[0][
                0] == 'Y'
        # exit(2)


    def test_dummy(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
