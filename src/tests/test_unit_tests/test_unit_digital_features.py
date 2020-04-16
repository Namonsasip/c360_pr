from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.re_usable_functions import l2_massive_processing_with_expansion
import pandas as pd
import random
from pyspark.sql import functions as F
from datetime import timedelta, datetime


class TestUnitDigital:

    def test_digital_cxenxse_user_profile(self,project_context):

        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        random_group = ['device-brand','ais-app-mpay','ais-app-myais','ais-serenade-flag']
        random_type = ['mobile','tablet','desktop']
        msisdn_type = ['ank5bkxpOWxOZStrY2FFaVpzeEd6UXl4MDNkanZnMlR0dGdjaGRyUERDUmI0aEx2S21QMVV3eHVhTGdUS2MxNA==','']
        date1 = '2019-01-01'
        date2 = '2019-04-01'
        # group_list =[]
        # brand_list =[]
        # # for i in range (0, len(my_dates)):
        
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        random_list = [random_group[random.randint(0, 3)] for iTemp in range(0, len(my_dates))]
        type_list = [random_type[random.randint(0, 2)] for iTemp in range(0, len(my_dates))]


        item_list =[]
        for i in range (0, len(random_list)):
            if(random_list[i] != 'device-brand'):
                item_list.append(random.choice(['y', 'n']))
            else:
                item_list.append(random.choice(['apple','samsung','huawei','oppo','ais','dtac','true']))

        df0 = spark.createDataFrame(zip(type_list,item_list, random_list,my_dates),
                                      schema=['device_type','item', 'groups','temp']) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("access_method_num", F.lit(1)) 

        df0  = df0.withColumn("start_of_month", F.to_date(F.date_trunc('month', df0.event_partition_date))) 

        df = node_from_config(df0,var_project_context.catalog.load(
            'params:l3_digital_cxenxse_user_profile_monthly'))
  
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_nummber_of_device_brands").collect()[0][
                0] == 24
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_apple").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_samsung").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_huwaei").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_oppo").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_ais").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_dtac").collect()[0][
                0] == 1

        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_brands_true").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_nummber_of_distinct_device_type").collect()[0][
                0] == 3
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_mobile").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_tablet").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_device_desktop").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_ais_mpay_used").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_my_ais_app_used").collect()[0][
                0] == 1
        assert \
            df.where("start_of_month = '2019-01-01'").select("digital_is_serenade").collect()[0][
                0] == 1
        
        df = df.withColumn("subscription_identifier",F.lit(1)) 

        df4 = l4_rolling_window(df,var_project_context.catalog.load(
            'params:l4_digital_cxenzxse_user_profile_monthly_features'))
        
     
        # min_digital_nummber_of_device_brands_monthly_last_month
        # min_digital_nummber_of_device_brands_monthly_last_three_month
        # min_digital_nummber_of_distinct_device_type_monthly_last_month
        # min_digital_nummber_of_distinct_device_type_monthly_last_three_month
        assert \
            df4.where("start_of_month = '2019-01-03'").select("min_digital_nummber_of_device_brands_monthly_last_month").collect()[0][
                0] == 25
        assert \
            df4.where("start_of_month = '2019-01-01'").select("min_digital_nummber_of_device_brands_monthly_last_three_month").collect()[0][
                0] == 24
        assert \
            df4.where("start_of_month = '2019-01-01'").select("min_digital_nummber_of_distinct_device_type_monthly_last_month").collect()[0][
                0] == 3
        assert \
            df4.where("start_of_month = '2019-01-01'").select("min_digital_nummber_of_distinct_device_type_monthly_last_three_month").collect()[0][
                0] == 3
        # max_digital_nummber_of_device_brands_monthly_last_month
        # max_digital_nummber_of_device_brands_monthly_last_three_month
        # max_digital_is_device_brands_apple_monthly_last_month
        # max_digital_is_device_brands_apple_monthly_last_three_month
        # max_digital_is_device_brands_samsung_monthly_last_month
        # max_digital_is_device_brands_samsung_monthly_last_three_month
        # max_digital_is_device_brands_huwaei_monthly_last_month
        # max_digital_is_device_brands_huwaei_monthly_last_three_month
        # max_digital_is_device_brands_oppo_monthly_last_month
        # max_digital_is_device_brands_oppo_monthly_last_three_month
        # max_digital_nummber_of_distinct_device_type_monthly_last_month
        # max_digital_nummber_of_distinct_device_type_monthly_last_three_month
        # max_digital_is_device_mobile_monthly_last_month
        # max_digital_is_device_mobile_monthly_last_three_month
        # max_digital_is_device_tablet_monthly_last_month
        # max_digital_is_device_tablet_monthly_last_three_month
        # max_digital_is_device_desktop_monthly_last_month
        # max_digital_is_device_desktop_monthly_last_three_month
        # max_digital_is_ais_mpay_used_monthly_last_month
        # max_digital_is_ais_mpay_used_monthly_last_three_month
        # max_digital_is_my_ais_app_used_monthly_last_month
        # max_digital_is_my_ais_app_used_monthly_last_three_month
        # max_digital_is_serenade_monthly_last_month
        # max_digital_is_serenade_monthly_last_three_month
        # max_digital_is_device_brands_ais_monthly_last_month
        # max_digital_is_device_brands_ais_monthly_last_three_month
        # max_digital_is_device_brands_dtac_monthly_last_month
        # max_digital_is_device_brands_dtac_monthly_last_three_month
        # max_digital_is_device_brands_true_monthly_last_month
        # max_digital_is_device_brands_true_monthly_last_three_month
        # avg_digital_nummber_of_device_brands_monthly_last_month
        # avg_digital_nummber_of_device_brands_monthly_last_three_month
        # avg_digital_nummber_of_distinct_device_type_monthly_last_month
        # avg_digital_nummber_of_distinct_device_type_monthly_last_three_month
