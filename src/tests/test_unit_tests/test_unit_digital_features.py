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
        browser_type = ['chrome','safari','line','opera','ie']
        reference_type = ['facebook','other','direct','internal','social']
        activetime_type =['','34']
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
        # random_list = [random_group[random.randint(0, 3)] for iTemp in range(0, len(my_dates))]
        # type_list = [random_type[random.randint(0, 2)] for iTemp in range(0, len(my_dates))]
        browser_list = [browser_type[random.randint(0, 4)] for iTemp in range(0, len(my_dates))]
        ref_list = [reference_type[random.randint(0, 4)] for iTemp in range(0, len(my_dates))]
        activetime_list =[activetime_type[random.randint(0, 1)] for iTemp in range(0, len(my_dates))]
        df0 = spark.createDataFrame(zip(browser_list,ref_list,my_dates,activetime_list),
                                      schema=['browser','referrerhostclass','temp','activetime']) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("subscription_identifier", F.lit(1)) \
            .withColumn("referrersocialnetwork", F.lit('facebook')) \
            .withColumn("host", F.lit('callingmelody.net')) \
            .withColumn("postalcode", F.lit('10120')) \
            .withColumn("referrerquery", F.lit('callingmelody.net')) \
            .withColumn("referrerhost", F.lit('callingmelody.net')) \
            .withColumn("access_method_num", F.lit(1)) 
	
        df0  = df0.withColumn("start_of_week", F.to_date(F.date_trunc('week', df0.event_partition_date))) 
  
        df_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_weekly'))

        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_browser_chrome").collect()[0][
                0] == 68
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_browser_safari").collect()[0][
                0] == 136
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_browser_line").collect()[0][
                0] == 68
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_browser_opera").collect()[0][
                0] == 0
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_referrersocialnetwork_facebook").collect()[0][
                0] == 272
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_referrerhostclass_other").collect()[0][
                0] == 0
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_referrerhostclass_direct").collect()[0][
                0] == 68
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_referrerhostclass_internal").collect()[0][
                0] == 136
        assert \
            df_weekly.where("start_of_week='2019-03-11'").select("digital_time_spent_referrerhostclass_social").collect()[0][
                0] == 0

        df_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_host_weekly'))
        assert \
            df_host_weekly.where("start_of_week='2019-03-11'").select("digital_popular_host").collect()[0][
                0] == 272

        df_postcode_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_postalcode_weekly'))
        assert \
            df_postcode_weekly.where("start_of_week='2019-03-11'").select("digital_popular_postalcode").collect()[0][
                0] == 21

        df_ref_q_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerquery_weekly'))
        assert \
            df_ref_q_weekly.where("start_of_week='2019-03-11'").select("digital_popular_referrerquery").collect()[0][
                0] == 21
        df_ref_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerhost_weekly'))
        assert \
            df_ref_host_weekly.where("start_of_week='2019-03-11'").select("digital_popular_referrerhost").collect()[0][
                0] == 21
        
        random_group = ['device-brand','ais-app-mpay','ais-app-myais','ais-serenade-flag']
        random_type = ['mobile','tablet','desktop']
        random_list = [random_group[random.randint(0, 3)] for iTemp in range(0, len(my_dates))]
        type_list = [random_type[random.randint(0, 2)] for iTemp in range(0, len(my_dates))]
        item_list =[]
        for i in range (0, len(random_list)):
            if(random_list[i] != 'device-brand'):
                item_list.append(random.choice(['y', 'n']))
            else:
                item_list.append(random.choice(['apple','samsung','huawei','oppo','ais','dtac','true']))

        df3 = spark.createDataFrame(zip(type_list,item_list, random_list,my_dates),
                                      schema=['device_type','item', 'groups','temp']) \
            .withColumn("event_partition_date", F.to_date('temp', 'dd-MM-yyyy')) \
            .withColumn("subscription_identifier", F.lit(1)) \
            .withColumn("access_method_num", F.lit(1)) 
        df3  = df3.withColumn("start_of_month", F.to_date(F.date_trunc('month', df3.event_partition_date))) 

        df_monthly = node_from_config(df3,var_project_context.catalog.load(
            'params:l3_digital_cxenxse_user_profile_monthly'))

        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_nummber_of_device_brands").collect()[0][
                0] == 27
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_apple").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_samsung").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_huwaei").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_oppo").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_ais").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_dtac").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_brands_true").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_nummber_of_distinct_device_type").collect()[0][
                0] == 3
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_mobile").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_tablet").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_device_desktop").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_ais_mpay_used").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_my_ais_app_used").collect()[0][
                0] == 1
        assert \
            df_monthly.where("start_of_month='2019-03-01'").select("digital_is_serenade").collect()[0][
                0] == 1
