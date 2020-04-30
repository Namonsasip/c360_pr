from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_ranked_window
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
        date1 = '2019-01-01'
        date2 = '2019-04-01'
        
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
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
        df_monthly = df_monthly.withColumn("subscription_identifier", F.lit(1)) 

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

    def test_l4_1(self,project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        browser_type = ['chrome','safari','line','opera','ie']
        reference_type = ['facebook','other','direct','internal','social']
        activetime_type =['','34']
        date1 = '2019-01-01'
        date2 = '2019-04-01'
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
        browser_list = [browser_type[random.randint(0, 4)] for iTemp in range(0, len(my_dates))]
        ref_list = [reference_type[random.randint(0, 4)] for iTemp in range(0, len(my_dates))]
        activetime_list =[activetime_type[random.randint(0, 1)] for iTemp in range(0, len(my_dates))]
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

        df_monthly = df_monthly.withColumn("subscription_identifier", F.lit(1)) 

        df41 = l4_rolling_window(df_monthly,var_project_context.catalog.load(
            'params:l4_digital_cxenzxse_user_profile_monthly_features'))
        # df41.show(df41.count(),False)
        # exit()
                #         subscription_identifier
                # start_of_month
                # min_digital_nummber_of_device_brands_monthly_last_month
                # min_digital_nummber_of_device_brands_monthly_last_three_month
                # min_digital_nummber_of_distinct_device_type_monthly_last_month
                # min_digital_nummber_of_distinct_device_type_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("min_digital_nummber_of_device_brands_monthly_last_month").collect()[0][
                0] == 27
        assert \
            df41.where("start_of_month='2019-04-01'").select("min_digital_nummber_of_device_brands_monthly_last_three_month").collect()[0][
                0] == 19
        assert \
            df41.where("start_of_month='2019-04-01'").select("min_digital_nummber_of_distinct_device_type_monthly_last_month").collect()[0][
                0] == 3
        assert \
            df41.where("start_of_month='2019-04-01'").select("min_digital_nummber_of_distinct_device_type_monthly_last_three_month").collect()[0][
                0] == 3
                # max_digital_nummber_of_device_brands_monthly_last_month
                # max_digital_nummber_of_device_brands_monthly_last_three_month
                # max_digital_is_device_brands_apple_monthly_last_month
                # max_digital_is_device_brands_apple_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_nummber_of_device_brands_monthly_last_month").collect()[0][
                0] == 27
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_nummber_of_device_brands_monthly_last_three_month").collect()[0][
                0] == 27
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_apple_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_apple_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_device_brands_samsung_monthly_last_month
                # max_digital_is_device_brands_samsung_monthly_last_three_month
                # max_digital_is_device_brands_huwaei_monthly_last_month
                # max_digital_is_device_brands_huwaei_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_samsung_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_samsung_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_huwaei_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_huwaei_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_device_brands_oppo_monthly_last_month
                # max_digital_is_device_brands_oppo_monthly_last_three_month
                # max_digital_nummber_of_distinct_device_type_monthly_last_month
                # max_digital_nummber_of_distinct_device_type_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_oppo_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_oppo_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_nummber_of_distinct_device_type_monthly_last_month").collect()[0][
                0] == 3
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_nummber_of_distinct_device_type_monthly_last_three_month").collect()[0][
                0] == 3
                # max_digital_is_device_mobile_monthly_last_month
                # max_digital_is_device_mobile_monthly_last_three_month
                # max_digital_is_device_tablet_monthly_last_month
                # max_digital_is_device_tablet_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_mobile_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_mobile_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_tablet_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_tablet_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_device_desktop_monthly_last_month
                # max_digital_is_device_desktop_monthly_last_three_month
                # max_digital_is_ais_mpay_used_monthly_last_month
                # max_digital_is_ais_mpay_used_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_desktop_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_desktop_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_ais_mpay_used_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_ais_mpay_used_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_my_ais_app_used_monthly_last_month
                # max_digital_is_my_ais_app_used_monthly_last_three_month
                # max_digital_is_serenade_monthly_last_month
                # max_digital_is_serenade_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_my_ais_app_used_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_my_ais_app_used_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_serenade_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_serenade_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_device_brands_ais_monthly_last_month
                # max_digital_is_device_brands_ais_monthly_last_three_month
                # max_digital_is_device_brands_dtac_monthly_last_month
                # max_digital_is_device_brands_dtac_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_ais_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_ais_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_dtac_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_dtac_monthly_last_three_month").collect()[0][
                0] == 1
                # max_digital_is_device_brands_true_monthly_last_month
                # max_digital_is_device_brands_true_monthly_last_three_month
                # avg_digital_nummber_of_device_brands_monthly_last_month
                # avg_digital_nummber_of_device_brands_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_true_monthly_last_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("max_digital_is_device_brands_true_monthly_last_three_month").collect()[0][
                0] == 1
        assert \
            df41.where("start_of_month='2019-04-01'").select("avg_digital_nummber_of_device_brands_monthly_last_month").collect()[0][
                0] == 27
        assert \
            abs(df41.where("start_of_month='2019-04-01'").select("avg_digital_nummber_of_device_brands_monthly_last_three_month").collect()[0][
                0] - 23.3333) < 0.001
                # avg_digital_nummber_of_distinct_device_type_monthly_last_month
                # avg_digital_nummber_of_distinct_device_type_monthly_last_three_month
        assert \
            df41.where("start_of_month='2019-04-01'").select("avg_digital_nummber_of_distinct_device_type_monthly_last_month").collect()[0][
                0] == 3
        assert \
            df41.where("start_of_month='2019-04-01'").select("avg_digital_nummber_of_distinct_device_type_monthly_last_three_month").collect()[0][
                0] == 3

    def test_l4_2(self,project_context):
        
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        browser_type = ['chrome','safari','line','opera','ie']
        reference_type = ['facebook','other','direct','internal','social']
        activetime_type =['','34']
        date1 = '2019-01-01'
        date2 = '2019-04-01'
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
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
        #   ----------------------------------------------------------------------------------------------
        df_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_weekly'))

        df_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_host_weekly'))
        
        df_postcode_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_postalcode_weekly'))
        
        df_ref_q_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerquery_weekly'))
        
        df_ref_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerhost_weekly'))
        # --------------------------------------------------------------------------------
        df42 = l4_rolling_window(df_weekly,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_weekly_features'))
        
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_chrome_weekly_last_week").collect()[0][
                0] == 34
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_chrome_weekly_last_two_week").collect()[0][
                0] == 204
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_chrome_weekly_last_four_week").collect()[0][
                0] == 374
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_chrome_weekly_last_twelve_week").collect()[0][
                0] == 918
                 # sum_digital_time_spent_browser_safari_weekly_last_week
                # sum_digital_time_spent_browser_safari_weekly_last_two_week
               # sum_digital_time_spent_browser_safari_weekly_last_four_week
                # sum_digital_time_spent_browser_safari_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_safari_weekly_last_week").collect()[0][
                0] == 102
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_safari_weekly_last_two_week").collect()[0][
                0] == 102
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_safari_weekly_last_four_week").collect()[0][
                0] == 238
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_safari_weekly_last_twelve_week").collect()[0][
                0] == 1156
                # sum_digital_time_spent_browser_line_weekly_last_week
                # sum_digital_time_spent_browser_line_weekly_last_two_week
                # sum_digital_time_spent_browser_line_weekly_last_four_week
                # sum_digital_time_spent_browser_line_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_line_weekly_last_week").collect()[0][
                0] == 0
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_line_weekly_last_two_week").collect()[0][
                0] == 102
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_line_weekly_last_four_week").collect()[0][
                0] == 272
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_line_weekly_last_twelve_week").collect()[0][
                0] == 646
                # sum_digital_time_spent_browser_opera_weekly_last_week
                # sum_digital_time_spent_browser_opera_weekly_last_two_week
                # sum_digital_time_spent_browser_opera_weekly_last_four_week
                # sum_digital_time_spent_browser_opera_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_opera_weekly_last_week").collect()[0][
                0] == 34
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_opera_weekly_last_two_week").collect()[0][
                0] == 68
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_opera_weekly_last_four_week").collect()[0][
                0] == 204
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_browser_opera_weekly_last_twelve_week").collect()[0][
                0] == 782
                # sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_week
                # sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_two_week
                # sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_four_week
                # sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_week").collect()[0][
                0] == 340
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_two_week").collect()[0][
                0] == 680
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_four_week").collect()[0][
                0] == 1292
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrersocialnetwork_facebook_weekly_last_twelve_week").collect()[0][
                0] == 4318
                # sum_digital_time_spent_referrerhostclass_other_weekly_last_week
                # sum_digital_time_spent_referrerhostclass_other_weekly_last_two_week
                # sum_digital_time_spent_referrerhostclass_other_weekly_last_four_week
                # sum_digital_time_spent_referrerhostclass_other_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_other_weekly_last_week").collect()[0][
                0] == 102
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_other_weekly_last_two_week").collect()[0][
                0] == 136
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_other_weekly_last_four_week").collect()[0][
                0] == 204
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_other_weekly_last_twelve_week").collect()[0][
                0] == 952
                # sum_digital_time_spent_referrerhostclass_direct_weekly_last_week
                # sum_digital_time_spent_referrerhostclass_direct_weekly_last_two_week
                # sum_digital_time_spent_referrerhostclass_direct_weekly_last_four_week
                # sum_digital_time_spent_referrerhostclass_direct_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_direct_weekly_last_week").collect()[0][
                0] == 102
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_direct_weekly_last_two_week").collect()[0][
                0] == 170
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_direct_weekly_last_four_week").collect()[0][
                0] == 340
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_direct_weekly_last_twelve_week").collect()[0][
                0] == 918
                # sum_digital_time_spent_referrerhostclass_internal_weekly_last_week
                # sum_digital_time_spent_referrerhostclass_internal_weekly_last_two_week
                # sum_digital_time_spent_referrerhostclass_internal_weekly_last_four_week
                # sum_digital_time_spent_referrerhostclass_internal_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_internal_weekly_last_week").collect()[0][
                0] == 68
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_internal_weekly_last_two_week").collect()[0][
                0] == 136
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_internal_weekly_last_four_week").collect()[0][
                0] == 306
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_internal_weekly_last_twelve_week").collect()[0][
                0] == 918
                # sum_digital_time_spent_referrerhostclass_social_weekly_last_week
                # sum_digital_time_spent_referrerhostclass_social_weekly_last_two_week
                # sum_digital_time_spent_referrerhostclass_social_weekly_last_four_week
                # sum_digital_time_spent_referrerhostclass_social_weekly_last_twelve_week
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_social_weekly_last_week").collect()[0][
                0] == 68
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_social_weekly_last_two_week").collect()[0][
                0] == 170
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_social_weekly_last_four_week").collect()[0][
                0] == 272
        assert \
            df42.where("start_of_week='2019-04-01'").select("sum_digital_time_spent_referrerhostclass_social_weekly_last_twelve_week").collect()[0][
                0] == 646
    def test_l4_3(self,project_context):
        
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        browser_type = ['chrome','safari','line','opera','ie']
        reference_type = ['facebook','other','direct','internal','social']
        activetime_type =['','34']
        date1 = '2019-01-01'
        date2 = '2019-04-01'
        my_dates_list = pd.date_range(date1, date2).tolist()
        my_dates = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
        my_dates = my_dates * 3
        random.seed(100)
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
        #   ----------------------------------------------------------------------------------------------
        df_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_weekly'))

        df_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_host_weekly'))
        
        df_postcode_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_postalcode_weekly'))
        
        df_ref_q_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerquery_weekly'))
        
        df_ref_host_weekly = node_from_config(df0,var_project_context.catalog.load(
            'params:l2_digital_cxenxse_site_traffic_popular_referrerhost_weekly'))
        # -----------------------hostt---------------------------------------------------------
        df_host_1 = l4_rolling_window(df_host_weekly,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_host_weekly_int'))
        assert \
            df_host_1.where("start_of_week='2019-04-01'").select("sum_digital_popular_host_weekly_last_week").collect()[0][
                0] == 340
        assert \
            df_host_1.where("start_of_week='2019-04-01'").select("sum_digital_popular_host_weekly_last_two_week").collect()[0][
                0] == 680
        assert \
            df_host_1.where("start_of_week='2019-04-01'").select("sum_digital_popular_host_weekly_last_four_week").collect()[0][
                0] == 1292
        assert \
            df_host_1.where("start_of_week='2019-04-01'").select("sum_digital_popular_host_weekly_last_twelve_week").collect()[0][
                0] == 4318
        df_host_2 = l4_rolling_ranked_window(df_host_1,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_host_weekly_features'))

        assert \
            df_host_2.where("start_of_week='2019-04-01'").select("fav_host_last_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_host_2.where("start_of_week='2019-04-01'").select("fav_host_last_two_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_host_2.where("start_of_week='2019-04-01'").select("fav_host_last_four_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_host_2.where("start_of_week='2019-04-01'").select("fav_host_last_twelve_week").collect()[0][
                0] == 'callingmelody.net'
                # ------------------------postalcode---------------------------------
        df_post1 = l4_rolling_window(df_postcode_weekly,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_int'))

        assert \
            df_post1.where("start_of_week='2019-04-01'").select("sum_digital_popular_postalcode_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df_post1.where("start_of_week='2019-04-01'").select("sum_digital_popular_postalcode_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df_post1.where("start_of_week='2019-04-01'").select("sum_digital_popular_postalcode_weekly_last_four_week").collect()[0][
                0] == 84
        assert \
            df_post1.where("start_of_week='2019-04-01'").select("sum_digital_popular_postalcode_weekly_last_twelve_week").collect()[0][
                0] == 252

        df_post2 = l4_rolling_ranked_window(df_post1,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_postalcode_weekly_features'))
        
        assert \
            df_post2.where("start_of_week='2019-04-01'").select("fav_postalcode_last_week").collect()[0][
                0] == '10120'
        assert \
            df_post2.where("start_of_week='2019-04-01'").select("fav_postalcode_last_two_week").collect()[0][
                0] == '10120'
        assert \
            df_post2.where("start_of_week='2019-04-01'").select("fav_postalcode_last_four_week").collect()[0][
                0] == '10120'
        assert \
            df_post2.where("start_of_week='2019-04-01'").select("fav_postalcode_last_twelve_week").collect()[0][
                0] == '10120'
        # ------------------------ref-q-----------------
        df_ref_q1 = l4_rolling_window(df_ref_q_weekly,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_int'))
        # df_ref_q1.show()

        assert \
            df_ref_q1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerquery_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df_ref_q1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerquery_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df_ref_q1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerquery_weekly_last_four_week").collect()[0][
                0] == 84
        assert \
            df_ref_q1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerquery_weekly_last_twelve_week").collect()[0][
                0] == 252

        df_ref_q2 = l4_rolling_ranked_window(df_ref_q1,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_referrerquery_weekly_features'))

        assert \
            df_ref_q2.where("start_of_week='2019-04-01'").select("fav_referrerquery_last_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_q2.where("start_of_week='2019-04-01'").select("fav_referrerquery_last_two_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_q2.where("start_of_week='2019-04-01'").select("fav_referrerquery_last_four_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_q2.where("start_of_week='2019-04-01'").select("fav_referrerquery_last_twelve_week").collect()[0][
                0] == 'callingmelody.net'

        # ------------------- ref host -------------------

        df_ref_h1 = l4_rolling_window(df_ref_host_weekly,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_int'))
        
        assert \
            df_ref_h1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerhost_weekly_last_week").collect()[0][
                0] == 21
        assert \
            df_ref_h1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerhost_weekly_last_two_week").collect()[0][
                0] == 42
        assert \
            df_ref_h1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerhost_weekly_last_four_week").collect()[0][
                0] == 84
        assert \
            df_ref_h1.where("start_of_week='2019-04-01'").select("sum_digital_popular_referrerhost_weekly_last_twelve_week").collect()[0][
                0] == 252


        df_ref_h2 = l4_rolling_ranked_window(df_ref_h1,var_project_context.catalog.load(
            'params:l4_digital_cxenxse_site_traffic_popular_referrerhost_weekly_features'))

        assert \
            df_ref_h2.where("start_of_week='2019-04-01'").select("fav_referrerhost_last_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_h2.where("start_of_week='2019-04-01'").select("fav_referrerhost_last_two_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_h2.where("start_of_week='2019-04-01'").select("fav_referrerhost_last_four_week").collect()[0][
                0] == 'callingmelody.net'
        assert \
            df_ref_h2.where("start_of_week='2019-04-01'").select("fav_referrerhost_last_twelve_week").collect()[0][
                0] == 'callingmelody.net'
        
