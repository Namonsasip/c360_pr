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
    def test_device_summary_with_config(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        date1 = '2020-01-01'
        date2 = '2020-06-01'
        brand_list = ['samsung', 'huwaei', 'oppo', 'apple', 'xiaomi', 'other']
        handsetType_list = ['smartphone', 'standardphone', 'legacyphone', 'netbook', 'tablet', 'phablet', 'module',
                            'handheld', 'vehicle', 'aircard/dongle', 'modem', 'wirelessrouter', 'smartwatch',
                            'watchphone', 'vehicle', None]
        os_list = ['android', 'ios', 'other']
        imei_list = ['123', '111', '222', '333', '456', '789']

        dummy_date = pd.date_range(date1, date2).tolist()
        dummy_date = [iTemp.date().strftime("%Y-%m-%d") for iTemp in dummy_date]
        random.seed(100)
        random_imei = [imei_list[random.randint(0, 5)] for iTemp in range(0, len(dummy_date))]
        random_launchprice_saleprice = [random.randint(2, 40) * 1000 for iTemp in range(0, len(dummy_date))]
        # random_saleprice = [random.randint() for iTemp in range(0, len(dummy_date))]
        random_handset_type = [handsetType_list[random.randint(0, len(handsetType_list) - 1)] for iTemp in
                               range(0, len(dummy_date))]
        random_gprs_handset_support = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_handset_support_3g_2100_yn = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_hsdpa = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_hs_support_lte_1800 = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_handset_channel = [['ais', 'AIS', 'abc', 'test'][random.randint(0, 3)] for iTemp in
                                  range(0, len(dummy_date))]
        random_dual_sim = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_video_call = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_google_map = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]
        random_handset_wds_flag = [['Y', 'N'][random.randint(0, 1)] for iTemp in range(0, len(dummy_date))]

        random_handset_brand_name = [brand_list[random.randint(0, len(brand_list) - 1)] for iTemp in
                                     range(0, len(dummy_date))]
        random_os = [os_list[random.randint(0, len(os_list) - 1)] for iTemp in range(0, len(dummy_date))]

        df = spark.createDataFrame(
            zip(dummy_date, random_imei, random_launchprice_saleprice, random_launchprice_saleprice,
                random_handset_type, random_gprs_handset_support, random_handset_support_3g_2100_yn
                , random_hsdpa, random_hs_support_lte_1800, random_handset_channel, random_dual_sim, random_video_call,
                random_google_map, random_handset_wds_flag, random_handset_brand_name, random_os),
            schema=['date_id', 'handset_imei', 'launchprice', 'saleprice', 'handset_type',
                    'gprs_handset_support', 'handset_support_3g_2100_yn', 'hsdpa',
                    'hs_support_lte_1800', 'handset_channel'
                , 'dual_sim', 'video_call', 'google_map', 'handset_wds_flag', 'handset_brand_name', 'os']) \
            .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
            .withColumn('subscription_identifier', F.lit(123))
        df = df.withColumn("start_of_week", F.to_date(F.date_trunc('week', F.to_date('date_id', 'yyyy-MM-dd'))))

        print('rawdata')
        df.show()
        l2_device_summary_with_config_weekly = node_from_config(df, var_project_context.catalog.load(
            'params:l2_device_summary_with_config'))

        print('l2test')
        l2_device_summary_with_config_weekly.orderBy('start_of_week').show(999, False)

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_number_of_phone_updates").collect()[0][
                0] == 5

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_samsung").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_huawei").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_oppo").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_apple").collect()[0][
                0] == 2

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_xiaomi").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_most_used_handset_others").collect()[0][
                0] == 3

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_high_range").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_mid_range").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_low_range").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_smartphone").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_standardphone").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_legacyphone").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_netbook").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_tablet").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_phablet").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_gprs").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_umts").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_hsdpa").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_lte").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_is_device_os_android").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_is_device_os_ios").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_is_device_os_others").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_purchased_from_ais").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_dual_sim").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_video_call").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_google_map").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_supports_wds").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_module").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_handheld").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_vehicle").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_air_card_or_dongle").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_modem").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_wirelessrouter").collect()[0][
                0] == 1

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_smartwatch").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_watchphone").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_iotvehicle").collect()[0][
                0] == 0

        assert \
            l2_device_summary_with_config_weekly.where("start_of_week = '2020-01-06'").select(
                "device_unknown").collect()[0][
                0] == 0

        l4_device_summary_features = l4_rolling_window(l2_device_summary_with_config_weekly,
                                                       var_project_context.catalog.load(
                                                           'params:l4_device_summary_with_config_features'))
        print('l4test')
        l4_device_summary_features.show(888, False)

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_number_of_phone_updates_weekly_last_four_week").collect()[0][
                0] == 18

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_samsung_weekly_last_four_week").collect()[0][
                0] == 5

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_huawei_weekly_last_four_week").collect()[0][
                0] == 6

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_oppo_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_apple_weekly_last_four_week").collect()[0][
                0] == 4

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_xiaomi_weekly_last_four_week").collect()[0][
                0] == 4

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "sum_device_most_used_handset_others_weekly_last_four_week").collect()[0][
                0] == 8

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "avg_device_number_of_phone_updates_weekly_last_four_week").collect()[0][
                0] == 4.5

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_high_range_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_mid_range_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_low_range_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_smartphone_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_standardphone_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_legacyphone_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_netbook_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_tablet_weekly_last_four_week").collect()[0][
                0] == 0

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_phablet_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_gprs_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_umts_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_hsdpa_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_is_device_os_android_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_lte_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_is_device_os_ios_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_is_device_os_others_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_purchased_from_ais_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_dual_sim_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_video_call_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_google_map_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_supports_wds_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_module_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_handheld_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_vehicle_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_air_card_or_dongle_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_modem_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_wirelessrouter_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_smartwatch_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_watchphone_weekly_last_four_week").collect()[0][
                0] == 1

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_iotvehicle_weekly_last_four_week").collect()[0][
                0] == 0

        assert \
            l4_device_summary_features.where("start_of_week = '2020-05-04'").select(
                "max_device_unknown_weekly_last_four_week").collect()[0][
                0] == 1

        # exit(2)

#
# class TestUnitDevice_Backup:
#
#     def test_number_of_phone_updates(self, project_context):
#         '''
#
#         l2_device_number_of_phone_updates
#         '''
#         var_project_context = project_context['ProjectContext']
#         spark = project_context['Spark']
#         date1 = '2020-01-01'
#         date2 = '2020-06-01'
#
#         '''
#         l2_device_number_of_phone_updates:
#   where_clause: ""
#   feature_list:
#     device_number_of_phone_updates: "count(distinct handset_imei)"
#   granularity: "start_of_month,start_of_week,mobile_no,register_date"'''
#
#         imei_list = ['123', '111', '222', '333', '456', '789']
#
#         dummy_date = pd.date_range(date1, date2).tolist()
#         dummy_date = [iTemp.date().strftime("%Y-%m-%d") for iTemp in dummy_date]
#         random.seed(100)
#         random_list = [imei_list[random.randint(0, 5)] for iTemp in range(0, len(dummy_date))]
#         random_list2 = [random.randint(1, 3) for iTemp in range(0, len(dummy_date))]
#
#         df = spark.createDataFrame(zip(dummy_date, random_list, random_list2, random_list2),
#                                    schema=['date_id', 'handset_imei', 'mobile_no', 'access_method_num']) \
#             .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd')) \
#             .withColumn('subscription_identifier', F.lit(123))
#
#         df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.to_date('date_id', 'yyyy-MM-dd')))) \
#             .withColumn("start_of_week", F.to_date(F.date_trunc('week', F.to_date('date_id', 'yyyy-MM-dd'))))
#         print('rawdata')
#         df.show(888, False)
#         l2_device_number_of_phone_updates_weekly = node_from_config(df, var_project_context.catalog.load(
#             'params:l2_device_number_of_phone_updates'))
#         print('testphoneupdate')
#         l2_device_number_of_phone_updates_weekly.orderBy('start_of_week').show(888, False)
#         assert \
#             l2_device_number_of_phone_updates_weekly.where("start_of_week = '2020-01-06'").where(
#                 'mobile_no="1"').select(
#                 "device_number_of_phone_updates").collect()[0][
#                 0] == 2
#
#         l3_device_number_of_phone_updates_monthly = node_from_config(l2_device_number_of_phone_updates_weekly,
#                                                                      var_project_context.catalog.load(
#                                                                          'params:l3_device_number_of_phone_updates'))
#         print('testl4phoneupdate')
#         l3_device_number_of_phone_updates_monthly.show(888, False)
#
#         # exit(2)
#
#     def test_device_used(self, project_context):
#         '''
#         l2_device_most_used
#         '''
#         var_project_context = project_context['ProjectContext']
#         spark = project_context['Spark']
#
#         '''l2_device_most_used:
#           where_clause: ""
#           feature_list:
#             device_used_days: "count(distinct date_id)"
#             device_most_used_handset_samsung: "case when handset_brand_name='SAMSUNG' then 'Y' else 'N' end"
#             device_most_used_handset_huawei: "case when handset_brand_name='HUAWEI' then 'Y' else 'N' end"
#             device_most_used_handset_oppo: "case when handset_brand_name='OPPO' then 'Y' else 'N' end"
#             device_most_used_handset_apple: "case when handset_brand_name='APPLE' then 'Y' else 'N' end"
#             device_most_used_handset_xiaomi: "case when handset_brand_name='XIAOMI' then 'Y' else 'N' end"
#             device_most_used_handset_others: "case when handset_brand_name not in ('SAMSUNG','HUAWEI','OPPO','APPLE','XIAOMI') then 'Y' else 'N' end"
#             rank: "row_number() over(partition by start_of_month,start_of_week,mobile_no,register_date
#                        order by count(distinct date_id) desc)"
#           granularity: "start_of_month,start_of_week,mobile_no,register_date,handset_brand_name"'''
#
#         date1 = '2020-01-01'
#         date2 = '2020-06-01'
#
#         device_list = ['SAMSUNG', 'HUAWEI', 'OPPO', 'APPLE', 'XIAOMI', 'other']
#
#         dummy_date = pd.date_range(date1, date2).tolist()
#         dummy_date = [iTemp.date().strftime("%Y-%m-%d") for iTemp in dummy_date]
#         random.seed(100)
#         random_list = [device_list[random.randint(0, 5)] for iTemp in range(0, len(dummy_date))]
#
#         df = spark.createDataFrame(zip(dummy_date, random_list),
#                                    schema=['date_id', 'handset_brand_name']) \
#             .withColumn("mobile_no", F.lit(1)) \
#             .withColumn("register_date", F.to_date(F.lit('2019-01-01'), 'yyyy-MM-dd'))
#         df = df.withColumn("start_of_month", F.to_date(F.date_trunc('month', F.to_date('date_id', 'yyyy-MM-dd')))) \
#             .withColumn("start_of_week", F.to_date(F.date_trunc('week', F.to_date('date_id', 'yyyy-MM-dd'))))
#         print('rawdata')
#         df.show(888, False)
#
#         device_most_used_weekly = node_from_config(df, var_project_context.catalog.load('params:l2_device_most_used'))
#         print('devicemostused')
#         device_most_used_weekly.orderBy('start_of_week').show(999, False)
#
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_used_days").collect()[0][
#                 0] == 2
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_samsung").collect()[0][
#                 0] == 'N'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_huawei").collect()[0][
#                 0] == 'N'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_oppo").collect()[0][
#                 0] == 'N'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="APPLE"').select(
#                 "device_most_used_handset_apple").collect()[0][
#                 0] == 'Y'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_xiaomi").collect()[0][
#                 0] == 'Y'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_others").collect()[0][
#                 0] == 'N'
#         assert \
#             device_most_used_weekly.where("start_of_week = '2020-01-06'").where('handset_brand_name="APPLE"').select(
#                 "rank").collect()[0][
#                 0] == 2
#         assert \
#             device_most_used_weekly.where('handset_brand_name="OPPO"').select(
#                 "device_most_used_handset_oppo").collect()[0][
#                 0] == 'Y'
#
#         '''
#         l3_device_most_used:
#           where_clause: ""
#           feature_list:
#             device_used_days: "count(distinct date_id)"
#             device_most_used_handset_samsung: "case when handset_brand_name='SAMSUNG' then 'Y' else 'N' end"
#             device_most_used_handset_huawei: "case when handset_brand_name='HUAWEI' then 'Y' else 'N' end"
#             device_most_used_handset_oppo: "case when handset_brand_name='OPPO' then 'Y' else 'N' end"
#             device_most_used_handset_apple: "case when handset_brand_name='APPLE' then 'Y' else 'N' end"
#             device_most_used_handset_xiaomi: "case when handset_brand_name='XIAOMI' then 'Y' else 'N' end"
#             device_most_used_handset_others: "case when handset_brand_name not in ('SAMSUNG','HUAWEI','OPPO','APPLE','XIAOMI') then 'Y' else 'N' end"
#             rank: "row_number() over(partition by start_of_month,mobile_no,register_date
#                        order by count(distinct date_id) desc)"
#           granularity: "start_of_month,mobile_no,register_date,handset_brand_name"
#         '''
#         l3_device_most_used_monthly = node_from_config(df,
#                                                        var_project_context.catalog.load('params:l3_device_most_used'))
#         print('l3mostuse')
#         l3_device_most_used_monthly.where('start_of_month="2020-02-01"').orderBy('start_of_month', 'rank').show(888,
#                                                                                                                 False)
#
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_used_days").collect()[0][
#                 0] == 5
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_samsung").collect()[0][
#                 0] == 'N'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_huawei").collect()[0][
#                 0] == 'N'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_oppo").collect()[0][
#                 0] == 'N'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="APPLE"').select(
#                 "device_most_used_handset_apple").collect()[0][
#                 0] == 'Y'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_xiaomi").collect()[0][
#                 0] == 'Y'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="XIAOMI"').select(
#                 "device_most_used_handset_others").collect()[0][
#                 0] == 'N'
#         assert \
#             l3_device_most_used_monthly.where("start_of_month = '2020-02-01'").where(
#                 'handset_brand_name="APPLE"').select(
#                 "rank").collect()[0][
#                 0] == 2
#         assert \
#             l3_device_most_used_monthly.where('handset_brand_name="OPPO"').select(
#                 "device_most_used_handset_oppo").collect()[0][
#                 0] == 'Y'
#
#         # exit(2)
#
#     def test_dummy(self, project_context):
#         var_project_context = project_context['ProjectContext']
#         spark = project_context['Spark']
