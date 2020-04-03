import pandas as pd
from pyspark.shell import spark
from pyspark.sql.types import *
import datetime
import random
from datetime import datetime
from pyspark.sql.types import *
from customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l1.to_l1_nodes import change_grouped_column_name
from kedro.pipeline import Pipeline, node

from customer360.utilities.config_parser import node_from_config

from src.customer360.utilities.config_parser import l4_rolling_window
from src.customer360.pipelines.data_engineering.nodes.complaints_nodes.to_l4.to_l4_nodes import l4_complaints_nps


class TestUnitComplaints:

    def test_l1(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        dummy_list_l0_usage_call = [
            ['CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY', '1', '10',
             datetime.strptime('2020-02-01', "%Y-%m-%d"),
             'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY'],
            ['CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY', '2', '20',
             datetime.strptime('2020-02-08', "%Y-%m-%d"),
             'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY'],
            ['CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY', '3', '30',
             datetime.strptime('2020-02-15', "%Y-%m-%d"),
             'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY'],
            ['s29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP', '4', '40',
             datetime.strptime('2020-02-22', "%Y-%m-%d"),
             's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP'],
            ['s29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP', '5', '50',
             datetime.strptime('2020-02-29', "%Y-%m-%d"),
             's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP'],
            ['s29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP', '6', '60',
             datetime.strptime('2020-03-07', "%Y-%m-%d"),
             's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP'],
            ['mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx', '7', '70',
             datetime.strptime('2020-03-14', "%Y-%m-%d"),
             'mU0rlxxrHIUalXUVlGF1ULzAOTcHyPxkt5bl4s87NMhHjNr.OJuO4iDk.ZLRKPMx'],
            ['E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1', '8', '80',
             datetime.strptime('2020-03-21', "%Y-%m-%d"),
             'E3pLnJoxYAY5jc5Aq0c2.HAEIgbFHqR3181FhZ8ijaJJXj9Zq0bTaIpIidUHewA1'],
            ['8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH', '9', '90',
             datetime.strptime('2020-03-28', "%Y-%m-%d"),
             '8iiFuP0akRDCs280HV8ZDa65wn+s.tTWjGDWw81aO+wM5.sWuE.Qet9xLanx8kIH'],

        ]

        rdd1 = spark.sparkContext.parallelize(dummy_list_l0_usage_call)
        df_l0_usage_call = spark.createDataFrame(rdd1,
                                                 schema=StructType([
                                                     StructField("called_no", StringType(), True),
                                                     StructField("total_successful_call", StringType(), True),
                                                     StructField("total_durations", StringType(), True),
                                                     StructField("day_id", DateType(), True),
                                                     StructField("caller_no", StringType(), True),
                                                 ]))
        # df_l0_usage_call.show()
        global l1_feature_1
        l1_feature_1 = node_from_config(df_l0_usage_call, var_project_context.catalog.load(
            'params:l1_complaints_call_to_competitor_features'))
        # l1_feature_1.orderBy('event_partition_date').show()

        dummy_list_l0_complaints_acc_atsr = [
            ['1', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['2', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['3', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['4', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['5', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['6', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['7', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['8', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['พอใจมาก', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
            ['พอใจมาก', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
        ]

        rdd2 = spark.sparkContext.parallelize(dummy_list_l0_complaints_acc_atsr)
        df_l0_ucomplaints_acc_atsr = spark.createDataFrame(rdd2,
                                                           schema=StructType([
                                                               StructField("qsc_1", StringType(), True),
                                                               StructField("mobile_no", StringType(), True),
                                                               StructField("cldate", DateType(), True),
                                                           ]))
        # df_l0_ucomplaints_acc_atsr.orderBy('cldate').show()
        global l1_feature_2
        l1_feature_2 = change_grouped_column_name(df_l0_ucomplaints_acc_atsr, var_project_context.catalog.load(
            'params:l1_complaints_nps_after_call'))
        # l1_feature_2.orderBy('event_partition_date').show()

        dummy_list_l0_acc_ai_chatbot = [
            ['1', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['2', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['3', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['4', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['5', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['1', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['2', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['3', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['4', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
            ['5', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
        ]

        rdd3 = spark.sparkContext.parallelize(dummy_list_l0_acc_ai_chatbot)
        df_l0_acc_ai_chatbot = spark.createDataFrame(rdd3,
                                                     schema=StructType([
                                                         StructField("qsc_1", StringType(), True),
                                                         StructField("mobile_number", StringType(), True),
                                                         StructField("request_datetime", DateType(), True),
                                                     ]))
        # df_l0_acc_ai_chatbot.orderBy('request_datetime').show()
        global l1_feature_3
        l1_feature_3 = change_grouped_column_name(df_l0_acc_ai_chatbot, var_project_context.catalog.load(
            'params:l1_complaints_nps_after_chatbot'))
        # l1_feature_3.orderBy('event_partition_date').show()

        dummy_list_l0_acc_qmt_csi = [
            ['1', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['2', 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5',
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['3', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['4', 'CcLxTG0FwxCbQGxnVsZBvaqAdCiaQ.obPXCpWxgSiAUn3.Wgu6Dhoaxk9dJ81IIL',
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['5', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['6', 'iAiWOKuH.o7otT537ffGEQxG0mj5HMLJ0kE6U69+nvhDIUrEcRAs6c22gXVP7Kci',
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['7', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['8', 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw',
             datetime.strptime('2020-02-01', "%Y-%m-%d")],
            ['9', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
            ['10', 'VTF.08ua58ekdcl51wAoQuxbLY3mUYuq1J3OTZphFB+p+ZU+80BeXcNZdmRRJUUT',
             datetime.strptime('2020-03-01', "%Y-%m-%d")],
        ]

        rdd4 = spark.sparkContext.parallelize(dummy_list_l0_acc_qmt_csi)
        df_l0_acc_qmt_csi = spark.createDataFrame(rdd4,
                                                  schema=StructType([
                                                      StructField("survey_nps_score", StringType(), True),
                                                      StructField("access_metod_num", StringType(), True),
                                                      StructField("create_dttm", DateType(), True),
                                                  ]))
        # df_l0_acc_qmt_csi.orderBy('create_dttm').show()
        global l1_feature_4
        l1_feature_4 = change_grouped_column_name(df_l0_acc_qmt_csi, var_project_context.catalog.load(
            'params:l1_complaints_nps_after_store_visit'))
        # l1_feature_4.orderBy('event_partition_date').show()

        # print("test l1 -->  l1_complaints_call_to_competitor_features")
        # assert \
        #     l1_feature_1.where \
        #             (
        #             "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and day_id='2020-02-01'") \
        #         .select("call_to_dtac_count").collect()[0][0] == 1
        #
        # assert \
        #     l1_feature_1.where( \
        #         "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and day_id='2020-02-01'") \
        #         .select("call_to_dtac_duration_sum").collect()[0][0] == 10
        # assert l1_feature_1.where \
        #                (
        #                "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and day_id='2020-02-22'") \
        #            .select("call_to_true_count").collect()[0][0] == 4
        # assert l1_feature_1.where \
        #                (
        #                "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and day_id='2020-02-22'") \
        #            .select("call_to_true_duration_sum").collect()[0][0] == 40
        #
        # print("test l1 -->  l1_complaints_nps_after_call")
        # assert \
        #     l1_feature_2.where \
        #             (
        #             "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and  event_partition_date='2019-11-01'") \
        #         .select("avg_nps").collect()[0][0] == 1.5
        # assert \
        #     l1_feature_2.where \
        #             (
        #             "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and event_partition_date='2019-11-01'") \
        #         .select("record_count").collect()[0][0] == 2
        #
        # print("test l1 -->  l1_complaints_nps_after_chatbot")
        # assert l1_feature_3.where \
        #                (
        #                "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and  event_partition_date='2019-11-01'") \
        #            .select("avg_nps").collect()[0][0] == 1.5
        #
        # assert \
        #     l1_feature_3.where \
        #             (
        #             "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and event_partition_date='2019-11-01'") \
        #         .select("record_count").collect()[0][0] == 2
        #
        # print("test l1 -->  l1_complaints_nps_after_store_visit")
        # assert \
        #     l1_feature_4.where \
        #             (
        #             "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and  event_partition_date='2019-11-01'") \
        #         .select("avg_nps").collect()[0][0] == 1.5
        # assert \
        #     l1_feature_4.where \
        #             (
        #             "access_method_num = 'zHD1URYLeFsOkFfNfGp3.2YgQdMDEKlrRBlbDC6.veRVgoBXFnNiWIYw52Ht4BG5' and event_partition_date='2019-11-01'") \
        #         .select("record_count").collect()[0][0] == 2

    def test_l2(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")
        global l2_feature_1
        l2_feature_1 = node_from_config(l1_feature_1, var_project_context.catalog.load(
            'params:l2_complaints_call_to_competitor_features'))
        global l2_feature_2
        l2_feature_2 = node_from_config(l1_feature_2, var_project_context.catalog.load(
            'params:l2_complaints_nps_scoring'))
        global l2_feature_3
        l2_feature_3 = node_from_config(l1_feature_3, var_project_context.catalog.load(
            'params:l2_complaints_nps_scoring'))
        global l2_feature_4
        l2_feature_4 = node_from_config(l1_feature_4, var_project_context.catalog.load(
            'params:l2_complaints_nps_scoring'))

        # print("test L2 -->  l2_complaints_call_to_competitor_features")
        # assert \
        #     l2_feature_1.where( \
        #         "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and "
        #         "start_of_week='2020-02-10'") \
        #         .select("call_to_dtac_count").collect()[0][0] == 3
        #
        # assert \
        #     l2_feature_1.where \
        #             (
        #             "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and "
        #             "start_of_week='2020-02-10'") \
        #         .select("call_to_dtac_duration_sum").collect()[0][0] == 30
        #
        # assert \
        #     l2_feature_1.where \
        #             (
        #             "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and  start_of_week='2020-03-02'") \
        #         .select("call_to_true_count").collect()[0][0] == 6
        # assert \
        #     l2_feature_1.where \
        #             (
        #             "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and "
        #             "start_of_week='2020-03-02'") \
        #         .select("call_to_true_duration_sum").collect()[0][0] == 60
        #
        # print("test L2 --> l2_complaints_call_to_competitor_features -->  success 4 feature")
        # print("test L2 -->  l2_complaints_nps_scoring  --> from complaints_nps_after_call")
        # assert \
        #     l2_feature_2.where( \
        #     "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #     "start_of_week='2020-01-27'").select("avg_nps").collect()[0][0] == 7.5
        #
        # assert \
        #     l2_feature_2.where \
        #             (
        #     "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #     "start_of_week='2020-02-10'").select("record_count").collect()[0][0] == 2
        # print("test L2 -->  l2_complaints_nps_scoring  --> from complaints_nps_after_call --> success 2 feature")

        # assert \
        #     l2_feature_3.where( \
        #     "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #     "start_of_week='2020-01-27'").select("avg_nps").collect()[0][0] == 2.5
        #
        # assert \
        #     l2_feature_3.where( \
        #     "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #     "start_of_week='2020-02-10'").select("record_count").collect()[0][0] == 2
        # print("test L2 -->  l2_complaints_nps_scoring  --> from complaints_nps_after_chatbot --> success 2 feature")
        #
        # print("test L2 -->  l2_complaints_nps_scoring  --> from complaints_nps_after_store_visit")
        # assert \
        #     l2_feature_4.where( \
        #         "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #         "start_of_week='2020-01-27'") \
        #         .select("avg_nps").collect()[0][0] == 7.5
        #
        # assert \
        #     l2_feature_4.where \
        #             (
        #             "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
        #             "start_of_week='2020-02-10'") \
        #         .select("record_count").collect()[0][0] == 2
        # print("test L2 -->  l2_complaints_nps_scoring  --> from complaints_nps_after_store_visit --> success 2 feature")
        # print("Total Success all of L2 --> 10 frature")
    #
    def test_l3(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")

        l3_feature_1 = node_from_config(l1_feature_1, var_project_context.catalog.load(
            'params:l3_complaints_call_to_competitor_features'))

        l3_feature_2 = node_from_config(l1_feature_2, var_project_context.catalog.load(
            'params:l3_complaints_nps_scoring'))

        l3_feature_3 = node_from_config(l1_feature_3, var_project_context.catalog.load(
            'params:l3_complaints_nps_scoring'))

        l3_feature_4 = node_from_config(l1_feature_4, var_project_context.catalog.load(
            'params:l3_complaints_nps_scoring'))

        print("test L3 -->  l3_complaints_call_to_competitor_features")

        assert \
            l3_feature_1.where( \
            "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and "
            "start_of_month='2020-01-10'") \
            .select("call_to_dtac_count").collect()[0][0] == 1
        assert \
             l3_feature_1.where ( \
                "caller_no = 'CBWF9FNPqZHBYm8bPKouZQdhlgBrVwKOfTebFbumu7jevrbXBLJwSnxbIc11DSbY' and "
                "start_of_month='2020-01-10'") \
                .select("call_to_dtac_duration_sum").collect()[0][0] == 10

        assert \
                l3_feature_1.where \
                (
                "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and  start_of_month='2020-02-01'") \
            .select("call_to_true_count").collect()[0][0] == 9


        assert \
            l3_feature_1.where \
                (
                "caller_no = 's29uZZn8axRu9Rs.hZl.mB3O3076InFK5+lFoG792dwSDVzy3w+.02SMc5ezMMMP' and "
                "start_of_month='2020-02-01'") \
            .select("call_to_true_duration_sum").collect()[0][0] == 90

        print("test L3 --> l3_complaints_call_to_competitor_features -->  success 4 feature")
        print("test L3 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_call")
        assert \
            l3_feature_2.where( \
            "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
            "start_of_month='2020-01-01'") \
            .select("avg_nps").collect()[0][0] == 7.5

        assert \
        l3_feature_2.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2

        print("test L2 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_call --> success 2 feature")

        print("test L2 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_chatbot")
        assert \
        l3_feature_3.where( \
            "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
            "start_of_month='2020-01-01'") \
            .select("avg_nps").collect()[0][0] == 2.5

        assert \
        l3_feature_3.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2

        print("test L2 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_chatbot --> success 2 feature")

        print("test L2 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_store_visit")
        assert \
        l3_feature_4.where( \
            "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
            "start_of_month='2020-01-01'") \
            .select("avg_nps").collect()[0][0] == 7.5

        assert \
        l3_feature_4.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2
        print("test L2 -->  l3_complaints_nps_scoring  --> from complaints_nps_after_store_visit --> success 2 feature")
        print("Total Success all of L3 --> 10 frature")


    def test_l4(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']

        spark.conf.set("spark.sql.parquet.binaryAsString", "true")

        l4_feature_1 = l4_rolling_window(l2_feature_1, var_project_context.catalog.load(
        'params:l4_complaints_call_to_competitor_features'))
        l4_feature_2 = l4_complaints_nps(l2_feature_2)
        l4_feature_3 = l4_complaints_nps(l2_feature_3)
        l4_feature_4 = l4_complaints_nps(l2_feature_4)

        l4_feature_1.show()
        assert \
        l4_feature_1.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2



        assert \
        l4_feature_2.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2

        assert \
        l4_feature_3.where \
                (
                "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
                "start_of_month='2020-01-01'") \
            .select("record_count").collect()[0][0] == 2


        assert \
            l4_feature_4.where \
            (
            "access_method_num = 'mv6fLn5EbNGOgfj8olB9yHg6r8rQRjv92iiccMAtQH71LOCmupE.lEk9eaS1ZyDw' and "
            "start_of_month='2020-01-01'") \
        .select("record_count").collect()[0][0] == 2





