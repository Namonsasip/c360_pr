# import pandas as pd
# from pyspark.shell import spark
from IPython.core.display import display
from pyspark.sql.types import *
from customer360.utilities.config_parser import node_from_config, expansion, l4_rolling_window
# from customer360.pipelines.data_engineering.nodes.customer_profile_nodes.to_l3.to_l3_nodes import *
# from datetime import datetime
import datetime
import pandas as pd
import random
from pyspark.sql import functions as F
from customer360.pipelines.data_engineering.nodes.revenue_nodes.to_l3 import merge_with_customer_prepaid_df, \
    merge_with_customer_postpaid_df


# import random
def generate_category(days, values_list):
    column = []
    for iTemp in range(0, days):
        rand = random.randint(0, len(values_list) - 1)
        column.append(values_list[rand])
    return column


def generate_int(days, min_num, max_num):
    return [random.randint(min_num, max_num) for iTemp in range(0, days)]


def generate_day_id(min_dt, max_dt):
    my_dates_list = pd.date_range(min_dt, max_dt).tolist()
    day_id = [iTemp.date().strftime("%d-%m-%Y") for iTemp in my_dates_list]
    return day_id


def date_diff(min_date, max_date):
    return (max_date - min_date).days + 1  # inclusive


# Global Variables
min_date = datetime.date(2020, 1, 31)
max_date = datetime.date(2020, 2, 29)
days = date_diff(min_date, max_date)  # range from min_date to max_date
random.seed(100)


class TestUnitRevenue:

    def test_revenue_features(self, project_context):
        from datetime import datetime
        from pyspark.sql.types import IntegerType, DateType, StructType, StructField, LongType
        var_project_context = project_context['ProjectContext']
        spark = project_context['Spark']
        date_str1 = "2020-01-31"
        date_str2 = "2019-12-31"
        date_str3 = "2019-11-30"
        date_str4 = "2019-10-31"

        dummy_list_post = [
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1-WSTCZGV', 'W-IN-13-6301-0971582', 305, 7, 297, 291, 0, 0, 6,
             0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 3, 0, 7],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1-X5IPPQC', 'W-IN-13-6301-0933162', 499, 5, 493, 493, 0, 0, 0,
             0, 5, 1, 7, 5, 0, 0, 0, 0, 0, 8, 0, 6, 4, 0, 0, 0, 0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1-XLS867F', 'W-IN-12-6301-0581872', 114, 0, 114, 114, 0, 0, 0,
             0, 0, 5, 0, 0, 5, 4, 0, 0, 4, 0, 0, 0, 0, 0, 0, 9, 0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1-QBHFYLJ', 'W-IN-12-6301-1057594', 641, 202, 438, 438, 0, 0, 0,
             0, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1-Q572J2A', 'W-IN-14-6301-0898804', 483, 483, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 8, 0, 0, 8, 0, 0, 0, 0, 0, 7, 0],

            [datetime.strptime(date_str2, "%Y-%m-%d"), '1-WSTCZGV', 'W-IN-13-6301-0971582', 306, 8, 298, 292, 1, 1, 7,
             1, 1, 1, 1, 1, 1, 1, 10, 1, 1, 1, 1, 1, 1, 1, 4, 1, 8],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '1-X5IPPQC', 'W-IN-13-6301-0933162', 499, 5, 493, 493, 0, 0, 0,
             0, 5, 1, 7, 5, 0, 0, 0, 0, 0, 8, 0, 6, 4, 0, 0, 0, 0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '1-XLS867F', 'W-IN-12-6301-0581872', 114, 0, 114, 114, 0, 0, 0,
             0, 0, 5, 0, 0, 5, 4, 0, 0, 4, 0, 0, 0, 0, 0, 0, 9, 0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '1-QBHFYLJ', 'W-IN-12-6301-1057594', 641, 202, 438, 438, 0, 0, 0,
             0, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '1-Q572J2A', 'W-IN-14-6301-0898804', 483, 483, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 8, 0, 0, 8, 0, 0, 0, 0, 0, 7, 0],

            [datetime.strptime(date_str3, "%Y-%m-%d"), '1-WSTCZGV', 'W-IN-13-6301-0971582', 307, 9, 299, 293, 2, 2, 8,
             2, 2, 2, 2, 2, 2, 2, 11, 2, 2, 2, 2, 2, 2, 2, 5, 2, 9],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '1-X5IPPQC', 'W-IN-13-6301-0933162', 499, 5, 493, 493, 0, 0, 0,
             0, 5, 1, 7, 5, 0, 0, 0, 0, 0, 8, 0, 6, 4, 0, 0, 0, 0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '1-XLS867F', 'W-IN-12-6301-0581872', 114, 0, 114, 114, 0, 0, 0,
             0, 0, 5, 0, 0, 5, 4, 0, 0, 4, 0, 0, 0, 0, 0, 0, 9, 0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '1-QBHFYLJ', 'W-IN-12-6301-1057594', 641, 202, 438, 438, 0, 0, 0,
             0, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '1-Q572J2A', 'W-IN-14-6301-0898804', 483, 483, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 8, 0, 0, 8, 0, 0, 0, 0, 0, 7, 0],

            [datetime.strptime(date_str4, "%Y-%m-%d"), '1-WSTCZGV', 'W-IN-13-6301-0971582', 308, 10, 300, 294, 3, 3, 9,
             3, 3, 3, 3, 3, 3, 3, 12, 3, 3, 3, 3, 3, 3, 3, 6, 3, 10],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '1-X5IPPQC', 'W-IN-13-6301-0933162', 499, 5, 493, 493, 0, 0, 0,
             0, 5, 1, 7, 5, 0, 0, 0, 0, 0, 8, 0, 6, 4, 0, 0, 0, 0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '1-XLS867F', 'W-IN-12-6301-0581872', 114, 0, 114, 114, 0, 0, 0,
             0, 0, 5, 0, 0, 5, 4, 0, 0, 4, 0, 0, 0, 0, 0, 0, 9, 0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '1-QBHFYLJ', 'W-IN-12-6301-1057594', 641, 202, 438, 438, 0, 0, 0,
             0, 1, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '1-Q572J2A', 'W-IN-14-6301-0898804', 483, 483, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 8, 0, 0, 8, 0, 0, 0, 0, 0, 7, 0],

        ]

        rdd1 = spark.sparkContext.parallelize(dummy_list_post)
        df_post = spark.createDataFrame(rdd1, schema=StructType([StructField("mgt_mth_id", DateType(), True),
                                                                 StructField("sub_id", StringType(), True),
                                                                 StructField("bill_stmnt_id", StringType(), True),
                                                                 StructField("total_net_revenue", IntegerType(), True),
                                                                 StructField("total_voice_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_vas_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_gprs_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_sms_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_mms_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_others_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_ir_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_idd_net_revenue", IntegerType(),
                                                                             True),
                                                                 StructField("total_voice_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_vas_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_gprs_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_sms_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_mms_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_others_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_ir_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_idd_net_tariff_rev_mth",
                                                                             IntegerType(), True),
                                                                 StructField("total_voice_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_vas_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_gprs_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_sms_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_mms_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_others_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_ir_net_tariff_rev_ppu",
                                                                             IntegerType(), True),
                                                                 StructField("total_idd_net_tariff_rev_ppu",
                                                                             IntegerType(), True)]))
        dummy_list_pre = [
            [datetime.strptime(date_str1, "%Y-%m-%d"), '1', '3GB13', 352, 'PLUG IN', 'null',
             datetime.strptime('2019-05-02', "%Y-%m-%d"), 60, 0, 0, 0, 0, 0, 0, 0, 60, 60, 0, 0, 0, 0, 0, 60, 0, 0, 0,
             0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '2', '3GA33', 495, 'PLUG IN', 'null',
             datetime.strptime('2016-02-19', "%Y-%m-%d"), 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '3', '3G455', 297, 'PLUG IN', 'null',
             datetime.strptime('2019-01-16', "%Y-%m-%d"), 0, 0, 0, 52, 0, 0, 0, 0, 52, 37, 0, 0, 0, 52, 0, 0, 0, 0, 0,
             0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '4', '3G737', 512, 'PLUG IN', 'null',
             datetime.strptime('2019-09-18', "%Y-%m-%d"), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str1, "%Y-%m-%d"), '5', '3GA26', 198, 'PLUG IN', 'null',
             datetime.strptime('2016-02-16', "%Y-%m-%d"), 2, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0],

            [datetime.strptime(date_str2, "%Y-%m-%d"), '1', '3GB13', 352, 'PLUG IN', 'null',
             datetime.strptime('2019-05-02', "%Y-%m-%d"), 61, 1, 1, 1, 1, 1, 1, 1, 61, 61, 1, 1, 1, 1, 1, 61, 1, 1, 1,
             1],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '2', '3GA33', 495, 'PLUG IN', 'null',
             datetime.strptime('2016-02-19', "%Y-%m-%d"), 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '3', '3G455', 297, 'PLUG IN', 'null',
             datetime.strptime('2019-01-16', "%Y-%m-%d"), 0, 0, 0, 52, 0, 0, 0, 0, 52, 37, 0, 0, 0, 52, 0, 0, 0, 0, 0,
             0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '4', '3G737', 512, 'PLUG IN', 'null',
             datetime.strptime('2019-09-18', "%Y-%m-%d"), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str2, "%Y-%m-%d"), '5', '3GA26', 198, 'PLUG IN', 'null',
             datetime.strptime('2016-02-16', "%Y-%m-%d"), 2, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0],

            [datetime.strptime(date_str3, "%Y-%m-%d"), '1', '3GB13', 352, 'PLUG IN', 'null',
             datetime.strptime('2019-05-02', "%Y-%m-%d"), 62, 2, 2, 2, 2, 2, 2, 2, 62, 62, 2, 2, 2, 2, 2, 62, 2, 2, 2,
             2],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '2', '3GA33', 495, 'PLUG IN', 'null',
             datetime.strptime('2016-02-19', "%Y-%m-%d"), 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '3', '3G455', 297, 'PLUG IN', 'null',
             datetime.strptime('2019-01-16', "%Y-%m-%d"), 0, 0, 0, 52, 0, 0, 0, 0, 52, 37, 0, 0, 0, 52, 0, 0, 0, 0, 0,
             0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '4', '3G737', 512, 'PLUG IN', 'null',
             datetime.strptime('2019-09-18', "%Y-%m-%d"), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str3, "%Y-%m-%d"), '5', '3GA26', 198, 'PLUG IN', 'null',
             datetime.strptime('2016-02-16', "%Y-%m-%d"), 2, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0],

            [datetime.strptime(date_str4, "%Y-%m-%d"), '1', '3GB13', 352, 'PLUG IN', 'null',
             datetime.strptime('2019-05-02', "%Y-%m-%d"), 63, 3, 3, 3, 3, 3, 3, 3, 63, 63, 3, 3, 3, 3, 3, 63, 3, 3, 3,
             3],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '2', '3GA33', 495, 'PLUG IN', 'null',
             datetime.strptime('2016-02-19', "%Y-%m-%d"), 3, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '3', '3G455', 297, 'PLUG IN', 'null',
             datetime.strptime('2019-01-16', "%Y-%m-%d"), 0, 0, 0, 52, 0, 0, 0, 0, 52, 37, 0, 0, 0, 52, 0, 0, 0, 0, 0,
             0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '4', '3G737', 512, 'PLUG IN', 'null',
             datetime.strptime('2019-09-18', "%Y-%m-%d"), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [datetime.strptime(date_str4, "%Y-%m-%d"), '5', '3GA26', 198, 'PLUG IN', 'null',
             datetime.strptime('2016-02-16', "%Y-%m-%d"), 2, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0],

        ]

        rdd2 = spark.sparkContext.parallelize(dummy_list_pre)
        df_pre = spark.createDataFrame(rdd2, schema=StructType([StructField("month_id", DateType(), True),
                                                                StructField("access_method_num", StringType(), True),
                                                                StructField("package_id", StringType(), True),
                                                                StructField("total_amount_mth", IntegerType(), True),
                                                                StructField("source_usage", StringType(), True),
                                                                StructField("allocate_group", StringType(), True),
                                                                StructField("register_date", DateType(), True),
                                                                StructField("total_voice_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_sms_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_mms_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_gprs_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_wifi_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_ir_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_idd_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_others_net_revenue_af",
                                                                            IntegerType(), True),
                                                                StructField("total_gross_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_net_revenue_af", IntegerType(),
                                                                            True),
                                                                StructField("total_voice_net_tariff_rev_mth_af",
                                                                            IntegerType(), True),
                                                                StructField("total_sms_net_tariff_rev_mth_af",
                                                                            IntegerType(), True),
                                                                StructField("total_mms_net_tariff_rev_mth_af",
                                                                            IntegerType(), True),
                                                                StructField("total_gprs_net_tariff_rev_mth_af",
                                                                            IntegerType(), True),
                                                                StructField("total_others_net_tariff_rev_mth_af",
                                                                            IntegerType(), True),
                                                                StructField("total_voice_net_tariff_rev_ppu_af",
                                                                            IntegerType(), True),
                                                                StructField("total_sms_net_tariff_rev_ppu_af",
                                                                            IntegerType(), True),
                                                                StructField("total_mms_net_tariff_rev_ppu_af",
                                                                            IntegerType(), True),
                                                                StructField("total_gprs_net_tariff_rev_ppu_af",
                                                                            IntegerType(), True),
                                                                StructField("total_others_net_tariff_rev_ppu_af",
                                                                            IntegerType(), True), ]))

        dummy_list_customer_post = [
            ['1', datetime.strptime(date_str1, "%Y-%m-%d"), '1-WSTCZGV', 'Post-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str1, "%Y-%m-%d"), '1-X5IPPQC', 'Post-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str1, "%Y-%m-%d"), '1-XLS867F', 'Post-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str1, "%Y-%m-%d"), '1-QBHFYLJ', 'Post-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str1, "%Y-%m-%d"), '1-Q572J2A', 'Post-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str2, "%Y-%m-%d"), '1-WSTCZGV', 'Post-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str2, "%Y-%m-%d"), '1-X5IPPQC', 'Post-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str2, "%Y-%m-%d"), '1-XLS867F', 'Post-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str2, "%Y-%m-%d"), '1-QBHFYLJ', 'Post-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str2, "%Y-%m-%d"), '1-Q572J2A', 'Post-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str3, "%Y-%m-%d"), '1-WSTCZGV', 'Post-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str3, "%Y-%m-%d"), '1-X5IPPQC', 'Post-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str3, "%Y-%m-%d"), '1-XLS867F', 'Post-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str3, "%Y-%m-%d"), '1-QBHFYLJ', 'Post-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str3, "%Y-%m-%d"), '1-Q572J2A', 'Post-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str4, "%Y-%m-%d"), '1-WSTCZGV', 'Post-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str4, "%Y-%m-%d"), '1-X5IPPQC', 'Post-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str4, "%Y-%m-%d"), '1-XLS867F', 'Post-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str4, "%Y-%m-%d"), '1-QBHFYLJ', 'Post-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str4, "%Y-%m-%d"), '1-Q572J2A', 'Post-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
        ]

        rdd3 = spark.sparkContext.parallelize(dummy_list_customer_post)
        df_cust_post = spark.createDataFrame(rdd3,
                                             schema=StructType([StructField("access_method_num", StringType(), True),
                                                                StructField("register_date", DateType(), True),
                                                                StructField("subscription_identifier", StringType(),
                                                                            True),
                                                                StructField("charge_type", StringType(), True),
                                                                StructField("subscription_status", StringType(), True),
                                                                StructField("age", IntegerType(), True),
                                                                StructField("gender", StringType(), True),
                                                                StructField("contract_expiry", DateType(), True),
                                                                StructField("time_with_current_package", IntegerType(),
                                                                            True),
                                                                StructField("customer_segment", StringType(), True),
                                                                StructField("serenade_status", StringType(), True),
                                                                StructField("network_type", StringType(), True),
                                                                StructField("acquisition_channel", StringType(), True),
                                                                StructField("subscriber_tenure", IntegerType(), True),
                                                                StructField("activation_region", StringType(), True),
                                                                StructField("current_package_name", StringType(),
                                                                            True),
                                                                StructField("current_package_id", StringType(), True),
                                                                StructField("company_type", StringType(), True),
                                                                StructField("national_id_card", StringType(), True),
                                                                StructField("cust_active_this_month", StringType(),
                                                                            True),
                                                                StructField("billing_account_no", StringType(), True),
                                                                StructField("norms_net_revenue", IntegerType(), True),
                                                                StructField("partition_month", DateType(), True),
                                                                ]))
        dummy_list_customer_pre = [
            ['1', datetime.strptime(date_str1, "%Y-%m-%d"), '1-WSTCZGV', 'Pre-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str1, "%Y-%m-%d"), '1-X5IPPQC', 'Pre-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str1, "%Y-%m-%d"), '1-XLS867F', 'Pre-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str1, "%Y-%m-%d"), '1-QBHFYLJ', 'Pre-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str1, "%Y-%m-%d"), '1-Q572J2A', 'Pre-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2020-01-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str2, "%Y-%m-%d"), '1-WSTCZGV', 'Pre-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str2, "%Y-%m-%d"), '1-X5IPPQC', 'Pre-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str2, "%Y-%m-%d"), '1-XLS867F', 'Pre-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str2, "%Y-%m-%d"), '1-QBHFYLJ', 'Pre-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str2, "%Y-%m-%d"), '1-Q572J2A', 'Pre-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-12-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str3, "%Y-%m-%d"), '1-WSTCZGV', 'Pre-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str3, "%Y-%m-%d"), '1-X5IPPQC', 'Pre-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str3, "%Y-%m-%d"), '1-XLS867F', 'Pre-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str3, "%Y-%m-%d"), '1-QBHFYLJ', 'Pre-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str3, "%Y-%m-%d"), '1-Q572J2A', 'Pre-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-11-01', "%Y-%m-%d")],

            ['1', datetime.strptime(date_str4, "%Y-%m-%d"), '1-WSTCZGV', 'Pre-paid', 'Active', 60, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 16, 'NU',
             '3G_Buffet_Con 499Dis299 1Mbps UL SWifi', '1', 'RESIDENTIAL_MOC', '1', 'Y', '31900025717337', 304,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['2', datetime.strptime(date_str4, "%Y-%m-%d"), '1-X5IPPQC', 'Pre-paid', 'Active', 64, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 10, 'SL',
             '4G_HotDeal MAX 499 7GB UL SWifi', '2', 'RESIDENTIAL_MOC', '2', 'Y', '31900026039944', 499,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['3', datetime.strptime(date_str4, "%Y-%m-%d"), '1-XLS867F', 'Pre-paid', 'Active', 47, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', '3G', 'TELEWIZ', 9, 'CB',
             '3G_Power4 MAXX 15GB UL SWifi Free12Bill', '3', 'RESIDENTIAL_MOC', '3', 'Y', '31900026297559', 114,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['4', datetime.strptime(date_str4, "%Y-%m-%d"), '1-QBHFYLJ', 'Pre-paid', 'Active', 61, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Emerald', 'Emerald', '3G', 'TELEWIZ', 23, 'CW',
             '3G_Buffet Plus_450B 2Mbps UL SWifi_EX', '4', 'RESIDENTIAL_MOC', '4', 'Y', '31800018518894', 634,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
            ['5', datetime.strptime(date_str4, "%Y-%m-%d"), '1-Q572J2A', 'Pre-paid', 'Active', 58, 'Female',
             datetime.strptime('2020-04-30', "%Y-%m-%d"), 20, 'Classic', 'Classic', 'FBB', 'TELEWIZ', 23, 'XL',
             'Power4 Package 50/20 Mbps 799 THB', '5', 'RESIDENTIAL_MOC', '5', 'Y', '31800018339489', 483,
             datetime.strptime('2019-10-01', "%Y-%m-%d")],
        ]

        rdd4 = spark.sparkContext.parallelize(dummy_list_customer_pre)
        df_cust_pre = spark.createDataFrame(rdd4,
                                            schema=StructType([StructField("access_method_num", StringType(), True),
                                                               StructField("register_date", DateType(), True),
                                                               StructField("subscription_identifier", StringType(),
                                                                           True),
                                                               StructField("charge_type", StringType(), True),
                                                               StructField("subscription_status", StringType(), True),
                                                               StructField("age", IntegerType(), True),
                                                               StructField("gender", StringType(), True),
                                                               StructField("contract_expiry", DateType(), True),
                                                               StructField("time_with_current_package", IntegerType(),
                                                                           True),
                                                               StructField("customer_segment", StringType(), True),
                                                               StructField("serenade_status", StringType(), True),
                                                               StructField("network_type", StringType(), True),
                                                               StructField("acquisition_channel", StringType(), True),
                                                               StructField("subscriber_tenure", IntegerType(), True),
                                                               StructField("activation_region", StringType(), True),
                                                               StructField("current_package_name", StringType(),
                                                                           True),
                                                               StructField("current_package_id", StringType(), True),
                                                               StructField("company_type", StringType(), True),
                                                               StructField("national_id_card", StringType(), True),
                                                               StructField("cust_active_this_month", StringType(),
                                                                           True),
                                                               StructField("billing_account_no", StringType(), True),
                                                               StructField("norms_net_revenue", IntegerType(), True),
                                                               StructField("partition_month", DateType(), True),
                                                               ]))

        l3_postpaid = node_from_config(df_post, var_project_context.catalog.load(
            'params:l3_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'))

        l3_prepaid = node_from_config(df_pre, var_project_context.catalog.load(
            'params:l3_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'))

        l3_postpaid_cust = merge_with_customer_postpaid_df(l3_postpaid, df_cust_post)
        l3_prepaid_cust = merge_with_customer_prepaid_df(l3_prepaid, df_cust_pre)

        korn = l4_rolling_window(l3_postpaid_cust, var_project_context.catalog.load(
            'params:l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly_int'))

        korn2 = node_from_config(korn, var_project_context.catalog.load(
            'params:l4_revenue_postpaid_ru_f_sum_revenue_by_service_monthly'))

        korn3 = l4_rolling_window(l3_prepaid_cust, var_project_context.catalog.load(
            'params:l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly_int'))

        korn4 = node_from_config(korn3, var_project_context.catalog.load(
            'params:l4_revenue_prepaid_ru_f_sum_revenue_by_service_monthly'))

        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.98
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 62
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.98
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 62
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.98
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 62
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.32
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 186
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.32
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 186
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.32
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 186
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.96
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 63
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.96
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 63
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 61
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.96
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 63
        assert korn4.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 1

        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 27
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.29
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 9
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.88
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 4
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 4
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 4
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.66
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 4
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 15
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.26
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 4
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_ppu_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 12
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.83
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 11
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.9
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_tariff_rev_mth_monthly_last_month_over_three_month").collect()[0][0] == 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_ir_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_idd_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 10
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 27
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.29
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_three_month").collect()[0][0] == 9
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_voice_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.88
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_mms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 292
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 292
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 292
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 294
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 292
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 879
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33 <= 0.34
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month").collect()[0][0] == 292
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_three_month").collect()[0][0] == 293
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_gprs_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 306
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 306
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 306
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 308
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 306
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 921
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33 <= 0.34
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month").collect()[0][0] == 306
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_three_month").collect()[0][0] == 307
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 298
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_revenue_monthly_last_three_month").collect()[0][0] == 298
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_vas_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 298
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_revenue_monthly_last_three_month").collect()[0][0] == 300
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_vas_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 298
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_revenue_monthly_last_three_month").collect()[0][0] == 897
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_vas_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33 <= 0.34
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_revenue_monthly_last_month").collect()[0][0] == 298
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_revenue_monthly_last_three_month").collect()[0][0] == 299
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_vas_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.99 <= 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 3
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.33
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 6
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.16
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_three_month").collect()[0][0] == 2
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_sms_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.5
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 7
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 7
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "min_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] == 1
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 7
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 9
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "max_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.77
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 7
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 24
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "sum_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.29
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month").collect()[0][0] == 7
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_three_month").collect()[0][0] == 8
        assert korn2.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
            "avg_rev_arpu_total_others_net_revenue_monthly_last_month_over_three_month").collect()[0][0] >= 0.87
    # test 1. Feature Reference = 798  -- >  ARPU Data w/o ITC, Roaming   > rev_arpu_total_gprs_net_revenue
    # print("test 1. Feature Reference = 798  -- >  ARPU Data w/o ITC, Roaming   > rev_arpu_total_gprs_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_revenue").collect()[0][0] == 291
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_revenue").collect()[0][0] == 0
    # print("test 1. ARPU Data w/o ITC, Roaming   ---   success ")
    #
    # # test 2  Feature Reference = 799  -- > ARPU total -- > rev_arpu_total_revenue
    # print("test 2  Feature Reference = 799  -- > ARPU total -- > rev_arpu_total_revenue")
    # assert \
    #     l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #         "rev_arpu_total_revenue").collect()[0][
    #         0] == 305
    # assert \
    #     l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #         "rev_arpu_total_revenue").collect()[0][
    #         0] == 60
    #
    # print("test 2 ARPU total   ---   success ")
    #
    # # test 3 Feature Reference = 800 -- > ARPU International Roaming -- > rev_arpu_total_ir_net_revenue
    # print("test 3 Feature Reference = 800 -- > ARPU International Roaming -- > rev_arpu_total_ir_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_ir_net_revenue").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_ir_net_revenue").collect()[0][0] == 0
    # print("test 3 ARPU International Roaming   ---   success ")
    #
    # # test 4Feature Reference = 801 --> ARPU Voice (incl. VoLTE) w/o ITC, Roaming -- > rev_arpu_total_voice_net_revenue
    # print(
    #     "test 4Feature Reference = 801 --> ARPU Voice (incl. VoLTE) w/o ITC, Roaming -- > rev_arpu_total_voice_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_revenue").collect()[0][0] == 7
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_revenue").collect()[0][0] == 60
    # print("test 4 ARPU Voice (incl. VoLTE) w/o ITC, Roaming   ---   success ")
    #
    # # test 5 Feature Reference = 802 --> ARPU SMS w/o ITC, Roaming -- > rev_arpu_total_sms_net_revenue
    # print("test 5 Feature Reference = 802 --> ARPU SMS w/o ITC, Roaming -- > rev_arpu_total_sms_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_revenue").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_revenue").collect()[0][0] == 0
    # print("test 5 ARPU SMS w/o ITC, Roaming   ---   success ")
    #
    # # test 6 Feature Reference = 804 --> ARPU MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_revenue
    # print("test 6 Feature Reference = 804 --> ARPU MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_revenue").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_revenue").collect()[0][0] == 0
    # print("test 6 ARPU MMS w/o ITC, Roaming   ---   success ")
    #
    # # test  7 Feature Reference = 806 --> ARPU Others -- > rev_arpu_total_others_net_revenue
    # print("test  7 Feature Reference = 806 --> ARPU Others -- > rev_arpu_total_others_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_revenue").collect()[0][0] == 6
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_revenue").collect()[0][0] == 0
    # print("test 7  ARPU Others   ---   success ")
    #
    # # test 8 Feature Reference = 803 --> ARPU VAS w/o ITC, Roaming -- > rev_arpu_total_vas_net_revenue
    # print("test 8 Feature Reference = 803 --> ARPU VAS w/o ITC, Roaming -- > rev_arpu_total_vas_net_revenue")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_vas_net_revenue").collect()[0][0] == 297
    # print("test 8 Feature Reference = 803 --> ARPU VAS w/o ITC, Roaming   ---   success ")
    #
    # # test 9 Feature Reference = 811 --> ARPU additional package Data w/o ITC, Roaming -- > rev_arpu_total_gprs_net_tariff_rev_mth
    # print(
    #     "test 9 Feature Reference = 811 --> ARPU additional package Data w/o ITC, Roaming -- > rev_arpu_total_gprs_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_tariff_rev_mth").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 9 Feature Reference = 811 --> ARPU additional package Data w/o ITC, Roaming   ---   success ")
    #
    # # test 10 Feature Reference = 814 --> ARPU additional package Voice (incl. VoLTE) w/o ITC, Roaming -- > rev_arpu_total_voice_net_tariff_rev_mth
    # print(
    #     "test 10 Feature Reference = 814 --> ARPU additional package Voice (incl. VoLTE) w/o ITC, Roaming -- > rev_arpu_total_voice_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_mth").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_mth").collect()[0][0] == 0
    # print(
    #     "test 10 Feature Reference = 814 --> ARPU additional package Voice (incl. VoLTE) w/o ITC, Roaming   ---   success ")
    #
    # # test 11 Feature Reference = 813 --> ARPU additional package SMS w/o ITC, Roaming -- > rev_arpu_total_sms_net_tariff_rev_mth
    # print(
    #     "test 11 Feature Reference = 813 --> ARPU additional package SMS w/o ITC, Roaming -- > rev_arpu_total_sms_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_mth").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 11 Feature Reference = 813 --> ARPU additional package SMS w/o ITC, Roaming   ---   success ")
    #
    # # test 12 Feature Reference = 815 --> ARPU additional package MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_tariff_rev_mth
    # print(
    #     "test 12 Feature Reference = 815 --> ARPU additional package MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_tariff_rev_mth").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 12 Feature Reference = 815 --> ARPU additional package MMS w/o ITC, Roaming  ---   success ")
    #
    # # test 13 Feature Reference = 808 --> ARPU additional package others w/o ITC, Roaming -- > rev_arpu_total_others_net_tariff_rev_mth
    # print(
    #     "test 13 Feature Reference = 808 --> ARPU additional package others w/o ITC, Roaming -- > rev_arpu_total_others_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_tariff_rev_mth").collect()[0][0] == 9
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 13 Feature Reference = 808 --> ARPU additional package others w/o ITC, Roaming ---   success ")
    #
    # # test 14 Feature Reference = 809 --> ARPU additional package IDD w/o ITC, Roaming	-- > rev_arpu_total_idd_net_tariff_rev_mth
    # print(
    #     "test 14 Feature Reference = 809 --> ARPU additional package IDD w/o ITC, Roaming	-- > rev_arpu_total_idd_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_idd_net_tariff_rev_mth").collect()[0][0] == 0
    # print(
    #     "test 10 Feature Reference = 814 --> ARPU additional package Voice (incl. VoLTE) w/o ITC, Roaming   ---   success ")
    #
    # # test 15 Feature Reference = 810 --> ARPU additional package IR w/o ITC, Roaming-- > rev_arpu_total_ir_net_tariff_rev_mth
    # print(
    #     "test 15 Feature Reference = 810 --> ARPU additional package IR w/o ITC, Roaming-- > rev_arpu_total_ir_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_ir_net_tariff_rev_mth").collect()[0][0] == 0
    # print(" test 15 Feature Reference = 810 --> ARPU additional package IR w/o ITC, Roaming   ---   success ")
    #
    # # test 16 Feature Reference = 812 --> ARPU additional package VAS w/o ITC, Roaming -- > rev_arpu_total_vas_net_tariff_rev_mth
    # print(
    #     "test 16 Feature Reference = 812 --> ARPU additional package VAS w/o ITC, Roaming -- > rev_arpu_total_vas_net_tariff_rev_mth")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_vas_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 16 Feature Reference = 812 --> ARPU additional package VAS w/o ITC, Roaming  ---   success ")
    #
    # # test 17 Feature Reference = 878 --> PPU VAS Revenue w/o ITC, Roaming -- > rev_arpu_total_vas_net_tariff_rev_ppu
    # print(
    #     "test 17 Feature Reference = 878 --> PPU VAS Revenue w/o ITC, Roaming -- > rev_arpu_total_vas_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_vas_net_tariff_rev_mth").collect()[0][0] == 0
    # print("test 17 Feature Reference = 878 --> PPU VAS Revenue w/o ITC, Roaming  ---   success ")
    #
    # # test 18 Feature Reference = 879 --> PPU Revenue MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_tariff_rev_ppu
    # print(
    #     "test 18 Feature Reference = 879 --> PPU Revenue MMS w/o ITC, Roaming -- > rev_arpu_total_mms_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_mms_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 18 Feature Reference = 879 --> PPU Revenue MMS w/o ITC, Roaming  ---   success ")
    #
    # # test 19 Feature Reference = 880 --> PPU Revenue SMS w/o ITC, Roaming  -- > rev_arpu_total_sms_net_tariff_rev_ppu
    # print(
    #     "test 19 Feature Reference = 880 --> PPU Revenue SMS w/o ITC, Roaming  -- > rev_arpu_total_sms_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 19 Feature Reference = 880 --> PPU Revenue SMS w/o ITC, Roaming  ---   success ")
    #
    # # test 20 Feature Reference = 881 --> PPU Revenue Data w/o ITC, Roaming  -- > rev_arpu_total_gprs_net_tariff_rev_ppu
    # print(
    #     "test 20 Feature Reference = 881 --> PPU Revenue Data w/o ITC, Roaming  -- > rev_arpu_total_gprs_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_gprs_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 20 Feature Reference = 881 --> PPU Revenue Data w/o ITC, Roaming  ---   success ")
    #
    # # test 21 Feature Reference = 882 --> PPU Revenue others w/o ITC, Roaming  -- > rev_arpu_total_others_net_tariff_rev_ppu
    # print(
    #     "test 21 Feature Reference = 882 --> PPU Revenue others w/o ITC, Roaming  -- > rev_arpu_total_others_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_tariff_rev_ppu").collect()[0][0] == 3
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_others_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 21 Feature Reference = 882 --> PPU Revenue others w/o ITC, Roaming  ---   success ")
    #
    # # test 22 Feature Reference = 883 --> PPU Revenue Voice (incl. VoLTE) w/o ITC, Roaming  -- > rev_arpu_total_voice_net_tariff_rev_ppu
    # print(
    #     "test 22 Feature Reference = 883 --> PPU Revenue Voice (incl. VoLTE) w/o ITC, Roaming  -- > rev_arpu_total_voice_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0] == 60
    # print("test 22 Feature Reference = 883 --> PPU Revenue Voice (incl. VoLTE) w/o ITC, Roaming  ---   success ")
    #
    # # test 23 Feature Reference = 884 --> 	PPU Revenue IR w/o ITC, Roaming  -- > rev_arpu_total_ir_net_tariff_rev_ppu
    # print(
    #     "test 23 Feature Reference = 884 --> 	PPU Revenue IR w/o ITC, Roaming  -- > rev_arpu_total_ir_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_ir_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_ir_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 23 Feature Reference = 884 --> 	PPU Revenue IR w/o ITC, Roaming  ---   success ")
    #
    # # test 24 Feature Reference = 885 --> 	Average PPU IDD Revenue w/o ITC, Roaming  -- > rev_arpu_total_idd_net_tariff_rev_ppu
    # print(
    #     "test 24 Feature Reference = 885 --> 	Average PPU IDD Revenue w/o ITC, Roaming  -- > rev_arpu_total_idd_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_idd_net_tariff_rev_ppu").collect()[0][0] == 7
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_idd_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 23 Feature Reference = 884 --> 	PPU Revenue IR w/o ITC, Roaming  ---   success ")
    #
    # # test 25 Feature Reference = 890 --> 	Average PPU Revenue Voice (incl. VoLTE) w/o ITC, Roaming  -- > rev_arpu_total_voice_net_tariff_rev_ppu
    # print(
    #     "test 25 Feature Reference = 890 --> 	Average PPU Revenue Voice (incl. VoLTE) w/o ITC, Roaming  -- > rev_arpu_total_voice_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV'").select(
    #     "rev_arpu_total_voice_net_tariff_rev_ppu").collect()[0][0] == 61
    # print("test 23 Feature Reference = 884 --> 	PPU Revenue IR w/o ITC, Roaming  ---   success ")
    #
    # # test 26 Feature Reference = 891 --> 	Average PPU Revenue SMS/MMS w/o ITC, Roaming  -- > rev_arpu_total_sms_net_tariff_rev_ppu
    # print(
    #     "test 26 Feature Reference = 891 --> 	Average PPU Revenue SMS/MMS w/o ITC, Roaming  -- > rev_arpu_total_sms_net_tariff_rev_ppu")
    # assert l3_postpaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0] == 0
    # assert l3_prepaid_cust.where("subscription_identifier = '1-WSTCZGV' and start_of_month='2020-01-01'").select(
    #     "rev_arpu_total_sms_net_tariff_rev_ppu").collect()[0][0] == 0
    # print("test 26 Feature Reference = 891 --> 	Average PPU Revenue SMS/MMS w/o ITC, Roaming  ---   success ")
