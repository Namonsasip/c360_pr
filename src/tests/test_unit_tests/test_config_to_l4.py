from customer360.utilities.config_parser import l4_rolling_window, l4_rolling_ranked_window
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import functions as F

# this is what i have added

class TestConfigToL4:

    def test_rolling_ranked_window(self, project_context):
        spark = project_context['Spark']
        dummy_list = [
            [datetime.strptime("2019-01-01", "%Y-%m-%d"), "a", 1, 100],
            [datetime.strptime("2019-01-08", "%Y-%m-%d"), "b", 1, 80],
            [datetime.strptime("2019-01-15", "%Y-%m-%d"), "c", 1, 60],
            [datetime.strptime("2019-01-22", "%Y-%m-%d"), "d", 1, 40],
            [datetime.strptime("2019-01-29", "%Y-%m-%d"), "e", 1, 20],

            [datetime.strptime("2019-01-29", "%Y-%m-%d"), "a", 1, 1],
            [datetime.strptime("2019-01-29", "%Y-%m-%d"), "b", 1, 1],
            [datetime.strptime("2019-01-29", "%Y-%m-%d"), "c", 1, 1],
            [datetime.strptime("2019-01-29", "%Y-%m-%d"), "d", 1, 1],
        ]
        rdd1 = spark.sparkContext.parallelize(dummy_list)
        input_df = spark.createDataFrame(rdd1, schema=StructType([StructField("start_of_week", DateType(), True),
                                                                  StructField("channel", StringType(), True),
                                                                  StructField("id", IntegerType(), True),
                                                                  StructField("value", IntegerType(), True)]))

        config_l4_rolling_window = {
            'partition_by': ["id", "channel"],
            'feature_list': {
                'sum': ["value"]
            },
            'read_from': "l2"
        }
        input_df = l4_rolling_window(input_df, config_l4_rolling_window)

        config_l4_rolling_ranked = {
            'feature_column': {
                'popular_topup_channel': "channel"
            },
            'partition_by': ["id", "start_of_week"],
            'order_by_column_prefix': "sum_value",
            'read_from': "l2",
            'to_join': True
        }
        df = l4_rolling_ranked_window(input_df, config_l4_rolling_ranked)
        df = df.filter(F.col("start_of_week") == F.to_date(F.lit("2019-01-29")))
        df.cache()

        assert df.select("popular_topup_channel_last_week").collect()[0][0] == 'd'
        assert df.select("popular_topup_channel_last_two_week").collect()[0][0] == 'c'
        assert df.select("popular_topup_channel_last_four_week").collect()[0][0] == 'a'
        assert df.select("popular_topup_channel_last_twelve_week").collect()[0][0] == 'a'
        print()
