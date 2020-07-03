from data_quality.dq_util import add_outlier_percentage_based_on_iqr
from pyspark.sql import SparkSession, Row

class TestDataQuality:
    def test_outlier(self, project_context):
        var_project_context = project_context['ProjectContext']
        spark_session = project_context['Spark']
        sc = spark_session.sparkContext

        df_metrics = sc.parallelize([
            ('123', '2019-10-01', 'feature_a', 0.50, 0.50),
            ('123', '2019-10-01', 'feature_b', 0.10, 0.20),
            ('123', '2019-10-01', 'feature_c', 0.40, 0.60),
        ])
        df_metrics = (spark_session.createDataFrame(
            df_metrics.map(
                lambda x: Row(
                    msisdn=x[0],
                    corresponding_date=x[1],
                    feature_column_name=x[2],
                    percentile_0_25=x[3],
                    percentile_0_75=x[4],
                    granularity="daily"
                )
            )
        ).withColumnRenamed("percentile_0_25", "`percentile_0.25`").withColumnRenamed("percentile_0_75", "`percentile_0.75`"))

        input_df = sc.parallelize([
                ('123', '2019-10-01', 0.1, 0.1, 0.1),
                ('123', '2019-10-01', 0.2, 0.2, 0.2),
                ('123', '2019-10-01', 0.3, 0.3, 0.3),
                ('123', '2019-10-01', 0.4, 0.4, 0.4),
                ('123', '2019-10-01', 0.5, 0.5, 0.5),
                ('123', '2019-10-01', 0.6, 0.6, 0.6),
                ('123', '2019-10-01', 0.7, 0.7, 0.7),
                ('123', '2019-10-01', 0.8, 0.8, 0.8),
                ('123', '2019-10-01', 0.9, 0.9, 0.9),
                ('123', '2019-10-01', 1.0, 1.0, 1.0)
            ])

        input_df = spark_session.createDataFrame(
            input_df.map(
                lambda x: Row(
                    msisdn=x[0],
                    partition_date=x[1],
                    feature_a=x[2],
                    feature_b=x[3],
                    feature_c=x[4],
                    granularity="daily"
                )
            )
        )

        output = add_outlier_percentage_based_on_iqr(input_df, df_metrics, "partition_date", [
            {"feature": "feature_a"},
            {"feature": "feature_b"},
            {"feature": "feature_c"}
        ])

        output.show()

        assert False
