import pyspark.sql.functions as f
from kedro.pipeline import Pipeline, node
from pyspark.sql import DataFrame
import os

running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def build_ops_report_dataset(data_frame: DataFrame) -> DataFrame:
    """
    Purpose: To build the C360 operational reports
    :param data_frame:
    :return:
    """
    # ops_report = data_frame.where("table_name not like '%l4%'")
    ops_report = data_frame.where("table_name not in ('int_l0_streaming_vimmi_table')")

    # Finding the latest record of every dataset based on updated_on and target_max_data_load_date column.
    ops_report = ops_report.withColumn("rn", f.expr(
        "row_number() over (partition by table_name order by updated_on desc,target_max_data_load_date desc)"))
    ops_report = ops_report.filter("rn = 1").drop("rn")

    # Calculate the Domain name and Feature layer from dataset path
    if running_environment == 'on_premise':
        ops_report = ops_report.withColumn("Domain_Name", f.split(f.col("table_path"), '/')[5]) \
            .withColumn("Feature_Layer", f.split(f.col("table_path"), '/')[6]) \
            .withColumnRenamed("table_name", "Dataset_Name") \
            .withColumnRenamed("updated_on", "Last_Data_Refresh_Date")
    else:
        ops_report = ops_report.withColumn("Domain_Name", f.split(f.col("table_path"), '/')[4]) \
            .withColumn("Feature_Layer", f.split(f.col("table_path"), '/')[5]) \
            .withColumnRenamed("table_name", "Dataset_Name") \
            .withColumnRenamed("updated_on", "Last_Data_Refresh_Date")

    # Calculate all the relevant columns of ops report.
    ops_report = ops_report.withColumn("Days_Since_Last_Refresh",
                                       f.datediff(f.current_date(), f.col("Last_Data_Refresh_Date"))) \
        .withColumnRenamed("target_max_data_load_date", "Latest_Data_Partition_Available") \
        .withColumn("Data_Refresh_Freq", f.when(f.lower(f.col("Feature_Layer")) == 'l1_features', "daily").when(
        f.lower(f.col("Feature_Layer")) == 'l2_features', "weekly").when(
        f.lower(f.col("Feature_Layer")) == 'l3_features', "monthly").when(
        (f.lower(f.col("Feature_Layer")) == 'l4_features') & (f.col("target_layer").like("%l4_daily%")), "daily").when(
        (f.lower(f.col("Feature_Layer")) == 'l4_features') & (f.col("target_layer").like("%l4_weekly%")),
        "weekly").when(
        (f.lower(f.col("Feature_Layer")) == 'l4_features') & (f.col("target_layer").like("%l4_monthly%")),
        "monthly").otherwise("No_frequency_defined")) \
        .withColumn("Data_Latency", f.when((f.col("Data_Refresh_Freq") == 'daily'),
                                           f.datediff(f.current_date(), f.col("Latest_Data_Partition_Available"))).when(
        (f.col("Data_Refresh_Freq") == 'weekly'),
        f.datediff(f.current_date(), f.date_sub(f.date_add(f.col("Latest_Data_Partition_Available"), 7), 1))).when(
        (f.col("Data_Refresh_Freq") == 'monthly'),
        f.datediff(f.current_date(), f.date_sub(f.add_months(f.col("Latest_Data_Partition_Available"), 1), 1)))) \
        .withColumn("Is_Data_Refresh_Today",
                    f.when(f.current_date() == f.col("Last_Data_Refresh_Date"), "Y").otherwise("N")) \
        .withColumnRenamed("table_path", "Dataset_Path") \
        .withColumn("Need_Supervision", f.when(
        (f.col("Data_Refresh_Freq") == 'daily') & (f.col("Days_Since_Last_Refresh") > 1) & (
                f.col("Is_Data_Refresh_Today") == 'N'), "Y").when(
        (f.col("Data_Refresh_Freq") == 'weekly') & (f.col("Days_Since_Last_Refresh") > 7) & (
                f.col("Is_Data_Refresh_Today") == 'N'), "Y").when(
        (f.col("Data_Refresh_Freq") == 'monthly') & (f.col("Days_Since_Last_Refresh") > 31) & (
                f.col("Is_Data_Refresh_Today") == 'N'), "Y").otherwise("N")) \
        .withColumn("ops_report_updated_date", f.current_date())

    ops_report = ops_report.select("Domain_Name", "Feature_Layer", "Dataset_Name", "Last_Data_Refresh_Date",
                                   "Days_Since_Last_Refresh", "Latest_Data_Partition_Available", "Data_Latency",
                                   "Data_Refresh_Freq", "Is_Data_Refresh_Today", "Need_Supervision", "Dataset_Path",
                                   "ops_report_updated_date")

    return ops_report


def ops_report_pipeline(**kwargs):
    return Pipeline(
        [

            node(
                build_ops_report_dataset,
                'util_audit_metadata_table',
                'util_ops_report'
            ),
        ]
    )
