from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from kedro.pipeline import Pipeline, node
from pyspark.sql import DataFrame
import os

running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def build_metadata_backup(metadata_table: DataFrame) -> DataFrame:
    """
    Purpose: To create the backup of metadata table.
    :param metadata_table:
    :return:
    """
    metadata_table = metadata_table.withColumn("metadata_table_backup_date", f.current_date())

    return metadata_table


def metadata_backup_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                build_metadata_backup,
                'util_audit_metadata_table',
                'util_metadata_backup'
            ),
        ]
    )
