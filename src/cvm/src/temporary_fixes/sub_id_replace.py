# This is a temporary fix replacing old style subscription id with new one using
import logging

import pyspark.sql.functions as func
from pyspark.sql import DataFrame, Window

from src.customer360.utilities.spark_util import get_spark_session


class SubIdReplacer:
    def __init__(self, profile_path):
        logging.getLogger(__name__).info(
            "Creating `subscription_identifier` replacement dictionary"
        )
        spark = get_spark_session()
        profile = spark.read.parquet(profile_path)
        window_latest = Window.partitionBy("old_subscription_identifier").orderBy(
            func.col("event_partition_date").desc()
        )
        self.replacement_dictionary = (
            profile.filter("charge_type == 'Pre-paid'")
            .withColumn("date_lp", func.row_number().over(window_latest))
            .filter("date_lp == 1")
            .select(["old_subscription_identifier", "subscription_identifier"])
        )

    def replace_old_sub_id(self, df):
        logging.getLogger(__name__).info(
            "Replacing `subscription_identifier` to new version"
        )
        return (
            df.withColumnRenamed(
                "subscription_identifier", "old_subscription_identifier"
            )
            .join(self.replacement_dictionary, on="old_subscription_identifier")
            .drop("old_subscription_identifier")
        )

    @staticmethod
    def check_if_replacement_needed(df):
        logging.getLogger(__name__).info(
            "Checking if replacing `subscription_identifier` needed"
        )
        max_sub_id_len = df.agg(
            func.max(func.length(func.col("subscription_identifier")))
        ).collect()[0][0]
        return max_sub_id_len < 16

    def replace_if_needed(self, df):
        if SubIdReplacer.check_if_replacement_needed(df):
            return self.replace_old_sub_id(df)
        else:
            logging.getLogger(__name__).info("Replacement not needed")
            return df


SubIdReplace = SubIdReplacer(
    "/mnt/customer360-blob-output/C360/PROFILE/"
    + "l1_features/l1_customer_profile_union_daily_feature/"
)


def replace_sub_ids(fun):
    def replace_if_applicable(obj):
        if isinstance(obj, DataFrame) and "subscription_identifier" in obj.columns:
            return SubIdReplace.replace_if_needed(obj)
        else:
            return obj

    def fun_new_sub_id(*args):
        return fun(*[replace_if_applicable(arg) for arg in args])

    return fun_new_sub_id
