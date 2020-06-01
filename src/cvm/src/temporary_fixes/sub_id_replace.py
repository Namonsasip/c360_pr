# This is a temporary fix replacing old style subscription id with new one using
import logging

import pyspark.sql.functions as func
from pyspark.sql import DataFrame


class SubIdReplacer:
    def __init__(self, replacement_dictionary):
        self.replacement_dictionary = replacement_dictionary

    def replace_old_sub_id(self, df):
        logging.getLogger(__name__).info(
            "Replacing `subscription_identifier` to new version"
        )
        if "old_subscription_identifier" in df.columns:
            df = df.drop("old_subscription_identifier")
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


def replace_sub_id_if_needed(
    df: DataFrame, replacement_dictionary: DataFrame
) -> DataFrame:
    """ Replace `subscription_identifier` in `df` only if needed.

    Args:
        df: table to replace `subscription_identifier`
        replacement_dictionary: table with sub id mapping
    """
    return SubIdReplacer(replacement_dictionary).replace_if_needed(df)
