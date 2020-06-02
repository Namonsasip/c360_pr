# This is a temporary fix replacing old style subscription id with new one using
import logging
import re

import pyspark.sql.functions as func
from pyspark.sql import DataFrame


class SubIdReplacer:
    def __init__(self, replacement_dictionary):
        self.replacement_dictionary = replacement_dictionary

    def replace_old_sub_id(self, df):
        logging.getLogger(__name__).info(
            "Replacing `subscription_identifier` to new version"
        )
        if "crm_sub_id" in df.columns:
            subscription_identifier_col = "crm_sub_id"
        else:
            subscription_identifier_col = "subscription_identifier"
        if "old_subscription_identifier" in df.columns:
            df = df.drop("old_subscription_identifier")
        return (
            df.withColumnRenamed(
                subscription_identifier_col, "old_subscription_identifier"
            )
            .join(self.replacement_dictionary, on="old_subscription_identifier")
            .drop("old_subscription_identifier")
            .withColumnRenamed("subscription_identifier", subscription_identifier_col)
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


def get_mapped_dataset_name(dataset_name: str, sample_type: str = None) -> str:
    """ Parse dataset catalog name to get name of dataset after mapping sub ids.

    Args:
        dataset_name: input dataset name
        sample_type: type of sample, may be "scoring", "training" or custom
    """
    new_suffix_prefix = "sub_ids_mapped"
    if sample_type is None:
        return dataset_name + "_" + new_suffix_prefix
    sample_type_regex = r"_{}$".format(sample_type)
    sample_type_found = re.search(sample_type_regex, dataset_name) is not None
    if not sample_type_found:
        return dataset_name + "_" + new_suffix_prefix
    dataset_name = re.sub(sample_type_regex, "", dataset_name)
    return dataset_name + "_" + new_suffix_prefix + "_" + sample_type
