from src.customer360.utilities.re_usable_functions import get_spark_session
from src.data_quality.dq_util import get_dq_context, run_accuracy_logic
from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def sample_subscription_identifier(
        cust_profile_df: DataFrame,
        sample_size: int
) -> DataFrame:
    """
    Sample subscription_identifier from a dataframe
    Args:
        cust_profile_df: Dataframe with subscription_identifier from which we have to take sample
        sample_size: Number of samples we want to take

    Returns:
        Dataframe with unique subscription_identifier
    """
    spark = get_spark_session()

    cust_profile_df.createOrReplaceTempView("cust_profile_df")
    distinct_sub_id_df = spark.sql("""
        select 
            distinct(subscription_identifier) as subscription_identifier
        from cust_profile_df
        where subscription_identifier is not null
          and lower(subscription_identifier) != 'na'
    """)
    distinct_sub_id_count = distinct_sub_id_df.count()

    sample_fraction = min(sample_size / distinct_sub_id_count, 1.0)
    sampled_sub_id_df = distinct_sub_id_df.sample(withReplacement=False, fraction=sample_fraction)
    sampled_sub_id_df = sampled_sub_id_df.withColumn("created_date", F.current_date())

    return sampled_sub_id_df


def check_catalog_and_feature_exist(
        dq_config: dict
):
    ctx = get_dq_context()
    missing_files = []
    for dataset_name in dq_config.keys():
        try:
            df = ctx.catalog.load(dataset_name)

            for col in dq_config[dataset_name]:
                if col not in df.columns:
                    missing_files.append("{}.{}".format(dataset_name, col))
        except:
            missing_files.append(dataset_name)

    if missing_files:
        err_msg = "These datasets/columns do not exist: {}".format(missing_files)
        raise FileNotFoundError(err_msg)
