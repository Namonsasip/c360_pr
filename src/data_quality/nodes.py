from src.customer360.utilities.re_usable_functions import get_spark_session

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
        Dataframe with unique msisdns
    """
    spark = get_spark_session()

    cust_profile_df.createOrReplaceTempView("cust_profile_df")
    distinct_sub_id_df = spark.sql("""
        select 
            distinct(crm_sub_id) as subscription_identifier
        from cust_profile_df
        where crm_sub_id is not null
          and lower(crm_sub_id) != 'na'
    """)
    distinct_sub_id_count = distinct_sub_id_df.count()

    sample_fraction = min(sample_size / distinct_sub_id_count, 1.0)
    sampled_sub_id_df = distinct_sub_id_df.sample(withReplacement=False, fraction=sample_fraction)
    sampled_sub_id_df.withColumn("created_date", F.current_date())

    return sampled_sub_id_df
