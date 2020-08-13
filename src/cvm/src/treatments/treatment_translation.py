from typing import Any, Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as func
from pyspark.sql.functions import col, when


def package_translation(
    df_contact: DataFrame,
    df_package: DataFrame,
    df_mapping: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """  Overwrite existing campaign_code with eligible package_preference

    Args:
        df_contact: treatment_chosen generated for send campaign.
        df_package: product of package preference with propensity table.
        df_mapping: offer ATL to BTL mapping.
        parameters: parameters defined in parameters.yml.

    Returns:
        df_out: result treatment table with package_preference offer overwritten.

    """
    # Filter max scoring_day
    date_filter = df_package.selectExpr("MAX(scoring_day)").collect()[0][0]
    df_package = df_package.filter(f"scoring_day == {date_filter}")
    df_package = df_package.filter("offer_Macro_product_type != 'BTL'")
    df_package = df_package.drop("old_subscription_identifier")  # drop redundant column

    # Join table
    df_join = df_contact.join(df_package, ["subscription_identifier"], "left_outer")

    # Fill the unavailable BTL with ATL
    df_mapping = df_mapping.withColumn(
        "MAID_BTL_DISC_10",
        func.when(func.col("MAID_BTL_DISC_10").isNull(), func.col("MAID_ATL")).otherwise(
            func.col("MAID_BTL_DISC_10")
        ),
    )
    df_mapping = df_mapping.withColumn(
        "DESC_BTL_DISC_10",
        func.when(func.col("DESC_BTL_DISC_10").isNull(), func.col("offer_package_name_report")).otherwise(
            func.col("DESC_BTL_DISC_10")
        ),
    )

    # # Constrain to limit new subs from receiving unlimited data package (temporary disable)
    # df_join = df_join.filter(
    #     "(register_date < '2019-02-01') OR offer_package_group != 'Fix UL'"
    # )

    # Prepare package preference table with anti down-sell rules
    offer_cond = (
        "(" + " AND ".join(parameters["treatment_package_rec"]["condition"]) + ")"
    )
    df_join = df_join.filter(offer_cond)
    window = Window.partitionBy('subscription_identifier').orderBy(func.col('propensity').desc())
    df_join = df_join.select('*', func.max(func.col('propensity')).over(window).alias('max_propensity'))
    df_join = df_join.filter("propensity == max_propensity")

    # Mapping package preference offer with available MAID
    df_join = df_join.join(df_mapping, ["offer_package_name_report"], "left_outer")
    df_join = df_join.filter("MAID_ATL IS NOT NULL").drop("max_propensity")

    # Get package type according to treatment_name
    df_join = df_join.withColumn("offer_map", func.lit(None))
    for rule in parameters["package_btl_mapping"]:
        df_join = df_join.withColumn(
            "offer_map",
            when(
                col("treatment_name").startswith(rule),
                func.lit(parameters["package_btl_mapping"][rule]),
            ).otherwise(col("offer_map")),
        )

    df_final = df_join.withColumn(
        "offer_id",
        when((col("MAID_ATL").isNotNull()) & (col("offer_map") == "ATL"), col("MAID_ATL")).when(
            (col("MAID_BTL_DISC_10").isNotNull()) & (col("offer_map") == "BTL_DISC_10"), col("MAID_BTL_DISC_10")
        ),
    )
    df_final = df_final.select('subscription_identifier', 'offer_id')
    df_out = df_contact.join(df_final, ['subscription_identifier'], 'left_outer')

    df_out = df_out.withColumn(
        "campaign_code",
        when(col("offer_id").isNotNull(), col("offer_id")).otherwise(
            col("campaign_code")
        ),
    )
    df_out = df_out.select(
        "microsegment",
        "subscription_identifier",
        "old_subscription_identifier",
        "macrosegment",
        "use_case",
        "campaign_code",
        "date",
        "treatment_name",
    ).dropDuplicates()

    return df_out
