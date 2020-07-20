from typing import Any, Dict

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as func
from pyspark.sql.functions import col, when


def package_translation(
    df_contact: DataFrame,
    df_map_id: DataFrame,
    df_package: DataFrame,
    df_mapping: DataFrame,
    parameters: Dict[str, Any],
) -> DataFrame:
    """  Overwrite existing campaign_code with eligible package_preference

    Args:
        df_contact: treatment_chosen generated for send campaign.
        df_map_id: customer profile contain analytic_id and crm_sub_id.
        df_package: product of package preference with propensity table.
        df_mapping: offer ATL to BTL mapping.
        parameters: parameters defined in parameters.yml.

    Returns:
        df_out: result treatment table with package_preference offer overwritten.

    """
    # Get latest data from customer profile and prepare data
    date_filter = df_map_id.selectExpr("MAX(ddate)").collect()[0][0]
    df_map_id = df_map_id.filter("ddate == '{}'".format(date_filter)).selectExpr(
        "analytic_id",
        "activation_date as register_date",
        "crm_sub_id as old_subscription_identifier",
    )
    df_package = df_package.withColumnRenamed("activation_date", "register_date")

    # Join table
    df_join = df_contact.join(df_map_id, ["old_subscription_identifier"], "left_outer")
    df_join = df_join.join(df_package, ["analytic_id", "register_date"], "left_outer")
    df_join = df_join.withColumnRenamed("offer_Price", "offer_price")

    # Fill the unavailable BTL with ATL
    df_mapping = df_mapping.withColumn(
        "MAID_BTL_DISC_10",
        func.when(func.col("MAID_BTL_DISC_10").isNull(), func.col("MAID_ATL")).otherwise(
            func.col("MAID_BTL_DISC_10")
        ),
    )
    df_mapping = df_mapping.withColumn(
        "DESC_BTL_DISC_10",
        func.when(func.col("DESC_BTL_DISC_10").isNull(), func.col("du_offer")).otherwise(
            func.col("DESC_BTL_DISC_10")
        ),
    )
    df_mapping = df_mapping.withColumnRenamed("du_offer", "offer")

    # Constrain to limit new subs from receiving unlimited data package
    df_join = df_join.filter(
        "(register_date < '2019-02-01') OR offer_package_group != 'Fix UL'"
    )
    # Prepare package preference table with anti down-sell rules
    offer_cond = (
        "(" + " AND ".join(parameters["treatment_package_rec"]["condition"]) + ")"
    )
    df_join = df_join.filter(offer_cond)
    window = Window.partitionBy("analytic_id", "register_date").orderBy(
        func.col("prob_calibrated_cmp").desc()
    )
    df_join = df_join.select(
        "*",
        func.max(func.col("prob_calibrated_cmp"))
        .over(window)
        .alias("max_prob_calibrated_cmp"),
    )
    df_join = df_join.filter("prob_calibrated_cmp == max_prob_calibrated_cmp")

    # Mapping package preference offer with available MAID
    df_join = df_join.join(df_mapping, ["offer"], "left_outer")
    df_join = df_join.filter("MAID_ATL IS NOT NULL").drop("max_prob_calibrated_cmp")

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

    df_out = df_join.withColumn(
        "offer_id",
        when(col("offer_map") == "ATL", col("MAID_ATL")).when(
            col("offer_map") == "BTL_DISC_10", col("MAID_BTL_DISC_10")
        ),
    )
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
    )

    return df_out
