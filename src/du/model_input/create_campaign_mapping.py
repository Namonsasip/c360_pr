from customer360.utilities.spark_util import get_spark_session
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

"""
Column required
Need to change column name accordingly for this function to work
Family	MA_Type	MA_Campaign	MA_ID	MA _Name	USSD_code	Promotion_code	campaign_category	offer_category	benefit_category	department	section	owners

Notes: some rows promotion_code may have /n in it, we need to remove that either manually before upload
or within the function in the future
"""


def create_mannual_campaign_mapping(
    l0_product_pru_m_ontop_master_for_weekly_full_load: DataFrame,
    path_to_ngcm_deploy_plan,
):
    spark = get_spark_session()

    # This csv file is manually upload to blob storage
    # DS receive this file from marketing owner to map campaign child code with product id
    # Product id should be available in the campaign history in the near future
    cmm_campaign_master = spark.read.format("csv").load(
        path_to_ngcm_deploy_plan, header=True
    )
    spark.conf.set("spark.sql.parquet.binaryAsString", "true")

    # Select latest ontop product master
    product_pru_m_ontop_master = l0_product_pru_m_ontop_master_for_weekly_full_load.withColumn(
        "partition_date_str", F.col("partition_date").cast(StringType())
    ).select(
        "*", F.to_date(F.col("partition_date_str"), "yyyyMMdd").alias("ddate")
    )
    max_master_date = (
        product_pru_m_ontop_master.withColumn("G", F.lit(1))
        .groupby("G")
        .agg(F.max("ddate").alias("ddate"))
        .collect()
    )
    product_pru_m_ontop_master = product_pru_m_ontop_master.where(
        "ddate = date('" + max_master_date[0][1].strftime("%Y-%m-%d") + "')"
    )

    # Join ontop product master with campaign child code mapping
    campaign_mapping = product_pru_m_ontop_master.join(
        cmm_campaign_master.withColumnRenamed("MA_ID", "campaign_child_code"),
        ["promotion_code"],
        "inner",
    )

    # Use regex to remove special symbol that could potentially break code in the future
    # rework_macro_product will be used as reference to data-upsell train model
    rework_macro_product = campaign_mapping.withColumn(
        "rework_macro_product",
        F.regexp_replace("package_name_report", "(\.\/|\/|\.|\+|\-|\(|\)|\ )", "_"),
    )
    # rework_macro_product.createOrReplaceTempView("rework_macro_product")
    # spark.sql(
    #     """CREATE TABLE prod_dataupsell.mapping_for_model_training"""
    #     + mapping_create_date_str
    #     + """
    #             AS
    #             SELECT * FROM rework_macro_product"""
    # )
    # Do not forget to update mapping_for_model_training in data upsell catalog_l0 accordingly
    return rework_macro_product
