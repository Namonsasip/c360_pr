import logging
from typing import Dict, List, Tuple

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import (
    PCA,
    Imputer,
    MinMaxScaler,
    StandardScaler,
    VectorAssembler,
)
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


def l5_all_subscribers_master_table_customer_level(
    df_master: DataFrame,
    propensities: DataFrame,
    l5_customer_ids: DataFrame,
    l4_streaming_visit_count_and_down_traffic_feature_full_load_data_blob: DataFrame,
    subset_features: Dict[str, List[str]],
):

    # Add streaming features from an old snapshot as the official version is not yet
    # available
    df_master = df_master.join(
        l4_streaming_visit_count_and_down_traffic_feature_full_load_data_blob.select(
            "access_method_num",
            "start_of_week",
            *subset_features[
                "l4_streaming_visit_count_and_download_traffic"
                "_feature_full_load_data_blob"
            ],
        ),
        on=["access_method_num", "start_of_week"],
        how="left",
    )

    df_master_with_customer_id = df_master.join(
        l5_customer_ids, on="subscription_identifier", how="left"
    ).join(propensities, on="subscription_identifier", how="left")
    df_master_with_customer_id = df_master_with_customer_id.withColumn(
        "customer_id_high_certainty",
        F.when(
            F.isnull("customer_id_high_certainty"), F.col("subscription_identifier")
        ).otherwise(F.col("customer_id_high_certainty")),
    )

    # Change the key of the table to be account (a.k.a. customer) level
    # For numeric features take the average and for non-numeric (date, string)
    # take the first observation
    df_master_customer_level = df_master_with_customer_id.groupby(
        "customer_id_high_certainty"
    ).agg(
        *(
            [
                (
                    F.first(column_name).alias(column_name)
                    if column_type in ["string", "date"]
                    else F.mean(column_name).alias(column_name)
                )
                for column_name, column_type in df_master_with_customer_id.dtypes
                if column_name != "customer_id_high_certainty"
            ]
            + [F.count(F.lit(1)).alias("number_sub_ids_in_account")]
        )
    )

    return df_master_customer_level


def personnas_clustering(
    df_master: DataFrame,
    clustering_features: List[str],
    n_pca_components: int,
    n_clusters: int = 200,
    clip_features_limits: Tuple[float, float] = (-5, 5),
):

    # TODO review this logic for account level
    df_master = df_master.withColumn(
        "number_of_products",
        F.count(F.col("subscription_identifier")).over(
            Window.partitionBy("national_id_card")
        ),
    )
    # If sub has >5 products count 5 maximum, also for NULL national ID card
    # count 1 product
    df_master = df_master.withColumn(
        "number_of_products",
        F.least(
            F.lit(5),
            F.when(F.isnull(F.col("national_id_card")), F.lit(1)).otherwise(
                F.col("number_of_products")
            ),
        ),
    )

    regions_mapping = {
        "CW": "CENTRAL_WEST",
        "CN": "CENTRAL_NORTH",
        "CB": "CENTRAL_BANGKOK",
        "XU": "UPPER_NORTH_EASTERN",
        "CE": "CENTRAL_EAST",
        "NU": "UPPER_NORTH",
        "NL": "LOWER_NORTH",
        "SL": "LOWER_SOUTH",
        "SU": "UPPER_SOUTH",
        "XL": "LOWER_NORTH_EASTERN",
    }

    for region_code, region_name in regions_mapping.items():
        df_master = df_master.withColumn(
            f"region_is_{region_name}",
            F.when(F.col("activation_region") == region_code, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )

    charge_type_mapping = {
        "Post-paid": "Post_paid",
        "Pre-paid": "Pre_paid",
        "Hybrid-Post": "Hybrid_Post",
    }
    for charge_type_code, charge_type_name in charge_type_mapping.items():
        df_master = df_master.withColumn(
            f"charge_type_is_{charge_type_name}",
            F.when(F.col("charge_type") == charge_type_code, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )

    # df_master = df_master.withColumn(
    #     "region_is_city",
    #     F.when(F.col("city_of_residence") == "CB", F.lit(1)).otherwise(F.lit(0)),
    # )

    # Fix age feature, since there are really large and small values
    df_master = df_master.withColumn(
        "fixed_age",
        F.when(F.col("age") <= 5, F.lit("18"))
        .when(F.col("age") >= 100, F.lit("65"))
        .otherwise(F.col("age")),
    )

    # Fix gender feature
    df_master = df_master.withColumn(
        "gender",
        F.when(F.col("gender").isin(["Male", "M"]), F.lit("Male"),)
        .when(F.col("gender").isin(["Female", "F"]), F.lit("Female"),)
        .when(F.isnull(F.col("gender")), F.lit(None),)
        .otherwise(F.lit("Other")),
    )

    # For sum and avg features, impute with 0 that makes more sense than the mean
    df_master = df_master.fillna(
        0,
        subset=[
            x
            for x in df_master.columns
            if (x.startswith("sum_") or x.startswith("avg_"))
        ],
    )

    original_features = df_master.columns
    # Cast all features to float as scaling cannot be performed on integers
    for f in clustering_features:
        df_master = df_master.withColumn(f, F.col(f).cast(FloatType()))

    # Impute features as k-means does not accept NAs
    imputed_features = [f"{x}_imputed" for x in clustering_features]
    imputer = Imputer(
        strategy="mean", inputCols=clustering_features, outputCols=imputed_features
    )
    assembler = VectorAssembler(inputCols=imputed_features, outputCol="features")
    # Scale features as k-means care about magnitude
    scaler = StandardScaler(
        inputCol="features",
        outputCol="standardised_features",
        withMean=True,
        withStd=True,
    )
    # Clip features to avoid outliers which can negatively affect clustering
    clipper = MinMaxScaler(
        inputCol="standardised_features",
        outputCol="clipped_features",
        min=clip_features_limits[0],
        max=clip_features_limits[1],
    )
    pca = PCA(
        k=n_pca_components, inputCol="clipped_features", outputCol="pca_components",
    )
    kmeans = KMeans(
        featuresCol="pca_components",
        predictionCol="cluster_id",
        k=n_clusters,
        seed=123,
        initSteps=2,
    )
    logging.warning(
        "This code might fail in Databricks connect. The code is"
        " correct, so if you get an error you can run it in a Notebook"
    )
    pipeline_model = Pipeline(
        stages=[imputer, assembler, scaler, clipper, pca, kmeans]
    ).fit(df_master)

    # Validate KMeans
    if len(pipeline_model.stages[-1].clusterCenters()) < n_clusters:
        raise ValueError("Kmeans failed to produce enough clusters.")

    df_master_with_clusters = pipeline_model.transform(df_master)

    # Delete all intermediate columns as they are not necessary
    df_master_with_clusters = df_master_with_clusters.select(
        *original_features, "cluster_id", "pca_components"
    )
    return [df_master_with_clusters, pipeline_model]


def l5_personnas_clustering_summary(
    l5_personnas_clustering_master: DataFrame,
    clustering_features: List[str],
    features_to_summarize: List[str],
) -> pd.DataFrame:
    pdf = (
        l5_personnas_clustering_master.groupby("cluster_id")
        .agg(
            F.count(F.lit(1)).alias("n_subscribers_in_cluster"),
            *(
                [
                    F.mean(feature).alias(f"average_{feature}")
                    for feature in (clustering_features + features_to_summarize)
                ]
                + [
                    F.expr(f"percentile({feature}, {percentile})").alias(
                        f"percentile_{percentile}_{feature}"
                    )
                    for percentile in [0.2, 0.8]
                    for feature in (clustering_features + features_to_summarize)
                ]
            ),
        )
        .toPandas()
        .sort_values(["n_subscribers_in_cluster"])
    )
    return pdf
