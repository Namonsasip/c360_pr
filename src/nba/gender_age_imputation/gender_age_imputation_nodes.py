import logging
from typing import List, Tuple

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, Imputer, VectorAssembler, MinMaxScaler
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType


def l5_all_subscribers_master_table_spine(
    l1_customer_profile_union_daily_feature_full_load: DataFrame,
    date_min: str,  # YYYY-MM-DD
    date_max: str,  # YYYY-MM-DD
) -> DataFrame:
    # l1_customer_profile_union_daily_feature_full_load.dtypes

    df_spine = l1_customer_profile_union_daily_feature_full_load.select(
        "event_partition_date",
        "old_subscription_identifier",
        "access_method_num",
        "subscription_identifier",
        "start_of_week",
        "start_of_month",
        F.col("start_of_month").alias("partition_month"),
    )

    df_spine = df_spine.filter(
        F.col("event_partition_date").between(date_min, date_max)
    )

    return df_spine


def create_gender_age_imputation_clusters(
    df_master: DataFrame,
    clustering_features: List[str],
    n_clusters: int = 200,
    clip_features_limits: Tuple[float, float] = (-5, 5),
) -> DataFrame:

    # In case a national ID card has more then 2 sims,
    # only keep the age and gender of one, choosing the
    # one with most tenure
    df_master = df_master.withColumn(
        "aux_order",
        F.row_number().over(
            Window.partitionBy("national_id_card", "event_partition_date").orderBy(
                F.col("subscriber_tenure").desc()
            )
        ),
    )
    for imputation_col in ["age", "gender"]:
        df_master = df_master.withColumn(
            f"original_{imputation_col}", F.col(imputation_col)
        )
        df_master = df_master.withColumn(
            imputation_col,
            F.when(
                F.col("aux_order") == 1, F.col(imputation_col),            ),
        )
    df_master = df_master.drop("aux_order")

    # Fix gender feature
    df_master = df_master.withColumn(
        "gender",
        F.when(F.col("gender").isin(["Male", "M"]), F.lit("Male"),)
        .when(F.col("gender").isin(["Female", "F"]), F.lit("Female"),)
        .when(F.isnull(F.col("gender")), F.lit(None),)
        .otherwise(F.lit("Other")),
    )

    # Fix age feature, since there are really large and small values
    df_master = df_master.withColumn(
        "age",
        F.when(F.col("age") <= 5, F.lit("18"))
        .when(F.col("age") >= 100, F.lit("65"))
        .otherwise(F.col("age")),
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
    model = KMeans(
        featuresCol="clipped_features",
        predictionCol="cluster_id",
        k=n_clusters,
        seed=123,
        initSteps=2,
    )
    logging.warning(
        "This code might fail in Databricks connect. The code is"
        " correct, so if you get an error you can run it in a Notebook"
    )
    pipeline = Pipeline(stages=[imputer, assembler, scaler, clipper, model,]).fit(
        df_master
    )

    # Validate KMeans
    if len(pipeline.stages[-1].clusterCenters()) < n_clusters:
        raise ValueError("Kmeans failed to produce enough clusters.")

    df_master_with_clusters = pipeline.transform(df_master)

    # Delete all intermediate columns as they are not necessary
    df_master_with_clusters = df_master_with_clusters.select(
        *original_features, "cluster_id"
    )

    return df_master_with_clusters


def l5_gender_age_imputed(
    l5_gender_age_imputation_master_with_clusters: DataFrame,
) -> DataFrame:
    def impute_same_distribution(col_name: str, by_columns=["cluster_id"], seed=123):
        return F.last(F.col(col_name), ignorenulls=True).over(
            Window.partitionBy(*by_columns)
            .orderBy(F.rand(seed=seed))
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

    df_imputed = l5_gender_age_imputation_master_with_clusters.withColumn(
        "age_imputed", impute_same_distribution("age", seed=123)
    )
    df_imputed = df_imputed.withColumn(
        "gender_imputed", impute_same_distribution("gender", seed=1234)
    )

    return df_imputed


def l5_gender_age_imputation_clustering_summary_report(
    l5_gender_age_imputed: DataFrame,
) -> pd.DataFrame:
    pdf = (
        l5_gender_age_imputed.groupby("cluster_id")
        .agg(
            F.count(F.lit(1)).alias("n_subscribers_in_cluster"),
            F.mean(F.isnull("gender").cast(FloatType())).alias("perc_null_gender"),
            F.mean(F.isnull("age").cast(FloatType())).alias("perc_null_age"),
            F.mean("age").alias("average_age"),
            F.mean("age_imputed").alias("average_imputed_age"),
            F.stddev("age").alias("stddev_age"),
            F.stddev("age_imputed").alias("stddev_imputed_age"),
            F.min("age").alias("min_age"),
            F.max("age").alias("max_age"),
            F.mean((F.col("gender") == "Male").cast(FloatType())).alias("perc_male"),
            F.mean((F.col("gender") == "Female").cast(FloatType())).alias(
                "perc_female"
            ),
        )
        .toPandas()
        .sort_values(["n_subscribers_in_cluster"])
    )
    return pdf
