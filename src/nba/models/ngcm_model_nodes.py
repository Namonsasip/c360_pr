import pickle
from datetime import datetime
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np

# import pai
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import seaborn as sns
from lightgbm import LGBMClassifier, LGBMRegressor
# from plotnine import *
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
from sklearn.metrics import auc, roc_curve
from sklearn.model_selection import train_test_split

from customer360.utilities.spark_util import get_spark_session

from nba.models.ngcm import Ingester

NGCM_OUTPUT_PATH = (
    "/dbfs/mnt/customer360-blob-output/users/thanasiy/ngcm_export/20210326/"
)


def create_ngcm_nba_model_classifier(
    l5_nba_master_table: pyspark.sql.DataFrame,
    nba_model_group_column,
    target_column,
    max_rows_per_group,
    model_params,
    extra_keep_columns,
    explanatory_features,
    train_sampling_ratio,
    nba_model_min_obs_per_class_for_model,
):
    df_master_only_necessary_columns = l5_nba_master_table.select(
        "subscription_identifier",
        "contact_date",
        "nba_spine_primary_key",
        nba_model_group_column,
        target_column,
        *(
            extra_keep_columns
            + [
                F.col(column_name).cast(FloatType())
                if column_type.startswith("decimal")
                else F.col(column_name)
                for column_name, column_type in l5_nba_master_table.select(
                    *explanatory_features
                ).dtypes
            ]
        ),
    )

    # Filter rows with NA target to reduce size
    df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
        ~F.isnull(F.col(target_column))
    )

    # Sample down if data is too large to reliably train a model
    if max_rows_per_group is not None:
        df_master_only_necessary_columns = df_master_only_necessary_columns.withColumn(
            "aux_n_rows_per_group",
            F.count(F.lit(1)).over(Window.partitionBy(nba_model_group_column)),
        )
        df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
            F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
        ).drop("aux_n_rows_per_group")
    df_master_only_necessary_columns.persist()
    nba_models = (
        df_master_only_necessary_columns.groupby(nba_model_group_column)
        .agg(F.count("*").alias("CNT"))
        .drop("CNT")
        .collect()
    )
    explanatory_features.sort()
    ingester = Ingester(output_folder=NGCM_OUTPUT_PATH)

    for models in nba_models:
        if models[0] != "NULL":
            current_group = models[0]
            pdf_master_chunk = df_master_only_necessary_columns.where(
                nba_model_group_column + " = '" + current_group + "'"
            ).toPandas()

            pdf_master_chunk = pdf_master_chunk[~pdf_master_chunk[target_column].isna()]

            modelling_perc_obs_target_null = np.mean(
                pdf_master_chunk[target_column].isna()
            )
            modelling_n_obs = len(pdf_master_chunk)
            modelling_n_obs_positive_target = len(
                pdf_master_chunk[pdf_master_chunk[target_column] == 1]
            )
            if modelling_perc_obs_target_null != 0:
                continue
            if modelling_n_obs < nba_model_min_obs_per_class_for_model:
                continue
            if modelling_n_obs_positive_target < nba_model_min_obs_per_class_for_model:
                continue
            pdf_master_chunk = pdf_master_chunk.sort_values(
                ["subscription_identifier", "contact_date"]
            )
            pdf_train, pdf_test = train_test_split(
                pdf_master_chunk, train_size=train_sampling_ratio, random_state=123,
            )
            try:
                print("Start to dump "+"Model_" + current_group + "_Classifier")
                lgbm_model = LGBMClassifier(**model_params).fit(
                    pdf_train[explanatory_features],
                    pdf_train[target_column],
                    eval_set=[
                        (pdf_train[explanatory_features], pdf_train[target_column],),
                        (pdf_test[explanatory_features], pdf_test[target_column],),
                    ],
                    eval_names=["train", "test"],
                    eval_metric="auc",
                )
                ingester.ingest(
                    model=lgbm_model,
                    tag="Model_" + current_group + "_Classifier",
                    features=explanatory_features,
                )
                print("Finished " + "Model_" + current_group + "_Classifier")
                print("=========================================")
            except:
                print(
                    "An exception occurred when trying to dump "
                    + "Model_"
                    + current_group
                    + "_Classifier"
                )

    return df_master_only_necessary_columns
