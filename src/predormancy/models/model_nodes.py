import os
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple
import logging
import numpy as np
import pandas as pd
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
)
from customer360.utilities.spark_util import get_spark_session
from pyspark.sql import DataFrame, Window
import datetime
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCols, HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import when
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.mllib.evaluation import BinaryClassificationMetrics

import pyspark
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import PipelineModel


def process_date(date):
    """
    This function processes date from string form (e.g. "01-01-2018") to proper date format.
    """
    date = datetime.datetime.strptime(date, "%d-%m-%Y").date()
    return date


def deprocess_date(date):
    """
    This function deprocesses date from date format to string form (e.g. "01-01-2018").
    """
    date = date.strftime("%d-%m-%Y")
    return date


def daterange(start_date, end_date):
    """
    This function returns list of dates: one for each day between start and end date passed as arguments.
    """
    start_date = process_date(start_date)
    end_date = process_date(end_date)
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def build_dates_table_query(start_date, end_date):
    """
    Builds query to build simple dates table from starting
    """
    dates_table_query = ""
    first_loop = True
    for d in daterange(start_date, end_date):
        if first_loop:
            dates_table_query = (
                dates_table_query
                + "CREATE TABLE mck.scoring_days \nSELECT TO_DATE('"
                + deprocess_date(d)
                + """', 'dd-MM-yyyy') AS SCORING_DAY"""
            )
        else:
            dates_table_query = (
                dates_table_query
                + """
    UNION ALL
SELECT TO_DATE('"""
                + deprocess_date(d)
                + """', 'dd-MM-yyyy') AS SCORING_DAY"""
            )
        first_loop = False
    return dates_table_query


class Caster(Transformer, HasInputCol, HasOutputCol):
    """Custom transformer: Casts column to specified type, double by default."""

    def __init__(self, inputCol=None, outputCol=None, type_to_cast="double"):
        self.type_to_cast = type_to_cast
        self.inputCol = inputCol
        self.outputCol = outputCol

    def fit(self, dataset):
        return self

    def transform(self, dataset):
        out_col = self.outputCol
        in_col = dataset[self.inputCol]
        return dataset.withColumn(out_col, in_col.cast(self.type_to_cast))


class Cutter(Transformer, HasInputCol, HasOutputCol):
    """Custom transformer: technically, it replaces all values bigger than n with n. In terms of data processing we use this on categorical columns after applying stringIndexer, so this means, that we accept only 31 most numerous labels for categorical variables and recode rest of them to 32."""

    def __init__(self, inputCol=None, outputCol=None, n=32):
        self.n = n
        self.inputCol = inputCol
        self.outputCol = outputCol

    def fit(self, dataset):
        return self

    def transform(self, dataset):
        out_col = self.outputCol
        in_col = dataset[self.inputCol]
        return dataset.withColumn(
            out_col, when(in_col > self.n, self.n).otherwise(in_col)
        )


class DateSubstractor(Transformer, HasInputCols, HasOutputCol):
    """
  Date Subtraction Transformer
    # in_cols = list of string
      - date1, date2 = string = column names to subtract
    # out_col = string
      - column name of result from subtraction
    # change timestamp to date type by using function to_date
    # return df result = (date1 - date2) by using function datediff
  """

    def __init__(
        self, inputCols=None, outputCol=None,
    ):
        self.inputCols = inputCols
        self.outputCol = outputCol

    def fit(self, dataset):
        return self

    def transform(self, dataset):
        out_col = self.outputCol
        in_cols = self.inputCols
        date1 = in_cols[0]
        date2 = in_cols[1]
        return dataset.withColumn(
            out_col, F.datediff(F.to_date(dataset[date1]), F.to_date(dataset[date2]))
        )


def build_pipeline(dtypes, technicalCols):
    # in this cell I scan dataframe for columns types to use these information to process data before passing to transformer.
    df_types = pd.DataFrame(dtypes, columns=["variable", "type"])

    # preparing lists of columns with specific types
    stringCols = list(
        set(df_types[df_types["type"] == "string"]["variable"].tolist())
        - set(technicalCols)
    )
    timestampCols = list(
        set(df_types[df_types["type"] == "timestamp"]["variable"].tolist())
        - set(technicalCols)
    )
    intCols = list(
        set(df_types[df_types["type"].isin(["int", "bigint"])]["variable"])
        - set(["analytic_id", "label"])
    )
    numericsCols = list(
        set(df_types["variable"].tolist())
        - set(technicalCols)
        - set(stringCols)
        - set(timestampCols)
        - set(intCols)
    )

    # adding stages to pipeline
    stages = []  # stages in our Pipeline

    for timestampCol in timestampCols:
        dateSubstractor = DateSubstractor(
            inputCols=["scoring_day", timestampCol],
            outputCol="days_since_" + timestampCol,
        )
        stages += [dateSubstractor]

    for intCol in intCols:
        caster = Caster(inputCol=intCol, outputCol=intCol + "_casted")
        stages += [caster]

    for stringCol in stringCols:
        stringIndexer = StringIndexer(
            inputCol=stringCol,
            outputCol=stringCol + "_Ind",
            stringOrderType="frequencyDesc",
        )
        stringIndexer = stringIndexer.setHandleInvalid("keep")
        stages += [stringIndexer]

        cutter = Cutter(
            inputCol=stringCol + "_Ind", outputCol=stringCol + "_Ind_Cut", n=32
        )
        stages += [cutter]

    cols_to_assemble = (
        list(map(lambda x: x + "_Ind_Cut", stringCols))
        + numericsCols
        + list(map(lambda x: "days_since_" + x, timestampCols))
        + list(map(lambda x: x + "_casted", intCols))
    )

    assembler = VectorAssembler(inputCols=cols_to_assemble, outputCol="features")
    stages += [assembler]

    pipe = Pipeline(stages=stages)
    return pipe


def fill_na(df):
    df = df.na.fill(-999)
    df = df.na.fill("null")
    return df


# function to calc AUC
def calc_auc(sdf, y_test="label", y_score="probability_1"):
    to_auc = sdf.select([y_score, y_test]).rdd
    metrics = BinaryClassificationMetrics(to_auc)
    return metrics.areaUnderROC


def calc_logloss(sdf, y_test="label", y_score="probability_1"):
    return sdf.select(y_test, y_score).agg(
        F.mean(
            F.when(F.col(y_test) == 1.0, F.lit(-F.log(F.col(y_score)))).otherwise(
                F.lit(-F.log(1.0 - F.col(y_score)))
            )
        ).alias("logloss")
    )



def train_predormancy_model(
    l5_predorm_master_table: DataFrame,  # Training Set
    train_date: str,  # Model training date str for model saving
):
    spark = get_spark_session()
    # 50:50 Train Test sampling for training
    target = l5_predorm_master_table.where("label = 1 ").limit(100000)
    non_target = l5_predorm_master_table.where("label = 0").limit(100000)

    # Cap maximum of 1M non dormance for testing
    all_target = l5_predorm_master_table.where("label = 1")
    l_non_target = l5_predorm_master_table.where("label = 0").limit(1000000)

    sdf_train = target.union(non_target)
    sdf_test = all_target.union(l_non_target)

    # Remove any record existed in training set from test set
    sdf_test = sdf_test.join(
        sdf_train.select("analytic_id", "register_date"),
        ["analytic_id", "register_date"],
        "leftanti",
    )

    # Keys columns that we do not want to impute
    technical_cols = [
        "analytic_id",
        "scoring_day",
        "label",
        "register_date",
        "subscription_identifier",
        "start_of_week",
        "event_partition_date",
        "start_of_month",
        "old_subscription_identifier",
        "partition_month",
        "national_id_card",
    ]

    # Build spark pipeline for training set
    pipe1 = build_pipeline(sdf_train.dtypes, technical_cols)

    # last step still not included in pipeline: replacing nulls
    sdf_train = fill_na(sdf_train)
    # fitting pipeline
    pipe1 = pipe1.fit(sdf_train)
    # transforming data with pipeline
    # (keep in mind that the same pipeline can be used on any other data, like test set, scoring set, etc.)
    sdf_train_transformed = pipe1.transform(sdf_train)

    # Persist trainset to spark memory because we will do multiple iterating with it
    sdf_train_transformed.persist()

    # Train a RandomForest model.
    # We do this to get most important features out of many so we could filter un-necessary features in prediction
    rf_mod1 = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=25,
        maxDepth=12,
        maxBins=256,
        featureSubsetStrategy="0.1",
    )
    mod1 = rf_mod1.fit(sdf_train_transformed)

    # storing names of the columns from vectorizer transformer
    preds = pipe1.stages[len(pipe1.stages) - 1].getInputCols()
    # retrieving importances
    importances_var_sel = dict(zip(preds, mod1.featureImportances))
    importances_var_sel = pd.DataFrame(
        list(zip(preds, mod1.featureImportances)), columns=["variable", "importance"]
    ).sort_values(["importance"], ascending=False)
    # removing not important variables
    importances_selected = importances_var_sel.loc[
        importances_var_sel.importance > 0.0001
    ]
    selected_preds = importances_var_sel.variable.tolist()
    selected_preds_indices = importances_var_sel.index.tolist()
    print("Number of selected variables: " + str(len(selected_preds)))

    slicer = VectorSlicer(
        inputCol="features",
        outputCol="selected_features",
        indices=selected_preds_indices,
    )
    sdf_train_p = slicer.transform(sdf_train_transformed).drop("features")

    sdf_train_p = sdf_train_p.repartition(800)
    sdf_train_p.persist(pyspark.StorageLevel.MEMORY_ONLY)

    ## Train a GBT model.
    gbt_mod1 = GBTClassifier(
        labelCol="label",
        featuresCol="selected_features",
        maxBins=256,
        maxDepth=12,
        maxIter=50,
        stepSize=0.3,
    )
    gbtmod1 = gbt_mod1.fit(sdf_train_p)

    sdf_train_p.unpersist()
    # Chain all pipelines
    pipe_full = PipelineModel(stages=[pipe1, slicer, gbtmod1])

    predictions = pipe_full.transform(sdf_test)
    predictions.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    predictions = predictions.withColumn(
        "label", predictions["label"].cast(FloatType())
    )  # y must be float to calculate AUC.
    getSecondElement = F.udf(lambda v: float(v[1]), FloatType())
    predictions = predictions.withColumn(
        "probability_1", getSecondElement(F.col("probability"))
    )
    # Save complete model to output path according to given train date
    gbtmod1.save("/mnt/arpu/PREDORMANCY/databases/model/" + train_date)
    calc_auc(predictions)
    calc_logloss(predictions, "label", "probability_1").show()
    return spark.createDataFrame(importances_var_sel)
