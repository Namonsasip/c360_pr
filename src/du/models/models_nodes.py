import os
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import pyspark
import seaborn as sns
from kedro.io import CSVLocalDataSet
from customer360.utilities.spark_util import get_spark_session
from lightgbm import LGBMClassifier, LGBMRegressor
from mlflow import lightgbm as mlflowlightgbm
from plotnine import *
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType, col, when
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    IntegerType,
    FloatType,
    StringType,
    TimestampType,
    DecimalType,
    LongType,
    ShortType,
)
from sklearn.metrics import auc, roc_curve, precision_score, recall_score
import pandas as pd
from sklearn.model_selection import train_test_split

MODELLING_N_OBS_THRESHOLD = 500


def calculate_extra_pai_metrics(
    df_master: pyspark.sql.DataFrame, target_column: str, by: str
) -> pd.DataFrame:
    """
    Calculates some extra metrics for performance AI
    This is done in Spark separately in case sampling is required in pandas udf
    training to have info on the original metrics
    Args:
        df_master: master table
        target_column: name of the column that contains the target variable
        by: name of the column for which models will be split

    Returns:
        A pandas DataFrame with some metrics to be logged into pai
    """
    pdf_extra_pai_metrics = (
        df_master.groupby(F.col(by).alias("group"))
        .agg(
            F.mean(F.isnull(target_column).cast(DoubleType())).alias(
                "original_perc_obs_target_null"
            ),
            F.count(target_column).alias("original_n_obs"),
            F.sum((F.col(target_column) == 1).cast(DoubleType())).alias(
                "original_n_obs_positive_target"
            ),
            F.mean(target_column).alias("original_target_mean"),
            (F.stddev(target_column)).alias("original_target_stdev"),
            (F.max(target_column)).alias("original_target_max"),
            (F.min(target_column)).alias("original_target_min"),
        )
        .toPandas()
    )
    return pdf_extra_pai_metrics


def clip(df, cols, lower=0.05, upper=0.95, relativeError=0.001):
    if not isinstance(cols, (list, tuple)):
        cols = [cols]
    # Create dictionary {column-name: [lower-quantile, upper-quantile]}
    quantiles = {
        c: (
            when(col(c) < lower, lower)  # Below lower quantile
            .when(col(c) > upper, upper)  # Above upper quantile
            .otherwise(col(c))  # Between quantiles
            .alias(c)
        )
        for c, (lower, upper) in
        # Compute array of quantiles
        zip(cols, df.stat.approxQuantile(cols, [lower, upper], relativeError))
    }

    return df.select([quantiles.get(c, col(c)) for c in df.columns])


def drop_null_columns(df, thres):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    :param thres: If the number of null exceeds the thres, remove it.
    """

    null_counts = (
        df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
        .collect()[0]
        .asDict()
    )

    total_length_of_data = df.count()

    to_drop = [k for k, v in null_counts.items() if v >= (total_length_of_data * thres)]
    df = df.drop(*to_drop)
    return df


def filter_valid_product(
    l5_du_master_tbl: pyspark.sql.DataFrame,
    model_type: str,
    min_obs_per_class_for_model: int,
) -> pyspark.sql.DataFrame:
    """
    Retrieve only the valid rework macro products that agree to the conditions.
    The conditions depend on the model type.
    The data checking process is required before running a model.

    :param l5_du_master_tbl: An output from model input's pipeline. It is the DataFrame that contains relevant features.
    :param model_type: binary or regression
    :param min_obs_per_class_for_model: Minimum observations within each class from the target variable.
    :return: l5_du_master_table with the rework macro products that pass all of the conditions and list of valid
    rework macro product's name
    """

    """
    The conditions are follows:

    1. Observation of data >= 500 
    2. Number of observation in both Positive and Negative class >= 200
    3. Clip the target (for the regression model only)
    """

    # Model Checking
    supported_model_types = ["binary", "regression"]
    if model_type not in supported_model_types:
        raise ValueError(
            f"Unrecognized model type {model_type}. Supported model types are: "
            f"{', '.join(supported_model_types)}"
        )

    print(f"Checking valid rework macro products for {model_type} model.")
    print("*" * 100)

    print("Check the number of observation of the rework_macro_product.")

    obs_count_in_each_rework_macro_product = l5_du_master_tbl.groupBy(
        "rework_macro_product"
    ).count()

    agree_with_the_condition_1 = (
        obs_count_in_each_rework_macro_product.filter(
            obs_count_in_each_rework_macro_product["count"] >= MODELLING_N_OBS_THRESHOLD
        )
        .select("rework_macro_product")
        .toPandas()
    )
    rework_macro_product_that_agree_the_condition_1 = agree_with_the_condition_1[
        "rework_macro_product"
    ].to_list()

    print("Check the number of observation of the positive target.")

    obs_positive_class = l5_du_master_tbl.filter(
        l5_du_master_tbl["target_response"] == 1
    ).select("rework_macro_product", "target_response")
    obs_positive_class_cnt = obs_positive_class.groupBy("rework_macro_product").count()
    agree_with_the_condition_2_positive = obs_positive_class_cnt.filter(
        obs_positive_class_cnt["count"] >= min_obs_per_class_for_model
    ).toPandas()
    rework_macro_product_that_agree_the_condition_2_positive = agree_with_the_condition_2_positive[
        "rework_macro_product"
    ].to_list()

    print("Check the number of observation of the negative target.")

    obs_negative_class = l5_du_master_tbl.filter(
        l5_du_master_tbl["target_response"] == 0
    ).select("rework_macro_product", "target_response")
    obs_negative_class_cnt = obs_negative_class.groupBy("rework_macro_product").count()
    agree_with_the_condition_2_negative = obs_negative_class_cnt.filter(
        obs_negative_class_cnt["count"] >= min_obs_per_class_for_model
    ).toPandas()
    rework_macro_product_that_agree_the_condition_2_negative = agree_with_the_condition_2_negative[
        "rework_macro_product"
    ].to_list()

    # Retrieve only the rework_macro_product that pass all of the conditions
    valid_rework_macro_product_list = list(
        set(rework_macro_product_that_agree_the_condition_1)
        .intersection(set(rework_macro_product_that_agree_the_condition_2_positive))
        .intersection(set(rework_macro_product_that_agree_the_condition_2_negative))
    )

    unused_rework_macro_product = list(
        set(obs_count_in_each_rework_macro_product.toPandas()["rework_macro_product"])
        - set(valid_rework_macro_product_list)
    )

    print(
        "Valid rework_macro_product are:"
        + ",".join(valid_rework_macro_product_list)
        + "with length = "
        + str(len(valid_rework_macro_product_list))
    )
    print(
        "Invalid rework_macro_product are:"
        + ",".join(unused_rework_macro_product)
        + "with length = "
        + str(len(unused_rework_macro_product))
    )

    l5_du_master_tbl_only_valid_rework_macro_product = l5_du_master_tbl[
        l5_du_master_tbl["rework_macro_product"].isin(valid_rework_macro_product_list)
    ]

    # Clip the target (for the regression model only)
    if model_type == "regression":

        print("Clipping target.")
        clipped_l5_du_master_tbl_only_valid_rework_macro_product = clip(
            l5_du_master_tbl_only_valid_rework_macro_product,
            "target_relative_arpu_increase_30d",
        )

        clipped_l5_du_master_tbl_only_valid_rework_macro_product = clipped_l5_du_master_tbl_only_valid_rework_macro_product.filter(
            "target_relative_arpu_increase_30d IS NOT NULL"
        )

        print(f"Successfully checked the valid products for {model_type} model.")
        print("*" * 100)

        return (
            clipped_l5_du_master_tbl_only_valid_rework_macro_product,
            valid_rework_macro_product_list,
        )

    elif model_type == "binary":

        print(f"Successfully checked the valid products for {model_type} model.")
        print("*" * 100)

        return (
            l5_du_master_tbl_only_valid_rework_macro_product,
            valid_rework_macro_product_list,
        )


def calculate_feature_importance(
    df_master: pyspark.sql.DataFrame,
    # explanatory_features: List,
    model_params: Dict[str, Any],
    binary_target_column: str,
    regression_target_column: str,
    train_sampling_ratio: float,
    model_type: str,
    min_obs_per_class_for_model: int,
    # filepath: str,
) -> None:
    """
    Retrieve the top features based on the feature importance from the LightGBM model.
    The result is saved in .csv format

    :param df_master: Master table generated from the model input's pipeline.
    :param explanatory_features: Specified list of features
    :param model_params: Model hyperparameters
    :param binary_target_column: Target column's name of a binary classification model
    :param regression_target_column: Target column's name of a regression model
    :param train_sampling_ratio: Ratio used in train_test_split function
    :param model_type: binary or regression
    :param min_obs_per_class_for_model: Minimum observations within each class from the target variable.
    :param filepath: A filepath to save the output.
    :return: None
    """

    # Get only valid rework macro product before running a model.
    (
        l5_du_master_tbl_with_valid_product,
        valid_rework_macro_product_list,
    ) = filter_valid_product(df_master, model_type, min_obs_per_class_for_model)

    print("Excluding NULL columns")
    # Remove the columns that contain many NULL, preventing the case that some columns may contain all NULL.
    l5_du_master_tbl_with_valid_product = drop_null_columns(
        l5_du_master_tbl_with_valid_product, thres=0.98
    )

    # Pre-process the feature selection of the upcoming features, especially the data type of the column.
    # Ex. We do not want the feature of type TimeStamp, StringType

    # Get only numerical columns
    valid_feature_cols = [
        col.name
        for col in l5_du_master_tbl_with_valid_product.schema.fields
        if isinstance(col.dataType, IntegerType)
        or isinstance(col.dataType, FloatType)
        or isinstance(col.dataType, DecimalType)
        or isinstance(col.dataType, DoubleType)
        or isinstance(col.dataType, LongType)
        or isinstance(col.dataType, ShortType)
    ]

    # Remove the target column from the list of valid features.
    valid_feature_cols.remove(binary_target_column)
    valid_feature_cols.remove(regression_target_column)
    valid_feature_cols.remove(
        "partition_date"
    )  # Explicitly remove this irrelevant feature as it is saved in numerical data type.
    valid_feature_cols.remove("sum_rev_arpu_total_net_rev_daily_last_thirty_day")
    valid_feature_cols.remove("sum_rev_arpu_total_net_rev_daily_last_thirty_day_after")
    valid_feature_cols.remove(
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day_avg_all_subs"
    )
    valid_feature_cols.remove("sum_rev_arpu_total_net_rev_daily_last_seven_day_after")
    valid_feature_cols.remove(
        "sum_rev_arpu_total_net_rev_daily_last_seven_day_avg_all_subs"
    )
    valid_feature_cols.remove(
        "sum_rev_arpu_total_net_rev_daily_last_seven_day_after_avg_all_subs"
    )
    valid_feature_cols.remove(
        "sum_rev_arpu_total_net_rev_daily_last_thirty_day_after_avg_all_subs"
    )
    valid_feature_cols.remove("sum_rev_arpu_total_net_rev_daily_last_seven_day")
    valid_feature_cols.remove("target_relative_arpu_increase_7d")
    valid_feature_cols.remove("target_relative_arpu_increase_7d_avg_all_subs")
    valid_feature_cols.remove("target_relative_arpu_increase_30d_avg_all_subs")
    feature_cols = valid_feature_cols

    ###########
    ## MODEL ##
    ###########
    # Use Window function to random maximum of 10K records for each model
    n = 10000
    w = Window.partitionBy(F.col("rework_macro_product")).orderBy(F.col("rnd_"))

    sampled_master_table = (
        l5_du_master_tbl_with_valid_product.withColumn(
            "rnd_", F.rand()
        )  # Add random numbers column
        .withColumn("rn_", F.row_number().over(w))  # Add rowNumber over window
        .where(F.col("rn_") <= n)  # Take n observations
        .drop("rn_")  # Drop helper columns
        .drop("rnd_")  # Drop helper columns
    )

    feature_cols.sort()

    df_feature_importance_list = []
    master_table_pdf = sampled_master_table.toPandas()
    for product in valid_rework_macro_product_list:
        # train_single_model_df = sampled_master_table.filter(
        #     sampled_master_table["rework_macro_product"] == product
        # )
        # train_single_model_df.persist()

        # Convert spark Dataframe to Pandas Dataframe
        # train_single_model_pdf = train_single_model_df.toPandas()
        train_single_model_pdf = master_table_pdf.loc[
            master_table_pdf["rework_macro_product"] == product
        ]

        print(f"Model: {product}")
        try:
            pdf_train, pdf_test = train_test_split(
                train_single_model_pdf,
                train_size=train_sampling_ratio,
                random_state=123,
            )
        except Exception as exc:
            print(exc)
            continue
        if model_type == "binary":
            try:
                target_column = binary_target_column
                model = LGBMClassifier(**model_params).fit(
                    pdf_train[feature_cols],
                    pdf_train[target_column],
                    eval_set=[
                        (pdf_train[feature_cols], pdf_train[target_column],),
                        (pdf_test[feature_cols], pdf_test[target_column],),
                    ],
                    eval_names=["train", "test"],
                    eval_metric="auc",
                )

                # List of important feature from model
                boost = model.booster_
                df_feature_importance = pd.DataFrame(
                    {
                        "feature": boost.feature_name(),
                        "importance": boost.feature_importance(),
                        "rework_macro_product": product,
                    }
                ).sort_values("importance", ascending=False)

                df_feature_importance_list.append(df_feature_importance)
            except Exception as exc:
                print(exc)
                continue

        elif model_type == "regression":

            try:
                target_column = regression_target_column
                model = LGBMRegressor(**model_params).fit(
                    pdf_train[feature_cols],
                    pdf_train[target_column],
                    eval_set=[
                        (pdf_train[feature_cols], pdf_train[target_column],),
                        (pdf_test[feature_cols], pdf_test[target_column],),
                    ],
                    eval_names=["train", "test"],
                    eval_metric="mae",
                )

                # List of important feature from model
                boost = model.booster_
                df_feature_importance = pd.DataFrame(
                    {
                        "feature": boost.feature_name(),
                        "importance": boost.feature_importance(),
                        "rework_macro_product": product,
                    }
                ).sort_values("importance", ascending=False)
                df_feature_importance_list.append(df_feature_importance)

            except Exception as exc:
                print(exc)
                continue

    #################################
    ## Calculate Feature Importance ##
    #################################

    # Assemble feature importance dataframe
    feature_importance_df = pd.DataFrame()

    for df in df_feature_importance_list:
        feature_importance_df = pd.concat(
            [feature_importance_df, df], ignore_index=False
        )

    sum_importance = feature_importance_df["importance"].sum()
    feature_importance_df["pct"] = (
        feature_importance_df["importance"] / sum_importance
    ) * 100

    mean_feature_importance = (
        feature_importance_df.groupby("feature")["pct"]
        .mean()
        .reset_index()
        .sort_values(by="pct", ascending=False)
        .reset_index()
        .drop(columns="index")
    )

    # Get the top 100
    top100_feature_importance = mean_feature_importance[0:100]
    # top100_feature_importance.to_csv(filepath, index=False)
    # spark = get_spark_session()
    # top100_feature_importance_sdf = spark.createDataFrame(top100_feature_importance)
    ### return top100_feature as pandas dataframe, result will be store automatically in blob path
    ### according to catalog, so we don't need to write path manually using to_csv
    return top100_feature_importance


def get_top_features(
    binary_feature_imp_filepath: str,
    regression_feature_imp_filepath: str,
    top_features_filepath: str,
    feature_importance_binary_model,
    feature_importance_regression_model,
) -> None:
    """
    Read the top 100 features from binary and regression model and finalize the important features.

    :param binary_feature_imp_filepath: A filepath that save the top-100 feature importance from classification model.
    :param regression_feature_imp_filepath: A filepath that save the top-100 feature importance from regression model.
    :param top_features_filepath: A filepath to save the output.
    :return:
    """
    spark = get_spark_session()
    binary_features_csv = CSVLocalDataSet(
        filepath=binary_feature_imp_filepath,
        load_args={"sep": ","},
        save_args={"mode": "error"},
    )

    binary_features_df = binary_features_csv.load()

    regression_features_csv = CSVLocalDataSet(
        filepath=regression_feature_imp_filepath,
        load_args={"sep": ","},
        save_args={"mode": "error"},
    )

    regression_features_df = regression_features_csv.load()

    top_features = list(
        set(binary_features_df["feature"]).intersection(
            regression_features_df["feature"]
        )
    )
    top_features_df = binary_features_df[
        binary_features_df["feature"].isin(top_features)
    ]
    # top_features_df.to_csv(top_features_filepath, index=False)
    top_features_sdf = spark.createDataFrame(top_features_df)
    return top_features_sdf


def create_model_function(
    as_pandas_udf: bool, **kwargs: Any,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """
    Creates a function to train a model
    Args:
        as_pandas_udf: If True, the function returned will be a pandas udf to be
            used in a spark cluster. If False a normal python function is returned
        **kwargs: all parameters for the modelling training function, for details see
            the documentation of train_single_model inside the code of this
            function

    Returns:
        A function that trains a model from a pandas DataFrame with all parameters
        specified
    """

    schema = StructType(
        [
            StructField("able_to_model_flag", IntegerType()),
            StructField("train_set_primary_keys", StringType()),
        ]
    )

    def train_single_model_wrapper(pdf_master_chunk: pd.DataFrame,) -> pd.DataFrame:
        """
        Wrapper that allows to build a pandas udf from the model training function.
        This functions is necessary because pandas udf require just one input parameter
        which should be a pandas DataFrame but we want our modelling function to be
        parametrizable
        Args:
            pdf_master_chunk: master table for training

        Returns:
            A pandas DataFrame with information about training
        """

        def train_single_model(
            pdf_master_chunk: pd.DataFrame,
            model_type: str,
            group_column: str,
            explanatory_features_list,
            target_column: str,
            train_sampling_ratio: float,
            model_params: Dict[str, Any],
            min_obs_per_class_for_model: int,
            extra_tag_columns: List[str],
            pai_run_prefix: str,
            pdf_extra_pai_metrics: pd.DataFrame,
            mlflow_model_version: int,
            regression_clip_target_quantiles: Tuple[float, float] = None,
        ) -> pd.DataFrame:
            """
            Trains a model and logs the process in pai
            Args:
                pdf_master_chunk: pandas DataFrame with the data (master table)
                model_type: type of model to train. Supports "binary" and "regression"
                group_column: column that contains an identifier for the group of the
                    model, this is useful in case many models want to be trained
                    using a similar structure
                top_features_path: path to top features list, result from top features selection process
                model_params: model hyperparameters
                min_obs_per_class_for_model: minimum observations within each class
                    from the target variable that are required to confidently train
                    a model
                extra_tag_columns: other columns from the master table to add as a tag,
                    they should be unique
                regression_clip_target_quantiles: (only applicable for regression)
                    Tuple with the quantiles to clip the target before model training
                pai_run_prefix: prefix that the pai run will have. The name will be the
                    prefix concatenated with the group
                pdf_extra_pai_metrics: extra pai metrics to log
                pai_runs_uri: uri where pai run will be stored
                pai_artifacts_uri: uri where pai run will be stored

            Returns:
                A pandas DataFrame with some info on the execution
            """

            # We declare the function within the pandas udf to avoid having dependencies
            # that would require to export the project code as an egg file and install
            # it as a cluster library in Databricks, being a major inconvenience for
            # development
            def plot_roc_curve(
                y_true,
                y_score,
                filepath=None,
                line_width=2,
                width=10,
                height=8,
                title=None,
                colors=("#FF0000", "#000000"),
            ):
                """
                Saves a ROC curve in a file or shows it on screen.
                :param y_true: actual values of the response (list|np.array)
                :param y_score: predicted scores (list|np.array)
                :param filepath: if given, the ROC curve is saved in the desired filepath. It should point to a png file in an
                existing directory. If not specified, the curve is only shown (str)
                :param line_width: number indicating line width (float)
                :param width: number indicating the width of saved plot (float)
                :param height: number indicating the height of saved plot (float)
                :param title: if given, title to add to the top side of the plot (str)
                :param colors: color specification for ROC curve and diagonal respectively (tuple of str)
                :return: None
                """
                fpr, tpr, _ = roc_curve(y_true=y_true, y_score=y_score)
                auc_score = auc(fpr, tpr)

                sns.set_style("whitegrid")
                fig = plt.figure(figsize=(width, height))
                major_ticks = np.arange(0, 1.1, 0.1)
                minor_ticks = np.arange(0.05, 1, 0.1)
                ax = fig.add_subplot(1, 1, 1)
                ax.set_xticks(major_ticks)
                ax.set_yticks(major_ticks)
                ax.set_xticks(minor_ticks, minor=True)
                ax.set_yticks(minor_ticks, minor=True)
                ax.grid(which="both", axis="both")
                ax.grid(which="minor", alpha=0.2)
                ax.grid(which="major", alpha=0.5)
                ax.tick_params(which="major", direction="out", length=5)
                plt.plot(
                    fpr,
                    tpr,
                    color=colors[0],
                    lw=line_width,
                    label="ROC curve (AUC = {:.4f})".format(
                        auc_score
                    ),  # getting decimal points in auc roc curves
                )
                plt.plot([0, 1], [0, 1], color=colors[1], lw=line_width, linestyle="--")
                plt.xlim([-0.001, 1.001])
                plt.ylim([-0.001, 1.001])
                plt.xlabel("False positive rate", fontsize=15)
                plt.ylabel("True positive rate", fontsize=15)
                if title:
                    plt.title(title, fontsize=30, loc="left")
                plt.legend(
                    loc="lower right", frameon=True, fontsize="xx-large", fancybox=True
                )
                plt.tight_layout()
                if filepath:
                    plt.savefig(filepath, dpi=70)
                    plt.close()
                else:
                    plt.show()

            def plot_important(features, importance, filepath, max_num_features=None):
                if max_num_features is None:
                    indices = np.argsort(importance)
                else:
                    indices = np.argsort(importance)[-max_num_features:]
                features = np.array(features)[indices]
                importance = importance[indices]
                num_features = len(features)
                # If num_features > 10, increase the figure height to prevent the plot
                # from being too dense.
                w, h = [6.4, 4.8]  # matplotlib's default figure size
                h = h + 0.1 * num_features if num_features > 10 else h
                fig, ax = plt.subplots(figsize=(w, h))
                yloc = np.arange(num_features)
                ax.barh(yloc, importance, align="center", height=0.5)
                ax.set_yticks(yloc)
                ax.set_yticklabels(features)
                ax.set_xlabel("Importance")
                ax.set_title("Feature Importance")
                fig.tight_layout()
                if filepath:
                    fig.savefig(filepath, dpi=70)
                else:
                    fig.show()

            def mean_absolute_percentage_error(y_true, y_pred):
                y_true, y_pred = np.array(y_true), np.array(y_pred)
                return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

            def calculate_precision_group(df):
                y_pred = df.y_pred
                y = df.y_true
                return precision_score(y_true=y, y_pred=y_pred)

            def calculate_recall_group(df):
                y_pred = df.y_pred
                y = df.y_true
                return recall_score(y_true=y, y_pred=y_pred)

            def get_metrics_by_percentile(y_true, y_pred, y_proba) -> pd.DataFrame:
                """
                 Performance report generation function to check model performance at percentile level.
                :param y_true: numpy array with the real value of the target variable
                :param y_pred: numpy array with the predicted value by the model
                :param y_proba: numpy array with the predicted value (in probability) by the model
                :return: pandas.DataFrame with the different KPIs:
                        - percentile: Percentile of the distribution
                        - population: Number of observations al percentile level
                        - positive_cases: Number of positive cases at percentile level
                        - avg_score: Model score average per percentile
                        - cum_y_true: Cumulative positive cases
                        - cum_population: Cumulative population
                        - cum_prob: Cumulative probability
                        - cum_percentage_target: Cumulative percentage of the total positive population captured
                        - uplift: Cumulative uplift
                """
                # --------------- UPLIFT ---------------
                report = pd.DataFrame(
                    {"y_true": y_true, "y_proba": y_proba, "y_pred": y_pred},
                    columns=["y_true", "y_proba", "y_pred"],
                )

                report["score_rank"] = report.y_proba.rank(
                    method="first", ascending=True, pct=True
                )
                report["percentile"] = np.floor((1 - report.score_rank) * 100) + 1

                report_copied_for_prc_recall_calculation = report.copy()

                report["population"] = 1
                report = (
                    report.groupby(["percentile"])
                    .agg({"y_true": "sum", "population": "sum", "y_proba": "mean"})
                    .reset_index()
                )
                report = report.rename(
                    columns={"y_proba": "avg_score", "y_true": "positive_cases"}
                )
                report = report[
                    ["percentile", "population", "positive_cases", "avg_score"]
                ]

                report["cum_y_true"] = report.positive_cases.cumsum()
                report["cum_population"] = report.population.cumsum()
                report["cum_prob"] = report.cum_y_true / report.cum_population
                report["cum_percentage_target"] = (
                    report["cum_y_true"] / report["cum_y_true"].max()
                )
                report["uplift"] = report.cum_prob / (
                    report.positive_cases.sum() / report.population.sum()
                )

                # --------------- PRECISION & RECALL ---------------

                precision_grp = (
                    report_copied_for_prc_recall_calculation.groupby(["percentile"])
                    .apply(calculate_precision_group)
                    .to_frame()
                    .reset_index()
                )
                precision_grp.columns = ["percentile", "precision"]

                recall_grp = (
                    report_copied_for_prc_recall_calculation.groupby(["percentile"])
                    .apply(calculate_recall_group)
                    .to_frame()
                    .reset_index()
                )
                recall_grp.columns = ["percentile", "recall"]

                report = report.merge(precision_grp, on="percentile", how="left").merge(
                    recall_grp, on="percentile", how="left"
                )

                return report

            def get_metrics_by_deciles(y_true, y_pred, y_proba) -> pd.DataFrame:

                # --------------- UPLIFT ---------------
                report = pd.DataFrame(
                    {"y_true": y_true, "y_proba": y_proba, "y_pred": y_pred},
                    columns=["y_true", "y_proba", "y_pred"],
                )

                report["score_rank"] = report.y_proba.rank(
                    method="first", ascending=True, pct=True
                )
                report["decile"] = np.floor((1 - report.score_rank) * 10) + 1

                report_copied_for_prc_recall_calculation = report.copy()

                report["population"] = 1
                report = (
                    report.groupby(["decile"])
                    .agg({"y_true": "sum", "population": "sum", "y_proba": "mean"})
                    .reset_index()
                )
                report = report.rename(
                    columns={"y_proba": "avg_score", "y_true": "positive_cases"}
                )
                report = report[["decile", "population", "positive_cases", "avg_score"]]

                report["cum_y_true"] = report.positive_cases.cumsum()
                report["cum_population"] = report.population.cumsum()
                report["cum_prob"] = report.cum_y_true / report.cum_population
                report["cum_percentage_target"] = (
                    report["cum_y_true"] / report["cum_y_true"].max()
                )
                report["uplift"] = report.cum_prob / (
                    report.positive_cases.sum() / report.population.sum()
                )

                # --------------- PRECISION & RECALL ---------------

                precision_grp = (
                    report_copied_for_prc_recall_calculation.groupby(["decile"])
                    .apply(calculate_precision_group)
                    .to_frame()
                    .reset_index()
                )
                precision_grp.columns = ["decile", "precision"]

                recall_grp = (
                    report_copied_for_prc_recall_calculation.groupby(["decile"])
                    .apply(calculate_recall_group)
                    .to_frame()
                    .reset_index()
                )
                recall_grp.columns = ["decile", "recall"]

                report = report.merge(precision_grp, on="decile", how="left").merge(
                    recall_grp, on="decile", how="left"
                )

                return report

            # MODEL CHECKING
            supported_model_types = ["binary", "regression"]
            if model_type not in supported_model_types:
                raise ValueError(
                    f"Unrecognized model type {model_type}. Supported model types are: "
                    f"{', '.join(supported_model_types)}"
                )

            if len(pdf_master_chunk[group_column].unique()) > 1:
                raise ValueError(
                    f"More than one group found in training table: "
                    f"{pdf_master_chunk[group_column].unique()}"
                )

            if (
                model_type == "regression"
                and regression_clip_target_quantiles is not None
            ):
                # Clip target to avoid that outliers affect the model
                pdf_master_chunk[target_column] = np.clip(
                    pdf_master_chunk[target_column],
                    a_min=pdf_master_chunk[target_column].quantile(
                        regression_clip_target_quantiles[0]
                    ),
                    a_max=pdf_master_chunk[target_column].quantile(
                        regression_clip_target_quantiles[1]
                    ),
                )

            # Sort features since MLflow does not guarantee the order
            explanatory_features_list.sort()

            current_group = pdf_master_chunk[group_column].iloc[0]

            # prefix %Y%m%d_%H%M%S_du_thanasiy_dev_{rework_macro_product}
            pai_run_name = pai_run_prefix + current_group + "_"

            pdf_extra_pai_metrics_filtered = pdf_extra_pai_metrics[
                pdf_extra_pai_metrics["group"] == current_group
            ]

            # Calculate some metrics on the data to log into pai
            pai_metrics_dict = {}

            original_metrics = [
                "original_perc_obs_target_null",
                "original_n_obs",
                "original_target_mean",
            ]
            if model_type == "binary":
                original_metrics += [
                    "original_n_obs_positive_target",
                ]
            elif model_type == "regression":
                original_metrics += [
                    "original_target_stdev",
                    "original_target_min",
                    "original_target_max",
                ]

            for metric in original_metrics:
                pai_metrics_dict[metric] = pdf_extra_pai_metrics_filtered[metric].iloc[
                    0
                ]

            modelling_perc_obs_target_null = np.mean(
                pdf_master_chunk[target_column].isna()
            )
            pai_metrics_dict[
                "modelling_perc_obs_target_null"
            ] = modelling_perc_obs_target_null

            pdf_master_chunk = pdf_master_chunk[~pdf_master_chunk[target_column].isna()]

            modelling_n_obs = len(pdf_master_chunk)
            pai_metrics_dict["modelling_n_obs"] = modelling_n_obs

            if model_type == "binary":
                modelling_n_obs_positive_target = len(
                    pdf_master_chunk[pdf_master_chunk[target_column] == 1]
                )
                pai_metrics_dict[
                    "modelling_n_obs_positive_target"
                ] = modelling_n_obs_positive_target
            elif model_type == "regression":
                modelling_target_stdev = pdf_master_chunk[target_column].std()
                pai_metrics_dict["modelling_target_stdev"] = modelling_target_stdev

            modelling_target_mean = np.mean(pdf_master_chunk[target_column])
            pai_metrics_dict["modelling_target_mean"] = modelling_target_mean

            # path for each model run
            mlflow_path = "/Shared/data_upsell/lightgbm"
            if mlflow.get_experiment_by_name(mlflow_path) is None:
                mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
            else:
                mlflow_experiment_id = mlflow.get_experiment_by_name(
                    mlflow_path
                ).experiment_id
            with mlflow.start_run(
                experiment_id=mlflow_experiment_id, run_name=current_group
            ):
                run_id = mlflow.tracking.fluent._get_or_start_run().info.run_id
                tp = pai_run_name + run_id
                tmp_path = Path("data/tmp") / tp
                os.makedirs(tmp_path, exist_ok=True)

                mlflow.log_params(
                    {
                        "Version": mlflow_model_version,
                        "target_column": target_column,
                        "model_objective": model_type,
                        "regression_clip_target_quantiles": regression_clip_target_quantiles,
                    }
                )

                able_to_model_flag = True
                if modelling_perc_obs_target_null != 0:
                    able_to_model_flag = False
                    mlflow.set_tag(
                        "Unable to model",
                        "There are observations with NA target in the modelling data",
                    )

                if modelling_n_obs < MODELLING_N_OBS_THRESHOLD:
                    able_to_model_flag = False
                    mlflow.set_tag(
                        "Unable to model",
                        f"There are not enough observations to reliably train a model. "
                        f"Minimum required is {MODELLING_N_OBS_THRESHOLD}, "
                        f"found {modelling_n_obs}",
                    )

                if pai_metrics_dict["original_perc_obs_target_null"] == 1:
                    able_to_model_flag = False
                    mlflow.set_tag(
                        "Unable to model",
                        "The are no observations with non-null target",
                    )

                if len(pdf_master_chunk[target_column].unique()) <= 1:
                    able_to_model_flag = False
                    mlflow.set_tag(
                        "Unable to model", "Target variable has only one unique value"
                    )

                if model_type == "binary":
                    if (
                        pai_metrics_dict["original_n_obs_positive_target"]
                        < min_obs_per_class_for_model
                    ):
                        able_to_model_flag = False
                        mlflow.set_tag(
                            "Unable to model",
                            f"The number of positive responses is not enough to reliably train a model. "
                            f"There are {pai_metrics_dict['original_n_obs_positive_target']} "
                            f"observations while minimum required is {min_obs_per_class_for_model}",
                        )
                    if (
                        pai_metrics_dict["original_n_obs"]
                        - pai_metrics_dict["original_n_obs_positive_target"]
                        < min_obs_per_class_for_model
                    ):
                        able_to_model_flag = False
                        mlflow.set_tag(
                            "Unable to model",
                            f"The number of negative responses is not enough to reliably train a model. "
                            f"There are {pai_metrics_dict['original_n_obs_positive_target']} "
                            f"observations while minimum required is {min_obs_per_class_for_model}",
                        )

                # build the DataFrame to return
                df_to_return = pd.DataFrame(
                    {
                        "able_to_model_flag": [int(able_to_model_flag)],
                        "train_set_primary_keys": ["NULL"],
                    }
                )
                if not able_to_model_flag:
                    # Unable to train a model, end execution
                    mlflow.log_param("Able_to_model", False)
                    return df_to_return
                else:
                    mlflow.log_param("Able_to_model", True)
                    # Train the model
                    pdf_master_chunk = pdf_master_chunk.sort_values(
                        ["subscription_identifier", "contact_date"]
                    )
                    pdf_train, pdf_test = train_test_split(
                        pdf_master_chunk,
                        train_size=train_sampling_ratio,
                        random_state=123,
                    )
                    mlflow.log_params(
                        {
                            "train_sampling_ratio": train_sampling_ratio,
                            "model_params": model_params,
                        }
                    )
                    if model_type == "binary":
                        model = LGBMClassifier(**model_params).fit(
                            pdf_train[explanatory_features_list],
                            pdf_train[target_column],
                            eval_set=[
                                (
                                    pdf_train[explanatory_features_list],
                                    pdf_train[target_column],
                                ),
                                (
                                    pdf_test[explanatory_features_list],
                                    pdf_test[target_column],
                                ),
                            ],
                            eval_names=["train", "test"],
                            eval_metric="auc",
                        )

                        test_predictions_proba = model.predict_proba(
                            pdf_test[explanatory_features_list]
                        )[:, 1]

                        test_predictions = model.predict(
                            pdf_test[explanatory_features_list]
                        )

                        plot_important(
                            explanatory_features_list,
                            model.feature_importances_,
                            filepath=tmp_path / "important_features.png",
                            max_num_features=20,
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "important_features.png"), artifact_path=""
                        )
                        mlflowlightgbm.log_model(model.booster_, artifact_path="")

                        train_auc = model.evals_result_["train"]["auc"][-1]
                        test_auc = model.evals_result_["test"]["auc"][-1]
                        recall = recall_score(
                            y_true=pdf_test[target_column],
                            y_pred=test_predictions,
                            pos_label=1,
                            average="binary",
                        )

                        mlflow.log_metric("train_auc", train_auc)
                        mlflow.log_metric("test_auc", test_auc)
                        mlflow.log_metric("train_test_auc_diff", train_auc - test_auc)
                        mlflow.log_metric("recall", recall)

                        if os.path.isfile(tmp_path / "roc_curve.png"):
                            raise AssertionError(
                                f"There is already a file in the tmp directory "
                                f"when running the {current_group} model"
                            )

                        # Plot ROC curve
                        plot_roc_curve(
                            y_true=pdf_test[target_column],
                            y_score=test_predictions_proba,
                            filepath=tmp_path / "roc_curve.png",
                        )

                        # Calculate and plot AUC per round
                        pdf_metrics = pd.DataFrame()
                        boost = model.booster_
                        df_feature_importance = pd.DataFrame(
                            {
                                "feature": boost.feature_name(),
                                "importance": boost.feature_importance(),
                            }
                        ).sort_values("importance", ascending=False)
                        df_feature_importance.to_csv(
                            tmp_path / "important_features.csv", index=False
                        )
                        for valid_set_name, metrics_dict in model.evals_result_.items():
                            metrics_dict["set"] = valid_set_name
                            pdf_metrics_partial = pd.DataFrame(metrics_dict)
                            pdf_metrics_partial["round"] = range(
                                1, pdf_metrics_partial.shape[0] + 1
                            )
                            pdf_metrics = pd.concat([pdf_metrics, pdf_metrics_partial])
                        pdf_metrics_melted = pdf_metrics.melt(
                            id_vars=["set", "round"], var_name="metric"
                        )
                        pdf_metrics_melted.to_csv(
                            tmp_path / "metrics_by_round.csv", index=False
                        )

                        (  # Plot the AUC of each set in each round
                            ggplot(
                                pdf_metrics_melted[
                                    pdf_metrics_melted["metric"] == "auc"
                                ],
                                aes(x="round", y="value", color="set"),
                            )
                            + ylab("AUC")
                            + geom_line()
                            + ggtitle(f"AUC per round (tree) for {current_group}")
                        ).save(tmp_path / "auc_per_round.png")

                        # Create a CSV report with percentile metrics
                        df_metrics_by_percentile = get_metrics_by_percentile(
                            y_true=pdf_test[target_column],
                            y_pred=test_predictions,
                            y_proba=test_predictions_proba,
                        )

                        df_metrics_by_deciles = get_metrics_by_deciles(
                            y_true=pdf_test[target_column],
                            y_pred=test_predictions,
                            y_proba=test_predictions_proba,
                        )
                        df_metrics_by_percentile.to_csv(
                            tmp_path / "metrics_by_percentile.csv", index=False
                        )
                        df_metrics_by_deciles.to_csv(
                            tmp_path / "metrics_by_deciles.csv", index=False
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "roc_curve.png"), artifact_path=""
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "metrics_by_round.csv"), artifact_path=""
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "auc_per_round.png"), artifact_path=""
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "metrics_by_percentile.csv"),
                            artifact_path="",
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "metrics_by_deciles.csv"), artifact_path="",
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "important_features.csv"), artifact_path="",
                        )

                    elif model_type == "regression":
                        model = LGBMRegressor(**model_params).fit(
                            pdf_train[explanatory_features_list],
                            pdf_train[target_column],
                            eval_set=[
                                (
                                    pdf_train[explanatory_features_list],
                                    pdf_train[target_column],
                                ),
                                (
                                    pdf_test[explanatory_features_list],
                                    pdf_test[target_column],
                                ),
                            ],
                            eval_names=["train", "test"],
                            eval_metric="mae",
                        )

                        test_predictions = model.predict(
                            pdf_test[explanatory_features_list]
                        )
                        train_predictions = model.predict(
                            pdf_train[explanatory_features_list]
                        )
                        mlflowlightgbm.log_model(model.booster_, artifact_path="")
                        test_mape = mean_absolute_percentage_error(
                            y_true=pdf_test[target_column], y_pred=test_predictions
                        )
                        train_mape = mean_absolute_percentage_error(
                            y_true=pdf_train[target_column], y_pred=train_predictions
                        )
                        mlflow.log_metric("train_mape", train_mape)
                        mlflow.log_metric("test_mape", test_mape)

                        train_mae = model.evals_result_["train"]["l1"][-1]
                        test_mae = model.evals_result_["test"]["l1"][-1]
                        mlflow.log_metric("train_mae", train_mae)
                        mlflow.log_metric("test_mae", test_mae)
                        mlflow.log_metric(
                            "test_benchmark_target_average",
                            np.mean(
                                np.abs(
                                    pdf_test[target_column]
                                    - pdf_test[target_column].mean()
                                )
                            ),
                        )
                        plot_important(
                            explanatory_features_list,
                            model.feature_importances_,
                            filepath=tmp_path / "important_features.png",
                            max_num_features=20,
                        )
                        boost = model.booster_
                        df_feature_importance = pd.DataFrame(
                            {
                                "feature": boost.feature_name(),
                                "importance": boost.feature_importance(),
                            }
                        ).sort_values("importance", ascending=False)

                        df_feature_importance.to_csv(
                            tmp_path / "important_features.csv", index=False
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "important_features.png"), artifact_path=""
                        )
                        mlflow.log_metric("train_test_mae_diff", train_mae - test_mae)
                        # Plot target and score distributions
                        (
                            ggplot(
                                pd.DataFrame(
                                    {
                                        "Real": pdf_test[target_column],
                                        "Predicted": test_predictions,
                                    }
                                ).melt(var_name="Source", value_name="ARPU_uplift"),
                                aes(x="ARPU_uplift", fill="Source"),
                            )
                            + geom_density(alpha=0.5)
                            + ggtitle(
                                f"ARPU uplift distribution for real target and model prediction"
                            )
                        ).save(tmp_path / "ARPU_uplift_distribution.png")

                        # Calculate and plot AUC per round
                        pdf_metrics = pd.DataFrame()
                        for valid_set_name, metrics_dict in model.evals_result_.items():
                            metrics_dict["set"] = valid_set_name
                            pdf_metrics_partial = pd.DataFrame(metrics_dict)
                            pdf_metrics_partial["round"] = range(
                                1, pdf_metrics_partial.shape[0] + 1
                            )
                            pdf_metrics = pd.concat([pdf_metrics, pdf_metrics_partial])
                        pdf_metrics_melted = pdf_metrics.melt(
                            id_vars=["set", "round"], var_name="metric"
                        )
                        pdf_metrics_melted.to_csv(
                            tmp_path / "metrics_by_round.csv", index=False
                        )

                        (  # Plot the MAE of each set in each round
                            ggplot(
                                pdf_metrics_melted[
                                    pdf_metrics_melted["metric"] == "l1"
                                ],
                                aes(x="round", y="value", color="set"),
                            )
                            + ylab("MAE")
                            + geom_line()
                            + ggtitle(f"MAE per round (tree) for {current_group}")
                        ).save(tmp_path / "mae_per_round.png")

                        mlflow.log_artifact(
                            str(tmp_path / "ARPU_uplift_distribution.png"),
                            artifact_path="",
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "important_features.csv"), artifact_path="",
                        )

                        mlflow.log_artifact(
                            str(tmp_path / "metrics_by_round.csv"), artifact_path=""
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "mae_per_round.png"), artifact_path=""
                        )
                    df_to_return = pd.DataFrame(
                        {
                            "able_to_model_flag": int(able_to_model_flag),
                            "train_set_primary_keys": pdf_train["du_spine_primary_key"],
                        }
                    )

                return df_to_return

        return train_single_model(pdf_master_chunk=pdf_master_chunk, **kwargs)

    model_function = train_single_model_wrapper

    if as_pandas_udf:
        model_function = pandas_udf(
            model_function, schema, functionType=PandasUDFType.GROUPED_MAP
        )

    return model_function


def train_multiple_models(
    df_master: pyspark.sql.DataFrame,
    group_column: str,
    target_column: str,
    du_top_features,
    undersampling,
    extra_keep_columns: List[str] = None,
    max_rows_per_group: int = None,
    **kwargs: Any,
) -> pyspark.sql.DataFrame:
    """
    Trains multiple models using pandas udf to distribute the training in a spark cluster
    Args:
        df_master: master table
        group_column: column name for the group, a model will be trained for each unique
            value of this column
        explanatory_features: list of features name to learn from. Must exist in
            df_master
        target_column: column with target variable
        extra_keep_columns: extra columns that will be kept in the master when doing the
            pandas UDF, should only be restricted to the ones that will be used since
            this might consume a lot of memory
        max_rows_per_group: maximum rows that the train set of the model will have.
            If for a group the number of rows is larget it will be randomly sampled down
        **kwargs: further arguments to pass to the modelling function, they are not all
            explicitly listed here for easier code maintenance but details can be found
            in the definition of train_single_model

    Returns:
        A spark DataFrame with info about the training
    """
    # explanatory_features = du_top_features.toPandas()
    ### Passing csv catalog allow kedro to load data into pandas dataframe automatically
    explanatory_features_list = du_top_features["feature"].to_list()

    explanatory_features_list.sort()

    if extra_keep_columns is None:
        extra_keep_columns = []

    # Increase number of partitions when training models to ensure data stays small
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2100)

    # To reduce the size of the pandas DataFrames only select the columns we really need
    # Also cast decimal type columns cause they don't get properly converted to pandas
    df_master_only_necessary_columns = df_master.select(
        "subscription_identifier",
        "contact_date",
        "du_spine_primary_key",
        group_column,
        target_column,
        *(
            extra_keep_columns
            + [
                F.col(column_name).cast(FloatType())
                if column_type.startswith("decimal")
                else F.col(column_name)
                for column_name, column_type in df_master.select(
                    *explanatory_features_list
                ).dtypes
            ]
        ),
    )

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        df_master_only_necessary_columns, target_column, group_column
    )

    # Filter rows with NA target to reduce size of pandas DataFrames within pandas udf
    df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
        ~F.isnull(F.col(target_column))
    )

    # Sample down if data is too large to reliably train a model
    if max_rows_per_group is not None:
        df_master_only_necessary_columns_aux = df_master_only_necessary_columns.withColumn(
            "aux_n_rows_per_group",
            F.count(F.lit(1)).over(Window.partitionBy(group_column)),
        )
        df_master_only_necessary_columns_aux = df_master_only_necessary_columns_aux.filter(
            F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
        ).drop(
            "aux_n_rows_per_group"
        )

    if undersampling:
        # Remove Disney+ out and we will fill it with undersampled Disney+ data later
        df_master_only_necessary_columns_aux = df_master_only_necessary_columns_aux.filter(
            df_master_only_necessary_columns_aux[group_column] != "DisneyPlusHotstar"
        )

        # Load Disney data
        disney = df_master.select(
            "subscription_identifier",
            "contact_date",
            "du_spine_primary_key",
            group_column,
            target_column,
            *(
                extra_keep_columns
                + [
                    F.col(column_name).cast(FloatType())
                    if column_type.startswith("decimal")
                    else F.col(column_name)
                    for column_name, column_type in df_master.select(
                        *explanatory_features_list
                    ).dtypes
                ]
            ),
        )

        # Filter rows with NA target to reduce size of pandas DataFrames within pandas udf
        disney = disney.filter(~F.isnull(F.col(target_column)))

        major_df = disney.filter(F.col("target_response") == 0)
        minor_df = disney.filter(F.col("target_response") == 1)

        major = major_df.count()
        minor = minor_df.count()
        ratio = int(major / minor)

        sampled_majority_df = major_df.sample(withReplacement=False, fraction=1 / ratio)

        # Union under-sampled Disney with main dataframe
        combined_df = sampled_majority_df.union(minor_df)
        df_master_with_undersampled_disney = df_master_only_necessary_columns_aux.union(
            combined_df
        )

        df_master = df_master_with_undersampled_disney.alias("df_master")
    else:
        df_master = df_master_only_necessary_columns_aux.alias("df_master")

    df_training_info = df_master.groupby(group_column).apply(
        create_model_function(
            as_pandas_udf=True,
            group_column=group_column,
            explanatory_features_list=explanatory_features_list,
            target_column=target_column,
            pdf_extra_pai_metrics=pdf_extra_pai_metrics,
            extra_tag_columns=extra_keep_columns,
            **kwargs,
        )
    )
    return df_training_info


def train_disney_models(
    df_disney: pyspark.sql.DataFrame,
    group_column: str,
    target_column: str,
    du_top_features,
    extra_keep_columns: List[str] = None,
    max_rows_per_group: int = None,
    **kwargs: Any,
) -> pyspark.sql.DataFrame:
    """
    Trains multiple models using pandas udf to distribute the training in a spark cluster
    Args:
        df_master: master table
        group_column: column name for the group, a model will be trained for each unique
            value of this column
        explanatory_features: list of features name to learn from. Must exist in
            df_master
        target_column: column with target variable
        extra_keep_columns: extra columns that will be kept in the master when doing the
            pandas UDF, should only be restricted to the ones that will be used since
            this might consume a lot of memory
        max_rows_per_group: maximum rows that the train set of the model will have.
            If for a group the number of rows is larget it will be randomly sampled down
        **kwargs: further arguments to pass to the modelling function, they are not all
            explicitly listed here for easier code maintenance but details can be found
            in the definition of train_single_model

    Returns:
        A spark DataFrame with info about the training
    """
    # explanatory_features = du_top_features.toPandas()
    ### Passing csv catalog allow kedro to load data into pandas dataframe automatically
    explanatory_features_list = du_top_features["feature"].to_list()

    explanatory_features_list.sort()

    if extra_keep_columns is None:
        extra_keep_columns = []

    # Increase number of partitions when training models to ensure data stays small
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2100)

    # To reduce the size of the pandas DataFrames only select the columns we really need
    # Also cast decimal type columns cause they don't get properly converted to pandas
    disney = df_disney.select(
        "subscription_identifier",
        "contact_date",
        "du_spine_primary_key",
        group_column,
        target_column,
        *(
            extra_keep_columns
            + [
                F.col(column_name).cast(FloatType())
                if column_type.startswith("decimal")
                else F.col(column_name)
                for column_name, column_type in df_disney.select(
                    *explanatory_features_list
                ).dtypes
            ]
        ),
    )

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        disney, target_column, group_column
    )

    # Sample down if data is too large to reliably train a model
    # if max_rows_per_group is not None:
    #     df_master_only_necessary_columns_aux = df_master_only_necessary_columns.withColumn(
    #         "aux_n_rows_per_group",
    #         F.count(F.lit(1)).over(Window.partitionBy(group_column)),
    #     )
    #     df_master_only_necessary_columns_aux = df_master_only_necessary_columns_aux.filter(
    #         F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
    #     ).drop("aux_n_rows_per_group")

    # Filter rows with NA target to reduce size of pandas DataFrames within pandas udf
    disney = disney.filter(~F.isnull(F.col(target_column)))

    major_df = disney.filter(F.col("target_response") == 0)
    minor_df = disney.filter(F.col("target_response") == 1)

    major = major_df.count()
    minor = minor_df.count()
    ratio = int(major / minor)

    sampled_majority_df = major_df.sample(withReplacement=False, fraction=24 / ratio)

    # Union under-sampled Disney with main dataframe
    df_master = sampled_majority_df.union(minor_df)

    df_training_info = df_master.groupby(group_column).apply(
        create_model_function(
            as_pandas_udf=True,
            group_column=group_column,
            explanatory_features_list=explanatory_features_list,
            target_column=target_column,
            pdf_extra_pai_metrics=pdf_extra_pai_metrics,
            extra_tag_columns=extra_keep_columns,
            **kwargs,
        )
    )
    return df_training_info


def train_single_model_call(
    df_master: pyspark.sql.DataFrame,
    group_column: str,
    explanatory_features: List[str],
    target_column: str,
    target_group,
    extra_keep_columns: List[str] = None,
    max_rows_per_group: int = None,
    **kwargs: Any,
) -> pyspark.sql.DataFrame:
    explanatory_features.sort()

    if extra_keep_columns is None:
        extra_keep_columns = []

    # Increase number of partitions when training models to ensure data stays small
    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", 2100)
    df_master = df_master.where("rework_macro_product ='" + target_group + "'")
    # To reduce the size of the pandas DataFrames only select the columns we really need
    # Also cast decimal type columns cause they don't get properly converted to pandas
    df_master_only_necessary_columns = df_master.select(
        "subscription_identifier",
        "contact_date",
        "du_spine_primary_key",
        group_column,
        target_column,
        *(
            extra_keep_columns
            + [
                F.col(column_name).cast(FloatType())
                if column_type.startswith("decimal")
                else F.col(column_name)
                for column_name, column_type in df_master.select(
                    *explanatory_features
                ).dtypes
            ]
        ),
    )

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        df_master_only_necessary_columns, target_column, group_column
    )

    # Filter rows with NA target to reduce size of pandas DataFrames within pandas udf
    df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
        ~F.isnull(F.col(target_column))
    )

    # df_training_info = df_master_only_necessary_columns.groupby(group_column).apply(
    #     create_model_functionup_column,
    #         explanatory_features=explanatory_features,
    #         target_column=target_column,
    #         pdf_extra_pai_metrics=pdf_extra_pai_metrics,
    #         extra_tag_columns=extra_keep_columns,
    #         **kwargs,
    #     )
    # )
    # Sample down if data is too large to reliably train a model
    if max_rows_per_group is not None:
        df_master_only_necessary_columns = df_master_only_necessary_columns.withColumn(
            "aux_n_rows_per_group",
            F.count(F.lit(1)).over(Window.partitionBy(group_column)),
        )
        df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
            F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
        ).drop("aux_n_rows_per_group")
    pdf_master_only_necessary_columns = df_master_only_necessary_columns.toPandas()
    df_training_info = lambda pdf_master_chunk, pdf_extra_pai_metrics: create_model_function(
        as_pandas_udf=False,
        group_column=group_column,
        explanatory_features=explanatory_features,
        target_column=target_column,
        pdf_extra_pai_metrics=pdf_extra_pai_metrics,
        extra_tag_columns=extra_keep_columns,
        pdf_master_chunk=pdf_master_only_necessary_columns,
        **kwargs,
    )(
        pdf_master_chunk
    )
    return df_training_info


#
# pai_run_prefix="dummy_acceptance_"
# df_master = catalog.load("l5_du_master_tbl")
# group_column = catalog.load("params:du_model_group_column")
# explanatory_features = catalog.load("params:du_model_explanatory_features")
# target_column= catalog.load("params:du_acceptance_model_target_column")
# train_sampling_ratio= catalog.load("params:du_model_train_sampling_ratio")
# model_params= catalog.load("params:du_model_model_params")
# max_rows_per_group= catalog.load("params:du_model_max_rows_per_group")
# min_obs_per_class_for_model= catalog.load("params:du_model_min_obs_per_class_for_model")
# extra_keep_columns= catalog.load("params:du_extra_tag_columns_pai")
# pai_runs_uri= catalog.load("params:du_pai_runs_uri")
# pai_artifacts_uri= catalog.load("params:du_pai_artifacts_uri")
# explanatory_features.sort()
#
# if extra_keep_columns is None:
#     extra_keep_columns = []
#
# # Increase number of partitions when training models to ensure data stays small
# spark = get_spark_session()
# spark.conf.set("spark.sql.shuffle.partitions", 2100)
#
# # To reduce the size of the pandas DataFrames only select the columns we really need
# # Also cast decimal type columns cause they don't get properly converted to pandas
# df_master_only_necessary_columns = df_master.select(
#     "subscription_identifier",
#     "contact_date",
#     "du_spine_primary_key",
#     group_column,
#     target_column,
#     *(
#             extra_keep_columns
#             + [
#                 F.col(column_name).cast(FloatType())
#                 if column_type.startswith("decimal")
#                 else F.col(column_name)
#                 for column_name, column_type in df_master.select(
#             *explanatory_features
#         ).dtypes
#             ]
#     ),
# )
#
# pdf_extra_pai_metrics = calculate_extra_pai_metrics(
#     df_master_only_necessary_columns, target_column, group_column
# )
#
# # Filter rows with NA target to reduce size of pandas DataFrames within pandas udf
# df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
#     ~F.isnull(F.col(target_column))
# )
#
# # Sample down if data is too large to reliably train a model
# if max_rows_per_group is not None:
#     df_master_only_necessary_columns = df_master_only_necessary_columns.withColumn(
#         "aux_n_rows_per_group",
#         F.count(F.lit(1)).over(Window.partitionBy(group_column)),
#     )
#     test_df = df_master_only_necessary_columns.filter(
#         F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
#     ).drop("aux_n_rows_per_group")
#
# pdf_extra_pai_metrics_test = calculate_extra_pai_metrics(
#     test_df, target_column, group_column
# )
#
# df_training_info = df_master_only_necessary_columns.groupby(group_column).apply(
#     create_model_function(
#         as_pandas_udf=True,
#         group_column=group_column,
#         explanatory_features=explanatory_features,
#         target_column=target_column,
#         pdf_extra_pai_metrics=pdf_extra_pai_metrics,
#         extra_tag_columns=extra_keep_columns,
#         model_type="binary",
#         pai_run_prefix=pai_run_prefix,
#         train_sampling_ratio=train_sampling_ratio,
#         model_params=model_params,
#         min_obs_per_class_for_model=min_obs_per_class_for_model,
#         pai_runs_uri=pai_runs_uri,
#         pai_artifacts_uri=pai_artifacts_uri,
#
#     ))


def score_du_models(
    df_master: pyspark.sql.DataFrame,
    primary_key_columns: List[str],
    model_group_column: str,
    models_to_score: Dict[str, str],
    explanatory_features: List[str],
    mlflow_model_version: int,
    scoring_chunk_size: int = 300000,
) -> pyspark.sql.DataFrame:
    spark = get_spark_session()
    # Define schema for the udf.
    primary_key_columns.append(model_group_column)
    schema = df_master.select(
        *(
            primary_key_columns
            + [
                F.lit(999.99).cast(DoubleType()).alias(prediction_colname)
                for prediction_colname in models_to_score.values()
            ]
        )
    ).schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def predict_pandas_udf(pdf):
        mlflow_path = "/Shared/data_upsell/lightgbm"
        if mlflow.get_experiment_by_name(mlflow_path) is None:
            mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
        else:
            mlflow_experiment_id = mlflow.get_experiment_by_name(
                mlflow_path
            ).experiment_id

        current_model_group = pdf[model_group_column].iloc[0]
        pd_results = pd.DataFrame()

        for current_tag, prediction_colname in models_to_score.items():
            # current_model_group = "Data_NonStop_4Mbps_1_ATL"
            # current_tag = "regression"
            # prediction_colname = "propensity"
            # mlflow_model_version = "2"
            mlflow_run = mlflow.search_runs(
                experiment_ids=mlflow_experiment_id,
                filter_string="params.model_objective='"
                + current_tag
                + "' AND params.Version='"
                + str(mlflow_model_version)
                + "' AND tags.mlflow.runName ='"
                + current_model_group
                + "'",
                run_view_type=1,
                max_results=1,
                order_by=None,
            )

            current_model = mlflowlightgbm.load_model(mlflow_run.artifact_uri.values[0])
            # We sort features because MLflow does not preserve feature order
            # Models should also be trained with features sorted
            explanatory_features.sort()
            X = pdf[explanatory_features]
            if "binary" == current_tag:
                pd_results[prediction_colname] = current_model.predict(
                    X, num_threads=1, n_jobs=1
                )
            elif "regression" == current_tag:
                pd_results[prediction_colname] = current_model.predict(
                    X, num_threads=1, n_jobs=1
                )
            else:
                raise ValueError(
                    "Unrecognized model type while predicting, model has"
                    "neither 'binary' or 'regression' tags"
                )
            # pd_results[model_group_column] = current_model_group
            for pk_col in primary_key_columns:
                pd_results.loc[:, pk_col] = pdf.loc[:, pk_col]

        return pd_results

    # This part of code suppose to distribute chuck of each sub required to be score
    # For each of the model
    df_master = df_master.withColumn(
        "partition",
        F.floor(
            F.count(F.lit(1)).over(Window.partitionBy(model_group_column))
            / scoring_chunk_size
            * F.rand()
        ),
    )
    df_master_necessary_columns = df_master.select(
        model_group_column,
        "partition",
        *(  # Don't add model group column twice in case it's a PK column
            list(set(primary_key_columns) - set([model_group_column]))
            + explanatory_features
        ),
    )

    df_scored = df_master_necessary_columns.groupby("model_name", "partition").apply(
        predict_pandas_udf
    )
    # df_scored = df_scored.drop("partition").join(df_master_necessary_columns.drop("model_name"),["du_spine_primary_key"],"left")
    return df_scored
    # # For Testing Purpose, Leave as comment for Later Test
    # df_master = catalog.load("l5_du_scoring_master")
    # explanatory_features = catalog.load("params:du_model_explanatory_features")
    # df_master_scored = score_du_models(
    #     df_master=df_master,
    #     primary_key_columns=["access_method_num"],
    #     model_group_column="model_name",
    #     models_to_score={"binary": "propensity", "regression": "arpu_uplift",},
    #     scoring_chunk_size=500000,
    #     mlflow_model_version=8,
    #     explanatory_features=explanatory_features,
    #     pai_runs_uri="pai_runs_uri",
    #     pai_artifacts_uri="pai_artifacts_uri",
    # )
    #
    # mlflow_path = "/Shared/data_upsell/lightgbm"
    # if mlflow.get_experiment_by_name(mlflow_path) is None:
    #     mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    # else:
    #     mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id
    #
    # all_run_data = mlflow.search_runs(
    #     experiment_ids=mlflow_experiment_id,
    #     filter_string="params.model_objective='binary' AND params.Able_to_model = 'True' AND params.Version=8",
    #     run_view_type=1,
    #     max_results=200,
    #     order_by=None,
    # )
    #
    # df_master = catalog.load("l5_du_scoring_master")
    # primary_key_columns = ["access_method_num"]
    # model_group_column = "model_name"
    # explanatory_features = catalog.load("params:du_model_explanatory_features")
    # df_master.withColumn(model_group_column, F.lit("1")).withColumn(
    #     "partition", F.lit("1")
    # ).select(
    #     model_group_column,
    #     "partition",
    #     *(  # Don't add model group column twice in case it's a PK column
    #         list(set(primary_key_columns) - set([model_group_column]))
    #         + explanatory_features
    #     ),
    # )
    # current_model_group = "Data_NonStop_4Mbps_1_ATL"
    # current_tag = "regression"
    # prediction_colname = "propensity"
    # mlflow_run = mlflow.search_runs(
    #     experiment_ids=mlflow_experiment_id,
    #     filter_string="params.model_objective='"
    #                   + current_tag
    #                   + "' AND params.Version=8 AND tags.mlflow.runName ='"
    #                   + current_model_group
    #                   + "'",
    #     run_view_type=1,
    #     max_results=1,
    #     order_by=None,
    # )
    # current_model = mlflowlightgbm.load_model(mlflow_run.artifact_uri.values[0])
    #
    # df_master_verysmall = df_master.select(explanatory_features).withColumn("model_name",F.lit("Data_NonStop_4Mbps_1_ATL")).limit(300000)
    # pdf = df_master_verysmall.toPandas()
    # explanatory_features.sort()
    #
    # X = pdf[explanatory_features]
    # pd_results = pd.DataFrame()
    # pd_results[prediction_colname] = current_model.predict(
    #     X, num_threads=1, n_jobs=1
    # )


def score_du_models_new_experiment(
    df_master: pyspark.sql.DataFrame,
    primary_key_columns: List[str],
    model_group_column: str,
    models_to_score: Dict[str, str],
    feature_importance_binary_model,
    feature_importance_regression_model,
    mlflow_model_version: int,
    scoring_chunk_size: int = 300000,
) -> pyspark.sql.DataFrame:
    spark = get_spark_session()

    # Define schema for the udf.
    primary_key_columns.append(model_group_column)
    schema = df_master.select(
        *(
            primary_key_columns
            + [
                F.lit(999.99).cast(DoubleType()).alias(prediction_colname)
                for prediction_colname in models_to_score.values()
            ]
        )
    ).schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def predict_pandas_udf(pdf):
        mlflow_path = "/Shared/data_upsell/lightgbm"
        if mlflow.get_experiment_by_name(mlflow_path) is None:
            mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
        else:
            mlflow_experiment_id = mlflow.get_experiment_by_name(
                mlflow_path
            ).experiment_id

        current_model_group = pdf[model_group_column].iloc[0]
        pd_results = pd.DataFrame()

        for current_tag, prediction_colname in models_to_score.items():
            # current_model_group = "Data_NonStop_4Mbps_1_ATL"
            # current_tag = "regression"
            # prediction_colname = "propensity"
            # mlflow_model_version = "2"
            mlflow_run = mlflow.search_runs(
                experiment_ids=mlflow_experiment_id,
                filter_string="params.model_objective='"
                + current_tag
                + "' AND params.Version='"
                + str(mlflow_model_version)
                + "' AND tags.mlflow.runName ='"
                + current_model_group
                + "'",
                run_view_type=1,
                max_results=1,
                order_by=None,
            )

            current_model = mlflowlightgbm.load_model(mlflow_run.artifact_uri.values[0])
            # We sort features because MLflow does not preserve feature order
            # Models should also be trained with features sorted

            X_binary = pdf[feature_importance_binary_model]
            X_regression = pdf[feature_importance_regression_model]
            if "binary" == current_tag:
                pd_results[prediction_colname] = current_model.predict(
                    X_binary, num_threads=1, n_jobs=1
                )
            elif "regression" == current_tag:
                pd_results[prediction_colname] = current_model.predict(
                    X_regression, num_threads=1, n_jobs=1
                )
            else:
                raise ValueError(
                    "Unrecognized model type while predicting, model has"
                    "neither 'binary' or 'regression' tags"
                )
            # pd_results[model_group_column] = current_model_group
            for pk_col in primary_key_columns:
                pd_results.loc[:, pk_col] = pdf.loc[:, pk_col]

        return pd_results

    # This part of code suppose to distribute chuck of each sub required to be score
    # For each of the model
    df_master = df_master.withColumn(
        "partition",
        F.floor(
            F.count(F.lit(1)).over(Window.partitionBy(model_group_column))
            / scoring_chunk_size
            * F.rand()
        ),
    )

    feature_important_list = set(feature_importance_binary_model).union(
        set(feature_importance_regression_model)
    )

    df_master_necessary_columns = df_master.select(
        model_group_column,
        "partition",
        *(  # Don't add model group column twice in case it's a PK column
            list(set(primary_key_columns) - set([model_group_column]))
            + list(feature_important_list)
        ),
    )

    df_scored = df_master_necessary_columns.groupby("model_name", "partition").apply(
        predict_pandas_udf
    )
    # df_scored = df_scored.drop("partition").join(df_master_necessary_columns.drop("model_name"),["du_spine_primary_key"],"left")
    return df_scored


def validate_model_scoring(df_master, explanatory_features, current_tag="regression"):
    # df_master = catalog.load("l5_du_scoring_master")
    # explanatory_features = catalog.load("params:du_model_explanatory_features")
    mlflow_path = "/Shared/data_upsell/lightgbm"
    if mlflow.get_experiment_by_name(mlflow_path) is None:
        mlflow_experiment_id = mlflow.create_experiment(mlflow_path)
    else:
        mlflow_experiment_id = mlflow.get_experiment_by_name(mlflow_path).experiment_id

    all_run_data = mlflow.search_runs(
        experiment_ids=mlflow_experiment_id,
        filter_string="params.model_objective='regression' AND params.Able_to_model = 'True' AND params.Version='9'",
        run_view_type=1,
        max_results=200,
        order_by=None,
    )
    print(all_run_data)
    primary_key_columns = ["access_method_num"]
    model_group_column = "model_name"

    df_master.withColumn(model_group_column, F.lit("1")).withColumn(
        "partition", F.lit("1")
    ).select(
        model_group_column,
        "partition",
        *(  # Don't add model group column twice in case it's a PK column
            list(set(primary_key_columns) - set([model_group_column]))
            + explanatory_features
        ),
    )
    pd_results = pd.DataFrame()
    for m in all_run_data["tags.mlflow.runName"].values:
        current_model_group = m
        prediction_colname = "propensity"
        mlflow_run = mlflow.search_runs(
            experiment_ids=mlflow_experiment_id,
            filter_string="params.model_objective='"
            + current_tag
            + "' AND params.Version='9' AND tags.mlflow.runName ='"
            + current_model_group
            + "'",
            run_view_type=1,
            max_results=1,
            order_by=None,
        )
        try:
            current_model = mlflowlightgbm.load_model(mlflow_run.artifact_uri.values[0])
            df_master_verysmall = (
                df_master.select(explanatory_features)
                .withColumn("model_name", F.lit(current_model_group))
                .limit(5000)
            )
            pdf = df_master_verysmall.toPandas()
            explanatory_features.sort()
            X = pdf[explanatory_features]
            pd_results[current_model_group] = current_model.predict(
                X, num_threads=1, n_jobs=1
            )
            print("Passed:", m)
        except:
            print("Failed:", m)
    return df_master_verysmall


def randomSplitValidationSet(l5_disney_master_tbl):
    (
        l5_disney_master_tbl_trainset,
        l5_disney_master_tbl_validset,
    ) = l5_disney_master_tbl.randomSplit([0.8, 0.2])
    return l5_disney_master_tbl_trainset, l5_disney_master_tbl_validset
