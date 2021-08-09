"""Ingests external models for NGCM"""
import codecs
import json
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple, Union
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
import seaborn as sns
import functools
from lightgbm import LGBMClassifier, LGBMRegressor
from plotnine import *
from pyspark.sql import Window, functions as F
from pyspark.sql import DataFrame
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
    ShortType
)
from sklearn.metrics import auc, roc_curve , precision_score, recall_score
from sklearn.model_selection import train_test_split
from kedro.io import CSVLocalDataSet
from customer360.utilities.spark_util import get_spark_session
import mlflow
from mlflow import lightgbm as mlflowlightgbm

NGCM_OUTPUT_PATH = (
    "/dbfs/mnt/customer360-blob-output/users/sitticsr/ngcm_export/20210806/"
)

# Minimum observations required to reliably train a ML model
MODELLING_N_OBS_THRESHOLD = 500


class Ingester:
    """Adapts McKinsey models into a format consumable by NGCM

    The models will be saved to a specified path. These files will have to be
    transferred to the correct NGCM input folder.
    """

    DAY_OF_MONTH = "Day of Month"
    """Constant specifying dynamic profile field for day of Month"""

    DAY_OF_WEEK = "Day of Week"
    """Constant specifying dynamic profile field for day of Week (1 - 7)"""

    def __init__(self, output_folder: str = None):
        """Initializes the ingester

        Parameters
        ----------
        output_folder : str
            Path to save the models
        """
        dump_path = os.path.join(output_folder, "models")
        if not os.path.exists(dump_path):
            os.makedirs(dump_path, exist_ok=True)

        self.folder = dump_path

    def ingest(
            self, model: Union[LGBMClassifier, LGBMRegressor], tag: str, features: List[str]
    ) -> str:
        """Ingests the supplied model

        The features must be set up in NGCM and also specified in the correct
        order (with respect to the model).

        Parameters
        ----------
        model: Union[LGBMClassifier, LGBMRegressor]
            The trained model
        tag: str
            An identifier used to refer to the model
        features: List[str]
            List of features used by the model in the correct order.
        """
        if not isinstance(model, (LGBMRegressor, LGBMClassifier)):
            raise TypeError("Model type %s not supported" % (type(model)))

        model = codecs.encode(pickle.dumps(model), "base64").decode()

        record = dict(features=list(features), pkl=model)

        create_time = datetime.now().strftime("%Y%m%d%H%M%S")
        file_path = os.path.join(self.folder, "%s_%s.json" % (tag, create_time))
        with open(file_path, "w") as output_file:
            json.dump(record, output_file)

        return f"Model Dumped to {file_path}"


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
        c: (when(F.col(c) < lower, lower)  # Below lower quantile
            .when(F.col(c) > upper, upper)  # Above upper quantile
            .otherwise(F.col(c))  # Between quantiles
            .alias(c))
        for c, (lower, upper) in
        # Compute array of quantiles
        zip(cols, df.stat.approxQuantile(cols, [lower, upper], relativeError))
    }

    return df.select([quantiles.get(c, F.col(c)) for c in df.columns])


def drop_null_columns(df, thres):
    """
        This function drops all columns which contain null values.
        :param df: A PySpark DataFrame
        :param thres: If the number of null exceeds the thres, remove it.
    """

    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()

    total_length_of_data = df.count()

    to_drop = [k for k, v in null_counts.items() if v >= (total_length_of_data * thres)]
    df = df.drop(*to_drop)

    return df


def mean_absolute_error(y_true, y_pred):
    y_true, y_pred = y_true.values, np.array(y_pred)
    # print('y_true',y_true)
    # print('y_pred',y_pred)
    mae = np.mean(np.abs(y_true - y_pred))
    # print('mape',mae)
    return mae


def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean([np.abs((y_true - y_pred) / y_true) for y_true, y_pred in zip(y_true, y_pred) if y_true != 0]) * 100


def filter_valid_campaign_child_code(l5_nba_master: pyspark.sql.DataFrame,
                                     model_type: str,
                                     nba_prioritized_campaigns_child_codes: List) -> pyspark.sql.DataFrame:
    """
        Retrieve only the valid rework macro products that agree to the conditions.
        The conditions depend on the model type.
        The data checking process is required before running a model.

        Parameters
        ----------
        l5_nba_master: An output from model input's pipeline. It is the DataFrame that contains relevant features.
        model_type: binary or regression
        min_obs_per_class_for_model: Minimum observations within each class from the target variable.
        nba_prioritized_campaigns_child_codes: list of prioritized_campaigns_child_codes
        valid_campaign_child_code_list: the prioritized_campaigns_child_codes that pass all of the conditions and
        list of valid prioritized campaigns_child_codes name
    """

    """ 
    The conditions are follows: 
    1. Observation of data >= 10000  
    2. Clip the target (for the regression model only) 
    """

    # Model Checking
    supported_model_types = ["binary", "regression"]
    if model_type not in supported_model_types:
        raise ValueError(
            f"Unrecognized model type {model_type}. Supported model types are: "
            f"{', '.join(supported_model_types)}"
        )

    print(f"Checking campaign child code list  for {model_type} model.")
    print("*" * 100)

    # Check the number of observation in each model_group
    count_in_each_model_group = l5_nba_master.groupBy('campaign_child_code').count().orderBy('count')

    # Store the model_group that agree to the first condition
    # Recall that MODELLING_N_OBS_THRESHOLD = 10000
    agree_with_the_condition_1 = count_in_each_model_group.filter(
        count_in_each_model_group['count'] >= MODELLING_N_OBS_THRESHOLD).select('campaign_child_code').toPandas()
    ccc_agree_with_the_condition_1 = agree_with_the_condition_1['campaign_child_code'].to_list()

    # Retrieve only the list of model_group that pass all of the conditions
    valid_campaign_child_code_list = list(set(ccc_agree_with_the_condition_1).intersection(
        set(nba_prioritized_campaigns_child_codes)))
    print('length of valid campaign child code list', len(valid_campaign_child_code_list))

    l5_nba_master_only_valid_ccc = l5_nba_master[
        l5_nba_master['campaign_child_code'].isin(valid_campaign_child_code_list)]

    # Clip the target (for the regression model only)
    if model_type == 'regression':

        print("Clipping target.")
        clipped_l5_nba_master_only_valid_ccc = clip(
            l5_nba_master_only_valid_ccc,
            'target_relative_arpu_increase_30d')

        clipped_l5_nba_master_only_valid_ccc = clipped_l5_nba_master_only_valid_ccc.filter(
            "target_relative_arpu_increase_30d IS NOT NULL")

        print(f"Successfully checked the valid campaign_child_code for {model_type} model.")
        print("*" * 100)

        return clipped_l5_nba_master_only_valid_ccc, valid_campaign_child_code_list

    elif model_type == 'binary':

        print(f"Successfully checked the valid campaign_child_code for {model_type} model.")
        print("*" * 100)

        return l5_nba_master_only_valid_ccc, valid_campaign_child_code_list


def calculate_feature_importance(
        df_master: pyspark.sql.DataFrame,
        group_column: str,
        explanatory_features: List,
        binary_target_column: str,
        regression_target_column: str,
        train_sampling_ratio: float,
        model_params: Dict[str, Any],
        model_type: str,
        campaigns_child_codes_list,
        filepath: str) -> None:
    """ Retrieve the top features based on the feature importance from the LightGBM model.
         The result is saved in .csv format
         Parameters
         ----------
              df_master: Master table generated from the model input's pipeline.
              group_column : prioritized_campaigns_child_codes
              explanatory_features: Specified list of features
              model_params: Model hyperparameters
              binary_target_column: Target column's name of a binary classification model
              regression_target_column: Target column's name of a regression model
              train_sampling_ratio: Ratio used in train_test_split function
              model_type: binary or regression
              min_obs_per_class_for_model: Minimum observations within each class from the target variable.
             filepath: A filepath to save the output.
         Return: None
    """
    print('#'*50)
    print('data master size: ', (df_master.count(), len(df_master.columns)))
    print('#' * 50)

    # Get only campaign_child_code before running a model
    l5_nba_master_with_valid_campaign_child_code, valid_campaign_child_code_list = filter_valid_campaign_child_code(
        df_master,
        model_type,
        campaigns_child_codes_list)

    print("Excluding NULL columns")
    # Remove the columns that contain many NULL, preventing the case that some columns may contain all NULL.
    l5_nba_master_with_valid_campaign_child_code = drop_null_columns(l5_nba_master_with_valid_campaign_child_code,
                                                                     thres=0.98)

    # Change target type
    # l5_nba_master_with_valid_campaign_child_code = l5_nba_master_with_valid_campaign_child_code.withColumn(
    #     binary_target_column ,F.col(binary_target_column).cast(IntegerType()))

    # Pre-process the feature selection of the upcoming features, especially the data type of the column.
    # Ex. We do not want the feature of type TimeStamp, StringType

    # Get only numerical columns
    valid_feature_cols = [col.name for col in l5_nba_master_with_valid_campaign_child_code.schema.fields if
                          isinstance(col.dataType, IntegerType) or
                          isinstance(col.dataType, FloatType) or
                          isinstance(col.dataType, DecimalType) or
                          isinstance(col.dataType, DoubleType) or
                          isinstance(col.dataType, LongType) or
                          isinstance(col.dataType, ShortType)]

    # Remove the target column from the list of valid features.
    valid_feature_cols.remove(binary_target_column)
    valid_feature_cols.remove(regression_target_column)
    valid_feature_cols.remove(
        'partition_date')  # Explicitly remove this irrelevant feature as it is saved in numerical data type.

    # Combine other features with the explanatory features (which is currently fixed with du_model_features_bau)
    feature_cols = list(set(explanatory_features).intersection(set(valid_feature_cols)))
    # feature_cols = explanatory_features

    ###########
    ## MODEL ##
    ###########

    # Use Window function to random maximum of 10K records for each model
    n = 10000
    w = Window.partitionBy(F.col(group_column)).orderBy(F.col("rnd_"))

    sampled_master_table = (l5_nba_master_with_valid_campaign_child_code
                            .withColumn("rnd_", F.rand())  # Add random numbers column
                            .withColumn("rn_", F.row_number().over(w))  # Add rowNumber over window
                            .where(F.col("rn_") <= n)  # Take n observations
                            .drop("rn_")  # Drop helper columns
                            .drop("rnd_")  # Drop helper columns
                            )

    feature_cols.sort()

    df_feature_importance_list = []
    # sampled_master_table_dataframe = sampled_master_table.toPandas()


    for campaign in valid_campaign_child_code_list[3:10]:

        #train_single_model_pdf = sampled_master_table_dataframe.loc[sampled_master_table_dataframe[group_column] == campaign]
        train_single_model = sampled_master_table.filter(sampled_master_table[group_column] == campaign)
        train_single_model.persist()

        print('//' * 50)
        # print('train_single_model_pdf shape:', train_single_model_pdf.shape)
        # Convert spark Dataframe to Pandas Dataframe
        train_single_model_pdf = train_single_model.toPandas()
        print('train_single_model_pdf shape:', train_single_model_pdf.shape)

        print(f"Model is: {campaign}, {model_type}")

        try:
            pdf_train, pdf_test = train_test_split(
                train_single_model_pdf,
                train_size=train_sampling_ratio,
                random_state=123456,
            )
        except Exception as exc:
            print(exc)
            continue

        print('pdf_train shape', pdf_train.shape)
        print('pdf_test shape', pdf_test.shape)
        # print('model_type regression', model_type , model_type == 'regression')
        # print('model_type binary', model_type , model_type == 'binary')

        if model_type == "binary":

            target_column = binary_target_column

            # The data checking process is required before training a model. The conditions are follows:
            # number of ratio target_respone = 1  > 2%
            # Note: 'target_response' == 1 is YES

            count_target_1 = train_single_model_pdf[train_single_model_pdf[target_column] == 1][target_column].count()
            count_target_all = train_single_model_pdf[target_column].count()
            pct_target_1 = (count_target_1 / count_target_all) * 100

            if pct_target_1 >= 2:
                print('Condition pass: pct_target is', pct_target_1)

                # try:
                #     pdf_train, pdf_test = train_test_split(
                #         train_single_model_pdf.copy(),
                #         train_size=train_sampling_ratio,
                #         random_state=123456,
                #     )
                # except Exception as exc:
                #     print(exc)
                #     continue

                model = LGBMClassifier(**model_params).fit(
                    pdf_train[feature_cols],
                    pdf_train[target_column],
                    eval_set=[
                        (
                            pdf_train[feature_cols],
                            pdf_train[target_column],
                        ),
                        (
                            pdf_test[feature_cols],
                            pdf_test[target_column],
                        ),
                    ],
                    eval_names=["train", "test"],
                    eval_metric="auc",
                )

                # List of important feature from model
                boost = model.booster_
                df_feature_importance = (
                    pd.DataFrame({
                        'feature': boost.feature_name(),
                        'importance': boost.feature_importance(),
                        'campaign_child_code': campaign
                    }).sort_values('importance', ascending=False)
                )

                df_feature_importance_list.append(df_feature_importance)
            else:
                print('Cannot Train {} model'.format(campaign))
                print('Condition not pass: pct_target_1 is', pct_target_1)

        elif model_type == 'regression':
            target_column = regression_target_column

            # try:
            #     pdf_train, pdf_test = train_test_split(
            #         train_single_model_pdf,
            #         train_size=train_sampling_ratio,
            #         random_state=123456,
            #     )
            # except Exception as exc:
            #     print(exc)
            #     continue

            # print('pdf_train shape', pdf_train.shape)
            # print('pdf_test shape', pdf_test.shape)

            model = LGBMRegressor(**model_params).fit(
                pdf_train[feature_cols],
                pdf_train[target_column],
                eval_set=[
                    (
                        pdf_train[feature_cols],
                        pdf_train[target_column],
                    ),
                    (
                        pdf_test[feature_cols],
                        pdf_test[target_column],
                    ),
                ],
                eval_names=["train", "test"],
                eval_metric="mae",
            )

            # Model predict
            # test_predictions = model.predict(pdf_test[feature_cols])
            # train_predictions = model.predict(pdf_train[feature_cols] )

            # # Model Error: MAE #TODO : Add to DataFrame below
            # test_mae = mean_absolute_error(y_true=pdf_test[target_column], y_pred=test_predictions)
            # train_mae = mean_absolute_error(y_true=pdf_train[target_column], y_pred=train_predictions)

            # List of important feature from model
            boost = model.booster_
            df_feature_importance = (
                pd.DataFrame({
                    'feature': boost.feature_name(),
                    'importance': boost.feature_importance(),
                    'campaign_child_code': campaign
                }).sort_values('importance', ascending=False)
            )

            df_feature_importance_list.append(df_feature_importance)

    #################################
    ## Calculate Feature Importance ##
    #################################

    print("Calculate Feature Importance")

    # Assemble feature importance dataframe

    feature_importance_df = pd.DataFrame()
    print('+'*50)
    print('shape of top df_feature_importance_list ', len(df_feature_importance_list))
    for df in df_feature_importance_list:
        print('+' * 50)
        print('shape of top feature ', len(df))
        feature_importance_df = pd.concat([feature_importance_df, df], ignore_index=False)
        print('shape of concat table top feature ', feature_importance_df.shape)

    sum_feature_importance = feature_importance_df['importance'].sum()
    feature_importance_df['pct_importance_values'] = (
                                                    feature_importance_df['importance'] / sum_feature_importance
                                                     ) * 100

    avg_feature_importance = feature_importance_df.groupby(
        'feature')['pct_importance_values']\
        .mean()\
        .reset_index()\
        .sort_values(by='pct_importance_values', ascending=False)\
        .reset_index()\
        .drop(columns='index')

    # Get the top 30 features
    feature_importance_top40 = avg_feature_importance[:40]
    # feature_importance_top40.to_csv(filepath, index=False)

    return feature_importance_top40


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

    def train_single_model_wrapper(pdf_master_chunk: pd.DataFrame, ) -> pd.DataFrame:
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

        print('#' * 50)
        print('train_single_model start ..................')

        def train_single_model(
                pdf_master_chunk: pd.DataFrame,
                model_type: str,
                group_column: str,
                explanatory_features_list: str,
                target_column: str,
                train_sampling_ratio: float,
                model_params: Dict[str, Any],
                min_obs_per_class_for_model: int,
                extra_tag_columns: List[str],
                pai_run_prefix: str,
                pdf_extra_pai_metrics: pd.DataFrame,
                pai_runs_uri: str,
                pai_artifacts_uri: str,
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
                explanatory_features: list of features names to use for learning
                target_column: column name that contans target variable
                train_sampling_ratio: percentage of pdf_master_chunk to be used for
                    training (sampled randomly), the rest will be used for validation
                    (a.k.a. test)
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
                return recall_score(y_true=y, y_pred=y_pred)

            def calculate_recall_group(df):
                y_pred = df.y_pred
                y = df.y_true
                return precision_score(y_true=y, y_pred=y_pred)

            def get_metrics_by_percentile(y_true, y_pred) -> pd.DataFrame:
                """
                 Performance report generation function to check model performance at percentile level.
                :param y_true: numpy array with the real value of the target variable
                :param y_pred: numpy array with  predicted value by the model
                :return: pandas.DataFrame wit the different KPIs:
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

                # ------------------------------------- Get Uplift -----------------------------------------
                report = pd.DataFrame(
                    {"y_true": y_true, "y_pred": y_pred, }, columns=["y_true", "y_pred"]
                )

                report["score_rank"] = report.y_pred.rank(
                    method="first", ascending=True, pct=True
                )
                report["percentile"] = np.floor((1 - report.score_rank) * 100) + 1

                # Copy report for calculate precision and recall
                report_copied_for_prc_recall_calculation = report.copy()

                report["population"] = 1
                report = (
                    report.groupby(["percentile"])
                        .agg({"y_true": "sum", "population": "sum", "y_pred": "mean"})
                        .reset_index()
                )
                report = report.rename(
                    columns={"y_pred": "avg_score", "y_true": "positive_cases"}
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

                # ---------------------------------- Get Precision & Recall ------------------------------------

                precision_grp = report_copied_for_prc_recall_calculation.groupby(["percentile"]).apply(
                    calculate_precision_group).to_frame().reset_index()
                precision_grp.columns = ['percentile', 'precision']

                recall_grp = report_copied_for_prc_recall_calculation.groupby(["percentile"]).apply(
                    calculate_recall_group).to_frame().reset_index()
                recall_grp.columns = ['percentile', 'recall']

                report = report.merge(precision_grp, on='percentile', how='left').merge(
                    recall_grp, on='percentile', how='left')

                return report

            # ingester = Ingester(output_folder=NGCM_OUTPUT_PATH)
            supported_model_types = ["binary", "regression"]
            if model_type not in supported_model_types:
                raise ValueError(
                    f"Unrecognized model type {model_type}. Supported model types are: "
                    f"{', '.join(supported_model_types)}"
                )

            print('*'*50)
            print('number of unique', pdf_master_chunk[group_column].nunique())
            # print('number of unique', pdf_master_chunk[group_column].unique())

            if pdf_master_chunk[group_column].nunique() > 1:
                raise ValueError(
                    f"More than one group found in training table: "
                    f"{pdf_master_chunk[group_column].nunique()}"
                )
            # ingester = Ingester(output_folder=NGCM_OUTPUT_PATH)

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

            pai_run_name = pai_run_prefix + current_group

            pdf_extra_pai_metrics_filtered = pdf_extra_pai_metrics[
                pdf_extra_pai_metrics["group"] == current_group
                ]

            # Calculate some metrics on the data to log into pai
            print('#' * 50)
            print('Calculate some metrics on the data to log into pai ..................')

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
            print('#' * 50)
            print('path for each model run ..................')

            mlflow_path = "/NBA"
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

                if pdf_master_chunk[target_column].nunique() <= 1:
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

                print('#'*50)
                print('Training model ..................')

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

                        nba_level = current_group.split("=")[0]
                        ngcm_MAID = current_group.split("=")[1]
                        ngcm_tag = (
                            current_group.split("=")[1]
                                .replace("=", "_")
                                .replace(" - ", "_")
                                .replace("-", "_")
                                .replace(".", "_")
                                .replace("/", "_")
                                .replace(" ", "_")
                        )
                        # ingester.ingest(model=model, tag=ngcm_tag + "_Classifier",
                        #                 features=explanatory_features_list )

                        test_predictions = model.predict_proba(
                            pdf_test[explanatory_features_list]
                        )[:, 1]
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
                        mlflow.log_metric("train_auc", train_auc)
                        mlflow.log_metric("test_auc", test_auc)
                        mlflow.log_metric("train_test_auc_diff", train_auc - test_auc)

                        if os.path.isfile(tmp_path / "roc_curve.png"):
                            raise AssertionError(
                                f"There is already a file in the tmp directory "
                                f"when running the {current_group} model"
                            )

                        # Plot ROC curve
                        plot_roc_curve(
                            y_true=pdf_test[target_column],
                            y_score=test_predictions,
                            filepath=tmp_path / "roc_curve.png",
                        )

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
                            y_true=pdf_test[target_column], y_pred=test_predictions
                        )
                        df_metrics_by_percentile.to_csv(
                            tmp_path / "metrics_by_percentile.csv", index=False
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

                        nba_level = current_group.split("=")[0]
                        ngcm_MAID = current_group.split("=")[1]
                        ngcm_tag = (
                            current_group.split("=")[1]
                                .replace("=", "_")
                                .replace(" - ", "_")
                                .replace("-", "_")
                                .replace(".", "_")
                                .replace("/", "_")
                                .replace(" ", "_")
                        )
                        # ingester.ingest(model=model, tag=ngcm_tag + "_Regressor",
                        #                 features=explanatory_features_list )

                        test_predictions = model.predict(pdf_test[explanatory_features_list])
                        train_predictions = model.predict(
                            pdf_train[explanatory_features_list]
                        )

                        # ingester.ingest(
                        #     model=model,
                        #     tag="Model_" + current_group + "_Regressor",
                        #     features=explanatory_features_list,
                        # )
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
                            str(tmp_path / "metrics_by_round.csv"), artifact_path=""
                        )
                        mlflow.log_artifact(
                            str(tmp_path / "mae_per_round.png"), artifact_path=""
                        )
                    df_to_return = pd.DataFrame(
                        {
                            "able_to_model_flag": int(able_to_model_flag),
                            "train_set_primary_keys": pdf_train[
                                "nba_spine_primary_key"
                            ],
                        }
                    )

                    return df_to_return

        return train_single_model(pdf_master_chunk=pdf_master_chunk, **kwargs)

    model_function = train_single_model_wrapper
    print('#' * 50)
    print('train_single_model_wrapper start ..................')

    if as_pandas_udf:
        model_function = pandas_udf(
            model_function, schema, functionType=PandasUDFType.GROUPED_MAP
        )

    return model_function


def train_multiple_models(
        df_master: pyspark.sql.DataFrame ,
        group_column: str,
        target_column: str,
        nba_top_features,
        undersampling,
        minimun_row,
        campaigns_child_codes_list,
        extra_keep_columns: List[str] = None,
        max_rows_per_group: int = None,
        **kwargs: Any,
) -> pyspark.sql.DataFrame:
    """
    Trains multiple models using pandas udf to distrbute the training in a spark cluster
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

    # Reduce data before sample data for train model
    # df_master = df_master.filter(df_master.event_partition_date >= "2021-05-16")
    # print('number of dataset :', df_master.count())

    # explanatory_features = nba_top_features.to_Pandas()
    explanatory_features_list = nba_top_features['feature'].to_list()

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
        "nba_spine_primary_key",
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
    # if max_rows_per_group is not None:
    #     df_master_only_necessary_columns = df_master_only_necessary_columns.withColumn(
    #         "aux_n_rows_per_group",
    #         F.count(F.lit(1)).over(Window.partitionBy(group_column)),
    #     )
    #     df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
    #         F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
    #     ).drop("aux_n_rows_per_group")

    # Under Sampling data for train single model
    if undersampling:
        print("Undersampling the data in each campaign_child_code...")

        df_master_undersampling_list = []
        for campaign in campaigns_child_codes_list[:30]:

            print(f"Undersampling product: {campaign}")
            major_df = df_master_only_necessary_columns.filter(
                (F.col("target_response") == 0) & (F.col("campaign_child_code") == campaign))
            minor_df = df_master_only_necessary_columns.filter(
                (F.col("target_response") == 1) & (F.col("campaign_child_code") == campaign))

            try:
                major = major_df.count()
                minor = minor_df.count()
                ratio = int(major / minor)

                print(f"{campaign} has major class = {major}")
                print(f"{campaign} has minor class = {minor}")

                if major >= minimun_row and minor >= minimun_row:
                    sampled_majority_df = major_df.sample(withReplacement=False, fraction=1 / ratio)
                    combined_df = sampled_majority_df.union(minor_df)
                    df_master_undersampling_list.append(combined_df)
                else:
                    df_master_undersampling_list.append(major_df.limit(1))

            except ZeroDivisionError as e:
                print(f"{campaign} has zero target response")
                df_master_undersampling_list.append(major_df.limit(1))  # Get only majority class

        print("Assemble all of the under-sampling dataframes...")
        df_master_only_necessary_columns = functools.reduce(DataFrame.union, df_master_undersampling_list)
        print('Data frame for train single model:',df_master_only_necessary_columns.count())

    # Sample down if data is too large to reliably train a model
    if max_rows_per_group is not None:
        df_master_only_necessary_columns_new = df_master_only_necessary_columns.withColumn(
            "aux_n_rows_per_group",
            F.count(F.lit(1)).over(Window.partitionBy(group_column)),
        )
        df_master_only_necessary_columns_new = df_master_only_necessary_columns_new.filter(
            F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
        ).drop("aux_n_rows_per_group")

    print('Data frame spine before train single model:', df_master_only_necessary_columns_new.count())

    df_training_info = df_master_only_necessary_columns_new.groupby(group_column).apply(
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


def score_nba_models(
        df_master: pyspark.sql.DataFrame,
        primary_key_columns: List[str],
        model_group_column: str,
        models_to_score: Dict[str, str],
        pai_runs_uri: str,
        pai_artifacts_uri: str,
        mlflow_model_version: int,
        explanatory_features: List[str] = None,
        missing_model_default_value: str = None,
        scoring_chunk_size: int = 500000,
) -> pyspark.sql.DataFrame:
    """
    Function for making predictions (scoring) using NBA models. Allows to score
    multiple models (sets of models) in one go,. If a model is not found for certain
    rows, the prediction for them will be None

    :param df_master: master table where the scoring will be done. Needs to contain all
        explanatory features required for the models
    :param primary_key_columns: columns that are a primary key of df_master.
        I.e. it's unique for every row
    :param model_group_column: column that defines the partition for which a different
        model has been trained for.
    :param models_to_score: Dictionary where each key is the tag of a set of models
        to score and each value is the name the column with the prediction of
        that model will have. Each tag should contain a model for each model group.
        The recommended tags to use for this function are the one that start with "z_"
        and were generated in training with the `pai_run_prefix` specified
    :param pai_runs_uri: URI where PAI runs are stored
    :param pai_artifacts_uri: URI where PAI artifacts are stored
    :param explanatory_features: list with all explanatory features that have been
        used in any of the models. Each model will find it's own explanatory features,
        but this is used for speedup in case features are known upfront. If None,
        the explanatory features list will be retrieved from PAI, but this is more
        time consuming
    :param scoring_chunk_size: number of rows each scoring chunk will have. Since
        scoring happens in a pandas UDF the size of the chunk should be small enough
        so that each chunk fits in memory of the task asssigned to the worker in the
        Spark cluster
    :param missing_model_default_value: Value to return for the prediction in case the
        model is not available. If None, an error will be returned (since None cannot be
        returned by a pandas udf)
    :return: df_master but with the columns with predictions added
    """

    # Define schema for the udf.
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
        mlflow_path = "/NBA"
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

    df_scored = df_master_necessary_columns.groupby(
        model_group_column, "partition"
    ).apply(predict_pandas_udf)
    df_master_scored = df_master.join(df_scored, on=primary_key_columns, how="left")

    return df_master_scored
