import logging
import os
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pai
import pandas as pd
import pyspark
import pyspark.sql.functions as F
import seaborn as sns
from lightgbm import LGBMClassifier, LGBMRegressor
from plotnine import *
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

# Minimum observations required to reliably train a ML model
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
            explanatory_features: List[str],
            target_column: str,
            train_sampling_ratio: float,
            model_params: Dict[str, Any],
            min_obs_per_class_for_model: int,
            extra_tag_columns: List[str],
            pai_run_prefix: str,
            pdf_extra_pai_metrics: pd.DataFrame,
            pai_runs_uri: str,
            pai_artifacts_uri: str,
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
                report = pd.DataFrame(
                    {"y_true": y_true, "y_pred": y_pred,}, columns=["y_true", "y_pred"]
                )

                report["score_rank"] = report.y_pred.rank(
                    method="first", ascending=True, pct=True
                )
                report["percentile"] = np.floor((1 - report.score_rank) * 100) + 1
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
                return report

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

            def pai_log_metrics_fix(metrics_dict):
                """
                Wrapper for logging pai metrics that supports numpy types
                Args:
                    metrics_dict:

                Returns:

                """
                fixed_metrics_dict = {
                    k: (int(v) if isinstance(v, np.int64) else float(v))
                    for k, v in metrics_dict.items()
                }
                pai.log_metrics(fixed_metrics_dict)

            ## Sort features since MLflow does not guarantee the order
            explanatory_features.sort()

            current_group = pdf_master_chunk[group_column].iloc[0]

            pai_run_name = pai_run_prefix + current_group

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
            # Configure and start pai run
            pai.set_config(
                experiment=current_group,
                storage_runs=pai_runs_uri,
                storage_artifacts=pai_artifacts_uri,
            )

            if pai.active_run():
                raise AssertionError(
                    f"There is already an active pai run when "
                    f"trying to start the {current_group} model"
                )

            with pai.start_run(run_name=pai_run_name):
                run_id = pai.current_run_uuid()
                # This path will be used to store artifacts for pai, don't rely
                # on them being available after function ends
                tmp_path = Path("data/tmp") / run_id
                os.makedirs(tmp_path, exist_ok=True)

                pai.add_tags(
                    [f"z_{pai_run_prefix}", model_type, f"modelled_by_{group_column}"]
                )

                if extra_tag_columns:
                    for c in extra_tag_columns:
                        if len(pdf_master_chunk[c].unique()) == 1:
                            pai.add_tags(
                                [  # Remove special characters as they are not allowed
                                    re.sub(
                                        r"[^A-Za-z0-9\.\-_]",
                                        "",
                                        f"{c}-{pdf_master_chunk[c].iloc[0]}".replace(
                                            " ", "_"
                                        ),
                                    )
                                ]
                            )
                        else:
                            pai.add_tags([f"{c} not unique"])
                            pai.log_note(
                                f"{c} is not unique for this run, possible values are "
                                f"{', '.join(pdf_master_chunk[c].fillna('NULL').unique())[:999]}"
                            )

                pai_log_metrics_fix(pai_metrics_dict)

                pai.log_params(
                    {
                        "target_column": target_column,
                        "model_objective": model_type,
                        "regression_clip_target_quantiles": regression_clip_target_quantiles,
                    }
                )

                # Check if we have enough data to train a model for the current group
                able_to_model_flag = True
                if modelling_perc_obs_target_null != 0:
                    pai.log_note(
                        "There are observations with NA target in the modelling data"
                    )
                    able_to_model_flag = False

                if modelling_n_obs < MODELLING_N_OBS_THRESHOLD:
                    pai.log_note(
                        f"There are not enough observations to reliably train a model. "
                        f"Minimum required is {MODELLING_N_OBS_THRESHOLD}, "
                        f"found {modelling_n_obs}"
                    )
                    able_to_model_flag = False

                if pai_metrics_dict["original_perc_obs_target_null"] == 1:
                    pai.log_note("The are no observations with non-null target")
                    able_to_model_flag = False

                if len(pdf_master_chunk[target_column].unique()) <= 1:
                    pai.log_note("Target variable has only one unique value")
                    able_to_model_flag = False

                if model_type == "binary":
                    if (
                        pai_metrics_dict["original_n_obs_positive_target"]
                        < min_obs_per_class_for_model
                    ):
                        pai.log_note(
                            f"The number of positive responses is not enough to reliably train a model. "
                            f"There are {pai_metrics_dict['original_n_obs_positive_target']} "
                            f"observations while minimum required is {min_obs_per_class_for_model}"
                        )
                        able_to_model_flag = False

                    if (
                        pai_metrics_dict["original_n_obs"]
                        - pai_metrics_dict["original_n_obs_positive_target"]
                        < min_obs_per_class_for_model
                    ):
                        pai.log_note(
                            f"The number of negative responses is not enough to reliably train a model. "
                            f"There are {pai_metrics_dict['original_n_obs_positive_target']} "
                            f"observations while minimum required is {min_obs_per_class_for_model}"
                        )
                        able_to_model_flag = False

                # build the DataFrame to return
                df_to_return = pd.DataFrame(
                    {
                        "able_to_model_flag": [int(able_to_model_flag)],
                        "train_set_primary_keys": ["NULL"],
                    }
                )

                if not able_to_model_flag:
                    # Unable to train a model, end execution
                    pai.add_tags(["Unable to model", "Finished"])
                    return df_to_return
                else:

                    # Train the model
                    pai.add_tags(["Able to model"])
                    pdf_master_chunk = pdf_master_chunk.sort_values(
                        ["subscription_identifier", "contact_date"]
                    )
                    pdf_train, pdf_test = train_test_split(
                        pdf_master_chunk,
                        train_size=train_sampling_ratio,
                        random_state=123,
                    )

                    pai.log_params(
                        {
                            "train_sampling_ratio": train_sampling_ratio,
                            "model_params": model_params,
                        }
                    )
                    if model_type == "binary":
                        model = LGBMClassifier(**model_params).fit(
                            pdf_train[explanatory_features],
                            pdf_train[target_column],
                            eval_set=[
                                (
                                    pdf_train[explanatory_features],
                                    pdf_train[target_column],
                                ),
                                (
                                    pdf_test[explanatory_features],
                                    pdf_test[target_column],
                                ),
                            ],
                            eval_names=["train", "test"],
                            eval_metric="auc",
                        )

                        test_predictions = model.predict_proba(
                            pdf_test[explanatory_features]
                        )[:, 1]

                        pai.log_model(model)

                        train_auc = model.evals_result_["train"]["auc"][-1]
                        test_auc = model.evals_result_["test"]["auc"][-1]

                        pai.log_features(
                            features=explanatory_features,
                            importance=list(
                                model.feature_importances_
                                / sum(model.feature_importances_)
                            ),
                        )

                        pai_log_metrics_fix(
                            {
                                "train_auc": train_auc,
                                "test_auc": test_auc,
                                "train_test_auc_diff": train_auc - test_auc,
                            }
                        )

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

                        # Log artifacts created and end the run
                        pai.log_artifacts(
                            {
                                "roc_curve": str(tmp_path / "roc_curve.png"),
                                "metrics_by_round": str(
                                    tmp_path / "metrics_by_round.csv"
                                ),
                                "auc_per_round": str(tmp_path / "auc_per_round.png"),
                                "metrics_by_percentile": str(
                                    tmp_path / "metrics_by_percentile.csv"
                                ),
                            }
                        )
                    elif model_type == "regression":
                        model = LGBMRegressor(**model_params).fit(
                            pdf_train[explanatory_features],
                            pdf_train[target_column],
                            eval_set=[
                                (
                                    pdf_train[explanatory_features],
                                    pdf_train[target_column],
                                ),
                                (
                                    pdf_test[explanatory_features],
                                    pdf_test[target_column],
                                ),
                            ],
                            eval_names=["train", "test"],
                            eval_metric="mae",
                        )

                        test_predictions = model.predict(pdf_test[explanatory_features])

                        pai.log_model(model)

                        train_mae = model.evals_result_["train"]["l1"][-1]
                        test_mae = model.evals_result_["test"]["l1"][-1]

                        pai.log_features(
                            features=explanatory_features,
                            importance=list(
                                model.feature_importances_
                                / sum(model.feature_importances_)
                            ),
                        )

                        pai_log_metrics_fix(
                            {
                                "train_mae": train_mae,
                                "test_mae": test_mae,
                                "test_benchmark_target_average": np.mean(
                                    np.abs(
                                        pdf_test[target_column]
                                        - pdf_test[target_column].mean()
                                    )
                                ),
                                "train_test_mae_diff": train_mae - test_mae,
                            }
                        )

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

                        # Log artifacts created and end the run
                        pai.log_artifacts(
                            {
                                "ARPU_uplift_distribution": str(
                                    tmp_path / "ARPU_uplift_distribution.png"
                                ),
                                "metrics_by_round": str(
                                    tmp_path / "metrics_by_round.csv"
                                ),
                                "mae_per_round": str(tmp_path / "mae_per_round.png"),
                            }
                        )
                    pai.add_tags(["Finished"])

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

    if as_pandas_udf:
        model_function = pandas_udf(
            model_function, schema, functionType=PandasUDFType.GROUPED_MAP
        )

    return model_function


def train_multiple_models(
    df_master: pyspark.sql.DataFrame,
    group_column: str,
    explanatory_features: List[str],
    target_column: str,
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

    explanatory_features.sort()

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

    # Sample down if data is too large to reliably train a model
    if max_rows_per_group is not None:
        df_master_only_necessary_columns = df_master_only_necessary_columns.withColumn(
            "aux_n_rows_per_group",
            F.count(F.lit(1)).over(Window.partitionBy(group_column)),
        )
        df_master_only_necessary_columns = df_master_only_necessary_columns.filter(
            F.rand() * F.col("aux_n_rows_per_group") / max_rows_per_group <= 1
        ).drop("aux_n_rows_per_group")

    df_training_info = df_master_only_necessary_columns.groupby(group_column).apply(
        create_model_function(
            as_pandas_udf=True,
            group_column=group_column,
            explanatory_features=explanatory_features,
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
        pai.set_config(
            storage_runs=pai_runs_uri, storage_artifacts=pai_artifacts_uri,
        )

        current_model_group = pdf[model_group_column].iloc[0]
        pd_results = pd.DataFrame()

        for pk_col in primary_key_columns:
            pd_results.loc[:, pk_col] = pdf.loc[:, pk_col]

        for current_tag, prediction_colname in models_to_score.items():
            current_run = pai.load_runs(
                experiment=current_model_group, tags=[current_tag]
            )
            if len(current_run) == 0:
                if missing_model_default_value is None:
                    raise ValueError(
                        f"There are no runs for experiment {current_model_group}"
                        f" and tag {current_tag}"
                    )
                else:
                    logging.info(
                        f"There is no PAI run for experiment "
                        f"{current_model_group} and tag {current_tag}"
                    )
                    pd_results[prediction_colname] = missing_model_default_value
                    continue

            assert len(current_run) < 2, (
                f"There are more than 1 runs for experiment "
                f"{current_model_group} and tag {current_tag}"
            )

            current_run_id = current_run["run_id"].iloc[0]
            if "model" not in pai.list_artifacts(run_id=current_run_id):
                if missing_model_default_value is None:
                    raise ValueError(
                        f"There is no model object in the PAI run for experiment"
                        f" {current_model_group} and tag {current_tag}"
                    )
                else:
                    logging.info(
                        f"There is no PAI run for experiment {current_model_group}"
                        f" and tag {current_tag}"
                    )
                    pd_results[prediction_colname] = missing_model_default_value
            else:
                current_model = pai.load_model(run_id=current_run_id)
                df_current_model_features = pai.load_features(run_id=current_run_id)
                current_model_features = df_current_model_features["feature_list"].iloc[
                    0
                ]
                # We sort features because MLflow does not preserve feature order
                # Models should also be trained with features sorted
                current_model_features.sort()
                X = pdf[current_model_features]
                if "binary" in current_run["tags"].iloc[0]:
                    pd_results[prediction_colname] = current_model.predict_proba(
                        X, num_threads=1, n_jobs=1
                    )[:, 1]
                elif "regression" in current_run["tags"].iloc[0]:
                    pd_results[prediction_colname] = current_model.predict(
                        X, num_threads=1, n_jobs=1
                    )
                else:
                    raise ValueError(
                        "Unrecognized model type while predicting, model has"
                        "neither 'binary' or 'regression' tags"
                    )

        return pd_results

    df_master = df_master.withColumn(
        "partition",
        F.floor(
            F.count(F.lit(1)).over(Window.partitionBy(model_group_column))
            / scoring_chunk_size
            * F.rand()
        ),
    )
    if not explanatory_features:
        # Keep only necessary columns to make the pandas transformation more lightweight
        pai.set_config(
            storage_runs=pai_runs_uri, storage_artifacts=pai_artifacts_uri,
        )
        explanatory_features = set()
        for current_tag in models_to_score.keys():

            df_features = pai.load_features(tags=current_tag)
            current_model_features = set(
                [
                    f.replace("importance_", "")
                    for f in df_features.columns
                    if f.startswith("importance_")
                ]
            )
            explanatory_features = explanatory_features.union(current_model_features)
        explanatory_features = list(explanatory_features)

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
