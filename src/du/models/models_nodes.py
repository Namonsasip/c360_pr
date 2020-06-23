import os
import re
from pathlib import Path
from typing import List, Any, Dict, Callable, Tuple

import matplotlib.pyplot as plt
import numpy as np
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
                                [#Remove special characters as they are not allowed
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
                            "train_set_primary_keys": pdf_train["nba_spine_primary_key"],
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