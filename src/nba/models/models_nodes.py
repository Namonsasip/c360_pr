# from pathlib import PurePath, Path
# from typing import List, Any, Dict
# 
# import numpy as np
# import pai
# import pandas as pd
# import pyspark
# import pyspark.sql.functions as F
# from lightgbm import LGBMClassifier
# from plotnine import *
# from pyspark.sql.types import DoubleType
# from sklearn.model_selection import train_test_split
#
# from customer360.utilities.plot_roc_curve import plot_roc_curve
#
#
# def calculate_extra_pai_metrics(
#     df_master: pyspark.sql.DataFrame, target_column: str, by: str
# ) -> pd.DataFrame:
#     pdf_extra_pai_metrics = (
#         df_master.groupby(F.col(by).alias("group"))
#         .agg(
#             F.mean(F.isnull(target_column).cast(DoubleType())).alias(
#                 "original_perc_obs_target_null"
#             ),
#             F.count(target_column).alias("original_n_obs"),
#             F.sum((F.col(target_column) == 1).cast(DoubleType())).alias(
#                 "original_n_obs_positive_target"
#             ),
#             F.mean(target_column).alias("original_target_mean"),
#         )
#         .toPandas()
#     )
#     return pdf_extra_pai_metrics
#
#
# def train_single_binary_model(
#     pdf_master_chunk: pd.DataFrame,
#     group_column: str,
#     explanatory_features: List[str],
#     target_column: str,
#     train_sampling_ratio: float,
#     model_params: Dict[str, Any],
#     min_obs_per_class_for_model: int,
#     pdf_extra_pai_metrics: pd.DataFrame,
#     pai_storage_path: str,  # = "/dbfs/mnt/customer360-blob-data/NBA/mlruns",
# ):
#
#     # context.load_node_inputs("debug_model_training")
#
#     tmp_path = Path("data/tmp")
#     current_group = pdf_master_chunk[group_column].iloc[0]
#
#     pdf_extra_pai_metrics_filtered = pdf_extra_pai_metrics[
#         pdf_extra_pai_metrics["group"] == current_group
#     ]
#
#     original_perc_obs_target_null = pdf_extra_pai_metrics_filtered[
#         "original_perc_obs_target_null"
#     ].iloc[0]
#     original_n_obs = pdf_extra_pai_metrics_filtered["original_n_obs"].iloc[0]
#     original_n_obs_positive_target = pdf_extra_pai_metrics_filtered[
#         "original_n_obs_positive_target"
#     ].iloc[0]
#     original_target_mean = pdf_extra_pai_metrics_filtered["original_target_mean"].iloc[
#         0
#     ]
#
#     modelling_perc_obs_target_null = np.mean(pdf_master_chunk[target_column].isna())
#
#     pdf_master_chunk = pdf_master_chunk[~pdf_master_chunk[target_column].isna()]
#
#     modelling_n_obs = len(pdf_master_chunk)
#     modelling_n_obs_positive_target = len(
#         pdf_master_chunk[pdf_master_chunk[target_column] == 1]
#     )
#     modelling_target_mean = np.mean(pdf_master_chunk[target_column])
#
#    pai.set_config(experiment=current_group, storage_runs=pai_storage_path)
#
#    pai.start_run()
#
#    pai.log_metrics(
#         {
#             "original_perc_obs_target_null": original_perc_obs_target_null,
#             "original_n_obs": original_n_obs,
#             "original_n_obs_positive_target": original_n_obs_positive_target,
#             "original_target_mean": original_target_mean,
#             "modelling_perc_obs_target_null": modelling_perc_obs_target_null,
#             "modelling_n_obs": modelling_n_obs,
#             "modelling_n_obs_positive_target": modelling_n_obs_positive_target,
#             "modelling_target_mean": modelling_target_mean,
#         }
#     )
#
#     pai.log_params(
#         {"target_column": target_column,}
#     )
#
#     able_to_model_flag = True
#
#     if modelling_perc_obs_target_null != 0:
#         pai.log_note("There are observations with NA target in the modelling data")
#         able_to_model_flag = False
#
#     if original_perc_obs_target_null == 1:
#         pai.log_note("The are no observations with tracked response")
#         able_to_model_flag = False
#
#     if original_n_obs_positive_target == 0:
#         pai.log_note("There are no observations with positive response")
#         able_to_model_flag = False
#
#     if original_n_obs_positive_target == original_n_obs:
#         pai.log_note("There are no observations for negative response")
#         able_to_model_flag = False
#
#     if original_n_obs_positive_target < min_obs_per_class_for_model:
#         pai.log_note(
#             f"The number of positive responses is not enough to reliably train a model. "
#             f"There are {original_n_obs_positive_target} observations while minimum required is {min_obs_per_class_for_model}"
#         )
#         able_to_model_flag = False
#
#     if original_n_obs - original_n_obs_positive_target < min_obs_per_class_for_model:
#         pai.log_note(
#             f"The number of negative responses is not enough to reliably train a model. "
#             f"There are {original_n_obs_positive_target} observations while minimum required is {min_obs_per_class_for_model}"
#         )
#         able_to_model_flag = False
#
#     if not able_to_model_flag:
#         pai.add_tags(["Unable to model"])
#         pai.end_run()
#         return
#     else:
#         pai.add_tags(["Able to model"])
#
#     pdf_train, pdf_test = train_test_split(
#         pdf_master_chunk, train_size=train_sampling_ratio
#     )
#
#     pai.log_params(
#         {"train_sampling_ratio": train_sampling_ratio, "model_params": model_params,}
#     )
#
#     model = LGBMClassifier(**model_params).fit(
#         pdf_train[explanatory_features],
#         pdf_train[target_column],
#         eval_set=[
#             (pdf_train[explanatory_features], pdf_train[target_column]),
#             (pdf_test[explanatory_features], pdf_test[target_column]),
#         ],
#         eval_names=["train", "test"],
#         eval_metric="auc",
#     )
#     pai.log_model(model)
#
#     train_auc = model.evals_result_["train"]["auc"][-1]
#     test_auc = model.evals_result_["test"]["auc"][-1]
#
#     pai.log_metrics(
#         {
#             "train_auc": train_auc,
#             "test_auc": test_auc,
#             "train_test_auc_diff": train_auc - test_auc,
#         }
#     )
#
#     pai.log_features(
#         features=explanatory_features,
#         importance=list(model.feature_importances_ / sum(model.feature_importances_)),
#     )
#
#     # Plot ROC curve
#     plot_roc_curve(
#         y_true=pdf_test[target_column],
#         y_score=model.predict_proba(pdf_test[explanatory_features])[:, 1],
#         filepath=tmp_path / "roc_curve.png",
#     )
#
#     ## Calculate and plot AUC per round
#     pdf_metrics = pd.DataFrame()
#     for valid_set_name, metrics_dict in model.evals_result_.items():
#         metrics_dict["set"] = valid_set_name
#         pdf_metrics_partial = pd.DataFrame(metrics_dict)
#         pdf_metrics_partial["round"] = range(1, pdf_metrics_partial.shape[0] + 1)
#         pdf_metrics = pd.concat([pdf_metrics, pdf_metrics_partial])
#     pdf_metrics_melted = pdf_metrics.melt(id_vars=["set", "round"], var_name="metric")
#     pdf_metrics_melted.to_csv(tmp_path / "metrics_by_round.csv", index=False)
#
#     (  # Plot the AUC of each set in each round
#         ggplot(
#             pdf_metrics_melted[pdf_metrics_melted["metric"] == "auc"],
#             aes(x="round", y="value", color="set"),
#         )
#         + ylab("Gini")
#         + geom_line()
#         + ggtitle("Gini per round (tree)")
#     ).save(tmp_path / "auc_per_round.png")
#
#     pai.log_artifacts(
#         {
#             "roc_curve": str(tmp_path / "roc_curve.png"),
#             "metrics_by_round": str(tmp_path / "metrics_by_round.csv"),
#             "auc_per_round": str(tmp_path / "auc_per_round.png"),
#         }
#     )
#
#     pai.end_run()
