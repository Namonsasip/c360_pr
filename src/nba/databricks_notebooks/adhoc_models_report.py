# This notebook creates a report with a summary of campaign data and models performance and results
# This is done in a notebook because requires accessing PAI and only some machines
# are allowed through the firewall

runs_to_track = [
    ("z_20200520_220241_acceptance_Miguel_", ""),
    ("z_20200520_220241_arpu_30d_Miguel_", "ARPU_30d"),
    ("z_20200520_220241_arpu_7d_Miguel_", "ARPU_7d"),
]

df_master = spark.read.parquet("/mnt/customer360-blob-data/C360/NBA/l5_nba_master_table/")

extra_keep_columns = [
    "campaign_child_code",
    "campaign_name",
    "campaign_category",
    "campaign_type",
    "campaign_sub_type",
    "campaign_type_contact_policy",
]

# COMMAND ----------

import logging
from typing import Dict, List, Tuple, Union

import pandas as pd
import pyspark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, TimestampType, DateType

import pai
import pandas as pd


def node_prioritized_campaigns_analysis(
        df_master: pyspark.sql.DataFrame,
        extra_keep_columns: List[str],
) -> pd.DataFrame:
    pdf_report = (
        df_master.groupby("model_group")
            .agg(
            F.count(F.lit(1)).alias("n_sent_contacts"),
            F.count(F.col("response")).alias("n_tracked_contacts"),
            F.sum("target_response").alias("n_positive_contacts"),
            F.mean((~F.isnull(F.col("response"))).cast(FloatType())).alias(
                "ratio_response_tracked"
            ),
            F.mean((F.col("response") == "Y").cast(FloatType())).alias(
                "acceptance_rate"
            ),
            F.min("contact_date").alias("min_contact_date"),
            F.max("contact_date").alias("max_contact_date"),
            # 7 day ARPU
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day_avg_all_subs").alias(
                "avg_arpu_7d_before_all_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day_after_avg_all_subs").alias(
                "avg_arpu_7d_after_all_subcribers"
            ),
            F.mean("target_relative_arpu_increase_7d_avg_all_subs").alias(
                "avg_arpu_7d_increase_all_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day").alias(
                "avg_arpu_7d_before_targeted_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_seven_day_after").alias(
                "avg_arpu_7d_after_targeted_subcribers"
            ),
            F.mean("target_relative_arpu_increase_7d").alias(
                "avg_arpu_7d_increase_targeted_subcribers"
            ),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_seven_day"),
                )
            ).alias("avg_arpu_7d_before_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_seven_day_after"),
                )
            ).alias("avg_arpu_7d_after_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("target_relative_arpu_increase_7d"),
                )
            ).alias("avg_arpu_7d_increase_positive_responses"),
            # 30 day ARPU
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_thirty_day_avg_all_subs").alias(
                "avg_arpu_30d_before_all_subcribers"
            ),
            F.mean(
                "sum_rev_arpu_total_net_rev_daily_last_thirty_day_after_avg_all_subs").alias(
                "avg_arpu_30d_after_all_subcribers"
            ),
            F.mean("target_relative_arpu_increase_30d_avg_all_subs").alias(
                "avg_arpu_30d_increase_all_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_thirty_day").alias(
                "avg_arpu_30d_before_targeted_subcribers"
            ),
            F.mean("sum_rev_arpu_total_net_rev_daily_last_thirty_day_after").alias(
                "avg_arpu_30d_after_targeted_subcribers"
            ),
            F.mean("target_relative_arpu_increase_30d").alias(
                "avg_arpu_30d_increase_targeted_subcribers"
            ),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_thirty_day"),
                )
            ).alias("avg_arpu_30d_before_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("sum_rev_arpu_total_net_rev_daily_last_thirty_day_after"),
                )
            ).alias("avg_arpu_30d_after_positive_responses"),
            F.mean(
                F.when(
                    (F.col("response") == "Y"),
                    F.col("target_relative_arpu_increase_30d"),
                )
            ).alias("avg_arpu_30d_increase_positive_responses"),
            *[
                F.first(F.when(F.col("campaign_prioritized") == 1, F.col(x))).alias(x)
                for x in extra_keep_columns
            ],
        )
            .toPandas()
    )

    #     pdf_report = pdf_report.merge(
    #         pd.DataFrame({"campaign_child_code": prioritized_campaign_child_codes}),
    #         on=["campaign_child_code"],
    #         how="outer",
    #     )

    pdf_report = pdf_report.sort_values(["n_sent_contacts"], ascending=False)

    return pdf_report


# COMMAND ----------

report = node_prioritized_campaigns_analysis(
    df_master=df_master,
    extra_keep_columns=extra_keep_columns,
)

# COMMAND ----------

import pai

pai.set_config(
    storage_runs="mssql+pyodbc://mck_ds_360:paisql#Apr2020@azdwh-serv-da.database.windows.net/pai-mssql-db?driver=ODBC+Driver+17+for+SQL+Server",
    storage_artifacts="dbfs://mnt/customer360-blob-data/NBA/pai_20200511/")
runs = pai.load_runs()
runs

# COMMAND ----------

report_with_metrics = report
for tag, suffix in runs_to_track:
    current_runs = runs[runs["tags"].apply(lambda x: tag in x)]

    n_binary_runs = len(current_runs[current_runs["tags"].apply(lambda x: "binary" in x)])
    n_regression_runs = len(current_runs[current_runs["tags"].apply(lambda x: "regression" in x)])

    assert (n_binary_runs + n_regression_runs != 0), f"Tag {tag} contains no runs"
    assert (n_binary_runs == 0) or (n_regression_runs == 0), f"Tag {tag} contains both binary and regression runs"
    assert current_runs[
        "experiment"].is_unique, f"Tag {tag} contains duplicate experiments: {', '.join(current_runs['experiment'][current_runs['experiment'].duplicated()])}"

    if n_binary_runs:
        df_model_performance = pd.DataFrame({
            "model_group": current_runs["experiment"],
            f"AUC{suffix}": current_runs["metrics"].apply(lambda x: x["test_auc"] if "test_auc" in x.keys() else None),
        })
    elif n_regression_runs:
        df_model_performance = pd.DataFrame({
            "model_group": current_runs["experiment"],
            f"regression_benchmark{suffix}": current_runs["metrics"].apply(
                lambda x: x["test_benchmark_target_average"] if "test_benchmark_target_average" in x.keys() else None),
            f"test_mae{suffix}": current_runs["metrics"].apply(
                lambda x: x["test_mae"] if "test_mae" in x.keys() else None),
            f"model_error_reduction{suffix}": current_runs["metrics"].apply(
                lambda x: 1 - (x["test_mae"] / x["test_benchmark_target_average"]) if "test_mae" in x.keys() else None),
        })
    report_with_metrics = report_with_metrics.merge(df_model_performance, on="model_group", how="left")

# COMMAND ----------

display(report_with_metrics)

# COMMAND ----------

sdf_report_with_metrics = spark.createDataFrame(report_with_metrics)
sdf_report_with_metrics.write.parquet("/mnt/customer360-blob-output/NBA/model_results_report.parquet/",
                                      mode="overwrite")

# COMMAND ----------


