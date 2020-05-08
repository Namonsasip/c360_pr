import os
import random
from datetime import date, datetime
from functools import partial
import pai
import pandas as pd
import pyspark
from plotnine import *

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, FloatType

from customer360.run import ProjectContext
from kedro.io import DataCatalog, PartitionedDataSet
from pyspark.sql import SparkSession

from customer360.pipeline import create_c360_pipeline
from datasets.spark_dbfs_dataset import SparkDbfsDataSet

if random.random() > 2:
    catalog = DataCatalog()
    context = ProjectContext()
    spark = SparkSession.builder.getOrCreate()

# df_master = context.load_node_inputs("l5_nba_master_table")

min_date = "2020-01-01"
max_date = "2020-03-31"
df_campaigns_tracking = catalog.load("l0_campaign_tracking_contact_list_pre_full_load")
df_campaigns_master = catalog.load("campaign_history_master_active")
prioritized_campaigns_list = catalog.load(
    "params:nba_prioritized_campaigns_child_codes"
)
dst_path = os.path.join("data", "nba_prioritized_campaigns_coverage",)
os.makedirs(dst_path, exist_ok=True)

x_columns = [
    "campaign_category",
    "campaign_type",
    "campaign_sub_type",
    "campaign_type_contact_policy",
]
# df_campaigns_tracking.select(F.min("contact_date"), F.max("contact_date")).show()

df_campaigns_tracking = df_campaigns_tracking.filter(
    F.col("contact_date").between(min_date, max_date)
)

df_campaigns_tracking_summary = df_campaigns_tracking.groupby(
    "campaign_child_code"
).agg(
    F.count(F.lit(1)).alias("n_sent_contacts"),
    F.mean((~F.isnull(F.col("response"))).cast(DoubleType())).alias(
        "ratio_response_tracked"
    ),
    F.mean((F.col("response") == "Y").cast(DoubleType())).alias("acceptance_rate"),
    F.mean(
        F.when(
            F.col("contact_control_group") == "Contact group",
            (F.col("response") == "Y").cast(DoubleType()),
        )
    ).alias("acceptance_rate_contact_group"),
    F.mean(
        F.when(
            F.col("contact_control_group") == "Control group",
            (F.col("response") == "Y").cast(DoubleType()),
        )
    ).alias("acceptance_rate_control_group"),
)


df_campaigns_tracking_summary = df_campaigns_tracking_summary.withColumn(
    "prioritized_campaign",
    F.when(F.col("campaign_child_code").isin(prioritized_campaigns_list), 1).otherwise(
        0
    ),
)

df_campaigns_tracking_summary = df_campaigns_tracking_summary.withColumn(
    "response_tracked",
    F.when(
        (F.col("ratio_response_tracked") > 0.25)
        & (F.col("acceptance_rate") > 0)
        & (F.col("acceptance_rate") < 1),
        1,
    ).otherwise(0),
)

df_campaigns = df_campaigns_tracking_summary.join(
    F.broadcast(
        df_campaigns_master.withColumnRenamed("child_code", "campaign_child_code")
    ),
    on="campaign_child_code",
    how="left",
)


pdf_campaigns = df_campaigns.toPandas().sort_values("n_sent_contacts", ascending=False)

pdf_campaigns.to_csv(
    os.path.join(dst_path, f"all_campaign_contacts.csv",), index=False,
)

pdf_campaigns_relevant = pdf_campaigns[
    (pdf_campaigns["campaign_type"] == "Rule-based")
    & (pdf_campaigns["campaign_sub_type"] == "Non-trigger")
    & (~pdf_campaigns["child_name"].str.contains("Pull", case=False, na=False))
]
pdf_campaigns_relevant.to_csv(
    os.path.join(dst_path, f"rule_based_push_contacts.csv",), index=False,
)


def csv_and_barplot(
    pdf: pd.DataFrame, color_column: str, x_column: str, dst_path, title: str
):
    os.makedirs(dst_path, exist_ok=True)

    pdf = pdf.fillna({x_column: "NULL", color_column: "NULL"})

    pdf_report = pdf.groupby([x_column, color_column], as_index=False)[
        "n_sent_contacts"
    ].sum()
    pdf_report.to_csv(
        os.path.join(dst_path, f"{x_column} by {color_column}.csv",), index=False,
    )

    (
        ggplot(
            pdf_report,
            aes(x=x_column, y="n_sent_contacts", fill=f"{color_column}.astype(str)"),
        )
        + geom_bar(stat="identity")
        + ggtitle(title)
        + theme(axis_text_x=element_text(rotation=60, hjust=1))
    ).save(os.path.join(dst_path, f"{x_column} by {color_column}.png",))


for x_column in x_columns:
    csv_and_barplot(
        pdf_campaigns_relevant,
        x_column=x_column,
        color_column="prioritized_campaign",
        dst_path=dst_path,
        title=f"Campaign contacts between {min_date} and {max_date}",
    )


# df_master = catalog.load("l5_nba_master_table")
