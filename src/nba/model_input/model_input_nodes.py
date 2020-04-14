from typing import Dict, List, Tuple

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

from nba.models.models_nodes import calculate_extra_pai_metrics


def node_l5_nba_master_table_spine(
    l0_campaign_tracking_contact_list_pre: DataFrame, min_feature_days_lag: int,
) -> DataFrame:

    # TODO change
    child_codes_to_keep = [
        "1-47997015444",
        "Prev_PrePMC.28",
        "1-62026255771",
        "1-63919285101",
        "1-77613407411",
        "Prev_PrePMC.29",
        "1-57028094991",
        "1-47997015450",
        "Prev_Sus.44",
        "1-71909079741",
        "Prev_PreP.83",
        "1-47997015447",
        "SOC_Pre.20",
        "1-61675239221",
        "Prev_PreI.2.3",
        "1-47997015523",
        "1-50466956657",
        "1-48343888951",
        "1-82928983971",
        "Prev_PreA.25",
        "1-86931924401",
        "Prev_PrePMC.26",
        "Prev_PreP.67",
        "SOC_Pre.21",
        "Prev_Sus.40",
        "1-86931924407",
        "1-84025237671",
        "Prev_Sus.39",
        "Prev_PrePMC.27",
        "Prev_PreW.4.2",
        "DataExp.5.3",
        "Prev_PrePC.120",
        "Prev_PreA.27.2",
        "Prev_PrePC.100",
        "1-52935867264",
        "1-52935867261",
        "Prev_PreA.28.2",
        "Prev_PrePC.104",
        "Prev_PrePC.102",
        "Prev_PrePC.122",
        "Prev_PreA.36.2",
        "DataExp.4.3",
        "1-11480817862",
        "DataExpPost.6.3",
        "1-77267324431",
        "SOC_Pre.23",
        "Prev_PrePC.108",
        "DataExpPost.7.3",
        "1-38962026033",
        "1-72335358410",
        "Prev_PrePMC.40",
        "1-32383424765",
        "1-24793922948",
        "1-38962026036",
        "1-52939737355",
        "1-52939737358",
        "1-67470210571",
        "1-72065208825",
        "Prev_PrePMC.33",
        "1-58054415211",
        "1-75166857694",
        "1-38962026039",
        "1-75110002473",
        "1-75166857691",
        "1-42724554544",
        "1-72150881191",
    ]

    df_spine = l0_campaign_tracking_contact_list_pre.filter(
        F.col("campaign_child_code").isin(child_codes_to_keep)
    )

    df_spine = df_spine.withColumn(
        "target_response",
        F.when(F.col("response") == "Y", 1)
        .when(F.col("response") == "N", 0)
        .otherwise(None),
    )

    # Add different timeframe columns to join with features
    # Need we assume a lag of min_feature_days_lag to create the features and
    # also need to subtract a month because start_of_month references the first day of
    # the month for which the feature was calculated
    df_spine = df_spine.withColumn(
        "start_of_month",
        F.add_months(
            F.date_trunc(
                "month", F.date_sub(F.col("contact_date"), days=min_feature_days_lag),
            ),
            months=-1,
        ),
    )
    df_spine = df_spine.withColumn("partition_month", F.col("start_of_month"))

    # start_of_week references the first day of the week for which the feature was
    # calculated so we subtract 7 days to not take future data
    df_spine = df_spine.withColumn(
        "start_of_week",
        F.date_sub(
            F.date_trunc(
                "week", F.date_sub(F.col("contact_date"), days=min_feature_days_lag)
            ),
            days=7,
        ),
    )

    # event_partition_date references the day for which the feature was calculated
    df_spine = df_spine.withColumn(
        "event_partition_date",
        F.date_sub(F.col("contact_date"), days=min_feature_days_lag),
    )

    return df_spine


def node_l5_nba_master_table(
    l5_nba_master_table_spine: DataFrame,
    subset_features: Dict[str, List[str]],
    **kwargs: DataFrame,
) -> DataFrame:

    non_date_join_cols = ["subscription_identifier"]

    df_master = l5_nba_master_table_spine
    possible_key_time_columns = [
        "partition_month",
        "event_partition_date",
        "start_of_month",
        "start_of_week",
    ]

    for table_name, df_features in kwargs.items():

        table_time_column_set = set(df_features.columns).intersection(
            set(possible_key_time_columns)
        )

        if len(table_time_column_set) > 1:
            raise ValueError(
                f"More then one date column found in features table {table_name}, "
                f"columns found are {', '.join(df_features.columns)}"
            )
        elif len(table_time_column_set) <= 0:
            raise ValueError(
                f"Could not find a known time column in features table {table_name}, "
                f"columns found are {', '.join(df_features.columns)}"
            )
        elif len(table_time_column_set) == 1:
            table_time_column = table_time_column_set.pop()

        key_columns = non_date_join_cols + [table_time_column]

        if table_name in subset_features.keys():
            df_features = df_features.select(
                *(key_columns + subset_features[table_name])
            )

        duplicated_columns = [
            col_name
            for col_name in df_master.columns
            if col_name in df_features.columns
        ]
        duplicated_columns = list(set(duplicated_columns) - set(key_columns))
        if duplicated_columns:
            # logging.warning(f"Duplicated column names {', '.join(duplicated_columns)}"
            #                 f" found when joining, they will be dropped from one table")
            raise ValueError(
                f"Duplicated column names {', '.join(duplicated_columns)} found"
                f" when joining features table {table_name} to the master table."
                f"Columns of {table_name} are: {', '.join(df_features.columns)}"
            )

        df_master = df_master.join(df_features, on=key_columns, how="left")

    # Cast decimal type columns cause they don't get properly converted to pandas
    df_master = df_master.select(
        *[
            F.col(column_name).cast(FloatType())
            if column_type.startswith("decimal")
            else F.col(column_name)
            for column_name, column_type in df_master.dtypes
        ],
    )

    return df_master


def node_l5_nba_master_table_chunk_debug(
    l5_nba_master_table: DataFrame, child_code: str, sampling_rate: float
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_chunk = l5_nba_master_table.filter(F.col("campaign_child_code") == child_code)

    pdf_extra_pai_metrics = calculate_extra_pai_metrics(
        l5_nba_master_table, target_column="target_response", by="campaign_child_code"
    )
    l5_nba_master_table_chunk_debug = (
        df_chunk.filter(~F.isnull(F.col("target_response")))
        .sample(sampling_rate)
        .toPandas()
    )
    return l5_nba_master_table_chunk_debug, pdf_extra_pai_metrics
