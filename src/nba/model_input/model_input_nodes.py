# import pandas as pd
# from kedro.io import ParquetLocalDataSet
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
#
# from nba.models.models_nodes import calculate_extra_pai_metrics
# ParquetLocalDataSet
#
# def node_l5_nba_master_table_spine(
#     l0_campaign_tracking_contact_list_pre: DataFrame, min_feature_days_lag: int,
# ) -> DataFrame:
#
#     # TODO change
#     child_codes_to_keep = ["Prev_Sus.44", "Prev_PreI.2.3", "TU39.2", "Voice.18.3.2"]
#
#     df_spine = l0_campaign_tracking_contact_list_pre.filter(
#         F.col("campaign_child_code").isin(child_codes_to_keep)
#     )
#
#     df_spine = df_spine.withColumn(
#         "target_response",
#         F.when(F.col("response") == "Y", 1)
#         .when(F.col("response") == "N", 0)
#         .otherwise(None),
#     )
#
#     # Add different timeframe columns to join with features
#     # Need we assume a lag of min_feature_days_lag to create the features and
#     # also need to subtract a month because start_of_month references the first day of
#     # the month for which the feature was calculated
#     df_spine = df_spine.withColumn(
#         "start_of_month",
#         F.add_months(
#             F.date_trunc(
#                 "month", F.date_sub(F.col("contact_date"), days=min_feature_days_lag),
#             ),
#             months=-1,
#         ),
#     )
#
#     # start_of_week references the first day of the week for which the feature was
#     # calculated so we subtract 7 days to not take future data
#     df_spine = df_spine.withColumn(
#         "start_of_week",
#         F.date_sub(
#             F.date_trunc(
#                 "week", F.date_sub(F.col("contact_date"), days=min_feature_days_lag)
#             ),
#             days=7,
#         ),
#     )
#
#     return df_spine
#
#
# def node_l5_nba_master_table(
#     l5_nba_master_table_spine: DataFrame, **kwargs: DataFrame
# ) -> DataFrame:
#
#     non_date_join_cols = ["subscription_identifier"]
#
#     df_master = l5_nba_master_table_spine
#     possible_key_time_columns = [
#         # "partition_month",
#         # "event_partition_date",
#         "start_of_month",
#         "start_of_week",
#     ]
#
#     for table_name, df_features in kwargs.items():
#         table_time_column_set = set(df_features.columns).intersection(
#             set(possible_key_time_columns)
#         )
#
#         if len(table_time_column_set) > 1:
#             raise ValueError(
#                 f"More then one date column found in features table {table_name}, "
#                 f"columns found are {', '.join(df_features.columns)}"
#             )
#         elif len(table_time_column_set) <= 0:
#             raise ValueError(
#                 f"Could not find a known time column in features table {table_name}, "
#                 f"columns found are {', '.join(df_features.columns)}"
#             )
#         elif len(table_time_column_set) == 1:
#             table_time_column = table_time_column_set.pop()
#
#         key_columns = non_date_join_cols + [table_time_column]
#
#         duplicated_columns = [
#             col_name
#             for col_name in df_master.columns
#             if col_name in df_features.columns
#         ]
#         duplicated_columns = list(set(duplicated_columns) - set(key_columns))
#         if duplicated_columns:
#             # logging.warning(f"Duplicated column names {', '.join(duplicated_columns)}"
#             #                 f" found when joining, they will be dropped from one table")
#             raise ValueError(
#                 f"Duplicated column names {', '.join(duplicated_columns)} found"
#                 f" when joining features table {table_name} to the master table."
#                 f"Columns of {table_name} are: {', '.join(df_features.columns)}"
#             )
#
#         df_master = df_master.join(df_features, on=key_columns, how="left")
#
#         return df_master
#
#
# def node_l5_nba_master_table_chunk_debug(
#     l5_nba_master_table: DataFrame, child_code: str, sampling_rate: float
# ) -> pd.DataFrame:
#     df_chunk = l5_nba_master_table.filter(F.col("campaign_child_code") == child_code)
#
#     pdf_extra_pai_metrics = calculate_extra_pai_metrics(
#         l5_nba_master_table, target_column="target_response", by="campaign_child_code"
#     )
#     l5_nba_master_table_chunk_debug = df_chunk.sample(sampling_rate).toPandas()
#     return l5_nba_master_table_chunk_debug, pdf_extra_pai_metrics
