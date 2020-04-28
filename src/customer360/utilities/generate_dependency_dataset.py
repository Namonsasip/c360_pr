import pandas as pd
import pyspark.sql.functions as f
import os
from customer360.utilities.spark_util import get_spark_session
from pathlib import Path
from kedro.context.context import load_context
conf = os.getenv("CONF", None)


def generate_dependency_dataset():
    """
    :param project_context:
    :return:
    """
    project_context = load_context(Path.cwd(), env=conf)
    catalog = project_context.catalog

    def get_path(catalog_name):
        return catalog._data_sets[catalog_name].__getattribute__("_filepath")

    all_data_set = catalog.list()
    all_list_dependency = []
    all_list_cols = []

    for data_set in all_data_set:
        parent_path = None
        child_path = None
        if type(catalog._data_sets[data_set]).__name__ == "SparkDbfsDataSet":
            parent_path = get_path(data_set)
            lookup_name = catalog._data_sets[data_set].__getattribute__("_lookup_table_name")
            if lookup_name and lookup_name != 'int_l1_streaming_sum_per_day':
                try:
                    child_path = get_path(lookup_name)
                except Exception as e:
                    child_path = get_path(lookup_name + '@save')
        # This is to create two columns with dependency DFS
        all_list_dependency.append(parent_path)
        all_list_dependency.append(child_path)
        # This is to create one column with columns
        all_list_cols.append(parent_path)
        all_list_cols.append(child_path)

    df_cols = pd.DataFrame(all_list_dependency, columns=['data_set_path']).drop_duplicates()
    print(df_cols.head())
    df_dependency = pd.DataFrame(all_list_dependency, columns=['parent_path', 'child_path'])
    df_dependency = df_dependency[df_dependency.child_path.notnull()]

    def get_children(id):
        list_of_children = []

        def dfs(id):
            child_ids = df_dependency[df_dependency["parent_path"] == id]["child_path"]
            if child_ids.empty:
                return
            for child_id in child_ids:
                list_of_children.append(child_id)
                dfs(child_id)

        dfs(id)
        list_of_children = list(set(list_of_children))
        return list_of_children

    def generate_l1_l2_l3_l4_cols(row):
        row["l1_datasets"] = [x for x in row["list_of_children"] if "l1_feat" in x]
        row["l2_datasets"] = [x for x in row["list_of_children"] if "l2_feat" in x]
        row["l3_datasets"] = [x for x in row["list_of_children"] if "l3_feat" in x]
        row["l4_datasets"] = [x for x in row["list_of_children"] if "l4_feat" in x]
        return row

    df_dependency["list_of_children"] = df_dependency["parent_path"].apply(get_children)
    df_dependency = df_dependency[df_dependency.parent_path.str.contains("customer360-blob-data")]
    df_dependency = df_dependency.apply(generate_l1_l2_l3_l4_cols, axis=1)
    for col in df_dependency.columns:
        df_dependency[col] = df_dependency[col].astype(str)
    spark = get_spark_session()
    spark_df = spark.createDataFrame(df_dependency).drop("child_path").drop_duplicates(subset=["parent_path"])
    spark_df = spark_df.withColumn("event_partition_date", f.current_date())
    util_dependency_report = spark_df

    def get_cols(row):
        try:
            curr_val = str(spark.read.parquet(row['data_set_path']).columns)
        except Exception as e:
            curr_val = ''
        row['features'] = curr_val
        return row

    df_cols = df_cols.apply(get_cols, axis=1)
    df_cols_spark = spark.createDataFrame(df_cols).drop_duplicates(subset=["data_set_path"]) \
        .withColumn("event_partition_date", f.current_date())
    util_feature_report = df_cols_spark

    return [util_dependency_report, util_feature_report]
