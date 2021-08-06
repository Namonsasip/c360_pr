import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import *
import os
import subprocess
from pyspark import SparkContext
from customer360.utilities.spark_util import get_spark_session
from pathlib import Path
from kedro.context.context import load_context
import logging
conf = os.getenv("CONF", None)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")


def generate_dependency_dataset():
    """
    Purpose: To generate the lineage datasets for dependency information
    :param running_env:
    :return:
    """
    logging.info("Running generate_dependency_dataset collecting catalog information :")
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
            if lookup_name:
                try:
                    child_path = get_path(lookup_name)
                except Exception as e:
                    print("could not find the child path for {}".format(lookup_name))
        # This is to create two columns with dependency DFS
        all_list_dependency.append((parent_path, child_path))

        # This is to create one column with columns
        all_list_cols.append(parent_path)
        all_list_cols.append(child_path)

    df_cols = pd.DataFrame(all_list_cols, columns=['data_set_path']).drop_duplicates()
    logging.info("Pandas df_cols generated :")
    df_dependency = pd.DataFrame(all_list_dependency, columns=['parent_path', 'child_path']).drop_duplicates()
    df_dependency = df_dependency[df_dependency.child_path.notnull()]
    logging.info("Pandas df_dependency generated :")

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

    logging.info("Running get_children collecting child information :")
    df_dependency["list_of_children"] = df_dependency["parent_path"].apply(get_children)
    contain_param = "c360/data|hdfs://10.237.82.9:8020/C360/" if running_environment.lower() == 'on_premise' else "customer360-blob"
    df_dependency = df_dependency[df_dependency.parent_path.str.contains(contain_param, na=False, regex=True)]
    logging.info("Running generate_l1_l2_l3_l4_cols collecting layer information :")
    df_dependency = df_dependency.apply(generate_l1_l2_l3_l4_cols, axis=1)
    for col in df_dependency.columns:
        df_dependency[col] = df_dependency[col].astype(str)
    spark = get_spark_session()
    spark_df = spark.createDataFrame(df_dependency).drop("child_path").drop_duplicates(subset=["parent_path"])
    spark_df = spark_df.withColumn("event_partition_date", f.current_date())
    util_dependency_report = spark_df
    return [util_dependency_report, df_cols]

    # def get_cols(row):
    #     try:
    #         curr_val = str(spark.read.parquet(row['data_set_path']).columns)
    #     except Exception as e:
    #         curr_val = ''
    #     row['features'] = curr_val
    #     return row
    # # This filter needs to be removed
    # # df_cols = df_cols[df_cols.data_set_path.str.contains("USAGE", na=False)]
    # # df_cols = df_cols[df_cols.data_set_path.str.contains("output", na=False)]
    # logging.info("df_cols row count :"+str(df_cols.shape))
    # ################################
    # logging.info("Running get_cols to get schema of all the paths :")
    # df_cols = df_cols.apply(get_cols, axis=1)
    # df_cols_spark = spark.createDataFrame(df_cols).drop_duplicates(subset=["data_set_path"]) \
    #     .withColumn("event_partition_date", f.current_date())
    # util_feature_report = df_cols_spark

def generate_list_of_feature(list_dataset):
    ## new
    df_cols=list_dataset
    spark = get_spark_session()
    def get_cols_sp(data_set_path):
        try:
            list_file = []
            list_file_temp = subprocess.check_output(
                        "hadoop fs -ls -d " + data_set_path + "*/ |awk -F' ' '{print $NF}' |grep =20",
                        shell=True).splitlines()
            for p_table_name in list_file_temp:
                list_file.append(p_table_name.decode('utf8'))
            df = spark.read.option("multiline", "true")\
                        .option("mode", "PERMISSIVE")\
                        .option("mergeSchema", "true")\
                        .option("basePath", data_set_path) \
                        .load(list_file[-1])

            features = str(df.columns)
        except Exception as e:
            features = ''
            # print(e)
            list_of_error.append(e)
        row = [data_set_path, features]
        return row

    df_cols_sp = spark.createDataFrame(df_cols)
    lst_data_set = df_cols_sp.select("data_set_path").drop_duplicates().rdd.flatMap(lambda x: x).collect()
    no_of_df = len(df_cols)
    progress = 1
    list_of_features = []
    list_of_error = []
    for data_set_path in lst_data_set:
        print('Process progress list file [' + str(progress) + ' / ' + str(no_of_df) + ']')
        list_features = get_cols_sp(data_set_path)
        list_of_features.append(list_features)
        progress+=1

    schema_list_of_features = StructType([
        StructField('data_set_path', StringType(), True),
        StructField('features', StringType(), True)
    ])
    df_list_of_features = spark.createDataFrame(list_of_features, schema_list_of_features).drop_duplicates()
    df_cols_spark = df_list_of_features.withColumn("event_partition_date", f.current_date())
    util_feature_report = df_cols_spark

    for ept in list_of_error:
        print(ept)
        print('-'*30)

    return util_feature_report
