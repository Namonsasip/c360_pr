import pandas as pd
from customer360.utilities.spark_util import get_spark_session
import pyspark.sql.functions as f


def generate_dependency_dataset(project_context):
    """
    :param project_context:
    :return:
    """
    catalog = project_context.catalog

    def get_path(catalog_name):
        return catalog._data_sets[catalog_name].__getattribute__("_filepath")

    all_data_set = catalog.list()
    # print(all_data_set)
    all_list = []

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
                    child_path = get_path(lookup_name+'@save')
        all_list.append((parent_path, child_path))

    df = pd.DataFrame(all_list, columns=['parent_path', 'child_path'])
    df = df[df.child_path.notnull()]

    def get_children(id):
        list_of_children = []

        def dfs(id):
            child_ids = df[df["parent_path"]==id]["child_path"]
            if child_ids.empty:
                return
            for child_id in child_ids:
                list_of_children.append(child_id)
                dfs(child_id)

        dfs(id)
        return list_of_children

    df["list_of_children"] = df["parent_path"].apply(get_children).astype(str)
    spark = get_spark_session()
    spark_df = spark.createDataFrame(df).drop("child_path").drop_duplicates(subset=["parent_path", "list_of_children"])
    spark_df = spark_df.withColumn("event_partition_date", f.current_date())
    return spark_df

