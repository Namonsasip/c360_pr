import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)


def generate_dependency_dataset(project_context):
    """
    :param project_context:
    :return:
    """
    catalog = project_context.catalog
    all_data_set = catalog.list()
    # print(all_data_set)
    all_list = []
    for data_set in all_data_set:
        if type(catalog._data_sets[data_set]).__name__ == "SparkDbfsDataSet":
            tup = (data_set, catalog._data_sets[data_set].__getattribute__("_filepath"),
                      catalog._data_sets[data_set].__getattribute__("_lookup_table_name"))
            all_list.append(tup)

    df = pd.DataFrame(all_list, columns=['kedro_data_set', 'file_path', 'target_data_set'])

    print(df.head())
