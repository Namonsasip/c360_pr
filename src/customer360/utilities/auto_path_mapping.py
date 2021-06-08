import logging


def auto_path_mapping_project_context(catalog, running_environment):
    """
    Purpose: This function is used to automatically convert the source and target paths in catalog entries as per the
             working environment(cloud/ on-premise).
    :param catalog:
    :param running_environment:
    :return:
    """
    temp_list = []
    if running_environment.lower() == 'on_premise':
        metadata_table = catalog.load("params:metadata_path")['on_premise_metadata']
        util_path = catalog.load("params:metadata_path")['on_premise_util']
        dq_path = catalog.load("params:metadata_path")['on_premise_dq']
    else:
        metadata_table = catalog.load("params:metadata_path")['on_cloud_metadata']
        util_path = catalog.load("params:metadata_path")['on_cloud_util']
        dq_path = catalog.load("params:metadata_path")['on_cloud_dq']
    for curr_domain in catalog.load("params:cloud_on_prim_path_conversion"):
        search_pattern = curr_domain["search_pattern"]
        replace_pattern = search_pattern.replace("/", "")
        if running_environment.lower() == 'on_premise':
            source_prefix = curr_domain["source_path_on_prem_prefix"]
            target_prefix = curr_domain["target_path_on_prem_prefix"]
            stage_prefix = curr_domain["stage_path_on_prem_prefix"]
        else:
            source_prefix = curr_domain["source_path_on_cloud_prefix"]
            target_prefix = curr_domain["target_path_on_cloud_prefix"]
            stage_prefix = curr_domain["target_path_on_cloud_prefix"]
        for curr_catalog in catalog.list():
            if type(catalog._data_sets[curr_catalog]).__name__ == "SparkDbfsDataSet"\
                    or type(catalog._data_sets[curr_catalog]).__name__ == "SparkIgnoreMissingPathDataset":
                original_path = str(catalog._data_sets[curr_catalog].__getattribute__("_filepath"))
                original_path_lower = original_path.lower()
                if search_pattern.lower() in original_path_lower:
                    if 'l1_features' in original_path_lower or 'l2_features' in original_path_lower or \
                            'l3_features' in original_path_lower or 'l4_features' in original_path_lower:
                        if("stage_path/" in original_path):
                            new_target_path = original_path.replace("stage_path/{}".format(replace_pattern),
                                                                    stage_prefix)
                        else:
                            new_target_path = original_path.replace("base_path/{}".format(replace_pattern),
                                                                    target_prefix)
                        catalog._data_sets[curr_catalog].__setattr__("_filepath", new_target_path)
                        t_tuple = (original_path, new_target_path)
                        temp_list.append(t_tuple)

                    else:
                        if ("stage_path/" in original_path):
                            new_source_path = original_path.replace("stage_path/{}".format(replace_pattern),
                                                                    stage_prefix)
                        else:
                            new_source_path = original_path.replace("base_path/{}".format(replace_pattern),
                                                                    source_prefix)

                        catalog._data_sets[curr_catalog].__setattr__("_filepath", new_source_path)
                        t_tuple = (original_path, new_source_path)
                        temp_list.append(t_tuple)
                    try:
                        meta_data_path = str(
                            catalog._data_sets[curr_catalog].__getattribute__("_metadata_table_path"))
                        new_meta_data_path = meta_data_path.replace("metadata_path", metadata_table)
                        catalog._data_sets[curr_catalog].__setattr__("_metadata_table_path", new_meta_data_path)
                    except Exception as e:
                        logging.info("No Meta-Data Found While Replacing Paths")

                if '/utilities/' in original_path_lower:
                    new_util_path = original_path.replace("util_path", util_path)
                    catalog._data_sets[curr_catalog].__setattr__("_filepath", new_util_path)
                    t_tuple = (original_path, new_util_path)
                    temp_list.append(t_tuple)

                if '/dq/' in original_path_lower:
                    new_dq_path = original_path.replace("dq_path", dq_path)
                    catalog._data_sets[curr_catalog].__setattr__("_filepath", new_dq_path)
                    t_tuple = (original_path, new_dq_path)
                    temp_list.append(t_tuple)

            elif type(catalog._data_sets[curr_catalog]).__name__ == "SparkDataSet":
                original_path = str(catalog._data_sets[curr_catalog].__getattribute__("_filepath"))
                original_path_lower = original_path.lower()
                if '/dq/' in original_path_lower:
                    new_dq_path = original_path.replace("dq_path", dq_path)
                    catalog._data_sets[curr_catalog].__setattr__("_filepath", new_dq_path)
                    t_tuple = (original_path, new_dq_path)
                    temp_list.append(t_tuple)

    return catalog
