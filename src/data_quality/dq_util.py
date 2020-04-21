from kedro.context import load_context
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from pathlib import Path

import os
from typing import *


def get_config_parameters(config_path="**/parameters.yml"):
    conf = os.getenv("CONF", None)
    config = load_context(Path.cwd(), env=conf)._get_config_loader().get(config_path)
    return config


def get_c360_catalog() -> List[str]:
    conf = os.getenv("CONF", None)
    if conf == 'local_fs':
        catalog_path = "**/catalog.yml"
    else:
        catalog_path = "**/C360/catalog.yml"

    c360_catalog = get_config_parameters(catalog_path)

    return c360_catalog


def some_func(df, config):
    print()
    return None


def generate_dq_nodes(
        catalog: list
):
    nodes = []
    selected_dataset = get_config_parameters()['features_for_dq']
    for dataset_name, feature_list in selected_dataset.items():
        node = Node(
            inputs=[dataset_name,
                    "params:features_for_dq"],
            func=some_func,
            outputs=None
        )
        nodes.append(node)

    return nodes
