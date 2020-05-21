# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application entry point."""

import importlib
import inspect
import logging
import logging.config
import os
from functools import partial
from pathlib import Path
from typing import Any, Dict, Union
from warnings import warn
import getpass

import findspark
from kedro.config import MissingConfigException
from kedro.context import KedroContext, load_context
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.versioning import Journal
from pyspark import SparkConf
from pyspark.sql import SparkSession

from customer360.utilities.spark_util import get_spark_session
from customer360.utilities.generate_dependency_dataset import generate_dependency_dataset

from customer360.pipeline import create_pipelines

try:
    findspark.init()
except ValueError as err:
    logging.info("findspark.init() failed with error " + str(err))

conf = os.getenv("CONF", None)


class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """
    def __init__(
        self,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(project_path, env, extra_params)
        self._spark_session = None
        self.init_spark_session()

    def init_spark_session(self, yarn=True) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        if self._spark_session:
            return self._spark_session
        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        spark_session_conf = (
            SparkSession.builder.appName(
                "{}_{}".format(self.project_name, getpass.getuser())
            )
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        if yarn:
            self._spark_session = spark_session_conf.master("yarn-client").getOrCreate()
        else:
            self._spark_session = spark_session_conf.getOrCreate()

        self._spark_session.sparkContext.setLogLevel("WARN")

    project_name = "project-samudra"
    project_version = "0.15.5"

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines()

    @property
    def params(self) -> Dict[str, Any]:
        """Read-only property referring to Kedro's parameters for this context.

        Returns:
            Parameters defined in `parameters.yml` with the addition of any
                extra parameters passed at initialization.
        """
        try:
            params = self.config_loader.get(
                "parameters*", "parameters*/**", "*/**/parameter*"
            )
        except MissingConfigException as exc:
            warn(
                "Parameters not found in your Kedro project config.\n{}".format(
                    str(exc)
                )
            )
            params = {}
        params.update(self._extra_params or {})
        return params

    def _get_catalog(
        self,
        save_version: str = None,
        journal: Journal = None,
        load_versions: Dict[str, str] = None,
    ) -> DataCatalog:
        """A hook for changing the creation of a DataCatalog instance.

        Returns:
            DataCatalog defined in `conf/base`.

        """
        conf_catalog = self.config_loader.get(
            "catalog*", "catalog*/**", "*/**/catalog*"
        )
        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())
        # This code is to handle cloud vs on-prem env
        running_environment = os.getenv("RUNNING_ENVIRONMENT", None)
        temp_list = []
        for curr_domain in catalog.load("params:cloud_on_prim_path_conversion"):
            search_pattern = curr_domain["search_pattern"]
            replace_pattern = search_pattern.replace("/", "")
            if running_environment.lower() == 'on_premise':
                source_prefix = curr_domain["source_path_on_prem_prefix"]
                target_prefix = curr_domain["target_path_on_prem_prefix"]
                metadata_table = catalog.load("params:metadata_path")['on_premise']
            else:
                source_prefix = curr_domain["source_path_on_cloud_prefix"]
                target_prefix = curr_domain["target_path_on_cloud_prefix"]
                metadata_table = catalog.load("params:metadata_path")['on_cloud']
            for curr_catalog in catalog.list():
                if type(catalog._data_sets[curr_catalog]).__name__ == "SparkDbfsDataSet":
                    original_path = str(catalog._data_sets[curr_catalog].__getattribute__("_filepath"))
                    original_path_lower = original_path.lower()
                    if search_pattern.lower() in original_path_lower:
                        print("Above", curr_catalog, original_path_lower)
                        if 'l1_features' in original_path_lower or 'l2_features' in original_path_lower or \
                            'l3_features' in original_path_lower or 'l4_features' in original_path_lower:
                            print("Below", curr_catalog)

                            new_target_path = original_path.replace("base_path/{}".format(replace_pattern), target_prefix)
                            catalog._data_sets[curr_catalog].__setattr__("_filepath", new_target_path)
                            t_tuple = (original_path, new_target_path)
                            temp_list.append(t_tuple)

                        else:
                            new_source_path = original_path.replace("base_path/{}".format(replace_pattern), source_prefix)
                            catalog._data_sets[curr_catalog].__setattr__("_filepath", new_source_path)
                            t_tuple = (original_path, new_source_path)
                            temp_list.append(t_tuple)
                        try:
                            meta_data_path = str(catalog._data_sets[curr_catalog].__getattribute__("_metadata_table_path"))
                            new_meta_data_path = meta_data_path.replace("metadata_path", metadata_table)
                            catalog._data_sets[curr_catalog].__setattr__("_metadata_table_path", new_meta_data_path)

                        except Exception as e:
                            logging.info("No Meta-Data Found While Replacing Paths")

        return catalog

    def run(self, **kwargs):
        # We override run so that spark gets initialized when
        # running kedro run
        spark = get_spark_session()
        return super().run(**kwargs)

    def load_node_inputs(
        self, node_name: str, pipeline_name: str = None, import_full_module: bool = True
    ):
        """
        Loads all inputs of a node into the namespace from which this function was called.
        This function is designed for being used in kedro ipython to debug nodes: after
        running this function you should be able to feed the function code into the
        interpreter line by line.
        Caution: this function alters the namespace from which it was called (its parent namespace)
        Args:
            node_name: Name of the node to load inputs from
            pipeline_name: Name of the pipeline where the node is
            import_full_module: If True, also imports all names defined within the
                node function's module. This is useful to not have to define function
                dependencies

        Raises:
            ValueError: If the pipeline does not contain a node named node_name

        Returns:
            Nothing, but the function alters the parent namespace
        """
        # Load the pipeline and find the node
        try:
            pipeline = self._get_pipeline(name=pipeline_name)
        except NotImplementedError:
            pipeline = self.pipeline
        sub_pipeline = pipeline.only_nodes(node_name)
        pipeline_nodes = sub_pipeline.nodes
        if not pipeline_nodes:
            raise ValueError(
                "Pipeline does not contain a node named {}".format(node_name)
            )
        node = pipeline_nodes[0]

        catalog = self._get_catalog()
        # Get both the dataset names and dataset contents of the node
        node_args, node_kwargs = Node._process_inputs_for_bind(node._inputs)
        node_loaded_args = [catalog.load(dataset_name) for dataset_name in node_args]
        node_loaded_kwargs = {
            key: catalog.load(dataset_name) for key, dataset_name in node_kwargs.items()
        }
        node_func = node._decorated_func

        # Retrieve the globals from the parent namespace, this is what we will use to
        # alter the parent's namespace
        caller_globals = dict(inspect.getmembers(inspect.stack()[1][0]))["f_globals"]

        # If the node was called using a partial function, also load the partial parameters
        if isinstance(node_func, partial):
            partial_args = node_func.args
            node_args = (
                ["parameter_from_functools_partial"] * len(partial_args)
            ) + node_args
            node_loaded_args = list(partial_args) + node_loaded_args
            partial_kwargs = node_func.keywords
            for key, value in partial_kwargs.items():
                node_kwargs[key] = "partial_parameter:{}".format(value)
                node_loaded_kwargs[key] = value
            node_func = node_func.func

        # Bind the node inputs to the parametes of the function
        bound_signature_parameters = (
            inspect.signature(node_func, follow_wrapped=False)
            .bind(*node_args, **node_kwargs)
            .signature.parameters
        )
        bound_params_dataset_names = (
            inspect.signature(node_func, follow_wrapped=False)
            .bind(*node_args, **node_kwargs)
            .arguments
        )
        bound_params_dataset_content = (
            inspect.signature(node_func, follow_wrapped=False)
            .bind(*node_loaded_args, **node_loaded_kwargs)
            .arguments
        )

        # We load the argument details to be able to identify *args and **kwargs
        argspec = inspect.getfullargspec(node_func)

        for parameter_name, parameter_value in bound_signature_parameters.items():
            # If the parameter was specified as a node input, load the dataset
            if parameter_name in bound_params_dataset_content.keys():
                loaded_dataset = bound_params_dataset_content[parameter_name]
                dataset_name = bound_params_dataset_names[parameter_name]
                logging.info(
                    "Loading dataset `{}` into `{}`".format(
                        dataset_name, parameter_name
                    )
                )
                caller_globals[parameter_name] = loaded_dataset
            # If the parameter was not specified and had a default value, load the default value
            elif parameter_value.default is not inspect._empty:
                default_param_value = parameter_value.default
                logging.info(
                    "Set `{}` to its default value: {}".format(
                        parameter_name, default_param_value
                    )
                )
                caller_globals[parameter_name] = default_param_value
            # If the parameter was not specified in inputs and does not have a default values
            # they might be the *args or **kwargs. In that case we load an empty list or
            # dictionary for them
            else:
                if parameter_name == argspec.varargs:
                    logging.info(
                        "Set `{}` (*args parameter) to [] (empty list)"
                        " since it was not specified".format(parameter_name)
                    )
                    caller_globals[parameter_name] = []
                if parameter_name == argspec.varkw:
                    logging.info(
                        "Set `"
                        + parameter_name
                        + "` (**kwrgs parameter) to {} (empty dictionary)"
                        " since it was not specified"
                    )
                    caller_globals[parameter_name] = {}


        # Import all names that are defined in the module,
        # so that all references and dependencies within
        # the function code are defined
        if import_full_module:
            logging.info(
                "Setting all objects from function's module: ({})".format(
                    node_func.__module__
                )
            )
            function_module = importlib.import_module(node_func.__module__)
            for obj_name in dir(function_module):
                if obj_name.startswith("__"):
                    continue
                else:
                    logging.info("Setting `{}`".format(obj_name))
                    caller_globals[obj_name] = getattr(function_module, obj_name)


def run_package(pipelines=None):

    # entry point for running pip-install projects
    # using `<project_package>` command
    project_context = load_context(Path.cwd(), env=conf)
    spark = get_spark_session()

    if pipelines is not None:
        for each_pipeline in pipelines:
            project_context.run(pipeline_name=each_pipeline)
        return
    # project_context.run(pipeline_name='customer_profile_to_l3_pipeline')
    # project_context.run()
    # Replace line above with below to run on databricks cluster
    # and Dont forget to clear state for every git pull in notebook
    # (change the pipeline name to your pipeline name)
    #
    # project_context = load_context(Path.cwd(), env='base')
    # project_context.run(pipeline_name="customer_profile_to_l4_pipeline")


def run_selected_nodes(pipeline_name, node_names=None, env="base"):
    # entry point for running pip-install projects
    # using `<project_package>` command
    project_context = load_context(Path.cwd(), env=env)
    project_context.run(node_names=node_names, pipeline_name=pipeline_name)


if __name__ == "__main__":
    # entry point for running pip-installed projects
    # using `python -m <project_package>.run` command
    run_package()
