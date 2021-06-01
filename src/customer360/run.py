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
import os, time
import ast
from functools import partial
from pathlib import Path
from typing import Any, Dict, Union
from warnings import warn

import findspark, datetime
from kedro.config import MissingConfigException
from kedro.context import KedroContext, load_context
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.versioning import Journal
from customer360.utilities.auto_path_mapping import auto_path_mapping_project_context

from customer360.utilities.spark_util import get_spark_session
from customer360.utilities.generate_dependency_dataset import generate_dependency_dataset

from customer360.pipeline import create_pipelines, create_dq_pipeline

try:
    findspark.init()
except ValueError as err:
    logging.info("findspark.init() failed with error " + str(err))

current_date = datetime.datetime.now()
cr_date = str((current_date - datetime.timedelta(days=0)).strftime('%Y%m'))

conf = os.getenv("CONF", None)
running_environment = os.getenv("RUNNING_ENVIRONMENT", "on_cloud")
pipeline_to_run = os.getenv("PIPELINE_TO_RUN", None)
p_increment = str(os.getenv("RUN_INCREMENT", "yes"))
os.environ['TZ'] = 'UTC'
time.tzset()

LOG_FILE_NAME = str(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M"))
if pipeline_to_run:
    LOG_FILE_NAME = "{}_{}".format(pipeline_to_run, str(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")))


class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """
    project_name = "project-samudra"
    project_version = "0.15.5"

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines()

    def _setup_logging(self) -> None:
        """Register logging specified in logging directory."""

        conf_logging = self.config_loader.get("logging*", "logging*/**")
        info_file_path = conf_logging['handlers']['info_file_handler']['filename']
        info_file_path_new = info_file_path.replace(".", "_{}.".format(LOG_FILE_NAME))

        error_file_path = conf_logging['handlers']['error_file_handler']['filename']
        error_file_path_new = error_file_path.replace(".", "_{}.".format(LOG_FILE_NAME))

        conf_logging['handlers']['info_file_handler']['filename'] = info_file_path_new
        conf_logging['handlers']['error_file_handler']['filename'] = error_file_path_new
        logging.config.dictConfig(conf_logging)

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
        if p_increment != "yes":
            h = str(conf_catalog).replace("'yes'", "'no'")
            conf_catalog = ast.literal_eval(h)
            def removekey(d, l1, l2, key):
                r = dict(d)
                try:
                    del r[l1][l2][key]
                except:
                    r = r
                return r
            for key, value in conf_catalog.items():
                for key1, value1 in value.items():
                    if (key1 == "save_args" or key1 == "load_args"):
                        if (key1 == "load_args" ):
                            increment_flag = (conf_catalog[key]['load_args'].get("increment_flag", None) if conf_catalog[key][
                                                                                    'load_args'] is not None else None)
                            if ( increment_flag == None):
                                conf_catalog[key]['load_args'] = {}
                            else:
                                conf_catalog[key]['load_args'] = {'increment_flag': 'no'}
                        g = removekey(conf_catalog, key, key1, "read_layer")
                        h = removekey(g, key, key1, "target_layer")
            conf_catalog = h
        # logging.info("catalog: {}".format(conf_catalog))
        # logging.info("catalog_type: {}".format(type(conf_catalog)))
        logging.info(">>>>>>  Create Catalog All  <<<<<")
        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())
        # This code is to handle cloud vs on-prem env
        catalog = auto_path_mapping_project_context(catalog, running_environment)
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


def run_package(pipelines=None, project_context=None, tags=None):
    # entry point for running pip-install projects
    # using `<project_package>` command
    if project_context is None:
        project_context = load_context(Path.cwd(), env=conf)
    spark = get_spark_session()

    if any([dq_pipeline in pipelines for dq_pipeline in create_dq_pipeline().keys()]):
        project_context = DataQualityProjectContext(project_path=Path.cwd(), env=conf)

    if pipelines is not None:
        for each_pipeline in pipelines:
            project_context.run(pipeline_name=each_pipeline, tags=tags)
    else:
        project_context.run(tags=tags)

    return

    # project_context.run(pipeline_name='customer_profile_to_l3_pipeline')
    # project_context.run()
    # Replace line above with below to run on databricks cluster
    # and Dont forget to clear state for every git pull in notebook
    # (change the pipeline name to your pipeline name)
    #
    # project_context = load_context(Path.cwd(), env='base')
    # project_context.run(pipeline_name="customer_profile_to_l4_pipeline")


class DataQualityProjectContext(ProjectContext):
    def _remove_increment_flag(self, catalog_dict):
        """
            Set all the incremental_flag in catalog to 'no'
            for data quality pipeline

            The incremental load in data quality will be handled separately
        """
        if catalog_dict.get("load_args") is None:
            return catalog_dict

        if catalog_dict.get("load_args").get("increment_flag") is not None:
            catalog_dict["load_args"]["increment_flag"] = 'no'


        return catalog_dict

    def _generate_dq_consistency_catalog(self):
        params = self.config_loader.get(
            "parameters*", "parameters*/**", "*/**/parameter*"
        )
        if running_environment.lower() == 'on_premise':
            dq_path = params['metadata_path']['on_premise_dq']
        else:
            dq_path = params['metadata_path']['on_cloud_dq']

        dq_consistency_path_prefix = params['dq_consistency_path_prefix']

        new_catalog_dict = {}
        for dataset_name in params["features_for_dq"].keys():
            new_catalog = {
                "type": "datasets.spark_ignore_missing_path_dataset.SparkIgnoreMissingPathDataset",
                "filepath": f"{dq_path}/{dq_consistency_path_prefix}/{dataset_name}",
                "file_format": "parquet",
                "save_args": {
                    "mode": "overwrite",
                    "partitionBy": ["corresponding_date"]
                }
            }
            new_catalog_dict[f"dq_consistency_benchmark_{dataset_name}"] = new_catalog
        return new_catalog_dict

    def _get_catalog(
            self,
            save_version: str = None,
            journal: Journal = None,
            load_versions: Dict[str, str] = None,
    ) -> DataCatalog:

        conf_catalog = self.config_loader.get(
            "catalog*", "catalog*/**", "*/**/catalog*"
        )

        for dataset_name, each_catalog in conf_catalog.items():
            self._remove_increment_flag(each_catalog)
        dq_consistency_catalog_dict = self._generate_dq_consistency_catalog()

        conf_catalog.update(dq_consistency_catalog_dict)

        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())

        catalog = auto_path_mapping_project_context(catalog, running_environment)

        logging.info("catalog1: {}".format(catalog))
        return catalog


def run_selected_nodes(pipeline_name, node_names=None, env="base"):
    # entry point for running pip-install projects
    # using `<project_package>` command
    project_context = load_context(Path.cwd(), env=env)
    project_context.run(node_names=node_names, pipeline_name=pipeline_name)

if __name__ == "__main__":
    # entry point for running pip-installed projects
    # using `python -m <project_package>.run` command
    # run_package()

    # uncomment below to run data_quality_pipeline locally
    run_package(
        pipelines=[],
    )
