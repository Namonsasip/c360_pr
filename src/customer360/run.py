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
from pathlib import Path
from typing import Dict

from kedro.context import KedroContext, load_context
from kedro.pipeline import Pipeline
from kedro.config import MissingConfigException
from warnings import warn
from typing import Any, Dict

from customer360.pipeline import create_pipelines

from kedro.io import DataCatalog
from kedro.versioning import Journal

import findspark
findspark.init()


class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here,
    or create new ones (e.g. as required by plugins)
    """

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
            params = self.config_loader.get("parameters*", "parameters*/**", "*/**/parameter*")
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
        conf_catalog = self.config_loader.get("catalog*", "catalog*/**", "*/**/catalog*")
        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())
        return catalog


def run_package():
    # entry point for running pip-install projects
    # using `<project_package>` command
    project_context = load_context(Path.cwd())

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    project_context.run(pipeline_name='revenue_to_l3_pipeline')
    # project_context.run(pipeline_name='revenue_to_l4_pipeline')
    # project_context.run(pipeline_name='customer_profile_to_l3_pipeline')

    # Replace line above with below to run on databricks cluster
    # and Dont forget to clear state for every git pull in notebook
    # (change the pipeline name to your pipeline name)
    #
    # project_context = load_context(Path.cwd(), env='base')
    # project_context.run(pipeline_name="customer_profile_to_l4_pipeline")


if __name__ == "__main__":
    # entry point for running pip-installed projects
    # using `python -m <project_package>.run` command
    run_package()
