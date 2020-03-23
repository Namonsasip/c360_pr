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
"""Pipeline construction."""
from typing import Dict

from kedro.pipeline import Pipeline

from cvm.data_prep.pipeline import (
    create_cvm_prepare_inputs_samples,
    create_cvm_targets,
    create_cvm_training_data,
    create_cvm_scoring_data,
)
from cvm.modelling.pipeline import create_train_model, create_predictions
from cvm.preprocessing.pipeline import (
    create_cvm_preprocessing,
    create_cvm_preprocessing_scoring,
)


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """

    ###########################################################################
    # Here you can find an example pipeline, made of two modular pipelines.
    #
    # PLEASE DELETE THIS PIPELINE ONCE YOU START WORKING ON YOUR OWN PROJECT AS
    # WELL AS pipelines/cvm AND pipelines/data_engineering
    # -------------------------------------------------------------------------

    return {
        "cvm_setup_training_data_sample": create_cvm_prepare_inputs_samples("sample")
        + create_cvm_targets("sample")
        + create_cvm_training_data("sample"),
        "cvm_training_preprocess_sample": create_cvm_preprocessing_scoring("sample"),
        "cvm_train_model_sample": create_train_model("sample"),
        "cvm_setup_scoring_data_sample": create_cvm_prepare_inputs_samples(
            "scoring_sample"
        )
        + create_cvm_scoring_data("scoring_sample"),
        "cvm_scoring_combine_data": create_cvm_scoring_data("scoring_sample"),
        "cvm_scoring_preprocess_sample": create_cvm_preprocessing_scoring(
            "scoring_sample"
        ),
        "cvm_predict_model_sample": create_predictions("scoring_sample", "dev"),
        "cvm_setup_training_data_dev": create_cvm_prepare_inputs_samples("dev")
        + create_cvm_targets("dev")
        + create_cvm_training_data("dev"),
        "cvm_training_combine_data": create_cvm_training_data("dev"),
        "cvm_training_preprocess_dev": create_cvm_preprocessing("dev"),
        "cvm_train_model_dev": create_train_model("dev"),
        "cvm_setup_scoring_data_dev": create_cvm_prepare_inputs_samples("scoring_dev")
        + create_cvm_scoring_data("scoring_dev"),
        "cvm_scoring_preprocess_dev": create_cvm_preprocessing_scoring("scoring_dev"),
        "cvm_predict_model_dev": create_predictions("scoring_dev", "dev"),
        "cvm_validate_model_dev": create_predictions(
            "sample", "dev", "l5_cvm_one_day_train_preprocessed_sample"
        ),
    }
