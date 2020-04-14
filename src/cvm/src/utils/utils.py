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
import string
from random import random
from typing import Dict, Callable, Any, List

from pyspark.sql import DataFrame

from cvm.src.utils.list_targets import list_targets


def map_over_deep_dict(
    deep_dict: Dict, fun: Callable, pass_dict_key: bool = False
) -> Dict:
    """ Iterates over dictionary of dictionaries.

    Args:
        deep_dict: dictionary of dictionaries with arguments in leaf nodes.
        fun: function to map on deep_dict.
        pass_dict_key: should dictionary keys be passed as arguments to fun.
    Returns:
        Dictionary of dictionaries with mapped values in leaves.
    """

    def _iter_deeper(sub_dict_or_arg, *args):
        if type(sub_dict_or_arg) is dict:
            to_return = {}
            for k in sub_dict_or_arg:
                to_return[k] = _iter_deeper(sub_dict_or_arg[k], (*args, k))
        else:
            if pass_dict_key:
                to_return = fun(sub_dict_or_arg, *args)
            else:
                to_return = fun(sub_dict_or_arg)
        return to_return

    return _iter_deeper(deep_dict, ())


def iterate_over_usecases_macrosegments_targets(
    fun: Callable, parameters: Dict[str, Any],
) -> object:
    """Iterates fun over every usecase, macrosegment, target.

    Args:
        fun: function to call, assumes to input usecase, macrosegment, target
        parameters: parameters defined in parameters.yml.
    """

    def _iter_for_macrosegment(use_case_chosen, macrosegment_chosen):
        fun_values = {}
        for target_chosen in target_cols[use_case_chosen]:
            fun_values[target_chosen] = fun(
                use_case_chosen, macrosegment_chosen, target_chosen
            )
        return fun_values

    def _iter_for_usecase(use_chosen):
        fun_values = {}
        for macrosegment_chosen in macrosegments[use_chosen]:
            fun_values[macrosegment_chosen] = _iter_for_macrosegment(
                use_chosen, macrosegment_chosen
            )
        return fun_values

    target_cols = list_targets(parameters, case_split=True)
    macrosegments = parameters["macrosegments"]

    fun_vals = {}
    for use_case in parameters["targets"]:
        fun_vals[use_case] = _iter_for_usecase(use_case)

    return fun_vals


def random_word(length=16):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def get_clean_important_variables(
    important_param: List[Any], parameters: Dict[str, Any],
) -> List[Any]:
    """ Returns important variables from before preprocessing stage.

    Args:
        important_param: List of important columns.
        parameters: parameters defined in parameters.yml.
    """
    suffix_list = parameters["preprocessing_suffixes"]
    for suffix in suffix_list:
        important_param = [
            col.replace(suffix_list[suffix], "") for col in important_param
        ]
    return important_param


def impute_from_parameters(df: DataFrame, parameters: Dict[str, Any],) -> DataFrame:
    """ Impute columns using dictionary defined in parameters.

    Args:
        df: Table to impute.
        parameters: parameters defined in parameters.yml.
    """
    default_values = parameters["feature_default_values"]
    default_values_to_apply = {
        col_name: default_values[col_name]
        for col_name in default_values
        if col_name in df.columns
    }
    return df.fillna(default_values_to_apply)
