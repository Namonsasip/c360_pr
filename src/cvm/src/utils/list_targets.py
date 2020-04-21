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


from typing import Any, Dict, List, Union


def list_targets(
    parameters: Dict[str, Any], case_split: bool = False
) -> Union[List[str], Dict[str, Any]]:
    """ Return all target column names.

    Args:
        parameters: parameters defined in parameters*.yml files.
        case_split: should per-use-case dictionary be returned, default False.
    Returns:
        Target column names.
    """

    targets_parameters = parameters["targets"]

    if case_split:
        target_colnames = {}
    else:
        target_colnames = []

    for use_case_name in targets_parameters:
        use_case_targets_parameters = targets_parameters[use_case_name]
        for specific_target in use_case_targets_parameters:
            target_colname = use_case_targets_parameters[specific_target]["colname"]
            if case_split:
                if use_case_name not in target_colnames:
                    target_colnames[use_case_name] = []
                target_colnames[use_case_name].append(target_colname)
            else:
                target_colnames.append(target_colname)

    return target_colnames
