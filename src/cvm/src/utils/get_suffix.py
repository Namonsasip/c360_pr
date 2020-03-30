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


def get_suffix(sample_type: str = None) -> str:
    """ Returns dataset suffix for given sample type.

    Args:
        sample_type: can be None (for full dataset),
            "dev" for small training sample,
            "sample" for medium training sample,
            "scoring" for full scoring sample,
            "scoring_dev" for small scoring sample,
            "scoring_sample" for medium scoring sample.
    """

    if sample_type not in [None, "dev", "sample", "scoring_dev", "scoring_sample"]:
        raise Exception("Sample type {} not implemented".format(sample_type))

    if sample_type is not None:
        suffix = "_" + sample_type
    else:
        suffix = ""

    return suffix


def is_scoring(sample_type: str = None) -> bool:
    """ Returns True if sample type contains scoring.

    Args:
        sample_type: can be None (for full dataset),
            "dev" for small training sample,
            "sample" for medium training sample,
            "scoring" for full scoring sample,
            "scoring_dev" for small scoring sample,
            "scoring_sample" for medium scoring sample.
    """

    if sample_type is None:
        return False
    return "scoring" in sample_type
