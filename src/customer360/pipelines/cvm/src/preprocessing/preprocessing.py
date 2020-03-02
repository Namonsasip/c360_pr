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

from abc import ABC, abstractmethod


class Preprocess:
    def __init__(self, parameters, possible_stages):
        self.stages = {}
        for stage in parameters["prepro_stages"]:
            self.stages[stage] = possible_stages[stage](parameters)

    def fit(self, df):
        artifacts = {}
        for stage in self.stages:
            df, artifact = self.stages[stage].fit(df)
            artifacts[artifact] = artifact
        return df, artifacts

    def predict(self, df, artifacts):
        for stage in self.stages:
            df = self.stages[stage].predict(df, artifacts[stage])
        return df


class Preprocess_stage(ABC):
    @abstractmethod
    def __init__(self, parameters):
        self.parameters = parameters

    @abstractmethod
    def fit(self, df):
        pass

    @abstractmethod
    def predict(self, df, artifact):
        pass


class Pick_cols(Preprocess_stage):
    def __init__(self, parameters):
        self.cols_to_pick = parameters["cols_to_pick"]

    def fit(self, df):
        return df.select(self.cols_to_pick), None

    def predict(self, df, artifact):
        return df.select(self.cols_to_pick)


possible_stages = {
    "Pick_cols": Pick_cols,
}
