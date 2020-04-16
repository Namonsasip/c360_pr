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
from pyspark.ml import Transformer, Estimator, Model
from pyspark.sql.functions import col, countDistinct

from cvm.src.utils.classify_columns import classify_columns
from cvm.src.utils.list_operations import list_intersection


class Selector(Transformer):
    """ Transformer selecting constant set of columns"""

    def __init__(self, cols_to_pick):
        super().__init__()
        self.cols_to_pick = cols_to_pick

    def _transform(self, dataset):
        to_pick = list_intersection(self.cols_to_pick, dataset.columns)
        return dataset.select(to_pick)

    def transform(self, dataset, params=None):
        return self._transform(dataset)


class TypeSetter(Transformer):
    """ Transformer converting types of columns"""

    def __init__(self, parameters):
        super().__init__()
        self.parameters = parameters

    def _transform(self, dataset):
        columns_cats = classify_columns(dataset, self.parameters)
        numerical_cols = list_intersection(dataset.columns, columns_cats["numerical"])
        for col_name in numerical_cols:
            dataset = dataset.withColumn(col_name, col(col_name).cast("float"))
        return dataset

    def transform(self, dataset, params=None):
        return self._transform(dataset)


class Dropper(Transformer):
    """ Transformer dropping constant set of columns"""

    def __init__(self, cols_to_drop):
        super().__init__()
        self.cols_to_drop = cols_to_drop

    def _transform(self, dataset):
        cols_to_drop = list_intersection(self.cols_to_drop, dataset.columns)
        return dataset.drop(*cols_to_drop)

    def transform(self, dataset, params=None):
        return self._transform(dataset)


class NullDroppers(Estimator):
    """ Drops columns with nothing but NULLs"""

    def _fit(self, dataset):
        nullColumns = []
        for k in dataset.columns:
            if dataset.agg(countDistinct(dataset[k])).collect()[0][0] == 0:
                nullColumns.append(k)
        transformer = Dropper(nullColumns)
        return Model(transformer)
