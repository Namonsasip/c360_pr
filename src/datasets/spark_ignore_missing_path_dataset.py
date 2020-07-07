from kedro.contrib.io.pyspark import SparkDataSet
from kedro.io.core import DataSetError

from src.customer360.utilities.re_usable_functions import get_spark_empty_df


class SparkIgnoreMissingPathDataset(SparkDataSet):

    def load(self):
        try:
            return super().load()
        except DataSetError as e:
            if "Path does not exist" in str(e):
                return get_spark_empty_df()
            else:
                raise e
