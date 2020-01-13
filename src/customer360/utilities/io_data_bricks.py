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
# The QuantumBlack Visual Analytics Limited (“QuantumBlack”) name and logo
# (either separately or in combination, “QuantumBlack Trademarks”) are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTE THIS CODE IS FOR PRODUCTION AND IS INCOMPLETE FOR NOW

"""``AbstractDataSet`` implementation to access Spark data frames using
``pyspark``
"""

import pickle
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from kedro.io import AbstractDataSet
from datetime import datetime


import logging

log = logging.getLogger(__name__)


class DatabricksVersionedSparkDataSet(AbstractDataSet):

    """``SparkDataSet`` loads and saves Spark data frames.

    Example:
    ::

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import (StructField, StringType,
        >>>                                IntegerType, StructType)
        >>>
        >>> from kedro.contrib.io.pyspark import SparkDataSet
        >>>
        >>> schema = StructType([StructField("name", StringType(), True),
        >>>                      StructField("age", IntegerType(), True)])
        >>>
        >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data,
        >>> schema)
        >>>
        >>> data_set = SparkDataSet(filepath="test_data")
        >>> data_set.save(spark_df)
        >>> reloaded = data_set.load()
        >>>
        >>> reloaded.take(4)
    """

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._filepath,
            file_format=self._file_format,
            load_args=self._load_args,
            save_args=self._save_args,
        )

    def __init__(
            self,
            filepath: str,
            metadata_path: str,
            metadata_table_name: str,
            validation_path: str = "",
            lookup_table_name: str = "",
            refresh_mode: str = "full",
            file_format: str = "parquet",
            write_mode: str = "append",
            partition_by_col: str = "",
            read_from_partitions_flg: bool = False,
            load_args: Optional[Dict[str, Any]] = None,
            save_args: Optional[Dict[str, Any]] = None,
    ) -> None:

        """Creates a new instance of ``SparkDataSet``.

        Args:
            filepath: path to a Spark data frame.
            file_format: file format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/sql-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame
        """

        self._file_format = file_format
        self._filepath = filepath if filepath.endswith("/") else filepath + "/"
        self._metadata_table_name = metadata_table_name
        self._metadata_path = metadata_path
        self._validation_path = validation_path
        self._lookup_table_name = lookup_table_name
        self._refresh_mode = refresh_mode
        self._write_mode = write_mode
        self._partition_by_col = partition_by_col
        self._read_from_partitions_flg = read_from_partitions_flg
        self._load_args = load_args if load_args is not None else {}
        self._save_args = save_args if save_args is not None else {}

    @staticmethod
    def _get_spark():
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return spark

    def _get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(self._get_spark().sparkContext)
        except ImportError:
            import IPython

            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

    def _get_date(self, dt_to_convert):
        date_patterns = ["%Y_%m_%d", "%Y_%m_%d_%H_%M_%S", "%Y-%m-%d"]
        for pattern in date_patterns:
            try:
                return datetime.strptime(dt_to_convert, pattern)
            except Exception as e:
                pass
        log.error("Date is not in expected format: %s" % (dt_to_convert))

    def _get_last_snapshot(self):
        try:
            spark = self._get_spark()
            # read the last snapshot from metadata table

            self._last_snapshot = (
                spark.read.format("parquet")
                    .option("mergeschema", "true")
                    .load(self._metadata_path)
                    .where(
                    'table_name="{0}" and operation="{1}"'.format(
                        self._lookup_table_name, "write"
                    )
                )
                    .selectExpr("max(snapshot_as_on)")
                    .collect()[0][0]
            )

            if self._last_snapshot is None:
                self._last_snapshot = self._get_date("1980_01_01")

        except Exception as e:
            self._last_snapshot = self._get_date("1980_01_01")
            log.info(str(e))

    def _get_last_partitions(self):
        """

        :return: Returns Last time partitions
        """
        try:
            spark = self._get_spark()
            # read the last snapshot from metadata table

            df = (
                spark.read.format("parquet")
                    .option("mergeschema", "true")
                    .load(self._metadata_path)
                    .where(
                    'table_name="{0}" and operation="{1}" and snapshot_as_on > "{2}" '.format(
                        self._metadata_table_name, "write", self._last_snapshot
                    )
                )
                    .groupby("table_name")
                    .agg(F.collect_set("partitions").alias("lst_of_partitions"))
            )

            lst_of_partitions = df.select("lst_of_partitions").collect()[0][0]

            self._partitions_impacted = list(
                set([item for sublist in lst_of_partitions for item in sublist])
            )

        except Exception as e:
            self._partitions_impacted = []
            log.info(str(e))

    def _get_snapshots_to_read(self):
        files_to_read = []
        all_files = []

        dbutils = self._get_dbutils()

        path = self._filepath
        file_format = self._file_format

        for i, val in enumerate(dbutils.fs.ls(path)):

            relative_path = path + (val[1])
            full_path = dbutils.fs.ls(relative_path)[0][0]
            if (self._last_snapshot is None) or (
                    self._last_snapshot < self._get_date((val[1])[:-1])
            ):
                if "spark.excel" in file_format.lower():
                    files_to_read.append(full_path)
                else:
                    files_to_read.append(relative_path)

            if "spark.excel" in file_format.lower():
                all_files.append(full_path)
            else:
                all_files.append(relative_path)

        if len(all_files) == 1:
            all_files = [
                file + "/" if not file.endswith("/") else file
                for file in all_files
            ]

        return files_to_read, all_files

    def _union_dataframes(self, df1, df2):

        # Add missing columns to df1
        left_df = df1
        for column in set(df2.columns) - set(df1.columns):
            left_df = left_df.withColumn(column, F.lit(None))

        # Add missing columns to df2
        right_df = df2
        for column in set(df1.columns) - set(df2.columns):
            right_df = right_df.withColumn(column, F.lit(None))

        # Make sure columns are ordered the same
        return left_df.union(right_df.select(left_df.columns))

    def _read_incremental_snapshot(self):
        spark = self._get_spark()
        self._get_last_snapshot()
        # some code needs to be written here which will add conf for layer 0
        # Below two lines are recently added to read entire data if it is being read for the first time.
        if self._last_snapshot == datetime.strptime("1980-01-01 00:00:00", '%Y-%m-%d %H:%M:%S'):
            return spark.read.format(self._file_format).load(self._filepath)

        if not self._read_from_partitions_flg and self._refresh_mode != "full":
            snapshots_to_read, all_snapshots = self._get_snapshots_to_read()
        elif (
                self._refresh_mode == "incremental"
                and self._read_from_partitions_flg
                and self._partition_by_col == ""
        ):
            log.error(
                """Please specify the partition by column for incremental load "
                since partition flag is true"""
            )

        file_format = self._file_format

        if self._refresh_mode == "incremental":
            try:
                if self._read_from_partitions_flg:
                    self._get_last_partitions()
                    if not self._partitions_impacted:
                        log.info(
                            """Pipeline don't have any new partitions to read"""
                        )
                        return spark.createDataFrame([], StructType([]))
                    else:
                        partition_col_refact = self._partition_by_col.replace(
                            "stg", "prt"
                        )
                        self._full_path = (
                                self._filepath
                                + partition_col_refact
                                + "={"
                                + (",".join(self._partitions_impacted)).replace(
                            "'", ""
                        )
                                + "}/"
                        )
                        df = spark.read.format(self._file_format).load(
                            self._full_path
                        )
                else:
                    if len(snapshots_to_read) == 0:
                        log.info(
                            """you're all caught up in incremental try running in
                            full mode!"""
                        )
                        return spark.createDataFrame([], StructType([]))
                    else:
                        for i, val in enumerate(snapshots_to_read):
                            self._full_path = val

                            if i == 0:
                                df = spark.read.load(
                                    self._full_path,
                                    self._file_format,
                                    **self._load_args
                                )
                            else:
                                df1 = spark.read.load(
                                    self._full_path,
                                    self._file_format,
                                    **self._load_args
                                )
                                df = self._union_dataframes(df, df1)
                    self._last_snapshot = self._get_date(
                        self._full_path.split("/")[-2]
                    )
            except AnalysisException as e:
                log.exception("Exception raised", str(e))

        elif self._refresh_mode == "full":
            # self._full_path = all_snapshots[-1]
            self._full_path = self._filepath
            if "spark.excel" in file_format.lower():
                for i in range(len(all_snapshots)):
                    self._full_path = all_snapshots[i]
                    if i == 0:
                        df = spark.read.load(
                            self._full_path, self._file_format, **self._load_args
                        )
                    else:
                        df1 = spark.read.load(
                            self._full_path, self._file_format, **self._load_args
                        )
                        df = self._union_dataframes(df, df1)
            else:
                df = spark.read.load(
                    self._filepath + "*/*", self._file_format, **self._load_args
                )

            self._last_snapshot = datetime.now()

        elif self._refresh_mode == "latest":
            self._full_path = all_snapshots[-1]
            df = spark.read.load(
                self._full_path, self._file_format, **self._load_args
            )
            self._last_snapshot = self._get_date(self._full_path.split("/")[-2])
        else:
            raise ValueError(
                """refresh mode should be incremental,latest, or
                full instead of """
                + self._refresh_mode
            )

        if len(df.head(1)) == 0 and self._refresh_mode == "latest":
            raise ValueError(
                """File is empty for refresh mode latest so no new records are processed,
                please correct the file"""
            )
        elif len(df.head(1)) == 0:
            log.info("File is empty so no new records are processed")
            return df

        df = df.withColumn(
            "snapshot_as_on",
            F.when(
                F.length(
                    F.regexp_extract(
                        F.input_file_name(), "(\d+_\d+_\d+_\d+_\d+_\d+)", 0
                    )
                )
                == 19,
                F.regexp_extract(
                    F.input_file_name(), "(\d+_\d+_\d+_\d+_\d+_\d+)", 0
                ),
                )
                .when(
                F.length(
                    F.regexp_extract(F.input_file_name(), "(\d+_\d+_\d+)", 0)
                )
                == 10,
                F.concat(
                    F.regexp_extract(F.input_file_name(), "(\d+_\d+_\d+)", 0),
                    F.lit("_00_00_00"),
                ),
                )
                .otherwise(None),
                )

        df = df.withColumn(
            "snapshot_as_on",
            F.to_timestamp(F.col("snapshot_as_on"), "yyyy_MM_dd_HH_mm_ss"),
        )

        self._update_metadata("read")

        return df

    def _get_partitions(self, input):

        if str(input.schema[self._partition_by_col].dataType) in (
                ["TimestampType", "DateType"]
        ):

            partitions_impacted = [
                row[self._partition_by_col]
                for row in input.filter(F.col(self._partition_by_col).isNotNull())
                    .select(F.col(self._partition_by_col).cast("date").cast("string"))
                    .distinct()
                    .collect()
            ]

        else:
            partitions_impacted = [
                row[self._partition_by_col]
                for row in input.filter(F.col(self._partition_by_col).isNotNull())
                    .select(F.col(self._partition_by_col).cast("string"))
                    .distinct()
                    .collect()
            ]
        return partitions_impacted

    def _prepare_partition_data(self, data):
        try:

            log.info("prepare partitioning...")

            partition_col_refact = self._partition_by_col.replace("stg", "prt")

            null_rows = data.filter(
                F.col(self._partition_by_col).isNull()
            ).count()

            if null_rows > 0:
                log.info(
                    str(null_rows)
                    + " rows with the partition by column as null are ignored"
                )

            data = data.filter(F.col(self._partition_by_col).isNotNull())

            if str(data.schema[self._partition_by_col].dataType) in (
                    ["TimestampType", "DateType"]
            ):
                data = data.withColumn(
                    partition_col_refact,
                    F.col(self._partition_by_col).cast("date"),
                )
            else:
                data = data.withColumn(
                    partition_col_refact, F.col(self._partition_by_col)
                )

            log.info("done partitioning...")
            return data, partition_col_refact

        except Exception as e:
            log.exception("Exception raised while preparing partitions:", str(e))

    def _write_snapshot(self, data):

        log.info("Entering the save mode...")

        self._last_snapshot = datetime.now()

        if data.count() > 0:
            if len(self._partition_by_col) == 0:
                last_snapshot_str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                self._full_path = self._filepath + last_snapshot_str

                data.write.mode(self._write_mode).save(
                    self._full_path, self._file_format, **self._save_args
                )

                self._update_metadata("write")
                #self._update_validation(data.count())

            else:
                partitions_impacted = self._get_partitions(data)
                if not partitions_impacted:
                    log.info("""you're don't have any new partitions to write""")
                else:
                    data, partition_col_refact = self._prepare_partition_data(
                        data
                    )
                    self._full_path = self._filepath

                    # option("maxRecordsPerFile", 30000)
                    data.repartition(partition_col_refact).write.partitionBy(partition_col_refact).mode(self._write_mode).save(
                        self._full_path, self._file_format, **self._save_args
                    )

                    self._update_metadata("write", partitions_impacted)
                    #self._update_validation(data.count())

        else:
            log.info("********* No new records to write in this run! *********")

    def _update_metadata(self, mode, partitions_impacted=[]):
        try:
            df = self._get_spark().read.parquet(self._metadata_path)
        except Exception as e:
            df = self._get_spark().range(1)
            print("Creating metadata table : ", str(e))

        df = (
            df.withColumn("table_name", F.lit(self._metadata_table_name))
                .withColumn(
                "snapshot_as_on", F.to_timestamp(F.lit(self._last_snapshot))
            )
                .withColumn("filepath", F.lit(self._full_path))
                .withColumn("refresh_mode", F.lit(self._refresh_mode))
                .withColumn("operation", F.lit(mode))
                .withColumn(
                "partitions", F.array([F.lit(x) for x in partitions_impacted])
            )
                .withColumn("updated_on", F.current_timestamp())
                .drop("id")
        )
        df = df.distinct()

        try:
            df.write.format("parquet").mode("append").save(self._metadata_path)
        except Exception as e:
            raise e

    def _update_validation(self, count):
        try:
            df = self._get_spark().read.parquet(self._validation_path)
        except Exception as e:
            df = self._get_spark().range(1)
            print("Creating validation table : ", str(e))

        df = (
            df.withColumn("table_name", F.lit(self._metadata_table_name))
                .withColumn("table_count", F.lit(count))
                .withColumn("updated_on", F.current_timestamp())
                .drop("id")
        )

        df = df.distinct()

        try:
            df.write.format("parquet").mode("append").save(self._validation_path)
        except Exception as e:
            raise e

    def _load(self) -> DataFrame:
        return self._read_incremental_snapshot()

    def _save(self, data: DataFrame) -> None:
        return self._write_snapshot(data)

    def _exists(self) -> bool:
        try:
            self._get_spark().read.load(self._filepath, self._file_format)
        except AnalysisException as exception:
            if exception.desc.startswith("Path does not exist:"):
                return False
            raise
        return True

    def __getstate__(self):
        raise pickle.PicklingError("PySpark datasets can't be serialized")
