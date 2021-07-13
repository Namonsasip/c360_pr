import pickle
from copy import deepcopy
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath, WindowsPath

from warnings import warn

from hdfs import HdfsError, InsecureClient
from pyspark.sql import DataFrame
from customer360.utilities.spark_util import get_spark_session
from pyspark.sql.utils import AnalysisException
from s3fs import S3FileSystem

from kedro.contrib.io import DefaultArgumentsMixIn
from kedro.io import AbstractVersionedDataSet, Version

from pyspark.sql import functions as F
from typing import *
import os
from pyspark.sql.types import *

import subprocess, ast
import datetime
from dateutil.relativedelta import relativedelta
import logging

log = logging.getLogger(__name__)

current_date = datetime.datetime.now()
cr_date = str((current_date - datetime.timedelta(days=0)).strftime('%Y%m%d'))

running_environment = str(os.getenv("RUNNING_ENVIRONMENT", "on_cloud"))
p_increment = str(os.getenv("RUN_INCREMENT", "yes"))
p_partition = str(os.getenv("RUN_PARTITION", "no_input"))
p_features = str(os.getenv("RUN_FEATURES", "feature_l1"))
p_path_output = str(os.getenv("RUN_PATH_OUTPUT", "no_input"))
path_job = str(os.getenv("RUN_PATH_JOB", "no_input"))
log_file = str(os.getenv("RUN_LOG_FILE", "no_input"))
matches = [".parq",".c000"]



def _parse_glob_pattern(pattern: str) -> str:
    special = ("*", "?", "[")
    clean = []
    for part in pattern.split("/"):
        if any(char in part for char in special):
            break
        clean.append(part)
    return "/".join(clean)


def _split_filepath(filepath: str) -> Tuple[str, str]:
    split_ = filepath.split("://", 1)
    if len(split_) == 2:
        return split_[0] + "://", split_[1]
    return "", split_[0]


def _strip_dbfs_prefix(path: str) -> str:
    return path[len("/dbfs"):] if path.startswith("/dbfs") else path


class KedroHdfsInsecureClient(InsecureClient):
    """Subclasses ``hdfs.InsecureClient`` and implements ``hdfs_exists``
    and ``hdfs_glob`` methods required by ``SparkDataSet``"""

    def hdfs_exists(self, hdfs_path: str) -> bool:
        """Determines whether given ``hdfs_path`` exists in HDFS.

        Args:
            hdfs_path: Path to check.

        Returns:
            True if ``hdfs_path`` exists in HDFS, False otherwise.
        """
        return bool(self.status(hdfs_path, strict=False))

    def hdfs_glob(self, pattern: str) -> List[str]:
        """Perform a glob search in HDFS using the provided pattern.

        Args:
            pattern: Glob pattern to search for.

        Returns:
            List of HDFS paths that satisfy the glob pattern.
        """
        prefix = _parse_glob_pattern(pattern) or "/"
        matched = set()
        try:
            for dpath, _, fnames in self.walk(prefix):
                if fnmatch(dpath, pattern):
                    matched.add(dpath)
                matched |= set(
                    "{}/{}".format(dpath, fname)
                    for fname in fnames
                    if fnmatch("{}/{}".format(dpath, fname), pattern)
                )
        except HdfsError:  # pragma: no cover
            # HdfsError is raised by `self.walk()` if prefix does not exist in HDFS.
            # Ignore and return an empty list.
            pass
        return sorted(matched)


class SparkDataSet(DefaultArgumentsMixIn, AbstractVersionedDataSet):

    def _describe(self) -> Dict[str, Any]:
        return dict(
            filepath=self._fs_prefix + str(self._filepath),
            file_format=self._file_format,
            load_args=self._load_args,
            save_args=self._save_args,
            version=self._version,
        )

    def __init__(  # pylint: disable=too-many-arguments
            self,
            filepath: str,
            file_format: str = "parquet",
            load_args: Dict[str, Any] = None,
            save_args: Dict[str, Any] = None,
            version: Version = None,
            metadata_table_path: str = "",
            credentials: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SparkDataSet``.
        "/mnt/customer360-blob-data/users/saurabh/metadata_table/"
        Args:
            filepath: path to a Spark data frame. When using Databricks
                and working with data written to mount path points,
                specify ``filepath``s for (versioned) ``SparkDataSet``s
                starting with ``/dbfs/mnt``.
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
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials to access the S3 bucket, such as
                ``aws_access_key_id``, ``aws_secret_access_key``, if ``filepath``
                prefix is ``s3a://`` or ``s3n://``. Optional keyword arguments passed to
                ``hdfs.client.InsecureClient`` if ``filepath`` prefix is ``hdfs://``.
                Ignored otherwise.
        """
        credentials = deepcopy(credentials) or {}
        fs_prefix, filepath = _split_filepath(filepath)

        if fs_prefix in ("s3a://", "s3n://"):
            if fs_prefix == "s3n://":
                warn(
                    "`s3n` filesystem has now been deprecated by Spark, "
                    "please consider switching to `s3a`",
                    DeprecationWarning,
                )
            _s3 = S3FileSystem(client_kwargs=credentials)
            exists_function = _s3.exists
            glob_function = _s3.glob
            path = PurePosixPath(filepath)

        elif fs_prefix == "hdfs://" and version:
            warn(
                "HDFS filesystem support for versioned {} is in beta and uses "
                "`hdfs.client.InsecureClient`, please use with caution".format(
                    self.__class__.__name__
                )
            )

            # default namenode address
            credentials.setdefault("url", "http://localhost:9870")
            credentials.setdefault("user", "hadoop")

            _hdfs_client = KedroHdfsInsecureClient(**credentials)
            exists_function = _hdfs_client.hdfs_exists
            glob_function = _hdfs_client.hdfs_glob
            path = PurePosixPath(filepath)

        else:
            exists_function = glob_function = None  # type: ignore
            path = Path(filepath)  # type: ignore

        super().__init__(
            load_args=load_args,
            save_args=save_args,
            filepath=path,
            version=version,
            exists_function=exists_function,
            glob_function=glob_function,
        )
        self._file_format = file_format
        self._fs_prefix = fs_prefix
        self._filepath = filepath if filepath.endswith("/") else filepath + "/"
        # if (p_increment != "yes"):
        #     self._load_args = {}
        # else:
        self._load_args = load_args if load_args is not None else {}
        self._save_args = save_args if save_args is not None else {}

        self._increment_flag_load = load_args.get("increment_flag", None) if load_args is not None else None
        self._increment_flag_save = save_args.get("increment_flag", None) if save_args is not None else None
        self._read_layer = load_args.get("read_layer", None) if load_args is not None else None
        self._target_layer = load_args.get("target_layer", None) if load_args is not None else None
        self._lookback = load_args.get("lookback", None) if load_args is not None else None
        self._lookup_table_name = load_args.get("lookup_table_name", None) if load_args is not None else None

        self._read_layer_save = save_args.get("read_layer", None) if save_args is not None else None
        self._target_layer_save = save_args.get("target_layer", None) if save_args is not None else None

        self._metadata_table_path = metadata_table_path if (
                    metadata_table_path is not None and metadata_table_path.endswith(
                "/")) else metadata_table_path + "/"

        self._partitionBy = save_args.get("partitionBy", None) if save_args is not None else None
        self._mode = save_args.get("mode", None) if save_args is not None else None
        self._mergeSchema = load_args.get("mergeSchema", None) if load_args is not None else None
        self._baseSource = load_args.get("baseSource", None) if load_args is not None else None

    @staticmethod
    def _get_spark():
        spark = get_spark_session()
        return spark

    def _create_metadata_table(self, spark):

        metadata_table_path = self._metadata_table_path

        df = spark.range(1)

        metadata_table_df = df.withColumn("table_name", F.lit("metadata_table")) \
            .withColumn("table_path", F.lit(metadata_table_path)) \
            .withColumn("write_mode", F.lit("None")) \
            .withColumn("target_max_data_load_date", F.current_date()) \
            .withColumn("updated_on", F.current_date()) \
            .drop("id")

        metadata_table_df.write.partitionBy("table_name").format("parquet").mode("append").save(metadata_table_path)

    def _get_metadata_max_data_date(self, spark, table_name):

        metadata_table_path = self._metadata_table_path
        lookup_table_name = table_name

        logging.info("metadata_table_path: {}".format(metadata_table_path))
        try:
            if len(metadata_table_path) == 0 or metadata_table_path is None:
                raise ValueError("Metadata table path can't be empty in incremental mode")
            else:
                logging.info("checking whether metadata table exist or not at path : {}".format(metadata_table_path))
                metadata_table = spark.read.parquet(metadata_table_path)

                logging.info("metadata table exists at path: {}".format(metadata_table_path))

        except AnalysisException as e:
            logging.info("metadata table doesn't exist. Creating new metadata table")
            log.exception("Exception raised", str(e))

            self._create_metadata_table(spark)
            metadata_table = spark.read.parquet(metadata_table_path)

        metadata_table.createOrReplaceTempView("mdtl")

        target_max_data_load_date = spark.sql(
            """select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date
            from mdtl where table_name = '{0}'""".format(lookup_table_name))

        try:
            if len(target_max_data_load_date.head(1)) == 0 or target_max_data_load_date is None:
                raise ValueError("Max data date of lookup table is None, Please check")

        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        return target_max_data_load_date

    def _get_metadata_master_max_data_date(self, spark, table_name):

        metadata_table_path = self._metadata_table_path
        lookup_table_name = table_name

        logging.info("metadata_table_path: {}".format(metadata_table_path))
        try:
            if len(metadata_table_path) == 0 or metadata_table_path is None:
                raise ValueError("Metadata table path can't be empty in incremental mode")
            else:
                logging.info("checking whether metadata table exist or not at path : {}".format(metadata_table_path))
                metadata_table = spark.read.parquet(metadata_table_path)

                logging.info("metadata table exists at path: {}".format(metadata_table_path))

        except AnalysisException as e:
            logging.info("metadata table doesn't exist. Creating new metadata table")
            log.exception("Exception raised", str(e))

            self._create_metadata_table(spark)
            metadata_table = spark.read.parquet(metadata_table_path)

        metadata_table.createOrReplaceTempView("mdtl")

        target_max_data_load_date = spark.sql(
            """select cast( to_date(nvl(max(target_max_data_load_date),'1970-01-01'),'yyyy-MM-dd') as String) as target_max_data_load_date
            from mdtl where table_name = '{0}'""".format(lookup_table_name))

        try:
            if len(target_max_data_load_date.head(1)) == 0 or target_max_data_load_date is None:
                raise ValueError("Max data date of lookup table is None, Please check")

        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        return target_max_data_load_date

    def _get_incremental_data_new(self):

        try:
            spark = self._get_spark()
            logging.info("Entered incremental load mode new (test)")
            filepath = self._filepath
            read_layer = self._read_layer
            target_layer = self._target_layer
            lookback = self._lookback
            lookup_table_name = self._lookup_table_name
            mergeSchema = self._mergeSchema
            base_source = self._baseSource

            logging.info("filepath: {}".format(filepath))
            logging.info("read_layer: {}".format(read_layer))
            logging.info("target_layer: {}".format(target_layer))
            logging.info("lookback: {}".format(lookback))
            logging.info("mergeSchema: {}".format(mergeSchema))
            logging.info("lookup_table_name: {}".format(lookup_table_name))
            #        logging.info("Fetching source data")

            if lookup_table_name is None or lookup_table_name == "":
                raise ValueError("lookup table name can't be empty")
            else:
                logging.info("Fetching max data date entry of lookup table from metadata table")
                target_max_data_load_date = self._get_metadata_max_data_date(spark, lookup_table_name)

            tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(lambda x: x).collect()
            if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == ['None']:
                raise ValueError(
                    "Please check the return date from _get_metadata_max_data_date function. It can't be empty")
            else:
                tgt_filter_date = ''.join(tgt_filter_date_temp)
                logging.info("Max data date entry of lookup table in metadata table is: {}".format(tgt_filter_date))

            logging.info("Checking the read and write layer combination")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

            if ("/" == load_path[-1:]):
                load_path = load_path
            else:
                load_path = load_path + "/"

            if read_layer is None or read_layer == "" or target_layer is None or target_layer == "":
                raise ValueError(
                    "Please check the read_layer/target_layer can't be None or empty for incremental load")

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l1_daily':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(to_date(cast('{0}' as String),'yyyy-MM-dd') , {1} )".format(
                    tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month',to_date(cast('{0}' as String)))),-{1})".format(
                    tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_monthly_1_month_look_back" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "1"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month',to_date(cast('{0}' as String)))),-{1})".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month',to_date(cast('{0}' as String)))),-{1})".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_weekly" and target_layer.lower() == 'l2_weekly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))), 7*({1}))".format(
                           tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l2_weekly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))),- 7*(1)), 1), 7*({1}))".format(
                           tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_daily':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "90"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(to_date(cast('{0}' as String)) , {1} )".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l1_daily':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(to_date(cast('{0}' as String)) , {1} )".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l2_weekly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))),- 7*(1)), 1), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l3_monthly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), 1),1),-{1})".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_monthly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), 1),1),-{1})".format(
                             tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), 1),1),-{1})".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), 1),1),-{1})".format(
                             tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_weekly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))),- 7*(1)), 1), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l2_weekly_read_custom_lookback" and target_layer.lower() == 'l4_weekly_write_custom_lookback':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "12"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l4_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select  date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l2_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select date_sub(date(date_trunc('week', to_date(cast('{0}' as String)))), 7*({1}))".format(
                            tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l3_monthly" and target_layer.lower() == 'l4_monthly':
                filter_col = "start_of_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "3"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), -{1})".format(
                             tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l3_monthly_customer_profile" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), -{1})".format(
                             tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l3_monthly" and target_layer.lower() == 'l3_monthly':
                filter_col = "start_of_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), -{1})".format(
                             tgt_filter_date, lookback_fltr)

            elif read_layer.lower() == "l3_monthly_customer_profile" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                sql_min_partition = "select add_months(date(date_trunc('month', to_date(cast('{0}' as String)))), -{1})".format(
                             tgt_filter_date, lookback_fltr)

            else:
                raise ValueError(
                    "read_layer and target_layer combination is not valid. Please specify a valid combination.")

            min_filter_date_temp = spark.sql(sql_min_partition)
            min_filter_date_temp = min_filter_date_temp.rdd.flatMap(lambda x: x).collect()
            min_filter_date = min_filter_date_temp[0]  ## date
            min_filter_date = datetime.datetime.combine(min_filter_date, datetime.datetime.min.time())  ## datetime
            max_tgt_filter_date = datetime.datetime.strptime(tgt_filter_date, '%Y-%m-%d')

            if (running_environment == "on_cloud"):
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        if (base_source != None and base_source.lower() == "dl2"):
                            try:
                                list_temp = subprocess.check_output(
                                    "ls -dl /dbfs" + load_path + "*/*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep /ld_",
                                    shell=True).splitlines()
                                if (list_temp == []):
                                    raise ValueError("Ok")
                            except:
                                list_temp = subprocess.check_output(
                                    "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep /ld_",
                                    shell=True).splitlines()
                        else:
                            list_temp = subprocess.check_output(
                                "ls -dl /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                except:
                    list_temp = ""
                list_path = []
                if (list_temp == ""):
                    list_path = "no_partition"
                else:
                    for read_path in list_temp:
                        list_path.append(str(read_path)[2:-1].split('dbfs')[1])
                r = "not"
                p_list_load_path = []
                p_new_path = []
                if (list_path != "no_partition"):
                    r = "run"
                    for line in list_path:
                        try:
                            if (base_source != None and base_source.lower() == "dl2"):
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-4].split('=')[1].replace('-', '') + line.split('/')[-3].split('=')[
                                        1].replace('-', '') + line.split('/')[-2].split('=')[1].replace('-', ''),
                                    '%Y%m%d')
                            else:
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-2].split('=')[1].replace('-', ''),
                                    '%Y%m%d')
                        except:
                            if (base_source != None and base_source.lower() == "dl2"):
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-3].split('=')[1].replace('-', '') + line.split('/')[-2].split('=')[
                                        1].replace('-', ''),
                                    '%Y%m%d')
                            else:
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-2].split('=')[1].replace('-', ''),
                                    '%Y%m')

                        if (max_tgt_filter_date < date_data):  ### check new partition
                            p_new_path.append(line)
                        if (min_filter_date < date_data):  ### list path load
                            p_list_load_path.append(line)


            else:
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        if (base_source != None and base_source.lower() == "dl2"):
                            try:
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/*/*/ |awk -F' ' '{print $NF}' |grep /ld_ |grep =20",
                                    shell=True).splitlines()
                                if any(x in str(list_temp[-1]) for x in matches):
                                    list_temp = subprocess.check_output(
                                        "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                        shell=True).splitlines()
                            except:
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep /ld_ |grep =20",
                                    shell=True).splitlines()
                                if any(x in str(list_temp[-1]) for x in matches):
                                    list_temp = subprocess.check_output(
                                        "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                        shell=True).splitlines()
                        else:
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()

                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()
                except:
                    list_temp = ""
                list_path = []
                if (list_temp == ""):
                    list_path = "no_partition"
                else:
                    for read_path in list_temp:
                        list_path.append(str(read_path)[2:-1])
                r = "not"
                p_list_load_path = []
                p_new_path = []
                if (list_path != "no_partition"):
                    r = "run"
                    for line in list_path:
                        try:
                            if (base_source != None and base_source.lower() == "dl2"):
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-3].split('=')[1].replace('-', '') + line.split('/')[-2].split('=')[
                                        1].replace('-', '') + line.split('/')[-1].split('=')[1].replace('-', ''),
                                    '%Y%m%d')
                            else:
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-1].split('=')[1].replace('-', ''),
                                    '%Y%m%d')
                        except:
                            if (base_source != None and base_source.lower() == "dl2"):
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-2].split('=')[1].replace('-', '') + line.split('/')[-1].split('=')[
                                        1].replace('-', ''),
                                    '%Y%m%d')
                            else:
                                date_data = datetime.datetime.strptime(
                                    line.split('/')[-1].split('=')[1].replace('-', ''),
                                    '%Y%m')
                        if (max_tgt_filter_date < date_data):  ### check new partition
                            p_new_path.append(line)
                        if (min_filter_date < date_data):  ### list path load
                            p_list_load_path.append(line)

            base_filepath = load_path
            if (len(p_new_path) == 0):
                p_list_load_path = []

            if (p_list_load_path == [] and r == "run"):
                logging.info("basePath: {}".format(base_filepath))
                logging.info("load_path: {}".format(list_path[-1]))
                logging.info("file_format: {}".format(self._file_format))
                logging.info("Source Data : No Update")
                src_data = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                    "basePath", base_filepath).load(list_path[-1]).limit(0)

            else:
                if (running_environment == "on_cloud"):
                    try:
                        if (len(line.split('/')[-2].split('=')[1].replace('-', '')) == 6):  ### partition_date YYYYMMDD
                            date_end = datetime.datetime.strptime(line.split('/')[-2].split('=')[1].replace('-', ''),
                                                                  '%Y%m').strftime('%Y-%m-%d')
                        else:
                            date_end = datetime.datetime.strptime(line.split('/')[-2].split('=')[1].replace('-', ''),
                                                                  '%Y%m%d').strftime('%Y-%m-%d')
                    except:
                        try:
                            date_end = datetime.datetime.strptime(
                                line.split('/')[-4].split('=')[1].replace('-', '') + line.split('/')[-3].split('=')[
                                    1].replace('-', '') + line.split('/')[-2].split('=')[1].replace('-', ''),
                                '%Y%m%d').strftime('%Y-%m-%d')
                        except:
                            date_end = datetime.datetime.strptime(
                                line.split('/')[-3].split('=')[1].replace('-', '') + line.split('/')[-2].split('=')[
                                    1].replace('-', ''),
                                '%Y%m').strftime('%Y-%m-%d')
                else:
                    try:
                        if (len(line.split('/')[-1].split('=')[1].replace('-', '')) == 6):  ### partition_date YYYYMMDD
                            date_end = datetime.datetime.strptime(line.split('/')[-1].split('=')[1].replace('-', ''),
                                                                  '%Y%m').strftime('%Y-%m-%d')
                        else:
                            date_end = datetime.datetime.strptime(line.split('/')[-1].split('=')[1].replace('-', ''),
                                                                  '%Y%m%d').strftime('%Y-%m-%d')
                    except:
                        try:
                            date_end = datetime.datetime.strptime(
                                line.split('/')[-3].split('=')[1].replace('-', '') + line.split('/')[-2].split('=')[
                                    1].replace('-', '') + line.split('/')[-1].split('=')[1].replace('-', ''),
                                '%Y%m%d').strftime('%Y-%m-%d')
                        except:
                            date_end = datetime.datetime.strptime(
                                line.split('/')[-2].split('=')[1].replace('-', '') + line.split('/')[-1].split('=')[
                                    1].replace('-', ''),
                                '%Y%m').strftime('%Y-%m-%d')

                logging.info("basePath: {}".format(base_filepath))
                logging.info("load_path: {}".format(load_path))
                logging.info("read_start: {}".format(tgt_filter_date))
                logging.info("read_end: {}".format(date_end))
                logging.info("file_format: {}".format(self._file_format))
                logging.info("Fetching source data")
                if ("no_partition" == list_path):
                    src_data = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "basePath", base_filepath).load(load_path, self._file_format, **self._load_args)
                    if (base_source != None and base_source.lower() == "dl2"):
                        try:
                            src_data = src_data.withColumn("partition_date", F.concat(src_data.ld_year, F.when(
                                F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                                F.col("ld_month")), F.when(F.length(F.col("ld_day")) == 1,
                                                       F.concat(F.lit("0"), F.col("ld_day"))).otherwise(
                                F.col("ld_day"))))
                        except:
                            src_data = src_data.withColumn("partition_month", F.concat(src_data.ld_year, F.when(
                                F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                                F.col("ld_month")), F.lit("01")))
                else:
                    src_data = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "basePath", base_filepath).load(p_list_load_path, self._file_format, **self._load_args)
                    if (base_source != None and base_source.lower() == "dl2"):
                        try:
                            src_data = src_data.withColumn("partition_date", F.concat(src_data.ld_year, F.when(
                                F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                                F.col("ld_month")), F.when(F.length(F.col("ld_day")) == 1,
                                                           F.concat(F.lit("0"), F.col("ld_day"))).otherwise(
                                F.col("ld_day"))))
                        except:
                            src_data = src_data.withColumn("partition_month", F.concat(src_data.ld_year, F.when(
                                F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                                F.col("ld_month")), F.lit("01")))


            return src_data
        except AnalysisException as e:
            log.exception("Exception raised", str(e))

    def _get_incremental_data(self):
        try:

            spark = self._get_spark()
            logging.info("Entered incremental load mode")
            filepath = self._filepath
            read_layer = self._read_layer
            target_layer = self._target_layer
            lookback = self._lookback
            lookup_table_name = self._lookup_table_name
            mergeSchema = self._mergeSchema

            logging.info("filepath: {}".format(filepath))
            logging.info("read_layer: {}".format(read_layer))
            logging.info("target_layer: {}".format(target_layer))
            logging.info("lookback: {}".format(lookback))
            logging.info("mergeSchema: {}".format(mergeSchema))
            logging.info("lookup_table_name: {}".format(lookup_table_name))
            logging.info("Fetching source data")

            src_data = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").load(
                filepath, self._file_format, **self._load_args)

            logging.info("Source data is fetched")
            logging.info("Checking whether source data is empty or not")

            try:
                if len(src_data.head(1)) == 0:
                    # if 1==2:
                    raise ValueError("Source dataset is empty")
                elif lookup_table_name is None or lookup_table_name == "":
                    raise ValueError("lookup table name can't be empty")
                else:
                    logging.info("Fetching max data date entry of lookup table from metadata table")
                    target_max_data_load_date = self._get_metadata_max_data_date(spark, lookup_table_name)



            # except error for year > 9999
            except Exception as e:
                if (str(e) == 'year 0 is out of range'):
                    logging.info("Fetching max data date entry of lookup table from metadata table")
                    target_max_data_load_date = self._get_metadata_max_data_date(spark, lookup_table_name)
                else:
                    raise e

            tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(lambda x: x).collect()

            if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == ['None']:
                raise ValueError(
                    "Please check the return date from _get_metadata_max_data_date function. It can't be empty")
            else:
                tgt_filter_date = ''.join(tgt_filter_date_temp)
                logging.info("Max data date entry of lookup table in metadata table is: {}".format(tgt_filter_date))

            src_data.createOrReplaceTempView("src_data")

            logging.info("Checking the read and write layer combination")
            if read_layer is None or read_layer == "" or target_layer is None or target_layer == "":
                raise ValueError(
                    "Please check the read_layer/target_layer can't be None or empty for incremental load")

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l1_daily':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where to_date(regexp_replace(cast({0} as String),'-',''),'yyyyMMdd') > date_sub(to_date(cast('{1}' as String)) , {2} )".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMM') > add_months(date(date_trunc('month',to_date(cast('{1}' as String)))),-{2})".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_monthly_1_month_look_back" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "1"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMM') > date(date_trunc('month', to_date(cast('{1}' as String)))) ".format(
                        filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where to_date(cast({0} as String),'yyyyMM') > add_months(date(date_trunc('month',to_date(cast('{1}' as String)))),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMM') > date(date_trunc('month', to_date(cast('{1}' as String)))) ".format(
                        filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where to_date(cast({0} as String),'yyyyMM') > add_months(date(date_trunc('month',to_date(cast('{1}' as String)))),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_weekly" and target_layer.lower() == 'l2_weekly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l2_weekly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))),- 7*(1)), 1), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_daily':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "90"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > to_date(cast('{1}' as String)) ".format(filter_col,
                                                                                                tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                # if 1==2:
                #     print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > date_sub(to_date(cast('{1}' as String)) , {2} )".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l1_daily':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(to_date(cast('{1}' as String)) , {2} )".format(
                        filter_col,
                        tgt_filter_date,
                        lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l2_weekly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))),- 7*(1)), 1), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l3_monthly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1) ".format(
                        filter_col,
                        tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                # if 1==2:
                #     print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_monthly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1) ".format(
                        filter_col,
                        tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1) ".format(
                        filter_col,
                        tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_daily" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1) ".format(
                        filter_col,
                        tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1),-{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_weekly':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))),- 7*(1)), 1) ".format(
                        filter_col,
                        tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > date_sub(date_sub(date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))),- 7*(1)), 1), 7*({2}))".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l2_weekly_read_custom_lookback" and target_layer.lower() == 'l4_weekly_write_custom_lookback':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "12"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date(date_trunc('week', to_date(cast('{1}' as String)))) ".format(
                        filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                # if 1==2:
                #     print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l4_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date(date_trunc('week', to_date(cast('{1}' as String)))) ".format(
                        filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                            filter_col, tgt_filter_date, lookback_fltr))


            elif read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l2_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l3_monthly" and target_layer.lower() == 'l4_monthly':
                filter_col = "start_of_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "3"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date(date_trunc('month', to_date(cast('{1}' as String)))) ".format(
                        filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                # if 1==2:
                #     print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                        "select * from src_data where {0} > add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), -{2})".format(
                            filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l3_monthly_customer_profile" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), -{2})".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l3_monthly" and target_layer.lower() == 'l3_monthly':
                filter_col = "start_of_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), -{2})".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l3_monthly_customer_profile" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), -{2})".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            else:
                raise ValueError(
                    "read_layer and target_layer combination is not valid. Please specify a valid combination.")

            logging.info("Incremental data created")
            return src_incremental_data

        except AnalysisException as e:
            log.exception("Exception raised", str(e))

    def _update_metadata_table(self, spark, metadata_table_path, target_table_name, filepath, write_mode, file_format,
                               partitionBy, read_layer, target_layer, mergeSchema):

        if mergeSchema is not None and mergeSchema.lower() == "true":
            current_target_data = spark.read.format(file_format).option("mergeSchema", 'true').load(filepath)
        else:
            current_target_data = spark.read.format(file_format).load(filepath)

        current_target_data.createOrReplaceTempView("curr_target")

        current_target_max_data_load_date = spark.sql(
            "select cast( nvl(max({0}),'1970-01-01') as String) from curr_target".format(partitionBy))

        metadata_table_update_max_date_temp = current_target_max_data_load_date.rdd.flatMap(lambda x: x).collect()

        if metadata_table_update_max_date_temp is None or metadata_table_update_max_date_temp == [
            None] or metadata_table_update_max_date_temp == ['None'] or metadata_table_update_max_date_temp == '':
            raise ValueError("Please check, the current_target_max_data_load_date can't be empty")
        else:
            metadata_table_update_max_date = ''.join(metadata_table_update_max_date_temp)

        logging.info("Updating metadata table for {} dataset with date: {} ".format(target_table_name,
                                                                                    metadata_table_update_max_date))

        metadata_table_update_df = spark.range(1)

        metadata_table_update_df = (
            metadata_table_update_df.withColumn("table_name", F.lit(target_table_name))
                .withColumn("table_path", F.lit(filepath))
                .withColumn("write_mode", F.lit(write_mode))
                .withColumn("target_max_data_load_date", F.to_date(F.lit(metadata_table_update_max_date), "yyyy-MM-dd"))
                .withColumn("updated_on", F.current_date())
                .withColumn("read_layer", F.lit(read_layer))
                .withColumn("target_layer", F.lit(target_layer))
                .drop("id")
        )

        try:
            metadata_table_update_df.write.partitionBy("table_name").format("parquet").mode("append").save(
                metadata_table_path)
        except AnalysisException as e:
            log.exception("Exception raised", str(e))

        logging.info("Metadata table updated for {} dataset".format(target_table_name))

    def _write_incremental_data(self, data):

        logging.info("Entered incremental save mode")
        spark = self._get_spark()
        filewritepath = self._filepath
        partitionBy = self._partitionBy
        mode = self._mode
        file_format = self._file_format
        metadata_table_path = self._metadata_table_path
        read_layer = self._read_layer_save
        target_layer = self._target_layer_save
        target_table_name = filewritepath.split('/')[-2]
        dataframe_to_write = data
        mergeSchema = self._mergeSchema

        logging.info("filewritepath: {}".format(filewritepath))
        logging.info("partitionBy: {}".format(partitionBy))
        logging.info("mode: {}".format(mode))
        logging.info("file_format: {}".format(file_format))
        logging.info("metadata_table_path: {}".format(metadata_table_path))
        logging.info("read_layer: {}".format(read_layer))
        logging.info("target_layer: {}".format(target_layer))
        logging.info("target_table_name: {}".format(target_table_name))
        logging.info("mergeSchema: {}".format(mergeSchema))

        logging.info("Checking whether the dataset to write is empty or not")
        if len(dataframe_to_write.head(1)) == 0:
            # if 1==2:
            logging.info("No new partitions to write from source")
        elif partitionBy is None or partitionBy == "" or partitionBy == '' or mode is None or mode == "" or mode == '':
            raise ValueError(
                "Please check, partitionBy and Mode value can't be None or Empty for incremental load")
        elif read_layer is None or read_layer == "" or target_layer is None or target_layer == "":
            raise ValueError(
                "Please check, read_layer and target_layer value can't be None or Empty for incremental load")
        else:
            if (read_layer.lower() == "l1_daily" and target_layer.lower() == "l4_daily") or (
                    read_layer.lower() == "l2_weekly" and target_layer.lower() == "l4_weekly") or (
                    read_layer.lower() == "l3_monthly" and target_layer.lower() == "l4_monthly") or (
                    read_layer.lower() == "l0_monthly_1_month_look_back" and target_layer.lower() == "l3_monthly") or (
                    read_layer.lower() == "l0_daily" and target_layer.lower() == "l3_monthly") or (
                    read_layer.lower() == "l1_daily" and target_layer.lower() == "l3_monthly") or (
                    read_layer.lower() == "l3_monthly" and target_layer.lower() == "l3_monthly"
            ):

                # #Remove after first run happens
                # logging.info("Writing dataframe with lookback scenario")
                # dataframe_to_write.write.partitionBy(partitionBy).mode(mode).format(
                #     file_format).save(filewritepath)
                # logging.info("Updating metadata table for lookback dataset scenario")
                # self._update_metadata_table(spark, metadata_table_path, target_table_name, filewritepath,
                #                             mode, file_format, partitionBy, read_layer, target_layer, mergeSchema)
                #
                # #Remove after first run happens

                logging.info("Selecting only new data partition to write for lookback scenario's")
                target_max_data_load_date = self._get_metadata_max_data_date(spark, target_table_name)
                tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(lambda x: x).collect()

                if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == [
                    'None'] or tgt_filter_date_temp == '':
                    raise ValueError(
                        "Please check the return date from _get_metadata_max_data_date function. It can't be empty")
                else:
                    tgt_filter_date = ''.join(tgt_filter_date_temp)

                logging.info("Max data date entry of lookup table in metadata table is: {}".format(tgt_filter_date))
                dataframe_to_write.createOrReplaceTempView("df_to_write")
                filter_col = partitionBy

                df_with_lookback_to_write = spark.sql(
                    "select * from df_to_write where {0} > to_date(cast('{1}' as String)) ".format(filter_col,
                                                                                                   tgt_filter_date))

                logging.info("Writing dataframe with lookback scenario")
                df_with_lookback_to_write.write.partitionBy(partitionBy).mode(mode).format(
                    file_format).save(filewritepath)
                logging.info("Updating metadata table for lookback dataset scenario")
                self._update_metadata_table(spark, metadata_table_path, target_table_name, filewritepath,
                                            mode, file_format, partitionBy, read_layer, target_layer, mergeSchema)

            else:
                logging.info("Writing dataframe without lookback scenario")
                dataframe_to_write.write.partitionBy(partitionBy).mode(mode).format(file_format).save(
                    filewritepath)

                logging.info("Updating metadata table")

                self._update_metadata_table(spark, metadata_table_path, target_table_name, filewritepath, mode,
                                            file_format, partitionBy, read_layer, target_layer, mergeSchema)

    def _load(self) -> DataFrame:
        logging.info("Entering load function")
        logging.info("increment_flag: {}".format(self._increment_flag_load))
        # if self._increment_flag_load is not None and self._increment_flag_load.lower() == "yes" and running_environment == 'on_cloud' and p_increment.lower() == "yes":
        #     logging.info("Entering incremental load mode because incremental_flag is 'yes'")
        #     return self._get_incremental_data()
        #
        # elif self._increment_flag_load is not None and self._increment_flag_load.lower() == "yes" and running_environment != 'on_cloud' and p_increment.lower() == "yes":
        #     logging.info("Entering incremental load mode because incremental_flag is 'yes'")
        #     return self._get_incremental_data_new()

        if self._increment_flag_load is not None and self._increment_flag_load.lower() == "yes" and p_increment.lower() == "yes":
            logging.info("Entering incremental load mode because incremental_flag is 'yes'")
            return self._get_incremental_data_new()

        elif (self._increment_flag_load is not None and self._increment_flag_load.lower() == "master"):
            logging.info("Skipping incremental load mode because incremental_flag is 'master'")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
            p_increment_flag_load = self._increment_flag_load
            logging.info("increment_flag: {}".format(p_increment_flag_load))
            if (running_environment == "on_cloud"):
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        list_temp = subprocess.check_output(
                            "ls -dl /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20 |sort -u|tail -1",
                            shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20 |sort -u|tail -1",
                            shell=True).splitlines()
                except:
                    list_temp = ""
                list_path = []
                if (list_temp == ""):
                    list_path = "no_partition"
                else:
                    for read_path in list_temp:
                        list_path = (str(read_path)[2:-1].split('dbfs')[1])

                base_filepath = load_path
                logging.info("basePath: {}".format(base_filepath))
                logging.info("load_path: {}".format(list_path))
                logging.info("file_format: {}".format(self._file_format))
                logging.info("Fetching source data")
                if ("no_partition" == list_path):
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "inferSchema", "true").load(load_path, self._file_format)
                else:
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "inferSchema", "true").option(
                        "basePath", base_filepath).load(list_path, self._file_format)
            else:
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        list_temp = subprocess.check_output(
                            "hadoop fs -ls -d " + load_path + "*/ |awk -F' ' '{print $NF}' |grep =20 |sort -u|tail -1",
                            shell=True).splitlines()
                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep =20 |sort -u|tail -1",
                            shell=True).splitlines()
                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()

                except:
                    list_temp = ""
                if (list_temp == ""):
                    list_path = ("no_partition")
                else:
                    for read_path in list_temp:
                        list_path = (str(read_path)[2:-1])

                base_filepath = load_path
                logging.info("basePath: {}".format(base_filepath))
                logging.info("load_path: {}".format(list_path))
                logging.info("file_format: {}".format(self._file_format))
                logging.info("Fetching source data")
                if ("no_partition" == list_path):
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "inferSchema", "true").load(load_path, self._file_format)
                else:
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "inferSchema", "true").option(
                        "basePath", base_filepath).load(list_path, self._file_format)
            return df


        elif (self._increment_flag_load is not None and self._increment_flag_load.lower() == "master_yes"):
            logging.info("Skipping incremental load mode because incremental_flag is 'master_yes'")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

            spark = self._get_spark()
            filepath = load_path
            read_layer = self._read_layer
            target_layer = self._target_layer
            lookup_table_name = self._lookup_table_name
            p_increment_flag_load = self._increment_flag_load
            logging.info("increment_flag: {}".format(p_increment_flag_load))
            logging.info("filepath: {}".format(filepath))
            logging.info("read_layer: {}".format(read_layer))
            logging.info("target_layer: {}".format(target_layer))
            logging.info("lookup_table_name: {}".format(lookup_table_name))
            logging.info("Fetching source data")

            try:
                if lookup_table_name is None or lookup_table_name == "":
                    raise ValueError("lookup table name can't be empty")
                else:
                    logging.info("Fetching max data date entry of lookup table from metadata table")
                    target_max_data_load_date = self._get_metadata_master_max_data_date(spark, lookup_table_name)

            # except error for year > 9999
            except Exception as e:
                if (str(e) == 'year 0 is out of range'):
                    logging.info("Fetching max data date entry of lookup table from metadata table")
                    target_max_data_load_date = self._get_metadata_master_max_data_date(spark, lookup_table_name)
                else:
                    raise e

            tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(lambda x: x).collect()
            logging.info("source data max date : {0}".format(tgt_filter_date_temp[0]))

            if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == [
                'None'] or tgt_filter_date_temp == '':
                raise ValueError(
                    "Please check the return date from _get_metadata_max_data_date function. It can't be empty")
            else:
                tgt_filter_date = tgt_filter_date_temp[0]
            if tgt_filter_date == "" or tgt_filter_date == None:

                tgt_filter_date = "1970-01-01"

            if (running_environment == "on_cloud"):
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        list_temp = subprocess.check_output(
                            "ls -dl /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                except:
                    list_temp = ""
                list_path = []
                if (list_temp == ""):
                    list_path = "no_partition"
                else:
                    for read_path in list_temp:
                        list_path.append(str(read_path)[2:-1].split('dbfs')[1])
                p_old_date = datetime.datetime.strptime(tgt_filter_date, '%Y-%m-%d')
                r = "not"
                p_load_path = []
                if (list_path != "no_partition"):
                    r = "run"
                    for line in list_path:
                        date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1].replace('-', ''),
                                                                   '%Y%m%d')
                        if (p_old_date <= date_data):
                            p_load_path.append(line)

                base_filepath = load_path
                if (p_load_path == [] and r == "run"):
                    os.environ[lookup_table_name] = tgt_filter_date
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(list_path[-1]))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")
                    df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "basePath", base_filepath).load(list_path[-1])
                else:
                    date_end = datetime.datetime.strptime(line.split('/')[-2].split('=')[1].replace('-', ''),
                                                          '%Y%m%d').strftime('%Y-%m-%d')
                    os.environ[lookup_table_name] = date_end
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path))
                    logging.info("read_start: {}".format(tgt_filter_date))
                    logging.info("read_end: {}".format(date_end))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")

                    if ("no_partition" == list_path):
                        df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "basePath", base_filepath).load(load_path)
                    else:
                        df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "basePath", base_filepath).load(p_load_path)

            else:
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                try:
                    try:
                        list_temp = subprocess.check_output(
                            "hadoop fs -ls -d " + load_path + "*/ |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()
                    except:
                        list_temp = subprocess.check_output(
                            "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep =20",
                            shell=True).splitlines()
                        if any(x in str(list_temp[-1]) for x in matches):
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                shell=True).splitlines()
                except:
                    list_temp = ""
                list_path = []
                if (list_temp == ""):
                    list_path = "no_partition"
                else:
                    for read_path in list_temp:
                        list_path.append(str(read_path)[2:-1])
                p_old_date = datetime.datetime.strptime(tgt_filter_date, '%Y-%m-%d')
                r = "not"
                p_load_path = []
                if (list_path != "no_partition"):
                    r = "run"
                    for line in list_path:
                        date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1].replace('-', ''),
                                                               '%Y%m%d')
                        if (p_old_date < date_data):
                            p_load_path.append(line)
                base_filepath = load_path
                if (p_load_path == [] and r == "run"):
                    os.environ[lookup_table_name] = tgt_filter_date
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(list_path[-1]))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")
                    df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                        "basePath", base_filepath).load(list_path[-1])

                else:
                    date_end = datetime.datetime.strptime(line.split('/')[-1].split('=')[1].replace('-', ''),
                                                          '%Y%m%d').strftime('%Y-%m-%d')
                    os.environ[lookup_table_name] = date_end
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path))
                    logging.info("read_start: {}".format(tgt_filter_date))
                    logging.info("read_end: {}".format(date_end))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")

                    if ("no_partition" == list_path):
                        df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "basePath", base_filepath).load(load_path)
                    else:
                        df = spark.read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "basePath", base_filepath).load(p_load_path)
            return df

        elif (p_increment.lower() == "no"):
            logging.info("Skipping incremental load mode because incremental_flag is 'no'")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
            p_increment_flag_load = self._increment_flag_load
            logging.info("p_partition: {}".format(p_partition))
            logging.info("p_features: {}".format(p_features))
            base_source = self._baseSource
            if base_source is None:
                base_source = "default"
            logging.info("baseSource: {}".format(base_source))
            p_no = "run"
            if (running_environment == "on_cloud"):
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                if ("_features/" in load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    try:
                        try:
                            list_temp = subprocess.check_output(
                                "ls -dl /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                        except:
                            list_temp = subprocess.check_output(
                                "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                    except:
                        list_temp = ""
                    list_path = []
                    if (list_temp == ""):
                        list_path.append("no_partition")
                    else:
                        for read_path in list_temp:
                            list_path.append(str(read_path)[2:-1].split('dbfs')[1])
                    if ("/event_partition_date=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "event_partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/start_of_week=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "start_of_week="
                        if (p_features == "feature_l2" or p_features == "feature_l1"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=12)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)


                    elif ("/start_of_month=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "start_of_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-01")
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(months=3)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_month=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "partition_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(months=3)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-2].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1] + "01",
                                                                       '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0] ):
                        base_filepath = str(load_path)
                        p_partition_type = "partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-2].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("no_partition" == list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                    else:
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                elif (
                        "/mnt/customer360-blob-output/C360/UTILITIES/metadata_table/" == load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    base_filepath = str(load_path)
                    p_month1 = ""
                elif (
                        "/customer360-blob-data/" in load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    base_filepath = str(load_path)
                    list_temp = ""
                    try:
                        try:
                            if (base_source != None and base_source.lower() == "dl2"):
                                try:
                                    list_temp = subprocess.check_output(
                                        "ls -dl /dbfs" + load_path + "*/*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep /ld_",
                                        shell=True).splitlines()
                                    if (list_temp == []):
                                        raise ValueError("Ok")
                                except:
                                    list_temp = subprocess.check_output(
                                        "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep /ld_",
                                        shell=True).splitlines()
                            else:
                                list_temp = subprocess.check_output(
                                    "ls -dl /dbfs" + load_path + "*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                                    shell=True).splitlines()
                        except:
                            list_temp = subprocess.check_output(
                                "ls -dl /dbfs" + load_path + "*/*/ |grep /dbfs |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                    except:
                        list_temp = ""
                    list_path = []
                    if (list_temp == ""):
                        list_path.append("no_partition")
                    else:
                        for read_path in list_temp:
                            list_path.append(str(read_path)[2:-1].split('dbfs')[1])
                    if ("/ld_year=" in list_path[0] and "/ld_month=" in list_path[0] and "/ld_day=" in list_path[0]) and (base_source != None and base_source.lower() == "dl2"):
                        p_partition_type = "ld_year=|ld_month=|ld_day="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            try:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-4].split('=')[1].replace('-', '') +
                                        line.split('/')[-3].split('=')[
                                            1].replace('-', '') + line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                            except:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-3].split('=')[1].replace('-', '') +
                                        line.split('/')[-2].split('=')[
                                            1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/ld_year=" in list_path[0] and "/ld_month=" in list_path[0] ) and (base_source != None and base_source.lower() == "dl2"):
                        p_partition_type = "ld_year=|ld_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            try:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-4].split('=')[1].replace('-', '') +
                                        line.split('/')[-3].split('=')[
                                            1].replace('-', '') + line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                            except:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-3].split('=')[1].replace('-', '') +
                                        line.split('/')[-2].split('=')[
                                            1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', ''),
                                        '%Y%m')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_month=" in list_path[0]):
                        p_partition_type = "partition_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-2].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1] + "01",
                                                                       '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0] and "=" not in list_path[0].split('/')[-3]):
                        p_partition_type = "partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-2].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0] and "=" in list_path[0].split('/')[-3]):
                        p_partition_type = "*=*/partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        if (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-2].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-2].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("no_partition" == list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                    else:
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                else:
                    base_filepath = str(load_path)
                    p_partition_type = ""
                    p_month1 = ""
                    p_no = "no"

                p_base_pass = "no"
                if ("/partition_date=" in base_filepath):
                    p_base_pass = "ok"
                    base_filepath = base_filepath.rsplit('/', 2)[0]
                if (base_source != None and base_source.lower() == "dl2"):
                    try:
                        load_path1 = str(load_path) + p_partition_type.split("|")[0] + str(p_month1)[:4] + "/" + \
                                     p_partition_type.split("|")[1] + str(p_month1)[4:6] + "/" + \
                                     p_partition_type.split("|")[2] + str(p_month1)[6:] + "/"
                    except:
                        load_path1 = str(load_path) + p_partition_type.split("|")[0] + str(p_month1)[:4] + "/" + \
                                     p_partition_type.split("|")[1] + str(p_month1)[4:6] + "/"
                else:
                    load_path1 = str(load_path) + p_partition_type + str(p_month1)
                if (p_features == "feature_l4" or p_features == "feature_l2" or p_features == "feature_l3"):
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path))
                    logging.info("file_format: {}".format(self._file_format))
                    if (p_no == "run"):
                        logging.info("partition_type: {}".format(p_partition_type.split('=')[0]))
                        logging.info("read_start: {}".format(p_month2))
                        logging.info("read_end: {}".format(p_month1))
                    logging.info("Fetching source data")
                else:
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path1))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")

                if ("/mnt/customer360-blob-output/C360/UTILITIES/metadata_table/" == load_path):
                    logging.info("load_path metadata_table: {}".format(load_path))
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").load(load_path,
                                                                                                              self._file_format,
                                                                                                              **self._load_args)
                elif (p_features == "feature_l4"):
                    a = "1"
                    if ("_features/" in load_path):
                        x = []
                        try:
                            x = subprocess.check_output("ls -dl /dbfs" + load_path + " |grep .parq",
                                                        shell=True).splitlines()
                        except:
                            x.append("partitions")
                    if ("/customer360-blob-data/" in load_path):
                        x = []
                        try:
                            x = subprocess.check_output("ls -dl /dbfs" + load_path + " |grep .parq",
                                                        shell=True).splitlines()
                        except:
                            x.append("partitions")
                    if (".parquet" in str(x[0])):
                        df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "inferSchema", "true").load(
                            load_path, self._file_format, **self._load_args)
                    else:
                        df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "inferSchema", "true").option(
                            "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                else:
                    if (("/mnt/customer360-blob-data/C360/" in load_path) or (
                            "/mnt/customer360-blob-output/C360/" in load_path)) and (
                            p_features == "feature_l2" or p_features == "feature_l3"):
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                        except:
                            try:
                                df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                    "inferSchema", "true").option(
                                    "basePath", base_filepath).load(load_path1, self._file_format, **self._load_args)
                            except:
                                raise ValueError("Path does not exist: "+load_path1)

                    elif ("_features/" in load_path) and (p_features == "feature_l2" or p_features == "feature_l3"):
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                        except:
                            try:
                                df = self._get_spark().read.option("multiline", "true").option("mode",
                                                                                               "PERMISSIVE").option(
                                    "inferSchema", "true").option(
                                    "basePath", base_filepath).load(load_path1, self._file_format, **self._load_args)
                            except:
                                raise ValueError("Path does not exist: " + load_path1)
                    else:
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(p_load_path, self._file_format)
                        except:
                            if(p_base_pass == "no"):
                                try:
                                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                    "inferSchema", "true").load(load_path1, self._file_format)
                                except:
                                    raise ValueError("Path does not exist: " + load_path1)
                            else:
                                try:
                                    df = self._get_spark().read.option("multiline", "true").option("mode",
                                                                                                   "PERMISSIVE").option(
                                        "inferSchema", "true").option(
                                        "basePath", base_filepath).load(load_path1, self._file_format)
                                except:
                                    raise ValueError("Path does not exist: " + load_path1)
            else:
                if ("/" == load_path[-1:]):
                    load_path = load_path
                else:
                    load_path = load_path + "/"
                if ("_features/" in load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    try:
                        try:
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/  |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                            if any(x in str(list_temp[-1]) for x in matches):
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                    shell=True).splitlines()
                        except:
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                            if any(x in str(list_temp[-1]) for x in matches):
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                    shell=True).splitlines()
                    except:
                        list_temp = ""
                    list_path = []
                    if (list_temp == ""):
                        list_path.append("no_partition")
                    else:
                        for read_path in list_temp:
                            list_path.append(str(read_path)[2:-1])
                    if ("/event_partition_date=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "event_partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/start_of_week=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "start_of_week="
                        if (p_features == "feature_l2" or p_features == "feature_l1"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-" + p_month[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=12)).strftime('%Y%m%d'))
                            p_month1 = str(p_week[:4] + "-" + p_week[4:6] + "-" + p_week[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/start_of_month=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "start_of_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + "-" + p_month[4:6] + "-01")
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(months=3)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            p_month2 = str(p_month_a[:4] + "-" + p_month_a[4:6] + "-" + p_month_a[6:8])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y-%m-%d')
                        p_load_path = []
                        for line in list_path:
                            date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_month=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "partition_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(months=3)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-1].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1] + "01",
                                                                       '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = "partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-1].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("no_partition" == list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                    else:
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                elif (
                        "/projects/prod/c360/data/UTILITIES/metadata_table/" == load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    base_filepath = str(load_path)
                    p_month1 = ""
                elif (
                        "hdfs://10.237.82.9:8020/" in load_path and p_partition != "no_input" and p_increment_flag_load == "no"):
                    base_filepath = str(load_path)
                    list_temp = ""
                    try:
                        try:
                            if (base_source != None and base_source.lower() == "dl2"):
                                try:
                                    list_temp = subprocess.check_output(
                                        "hadoop fs -ls -d " + load_path + "*/*/*/ |awk -F' ' '{print $NF}' |grep /ld_ |grep =20",
                                        shell=True).splitlines()
                                    if any(x in str(list_temp[-1]) for x in matches):
                                        list_temp = subprocess.check_output(
                                            "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                            shell=True).splitlines()
                                except:
                                    list_temp = subprocess.check_output(
                                        "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep /ld_ |grep =20",
                                        shell=True).splitlines()
                                    if any(x in str(list_temp[-1]) for x in matches):
                                        list_temp = subprocess.check_output(
                                            "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                            shell=True).splitlines()
                            else:
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/ |awk -F' ' '{print $NF}' |grep =20",
                                    shell=True).splitlines()
                                if any(x in str(list_temp[-1]) for x in matches):
                                    list_temp = subprocess.check_output(
                                        "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                        shell=True).splitlines()
                        except:
                            list_temp = subprocess.check_output(
                                "hadoop fs -ls -d " + load_path + "*/*/ |awk -F' ' '{print $NF}' |grep =20",
                                shell=True).splitlines()
                            if any(x in str(list_temp[-1]) for x in matches):
                                list_temp = subprocess.check_output(
                                    "hadoop fs -ls -d " + load_path + "*/ |grep C360 |awk -F' ' '{print $NF}' |grep Benz",
                                    shell=True).splitlines()
                    except:
                        list_temp = ""
                    list_path = []
                    if (list_temp == ""):
                        list_path.append("no_partition")
                    else:
                        for read_path in list_temp:
                            list_path.append(str(read_path)[2:-1])

                    if ("/ld_year=" in list_path[0] and "/ld_month=" in list_path[0] and "/ld_day=" in list_path[
                        0]) and (base_source != None and base_source.lower() == "dl2"):
                        p_partition_type = "ld_year=|ld_month=|ld_day="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            try:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-3].split('=')[1].replace('-', '') +
                                        line.split('/')[-2].split('=')[
                                            1].replace('-', '') + line.split('/')[-1].split('=')[1].replace('-',
                                                                                                            ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-1].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                            except:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', '') +
                                        line.split('/')[-1].split('=')[
                                            1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-1].split('=')[1].replace('-', ''),
                                        '%Y%m')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/ld_year=" in list_path[0] and "/ld_month=" in list_path[0]) and (
                            base_source != None and base_source.lower() == "dl2"):
                        p_partition_type = "ld_year=|ld_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            try:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-3].split('=')[1].replace('-', '') +
                                        line.split('/')[-2].split('=')[
                                            1].replace('-', '') + line.split('/')[-1].split('=')[1].replace('-',
                                                                                                            ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-1].split('=')[1].replace('-', ''),
                                        '%Y%m%d')
                            except:
                                if (base_source != None and base_source.lower() == "dl2"):
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-2].split('=')[1].replace('-', '') +
                                        line.split('/')[-1].split('=')[
                                            1].replace('-', ''),
                                        '%Y%m%d')
                                else:
                                    date_data = datetime.datetime.strptime(
                                        line.split('/')[-1].split('=')[1].replace('-', ''),
                                        '%Y%m')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_month=" in list_path[0]):
                        p_partition_type = "partition_month="
                        if (p_features == "feature_l2" or p_features == "feature_l1" or p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[:4] + p_month[4:6])
                            p_month2 = str(p_month_a[:4] + p_month_a[4:6])
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month[0:6])
                            p_month2 = str(p_month_a[0:6])
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-1].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1] + "01",
                                                                       '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0] and "=" not in list_path[0].split('/')[-2]):
                        p_partition_type = "partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l2"):
                            p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_start = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                            p_current_date = p_start + datetime.timedelta(days=6)
                            p_week = str(p_current_date.strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(weeks=1)).strftime('%Y%m%d'))
                            p_month1 = str(p_week)
                            p_month2 = str(p_month_a)
                        elif (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        else:
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date - relativedelta(days=90)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-1].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("/partition_date=" in list_path[0] and "=" in list_path[0].split('/')[-2]):
                        p_partition_type = "*=*/partition_date="
                        if (p_features == "feature_l1"):
                            p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                            p_month_a = str((p_current_date - relativedelta(days=0)).strftime('%Y%m%d'))
                            if ("-" in list_path[0]):
                                p_month1 = str(p_partition[0:4] + "-" + p_partition[4:6] + "-" + p_partition[6:8])
                            else:
                                p_month1 = str(p_partition)
                            p_month2 = str(p_month_a)
                        if (p_features == "feature_l3"):
                            p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                            end_month = (p_current_date + relativedelta(months=1))
                            p_month = str((end_month - relativedelta(days=1)).strftime('%Y%m%d'))
                            p_month_a = str((p_current_date + relativedelta(months=0)).strftime('%Y%m%d'))
                            p_current_date = (end_month - relativedelta(days=1))
                            p_month1 = str(p_month)
                            p_month2 = str(p_month_a)
                        p_old_date = datetime.datetime.strptime(p_month2, '%Y%m%d')
                        p_load_path = []
                        for line in list_path:
                            if ("-" in line.split('/')[-1].split('=')[1]):
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y-%m-%d')
                            else:
                                date_data = datetime.datetime.strptime(line.split('/')[-1].split('=')[1], '%Y%m%d')
                            if (p_old_date <= date_data <= p_current_date):
                                p_load_path.append(line)

                    elif ("no_partition" == list_path[0]):
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                    else:
                        base_filepath = str(load_path)
                        p_partition_type = ""
                        p_month1 = ""
                        p_no = "no"

                else:
                    base_filepath = str(load_path)
                    p_partition_type = ""
                    p_month1 = ""
                    p_no = "no"

                p_base_pass = "no"
                if ("/partition_date=" in base_filepath):
                    p_base_pass = "ok"
                    base_filepath = base_filepath.rsplit('/', 2)[0]
                if (base_source != None and base_source.lower() == "dl2"):
                    try:
                        load_path1 = str(load_path) + p_partition_type.split("|")[0] + str(p_month1)[:4] + "/" + \
                                     p_partition_type.split("|")[1] + str(p_month1)[4:6] + "/" + \
                                     p_partition_type.split("|")[2] + str(p_month1)[6:] + "/"
                    except:
                        load_path1 = str(load_path) + p_partition_type.split("|")[0] + str(p_month1)[:4] + "/" + \
                                     p_partition_type.split("|")[1] + str(p_month1)[4:6] + "/"
                else:
                    load_path1 = str(load_path) + p_partition_type + str(p_month1)
                if (p_features == "feature_l4" or p_features == "feature_l2" or p_features == "feature_l3"):
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path))
                    logging.info("file_format: {}".format(self._file_format))
                    if (p_no == "run"):
                        logging.info("partition_type: {}".format(p_partition_type.split('=')[0]))
                        logging.info("read_start: {}".format(p_month2))
                        logging.info("read_end: {}".format(p_month1))
                    logging.info("Fetching source data")
                else:
                    logging.info("basePath: {}".format(base_filepath))
                    logging.info("load_path: {}".format(load_path1))
                    logging.info("file_format: {}".format(self._file_format))
                    logging.info("Fetching source data")

                if ("/projects/prod/c360/data/UTILITIES/metadata_table/" == load_path):
                    logging.info("load_path metadata_table: {}".format(load_path))
                    df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").load(load_path,
                                                                                                              self._file_format,
                                                                                                              **self._load_args)
                elif (p_features == "feature_l4"):
                    a = "1"
                    if ("_features/" in load_path):
                        x = []
                        try:
                            x = subprocess.check_output("hadoop fs -ls hdfs://datalake" + load_path + " |grep .parq",
                                                        shell=True).splitlines()
                        except:
                            x.append("partitions")
                    if ("hdfs://10.237.82.9:8020/" in load_path):
                        x = []
                        try:
                            x = subprocess.check_output("hadoop fs -ls " + load_path + " |grep .parq",
                                                        shell=True).splitlines()
                        except:
                            x.append("partitions")
                    if (".parquet" in str(x[0])):
                        df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "inferSchema", "true").load(
                            load_path, self._file_format, **self._load_args)
                    else:
                        df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                            "inferSchema", "true").option(
                            "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                else:
                    if (p_features == "feature_l2" or p_features == "feature_l3"):
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                        except:
                            try:
                                df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(load_path1, self._file_format, **self._load_args)
                            except:
                                raise ValueError("Path does not exist: " + load_path1)
                    elif ("_features/" in load_path) and (p_features == "feature_l2" or p_features == "feature_l3"):
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(p_load_path, self._file_format, **self._load_args)
                        except:
                            try:
                                df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option(
                                "basePath", base_filepath).load(load_path1, self._file_format, **self._load_args)
                            except:
                                raise ValueError("Path does not exist: " + load_path1)
                    else:
                        try:
                            df = self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").option(
                                "inferSchema", "true").option("basePath", base_filepath).load(p_load_path, self._file_format)
                        except:
                            if (p_base_pass == "no"):
                                try:
                                    df = self._get_spark().read.option("multiline", "true").option("mode",
                                                                                                   "PERMISSIVE").option(
                                        "inferSchema", "true").load(load_path1, self._file_format)
                                except:
                                    raise ValueError("Path does not exist: " + load_path1)
                            else:
                                try:
                                    df = self._get_spark().read.option("multiline", "true").option("mode",
                                                                                                   "PERMISSIVE").option(
                                        "inferSchema", "true").option(
                                        "basePath", base_filepath).load(load_path1, self._file_format)
                                except:
                                    raise ValueError("Path does not exist: " + load_path1)

            if (base_source != None and base_source.lower() == "dl2"):
                try:
                    df = df.withColumn("partition_date", F.concat(df.ld_year, F.when(
                        F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                        F.col("ld_month")), F.when(F.length(F.col("ld_day")) == 1,
                                                   F.concat(F.lit("0"), F.col("ld_day"))).otherwise(F.col("ld_day"))))
                except:
                    df = df.withColumn("partition_month", F.concat(df.ld_year, F.when(
                        F.length(F.col("ld_month")) == 1, F.concat(F.lit("0"), F.col("ld_month"))).otherwise(
                        F.col("ld_month")), F.lit("01")))
            return df
        else:
            logging.info("Skipping incremental load mode because incremental_flag is 'default'")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
            # Old Version
            # return self._get_spark().read.load(
            #     load_path, self._file_format, **self._load_args
            #                 )
            # New Version: 2020-10-15
            return self._get_spark().read.option("multiline", "true").option("mode", "PERMISSIVE").load(
                load_path, self._file_format, **self._load_args
            )

    def _save(self, data: DataFrame) -> None:
        logging.info("Entering save function")

        if self._increment_flag_save is not None and self._increment_flag_save.lower() == "yes" and p_increment.lower() == "yes":
            logging.info("Entering incremental save mode because incremental_flag is 'yes")
            self._write_incremental_data(data)

        elif (self._increment_flag_save is not None and self._increment_flag_save.lower() == "master_yes"):
            logging.info("Entering incremental save mode because incremental_flag is 'master_yes")
            save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))


            spark = self._get_spark()
            filewritepath = save_path
            partitionBy = self._partitionBy
            if(partitionBy == None):
                partitionBy = "master"
            mode = self._mode
            file_format = self._file_format
            metadata_table_path = self._metadata_table_path
            read_layer = self._read_layer_save
            target_layer = self._target_layer_save
            target_table_name = filewritepath.split('/')[-2]
            dataframe_to_write = data
            mergeSchema = self._mergeSchema
            max_data_date = str(os.getenv(target_table_name, "2020-01-01"))

            logging.info("filewritepath: {}".format(filewritepath))
            logging.info("partitionBy: {}".format(partitionBy))
            logging.info("mode: {}".format(mode))
            logging.info("file_format: {}".format(file_format))
            logging.info("metadata_table_path: {}".format(metadata_table_path))
            logging.info("read_layer: {}".format(read_layer))
            logging.info("target_layer: {}".format(target_layer))
            logging.info("target_table_name: {}".format(target_table_name))
            logging.info("mergeSchema: {}".format(mergeSchema))

            def update_metadata_table(metadata_table_path, target_table_name, filepath, write_mode, file_format,
                                      partitionBy, read_layer, target_layer, mergeSchema, max_data_date):
                metadata_table_update_max_date_temp = max_data_date
                if metadata_table_update_max_date_temp is None or metadata_table_update_max_date_temp == [
                    None] or metadata_table_update_max_date_temp == [
                    'None'] or metadata_table_update_max_date_temp == '':
                    raise ValueError("Please check, the current_target_max_data_load_date can't be empty")
                else:
                    metadata_table_update_max_date = ''.join(metadata_table_update_max_date_temp)
                print("Updating metadata table for {} dataset with date: {} ".format(target_table_name,
                                                                                     metadata_table_update_max_date))
                metadata_table_update_df = spark.range(1)
                metadata_table_update_df = (
                    metadata_table_update_df.withColumn("table_name", F.lit(target_table_name))
                        .withColumn("table_path", F.lit(filepath))
                        .withColumn("write_mode", F.lit(write_mode))
                        .withColumn("target_max_data_load_date",
                                    F.to_date(F.lit(metadata_table_update_max_date), "yyyy-MM-dd"))
                        .withColumn("updated_on", F.current_date())
                        .withColumn("read_layer", F.lit(read_layer))
                        .withColumn("target_layer", F.lit(target_layer))
                        .drop("id")
                )
                try:
                    metadata_table_update_df.write.partitionBy("table_name").format("parquet").mode("append").save(
                        metadata_table_path)
                #         metadata_table_update_df.show(10)
                except AnalysisException as e:
                    log.exception("Exception raised", str(e))
                print("Metadata table updated for {} dataset".format(target_table_name))

            logging.info("Selecting only new data partition to write for lookback scenario's")
            target_max_data_load_date = self._get_metadata_max_data_date(spark, target_table_name)
            tgt_filter_date_temp = target_max_data_load_date.rdd.flatMap(lambda x: x).collect()

            if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == [
                'None'] or tgt_filter_date_temp == '':
                raise ValueError(
                    "Please check the return date from _get_metadata_max_data_date function. It can't be empty")
            else:
                tgt_filter_date = tgt_filter_date_temp[0]

            if(max_data_date == tgt_filter_date):
                logging.info("No data update")
            # elif partitionBy is None or partitionBy == "" or partitionBy == '' or mode is None or mode == "" or mode == '':
            #     raise ValueError(
            #         "Please check, partitionBy and Mode value can't be None or Empty for incremental load")
            elif read_layer is None or read_layer == "" or target_layer is None or target_layer == "":
                raise ValueError(
                    "Please check, read_layer and target_layer value can't be None or Empty for incremental load")
            else:
                logging.info("Writing dataframe with lookback scenario")
                dataframe_to_write.write.mode(mode).format(
                    file_format).save(filewritepath)
                logging.info("Updating metadata master table")
                update_metadata_table(metadata_table_path, target_table_name, filewritepath, mode, file_format,
                                      partitionBy, read_layer, target_layer, mergeSchema, max_data_date)

        else:
            logging.info("Skipping incremental save mode because incremental_flag is 'no'")
            # if data.count() == 0:
            if (data.limit(1).rdd.count() == 0):
                logging.info("No new partitions to write from source")
            else:
                save_path1 = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
                if (p_path_output == "no_input"):
                    save_path = save_path1
                else:
                    save_path = p_path_output+save_path1.split('C360/')[1]
                logging.info("save_path: {}".format(save_path))
                logging.info("target_table_name: {}".format(str(self._filepath).split('/')[-2]))
                logging.info("partitionBy: {}".format(str(self._partitionBy)))
                logging.info("mode: {}".format(self._mode))
                logging.info("file_format: {}".format(self._file_format))
                p_partitionBy = str(self._partitionBy)
                if (p_increment == "yes"):
                    logging.info("Save_Data: Default Kedro")
                    data.write.save(save_path, self._file_format, **self._save_args)
                else:
                    if (p_partitionBy == "None"):
                        logging.info("Save_Data: No_Partition")
                        if(self._mode != None) :
                            data.write.mode(self._mode).save(save_path, self._file_format, **self._save_args)
                        else:
                            data.write.save(save_path, self._file_format, **self._save_args)
                    else:
                        if (p_increment == "yes"):
                            data.write.save(save_path, self._file_format, **self._save_args)
                        elif (p_partition != "no_input"):
                            if (p_partitionBy == "event_partition_date"):
                                p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                                p_month = str(p_current_date.strftime('%Y-%m-%d'))
                            if (p_partitionBy == "start_of_week"):
                                p_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                                p_current_date = p_date - datetime.timedelta(days=p_date.weekday() % 7)
                                p_month = str(p_current_date.strftime('%Y-%m-%d'))
                            if (p_partitionBy == "start_of_month"):
                                p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                                p_month = str(p_current_date.strftime('%Y-%m-%d'))
                            if (p_partitionBy == "partition_date"):
                                p_current_date = datetime.datetime.strptime(p_partition, '%Y%m%d')
                                p_month = str(p_current_date.strftime('%Y-%m-%d'))
                            if (p_partitionBy == "partition_month"):
                                p_current_date = datetime.datetime.strptime(p_partition[0:6] + "01", '%Y%m%d')
                                p_month = str(p_current_date.strftime('%Y-%m-%d'))
                            logging.info("Save_Data: {}".format(p_month))
                            data = data.where("regexp_replace(cast(" + p_partitionBy + " as string),'-','') = regexp_replace('" + p_month + "','-','')")
                            data.write.save(save_path, self._file_format, **self._save_args)
                        else:
                            data.write.save(save_path, self._file_format, **self._save_args)

    def _exists(self) -> bool:
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            self._get_spark().read.load(load_path, self._file_format)
        except AnalysisException as exception:
            if exception.desc.startswith("Path does not exist:"):
                return False
            raise
        return True

    def __getstate__(self):
        raise pickle.PicklingError("PySpark datasets can't be serialized")


class SparkDbfsDataSet(SparkDataSet):
    """
    Fixes bugs from SparkDataSet
    """

    def __init__(  # pylint: disable=too-many-arguments
            self,
            filepath: str,
            file_format: str = "parquet",
            load_args: Dict[str, Any] = None,
            save_args: Dict[str, Any] = None,
            version: Version = None,
            metadata_table_path: str = "",
            credentials: Dict[str, Any] = None,
    ) -> None:
        super().__init__(
            filepath, file_format, load_args, save_args, version, metadata_table_path, credentials,
        )

        # Fixes paths in Windows
        if isinstance(self._filepath, WindowsPath):
            self._filepath = PurePosixPath(str(self._filepath).replace("\\", "/"))