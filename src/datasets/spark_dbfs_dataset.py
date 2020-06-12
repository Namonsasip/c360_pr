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

import logging

log = logging.getLogger(__name__)


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
    return path[len("/dbfs") :] if path.startswith("/dbfs") else path


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

        self._metadata_table_path = metadata_table_path if (metadata_table_path is not None and metadata_table_path.endswith("/")) else metadata_table_path + "/"

        self._partitionBy = save_args.get("partitionBy", None) if save_args is not None else None
        self._mode = save_args.get("mode", None) if save_args is not None else None
        self._mergeSchema = load_args.get("mergeSchema", None) if load_args is not None else None

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

            src_data = spark.read.load(filepath, self._file_format, **self._load_args)

            logging.info("Source data is fetched")
            logging.info("Checking whether source data is empty or not")

            if len(src_data.head(1)) == 0:
            #if 1==2:
                raise ValueError("Source dataset is empty")
            elif lookup_table_name is None or lookup_table_name == "":
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
                    "select * from src_data where to_date(cast({0} as String),'yyyyMMdd') > date_sub(to_date(cast('{1}' as String)) , {2} )".format(
                    filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l3_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                "select * from src_data where to_date(cast({0} as String),'yyyyMM') > add_months(date(date_trunc('month',to_date(cast('{1}' as String)))),-{2})".format(
                    filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l0_monthly" and target_layer.lower() == 'l4_monthly':
                filter_col = "partition_month"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
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
                new_data = spark.sql("select * from src_data where {0} > to_date(cast('{1}' as String)) ".format(filter_col,tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                # if 1==2:
                #     print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(to_date(cast('{1}' as String)) , {2} )".format(filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l1_daily" and target_layer.lower() == 'l1_daily':
                filter_col = "event_partition_date"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                "select * from src_data where {0} > date_sub(to_date(cast('{1}' as String)) , {2} )".format(filter_col,
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
                src_incremental_data = spark.sql(
                "select * from src_data where {0} > add_months(date_sub(add_months(date(date_trunc('month', to_date(cast('{1}' as String)))), 1),1),-{2})".format(
                    filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "12"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                new_data = spark.sql(
                    "select * from src_data where {0} > date(date_trunc('week', to_date(cast('{1}' as String)))) ".format(filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
                #if 1==2:
                    #print("remove after first run")
                else:
                    src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                    filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l2_weekly_read_custom_lookback" and target_layer.lower() == 'l4_weekly_write_custom_lookback':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
                src_incremental_data = spark.sql(
                    "select * from src_data where {0} > date_sub(date(date_trunc('week', to_date(cast('{1}' as String)))), 7*({2}))".format(
                        filter_col, tgt_filter_date, lookback_fltr))

            elif read_layer.lower() == "l4_weekly" and target_layer.lower() == 'l4_weekly':
                filter_col = "start_of_week"
                lookback_fltr = lookback if ((lookback is not None) and (lookback != "") and (lookback != '')) else "0"
                print("filter_col:", filter_col)
                print("lookback_fltr:", lookback_fltr)
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
                    "select * from src_data where {0} > date(date_trunc('month', to_date(cast('{1}' as String)))) ".format(filter_col, tgt_filter_date))
                if len(new_data.head(1)) == 0:
                    return new_data
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

    def _update_metadata_table(self, spark, metadata_table_path, target_table_name, filepath, write_mode, file_format, partitionBy, read_layer, target_layer, mergeSchema):

        if mergeSchema is not None and mergeSchema.lower() == "true":
            current_target_data = spark.read.format(file_format).option("mergeSchema", 'true').load(filepath)
        else:
            current_target_data = spark.read.format(file_format).load(filepath)

        current_target_data.createOrReplaceTempView("curr_target")

        current_target_max_data_load_date = spark.sql(
            "select cast( nvl(max({0}),'1970-01-01') as String) from curr_target".format(partitionBy))

        metadata_table_update_max_date_temp = current_target_max_data_load_date.rdd.flatMap(lambda x: x).collect()

        if metadata_table_update_max_date_temp is None or metadata_table_update_max_date_temp == [None] or metadata_table_update_max_date_temp == ['None'] or metadata_table_update_max_date_temp == '':
            raise ValueError("Please check, the current_target_max_data_load_date can't be empty")
        else:
            metadata_table_update_max_date = ''.join(metadata_table_update_max_date_temp)

        logging.info("Updating metadata table for {} dataset with date: {} ".format(target_table_name, metadata_table_update_max_date))

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
            metadata_table_update_df.write.partitionBy("table_name").format("parquet").mode("append").save(metadata_table_path)
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
        #if 1==2:
            logging.info("No new partitions to write from source")
        elif partitionBy is None or partitionBy == "" or partitionBy == '' or mode is None or mode == "" or mode == '':
            raise ValueError(
                "Please check, partitionBy and Mode value can't be None or Empty for incremental load")
        elif read_layer is None or read_layer == "" or target_layer is None or target_layer == "":
            raise ValueError(
                "Please check, read_layer and target_layer value can't be None or Empty for incremental load")
        else:
            if (read_layer.lower() == "l1_daily" and target_layer.lower() == 'l4_daily') or (
                    read_layer.lower() == "l2_weekly" and target_layer.lower() == 'l4_weekly') or (
                    read_layer.lower() == "l3_monthly" and target_layer.lower() == 'l4_monthly'):

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

                if tgt_filter_date_temp is None or tgt_filter_date_temp == [None] or tgt_filter_date_temp == ['None'] or tgt_filter_date_temp == '':
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

                if len(df_with_lookback_to_write.head(1)) == 0:
              #  if 1==2:
                    logging.info("No new partitions to write at target dataset")
                else:
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

        if self._increment_flag_load is not None and self._increment_flag_load.lower() == "yes":
            logging.info("Entering incremental load mode because incremental_flag is 'yes")
            return self._get_incremental_data()

        else:
            logging.info("Skipping incremental load mode because incremental_flag is 'no")
            load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
            return self._get_spark().read.load(
                load_path, self._file_format, **self._load_args
                            )

    def _save(self, data: DataFrame) -> None:
        logging.info("Entering save function")

        if self._increment_flag_save is not None and self._increment_flag_save.lower() == "yes":
            logging.info("Entering incremental save mode because incremental_flag is 'yes")
            self._write_incremental_data(data)

        else:
            logging.info("Skipping incremental save mode because incremental_flag is 'no")
            save_path = _strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
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

