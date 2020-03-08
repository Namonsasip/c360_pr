from pathlib import PurePosixPath, WindowsPath
from typing import Any, Dict

from kedro.contrib.io.pyspark import SparkDataSet
from kedro.io import Version


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
        credentials: Dict[str, Any] = None,
    ) -> None:
        super().__init__(
            filepath, file_format, load_args, save_args, version, credentials,
        )

        # Fixes paths in Windows
        if isinstance(self._filepath, WindowsPath):
            self._filepath = PurePosixPath(str(self._filepath).replace("\\", "/"))
