import logging.config
import os
import sys
from pathlib import Path

from IPython.core.magic import register_line_magic
from pyspark.sql import SparkSession


def init_spark_session() -> SparkSession:
    """
    Initialize the Spark session

    """
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set("spark.sql.parquet.binaryAsString", "true")
    # Dont delete this line. This allow spark to only overwrite the partition
    # saved to parquet instead of entire table folder
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

    return spark


# Find the project root (./../../../)
startup_error = None
try:
    if Path(__file__).name != "00-kedro-init.py":
        # This exception should be captured by the statement below
        raise NameError("Not running from 00-kedro-init.py")
    project_path = str(Path(__file__).parents[3].resolve())
except NameError:
    # If we're not running this code by sourcing the file (i.e __file__ does not exist),
    # try checking if the current working directory is the project directory
    kedro_init_file_path = os.path.join(
        ".ipython", "profile_default", "startup", "00-kedro-init.py"
    )
    if os.path.isfile(kedro_init_file_path):
        project_path = os.getcwd()
    else:
        # If the working directory is not the project, raise the exception
        raise

@register_line_magic
def reload_kedro(path, line=None):
    """"Line magic which reloads all Kedro default variables."""
    global startup_error
    global context
    global catalog
    global spark

    try:
        import kedro.config.default_logger
        from kedro.context import KEDRO_ENV_VAR, load_context
        from kedro.cli.jupyter import collect_line_magic
    except ImportError:
        logging.error(
            "Kedro appears not to be installed in your current environment "
            "or your current IPython session was not started in a valid Kedro project."
        )
        raise

    try:
        path = path or project_path
        logging.debug("Loading the context from %s", str(path))

        context = load_context(path, env=os.getenv(KEDRO_ENV_VAR))
        catalog = context.catalog

        # remove cached user modules
        package_name = context.__module__.split(".")[0]
        to_remove = [mod for mod in sys.modules if mod.startswith(package_name)]
        for module in to_remove:
            del sys.modules[module]

        spark = init_spark_session()

        logging.info("** Kedro project %s", str(context.project_name))
        logging.info("Defined global variable `context`, `catalog` and `spark`")

        for line_magic in collect_line_magic():
            register_line_magic(line_magic)
            logging.info("Registered line magic `%s`", line_magic.__name__)
    except Exception as err:
        startup_error = err
        logging.exception(
            "Kedro's ipython session startup script failed:\n%s", str(err)
        )
        raise err


reload_kedro(project_path)
