# Getting started

User can run anything using `kedro` pipelines. How can one do it? Go to [example notebook](https://southeastasia.azuredatabricks.net/?o=334552184297553#notebook/904855813142769).
This notebook does the following:
- downloads the most recent code version from branch `cvm/master`
- downloads dependencies from `pypi`
- runs the pipeline

Pipelines are listed in `src/customer360/pipeline.py`. If you want to change the pipeline run change the name of the pipeline.
If you want to see the details of a pipeline checkout [how-to page](04_howtos.md).
