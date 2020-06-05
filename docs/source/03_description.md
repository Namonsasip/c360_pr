### Overall design
Description of models, pipelines, data.

### Scoring experiment
Scoring experiment is set of tables that are changed during one run of scoring.
If you want to have a persistent set of scoring sets use scoring experiment.
Scoring experiment sets are described in `conf/base/CVM/L5/catalog_scoring_experiment.yml`.
These are output path for a scoring experiment sets.
To run scoring experiment use pipeline `cvm_full_scoring_experiment` from `src/customer360/pipeline.py`.