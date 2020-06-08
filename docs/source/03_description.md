### Overall design

The CVM pipeline consists of following submodules:
- sampling
- data preparation
- modelling
- treatments
- report

To run any pipeline, any tool we need data.
CVM pipeline sources the data from [C360](https://customer360.atlassian.net/wiki/spaces/C3/overview).
The data on C360 may be huge.
In order to speed up the computation and reduce memory usage data is first sampled in sampling submodule.

Once we have reduced data tables we can then proceed to joining the data to create master table for our task of prediction of churn or revenue dilution.
In the end we want to run a model that would predict those so we have to be able to prepare training / test / daily scoring datasets.
It would be also nice to prepare datasets for feature extraction phase.
Sometimes we want to add transformations of existing C360 features.
All of those things are meant to be done in data preparation submodule.

Once we have all features from C360 joined in one big master table we would like to train a model.
But every model requires preprocessing.
Preprocessing pipeline has to be fitted and saved for future reuse.
Once we have preprocessed data we have to split it train and test dataset.
Finally, we can train (multiple) models and save them in Performance AI.
Once we have a model we can use it to create propensity score for every user.
This is the modelling submodule.

The whole purpose of CVM pipeline is to send treatments to clients.
We have the propensities produced, but we still have to map them to treatments and deploy the
treatments to the campaign team. Treatments submodule takes care of that.

To be able to test and learn we have to monitor some chosen KPIs. 
This is the reason why report submodule exists.

### Sampling

### Data preparation

### Treatments

### Report

### Scoring experiment
Scoring experiment is set of tables that are changed during one run of scoring.
If you want to have a persistent set of scoring sets use scoring experiment.
Scoring experiment sets are described in `conf/base/CVM/L5/catalog_scoring_experiment.yml`.
These are output path for a scoring experiment sets.
To run scoring experiment use pipeline `cvm_full_scoring_experiment` from `src/customer360/pipeline.py`.