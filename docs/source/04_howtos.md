# How tos

### How to run code?
Use main-kedro notebook.

### How to modify microsegments?
Microsegments are built using the definition in file `conf/base/CVM/L5/parameters_microsegments.yml`.
Microsegments are assigned to users using the conditions stated below microsegment name in yaml file.
Check out function `cvm.src.utils.parametrized_features.build_feature_from_parameters` for more details.

### How to score for a different day?
1. Modify `scoring_experiment.chosen_date` in `conf/base/CVM/L5/parameters.yml` to desired one.
2. Run `cvm_full_scoring_experiment`.
3. Datasets are saved according to `conf/base/CVM/L5/catalog_scoring_experiment.yml`.

### How to modify assigned treatment codes?
Modify `conf/base/CVM/L5/parameters_treatment_rules.yml`.

### How to checkout models on PAI?
Forward port 5000 from 20.188.107.215 machine (ask k'Non for credentials).
Then the PAI is accessible on `localhost:5000`.

![](.images/03_description_images/0a8d68ae.png)

### How to retrain model?
Change experiment name in `parameters.yml`. Experiment name is the identifier used to
save models on PAI server. Then run `cvm_full_training` pipeline to retrain the model.
The existing model saved in `random_forest` will be overwritten. Old models can be restored 
from PAI server. Checkout PAI documentation on how to do it.

### How to change model type?
Modify the constructor of `sklearn` model in `src/cvm/modelling/nodes.py` and retrain the model.

![](.images/03_description_images/0b947f51.png)

If you want to change parameters / model itself, modify constructor above. Currently using
`RandomForestClassifier`.

### How to modify the preprocessing step?

### How to access and edit documentation?

### How to browse through all the data set available?

### How to select input for model training?

### How to edit target for model training?

### How to deploy campaign contact list from model?

### How to run nodes in databricks?

### How to check the logging?