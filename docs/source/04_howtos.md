# How tos
### How to run code?
Use main-kedro notebook.
### How to modify `x`?

### How to modify microsegments?
Microsegments are built using the definition in file `conf/base/CVM/L5/parameters_microsegments.yml`.
Microsegments are assigned to users using the conditions stated below microsegment name in yaml file.
Check out function `cvm.src.utils.parametrized_features.build_feature_from_parameters` for more details.