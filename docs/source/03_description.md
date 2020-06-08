## Overall design

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

Sometimes CVM pipeline's user would like to modify the details of the pipeline's run.
That could include for example scoring date, size of training sample, flags for sending the treatments etc.
These could be changed in `parameters_*.yml` files.

## Sampling submodule

## Data preparation submodule

## Treatments submodule
Treatments are the only reason for existence of C360 pipeline.
The treatments submodule runs using data created in previous stages - it uses propensity scores,
microsegments, macrosegments and also C360 data.
It picks clients to be targeted with a campaign as well as the campaign code itself.
The code for this section can be found at `src/cvm/treatments`.

### How are users and campaigns chosen?
Users are chosen according to `conf/base/CVM/L5/parameters_treatment_rules.yml` 
and `conf/base/CVM/L5/parameters_treatment.yml`.

Firstly users recently contacted are dropped.
The recently contacted users are sourced from `treatments_chosen_history_input` 
(checkout in catalog).
The user cannot be scored more then once for every `treatment_cadence` days.

![](.images/03_description_images/998e05fb.png)

Then the rules from `parameters_treatment_rules.yml` are applied.
What is a rule?

![](.images/03_description_images/1a025de8.png)

Rule is a yaml statement with a campaign code that is meant to be assigned to users.
Users must meet conditions listed in `conditions` section. All of them.

But we cannot assign campaign to everybody meeting those conditions.
In order to be able to run test and learn runs we want to control the size of the group of users
who can get a specific treatment. This is what `limit_per_code` is for. It is an upper bound
of number of users who can get assigned to the campaign code by this rule for this variant. 
Let me restate it, because this is important. **Limits are calculated per rule and variant**.

But what is `variant`? If we want to run test and learn or have a local control group for a 
microsegment we want to give different treatments randomly for people in the same microsegment.
Before assigning rules each user is randomly assigned to one of the variants defined in rules.

We can use to create local control group. We just have to create 2 rules - one with 
the campaign code we want to check and the other with campaign code for control group.
Those have to have different value in `variant` field. For example:

![](.images/03_description_images/502d0abb.png)

This is a rule with a variant - some people would be assigned to rule with `variant` 1, and 
some to rule with `variant` 2. The random character you see on a screenshot are rules ids.
They don't matter, they just have to be unique.

But this is not whole story yet. Rules are grouped in treatments.

![](.images/03_description_images/6b72c1c5.png)

Treatments are defined in `conf/base/CVM/L5/parameters_treatment_rules.yml`.
They have a list of rules included in `rules` field.
But also some extra fields.

The field called `use_case` is used to determine what use case is the treatment assigned to.
It influences deployment. You send different use cases to different paths.

We can see an algebraic expression in field `order_policy`. 
There is one thing in rules that is not explained.
How are the users picked? We know that they have meet the conditions,
we know that they are randomly selected to some variants but what if there is more users that
meet all the necessary conditions then `limit_per_code`?
This is where `order_policy` steps in. This field is used to determine the priority in 
which users are assigned to rules. The bigger the value of `order_policy` the more likely 
user is to be targeted with campaign. `order_policy` can be defined using any feature,
not only propensities. So for example we could pick users according to `churn60_pre * size_of_shoe` 
had we had `size_of_shoe` feature.

The set of users assigned to a campaign by rule is limited by `limit_per_code`.
There is another, global limit for all rules.
This the `treatment_size` field.
After assigning all the rules there is more users assigned then `treatment_size` then
top `treatment_size` users are picked. According to `order_policy`. `treatment_size` is applied
to treatment variant, so **maximum number of users assigned to treatment is `treatment_size` * number of variants**.
Rules are applied in the same order as in `conf/base/CVM/L5/parameters_treatment_rules.yml` file.

### What happens when we have chosen campaign codes?
The treatments history is updated and treatments are saved and deployed.

### What does it mean to deploy the treatments?
Table with users and campaign codes (`treatments_chosen` in catalog) is loaded and transformed
to format agreed with campaign team. Then the tables, one for each use case are saved
in agreed paths. 

### What parameters can I modify to change the treatments?
First and foremost you can modify whole `conf/base/CVM/L5/parameters_treatment_rules.yml`
as described above. 

![](.images/03_description_images/bcd42654.png)

You can modify `skip_sending` field in `conf/base/CVM/L5/parameters_treatment.yml`.
If set to `"yes"` then the produced table with users and campaign codes assigned will
not be deployed (sent to campaign team), nor will it be saved to treatments history.
Anything different then `"yes"`, for example `"no"` or `"ไม่"` will send and save the
treatments chosen.

![](.images/03_description_images/002a6224.png)

Another parameter - `treatments_cadence` from `conf/base/CVM/L5/parameters_treatment.yml`.
If it set to `7` it means that every user can get a campaign no more often then once
every `7` days.

![](.images/03_description_images/a13e8066.png)

Paths from `conf/base/CVM/L5/parameters_treatment.yml`.
Those are used to tell the pipeline where to save the users and campaigns table.
The `{}` here are filled with timestamp on time of saving with timestamp in the format
defined in the same file.

![](.images/03_description_images/57778792.png)

## Report submodule

## Scoring experiment
Scoring experiment is set of tables that are changed during one run of scoring.
If you want to have a persistent set of scoring sets use scoring experiment.
Scoring experiment sets are described in `conf/base/CVM/L5/catalog_scoring_experiment.yml`.
These are output path for a scoring experiment sets.
To run scoring experiment use pipeline `cvm_full_scoring_experiment` from `src/customer360/pipeline.py`.


