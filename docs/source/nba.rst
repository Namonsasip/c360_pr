NBA Documentation
=================

Welcome to the NBA documentation. For details about the code and functions please refer
to the `subpackages` section below.

For an overall high-level description of the methodology please to PowerPoint
presentations about NBA.

NBA's codebase purpose is to train and apply machine learning models on campaigns in
order to select the best campaign to send to a subscriber. Best campaign is defined as
the one that has tha highest NBA score (a.k.a. value score)

A summary of the NBA codebase workflow and steps follows:

 - NBA features: NBA requires some features and tables specific for the use case (L5),
   which are slight processing or variations over C360 features or other C360 cloud
   assets:
    - NBA campaign master (:py:meth:`nba.model_input.model_input_nodes.node_l5_nba_campaign_master`):
      this is the campaign master (please don't confuse with
      master table), which has information about all campaigns. NBA needs to do some
      pre-processing and cleaning on it
    - NBA customer profile (:py:meth:`nba.model_input.model_input_nodes.node_l5_nba_customer_profile`):
      This contains demographic and other features from
      subscribers. NBA needs to convert some of them to numeric to use them in models
 - NBA master table spine (:py:meth:`nba.model_input.model_input_nodes.node_l5_nba_master_table_spine`):
   the master table spine is the key of NBA, it contains all
   relevant rows for training, some informative and relevant NBA specific features and
   the target variable for the models. The spine is basically the master table but
   without explanatory features
 - NBA master tables: the master table is like the spine but with explanatory features
   used in the models joined as columns. The master table is the input for the models
   and contains all that is required to train them. Actually there are different master
   tables for NBA, the complete master table
   (:py:meth:`nba.model_input.model_input_nodes.node_l5_nba_master_table`)
   is used for acceptance models and a master table that contains only positive
   (a.k.a. accepted) responses is used for ARPU models
   (:py:meth:`nba.model_input.model_input_nodes.node_l5_nba_master_table_only_accepted`)
 - NBA models training :py:meth:`nba.model_input.models_nodes`):
   NBA models are trained in a distributed way. This is probably the most complex part
   of the code but
   strongly accelerates training by leveraging the Spark cluster. Models are trained
   using a scikit-learn like syntax (using lightbm) and each of the training is
   distributed using a pandas UDF, so that each spark task in the worker trains a model
   and persists it using PAI (performance AI) so that it can be accessed afterwards.
   It's necessary to be careful about the size of the pandas chunk that gets sent to
   spark workers for training as the memory available for each spark task is limited.
 - NBA models scoring: please use this function to make predictions using NBA models:
   :py:meth:`nba.model_input.models_nodes.score_nba_models`





nba package
===========

.. automodule:: nba
    :members:
    :undoc-members:
    :show-inheritance:

Subpackages
-----------

.. toctree::

    nba.backtesting
    nba.databricks_notebooks
    nba.model_input
    nba.models

