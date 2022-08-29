The zip file contains scripts for mockup data creation and SQL performance.
1. mockup_request_decision_data.py creates mockup request and decision data in temporary folder for last 30 days and loads data into sqlite.
2. load_mock_prediction_data.py extracts data from sqlite and add prediction values with mockup function then loads data into sqlite.
3. dbt_model.py runs dbt models to unnest prediction and decision data to column tables, base_prediction and base_decision, then constructs performance table by joining the two. Then output a performance score in the task log.
4. fourthline_dbt_model folder contains the dbt files in marts folder.

To make it work, you can run command.sh, which will install airflow and dbt plugin in virtual env, as well as copy dag script and dbt folder to $HOME/airflow/dags/.
After running all commands you can check http://0.0.0.0:8080/ with user and password "admin" to play with the dags

Before you can run the dbt dag, you need to configure profiles.yml in $HOME/.dbt folder, which will allow dbt to connect sqlite.
Change "$HOME" to your corresponding folder name and add those info to $HOME/.dbt/profiles.yml.
As the sqlite db location is used in the dag, so if you don't want to create the db in your $HOME folder, just remember to change it also in the dag file.

"""
dbt-sqlite:
  target: local
  outputs:
    local:
      type: sqlite
      threads: 1
      database: 'database'
      schema: 'main'
      schemas_and_paths:
        main: '$HOME/fourthline.db'
      schema_directory: '$HOME'
"""
