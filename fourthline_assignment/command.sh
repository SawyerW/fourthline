python3 -m venv fourthline_case_airflow
source fourthline_case_airflow/bin/activate
yes | pip install apache-airflow
#install dbt package for sqlite
yes | pip dbt-sqlite

airflow db init
#create dags folder if it does not exists
if [ -d "$HOME/airflow/dags" ]; then
  ### Take action if $DIR exists ###
  echo "$HOME/airflow/dags already eists, skipping..."
else
  mkdir $HOME/airflow/dags
  exit 1
fi
#copy dbt project to airflow folder, the path will be used for airflow to schedule dbt model
cp -r ./fourthline_dbt_model $HOME/airflow/dags
#copy dags script to dags in airflow folder
cp dbt_model.py $HOME/airflow/dags/
cp load_mock_prediction_data.py $HOME/airflow/dags/
cp mockup_request_decision_data.py $HOME/airflow/dags/
airflow webserver -D
airflow scheduler -D

#go to http://0.0.0.0:8080/ with user and password admin to check the dags
