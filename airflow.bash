airflow db init
airflow webserver --port 8080
airflow scheduler
cp superstore_pipeline.py ~/airflow/dags

pkill -f "airflow webserver --port 8080"
pkill -f "airflow scheduler"