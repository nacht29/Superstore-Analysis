tmux new-session -d -s airflow_web "airflow db init && airflow webserver -p 8080"
tmux new-session -d -s airflow_sched "airflow scheduler"