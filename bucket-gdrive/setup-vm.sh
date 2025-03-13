sudo apt-get update
sudo apt-get upgrade
sudo apt install pip -y
sudo apt install python3-venv -y
sudo apt install git -y
sudo apt install tmux -y
sudo apt install telnet -y

python3 -m venv myvenv
source myvenv/bin/activate

pip install google-cloud-bigquery
pip install --upgrade google-cloud google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 google-cloud-storage pydrive
pip instaa --upgrade db-dtypes xlrd pandas apache-airflow

# SDK

gcloud auth login
gcloud auth list
gcloud config set project <PROJECT ID>
gcloud config list
gcloud compute start-iap-tunnel gchexpapp01p 8080     --local-host-port=localhost:3389     --zone=asia-southeast1-a