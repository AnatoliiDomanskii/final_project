#!/usr/bin/env bash
# NB:install Apache Airflow first using install_airflow.sh script

# TODO: Change this to the path where airflow directory is located
# (default is ~/airflow)
export AIRFLOW_HOME=/Users/hunting/projects/r_d/DE2022/lect_07/airflow
airflow standalone
