#!/usr/bin/env bash
# NB:install Apache Airflow first using install_airflow.sh script

# TODO: Change this to the path where airflow directory is located
export AIRFLOW_HOME=~/airflow
airflow standalone
