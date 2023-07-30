#!/bin/bash
export AIRFLOW_HOME="."
cd $AIRFLOW_HOME

pkill --signal 15 -u $USER airflow

exit 0;