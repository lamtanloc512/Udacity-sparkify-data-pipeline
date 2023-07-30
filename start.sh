#!/bin/bash
export AIRFLOW_HOME="."
cd $AIRFLOW_HOME

nohup airflow standalone &

exit 0;

