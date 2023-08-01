AIRFLOW_VERSION=2.6.3

export AIRFLOW_HOME="."

cd $AIRFLOW_HOME

# Extract the version of Python you have installed. If you're currently using Python 3.11 you may want to set this manually as noted above, Python 3.11 is not yet supported.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.7.txt"
# For example this would install 2.6.3 with python 3.7: https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.7.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install psycopg2

pip install apache-airflow-providers-amazon

airflow db init

airflow airflow users create --username admin --firstname Lucas --lastname Lam --role Admin --email lucas@example.org

exit 0;