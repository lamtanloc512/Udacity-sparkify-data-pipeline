from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class CreateTablesOperator(BaseOperator):
    ui_color = '#358145'

    def __init__(self,
                 postgres_conn_id="",
                 sql_file="",
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.sql_file = sql_file

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        sql_file = open(self.sql_file, "r")
        sql_commands = sql_file.read().split(";")

        sql_file.close()

        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)
