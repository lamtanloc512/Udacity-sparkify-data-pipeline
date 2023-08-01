from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 SQLquery,
                 table,
                 Truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.SQLquery = SQLquery
        self.table = table
        self.Truncate = Truncate

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.Truncate:
            self.log.info(f'Start truncate statement on table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Start insert data into Dimension Table {self.table}')

        redshift.run(self.SQLquery)

        self.log.info(f'Finished insert data into Dimension Table {self.table}')
