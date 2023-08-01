from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 SQLquery,
                 Truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table = table,
        self.SQLquery = SQLquery,
        self.Truncate = Truncate,

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.Truncate == True:
            self.log.info(
                f"Start truncate statement on table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Start insert data into Fact Table {self.table}")
        
        redshift.run(self.SQLquery)
        
        self.log.info(f"Finished insert fact Table {self.table}")
