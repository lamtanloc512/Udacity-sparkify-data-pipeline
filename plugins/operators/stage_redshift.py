from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credientials_id,
                 redshift_conn_id,
                 table,
                 s3_bucket,
                 s3_path,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credientials_id = aws_credientials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credientials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)

        path = "s3://{}/{}".format(self.s3_bucket, self.s3_path)
        
        self.log.info(f'Start staging table {self.table} to RedShift')

        query = """
            COPY {table}
            FROM '{path}'
            ACCESS_KEY_ID '{access_key_id}'
            SECRET_ACCESS_KEY '{secret_acess_key}'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            FORMAT AS JSON 'auto'
            TIMEFORMAT AS 'auto';
        """.format(table=self.table,
                   path=path,
                   access_key_id=credentials.access_key,
                   secret_acess_key=credentials.secret_key)

        redshift.run(query)

        self.log.info(f'Finish staging table {self.table} to RedShift')
