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

        self.s3_path = self.s3_path.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_path}/"

        self.log.info(
            f'Start staging table {self.table} to RedShift')

        redshift.run(f"COPY {self.table} FROM '{s3_path}' \
                       ACCESS_KEY_ID '{credentials.access_key}' \
                       SECRET_ACCESS_KEY '{credentials.secret_key}' \
                       FORMAT AS JSON '{self.json_path}'")

        self.log.info(f'Finish staging table {self.table} to RedShift')
