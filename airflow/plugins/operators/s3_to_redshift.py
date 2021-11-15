from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 s3_path='',
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_path = s3_path

    def execute(self):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Start copy data from S3 to Redshift')

        copy = '''
            COPY {}
            FROM {}
            credentials 
            'aws_iam_role=arn:aws:iam::0123456789012:role/MyRedshiftRole'
        '''.format(self.table, self.s3_path)

        redshift.run(copy)
        self.log.info('Finish copy data from S3 to Redshift')