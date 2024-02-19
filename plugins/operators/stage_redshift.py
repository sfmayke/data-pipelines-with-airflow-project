from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import Variable

class StageToRedshiftOperator(BaseOperator):
    copy_sql = """
        copy {} from '{}'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_data="",
                 jsonPath="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_data = s3_data
        self.aws_credentials_id = aws_credentials_id
        self.jsonPath = jsonPath

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_iam_role = Variable.get('aws_iam_role')

        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_data)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_iam_role,
            self.jsonPath
        )
        
        redshift.run(formatted_sql)
