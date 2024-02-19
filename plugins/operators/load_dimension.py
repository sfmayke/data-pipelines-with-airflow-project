from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    load_sql = "INSERT INTO {}({}) {}" 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 table_fields="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.table_fields = table_fields
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncade the table before the INSERT")
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info("Load dimension data")
        redshift.run(LoadDimensionOperator.load_sql.format(self.table, self.table_fields, self.sql))
