from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks=[],
                 expected_results="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.expected_results = expected_results
        self.checks = checks

    def execute(self, context):
        for check, result in zip(self.checks, self.expected_results):
            self.log.info(f"Data quality check")

            redshift_hook = PostgresHook(self.redshift_conn_id)
            records = redshift_hook.get_records(check)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed.")
            num_records = records[0][0]
            if num_records != result:
                raise ValueError(f"Data quality check failed.")

            self.log.info(f"Data quality check passed with {records[0][0]} records")