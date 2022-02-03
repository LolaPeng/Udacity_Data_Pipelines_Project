from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 readshift_conn_id = "",
                 test_results="",
                 expected_results="",                
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.readshift_conn_id = readshift_conn_id
        self.test_results=test_results
        self.expected_results=expected_results

    def execute(self, context):
        self.log.info('Getting login information.')
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Running test.')
        records=redshift_hook.get_records(self.test_results)
        if records[0][0] != self.expected_results:
            raise ValueError(f"""
            Data quality check failed. {records[0][0]} does not equal to {self.expected_results}
            """)
        else:
            self.log.info("Congrats! The test results is the same as expected results")