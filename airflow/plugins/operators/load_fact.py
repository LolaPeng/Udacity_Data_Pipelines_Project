from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 is_truncate="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.select_sql=select_sql
        self.is_truncate=is_truncate

    def execute(self, context):
        self.log.info('Getting login information.')
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading data into fact table in Redshift.")
        table_insert_sql=f"""
            INSERT INTO {self.table}
            {self.select_sql}
        
        """
        redshift_hook.run(table_insert_sql)
