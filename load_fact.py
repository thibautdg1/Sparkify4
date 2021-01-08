from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_connect_id = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_connect_id
        self.sql_query = sql.query

    def execute(self, context):
        hook_redshift = PostgresHook(postgres_connect_id = self.redshift_connect_id)
        hook_redshift.run(self.sql_query)
        self.log.info(f"Dimension Table {self.table} loaded.")
   
    self.log.info("LoadFactOperator not implemented yet")