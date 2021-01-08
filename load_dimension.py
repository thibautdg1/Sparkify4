from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_connect_id = "",
                 sql_query = "",
                 table = "",
                 delete_load = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        self.sql_query = sql_query
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        hook_redshift = PostgresHook(postgres_connect_id = self.redshift_connect_id)
        
            if self.delete_load:
                hook_redshift.run(f"DELETE FROM {self.table}")
        
            hook_redshift.run(self.sql_query)
            self.log.info(f"Dimension Table {self.table} loaded.")
            
        self.log.info('LoadDimensionOperator not implemented yet')