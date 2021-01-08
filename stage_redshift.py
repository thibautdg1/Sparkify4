from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_query = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    "
    
    @apply_defaults
    
    def __init__(self,
                 redshift_connect_id = "",
                 aws_credentials = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 file_format = "",
                 log_json_file = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id,
        self.aws_credentials = aws_credentials,
        self.table = table,
        self.s3_bucket = s3_bucket,
        self.s3_key = s3_key,
        self.file_format = file_format,
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get("execution_date")
        
    def execute(self, context):
        hook_aws = AwsHook(self.aws_credentials)
        credentials = hook_aws.get_credentials()
        
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Staging file for table {self.table_name} is picking from : {s3_path}")
        
        if self.log_json_file != "":
           self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
           copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, self.log_json_file)
        else:
            copy_query = self.copy_query.format(self.table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        self.log.info(f"Copying query : {copy_query}")
        
        hook_redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        hook_redshift.run(copy_query)
        self.log.info(f"Staging table {self.table_name} is loaded")
    
    self.log.info('StageToRedshiftOperator not implemented yet')