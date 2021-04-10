from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to copy the json data from S3 bucket to staging tables on Redshift
    args:
        task_id (str): DAG task id 
        dag (DAG object): dag instance
        s3_bucket (str): S3 bucket name
        s3_prefix (str): directory in S3 where data is located
        redshift_conn_id (str): Redshift connection id name
        aws_credentials (str): AWS credentials ID name
        table (str): name for the target table
        region (str): AWS Region
        copy_json (str): copy json option [default = auto]

        
    """
    ui_color = '#358140'
    
    query_template = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION '{}'
                FORMAT as json '{}';
                """

    @apply_defaults
    def __init__(self,
                 s3_bucket = "",
                 s3_prefix = "",
                 redshift_conn_id = "",
                 aws_credentials = "",
                 table = "",
                 region = "",
                 copy_json = "auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.region = region
        self.copy_json = copy_json
        

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        
        self.log.info("Copying the data from S3 to AWS Redshift...")
        rendered_key = self.s3_prefix.format(**context)
        s3_directory = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        query = StageToRedshiftOperator.query_template.format(
                    self.table,
                    s3_directory,
                    credentials.access_key,
                    credentials.secret_key,
                    self.region,
                    self.copy_json         
        )
        self.log.info("Executing the query: {}".format(query))
        redshift.run(query)
        self.log.info("Query executed successfully!")





