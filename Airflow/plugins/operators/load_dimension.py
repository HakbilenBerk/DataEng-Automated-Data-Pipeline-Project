from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to insert data into the dimension tables in Redshift
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate
        
        

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Inserting data into {}".format(self.table))
        
        if self.truncate:
            redshift.run("DELETE FROM {}".format(self.table))
         
        
        insert_query = """
                INSERT INTO {}
                {}
        """.format(self.table,self.sql)
        self.log.info("Executing query: {}".format(insert_query))
        redshift.run(insert_query)
        self.log.info("Query executed successfully!")   
