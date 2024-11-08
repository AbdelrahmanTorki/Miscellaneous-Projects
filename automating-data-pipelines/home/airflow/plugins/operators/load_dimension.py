from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 sql = '',
                 table = '',
                 truncate_insert = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.table = table
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.truncate_insert:
            self.log.info("Truncate-Insert Mode :: Clearing data from dimension table before insert")
            redshift.run("DELETE FROM {}".format(self.table))
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.sql
            )

        redshift.run(formatted_sql)