from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 sql = '',
                 table = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator Execution')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(
                self.table,
                self.sql
            )

        redshift.run(formatted_sql)