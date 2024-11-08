from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class DataQualityOperator(BaseOperator):

    """
    The operator accepts different quality checks yet currently only 'sql' aggregate type is implemented
    """
    ui_color = '#89DA59'

    def validate_sql(self, query, op, result, hook):
        records = hook.get_records(query)

        self.log.info(f'Returned Records  ::\n {records}')

        if records is None or len(records) < 1 or len(records[0]) < 1:
            self.log.error(f"No records are found for the following query")
            raise ValueError(
                f"Data quality check failed. No Results returned for the following query: \n{query}")

        num_records = records[0][0]
        self.log.info(f'Number of records :: {num_records}')

        if op == 'eq' and num_records == result:
            self.log.info(
                f"Data quality check passed with the result of {num_records} equal to desired value of {result}")

        if op == 'gt' and num_records > result:
            self.log.info(
                f"Data quality check passed with the result of {num_records} greater than desired value of {result}")

        if op == 'lt' and num_records < result:
            self.log.info(
                f"Data quality check passed with the result of {num_records} less than desired value of {result}")

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.checks = checks

    def execute(self, context):
        self.log.info('DataQualityOperator Execution')
        redshift_hook = PostgresHook(self.conn_id)

        for check in self.checks:
            if check['type'] == 'sql':
                self.log.info('error Reason')
                DataQualityOperator.validate_sql(self, 
                                                 check['query'],
                                                 check['op'],
                                                 check['result'],
                                                 redshift_hook)