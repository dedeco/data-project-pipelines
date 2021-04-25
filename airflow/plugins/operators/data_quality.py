from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):

        self.log.info("Running db checks.")

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            records = redshift_hook.get_records(sql)
            if len(records) == 0:
                raise ValueError("Table {0} has NULL records".format(table))

            records = redshift.get_records(sql)[0]

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {check} returned no results")

            if records != exp_result:
                logging.error(f"Data quality check failed. {check} returned some diferrent expected results")
