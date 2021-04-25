from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_table="",
                 sql_statement="",
                 append_data=True,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Running the query against redshift")

        if self.append_data:
            sql_statement = 'INSERT INTO %s %s' % (self.destination_table, self.sql_statement)
            redshift_hook.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.destination_table
            redshift_hook.run(sql_statement)

            sql_statement = 'INSERT INTO %s %s' % (self.destination_table, self.sql_statement)
            redshift_hook.run(sql_statement)

        self.log.info("Table {} updated.")
