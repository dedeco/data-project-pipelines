from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 destination_table="",
                 append_data=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.destination_table = destination_table
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

        self.log.info("Table {} updated".format(self.destination_table))
