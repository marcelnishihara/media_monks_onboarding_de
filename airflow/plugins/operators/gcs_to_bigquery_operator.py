from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class GCSToBigQueryOperator(BaseOperator):
    def __init__(self, gcp_conn_id, gcp_bucket, gcp_object, bigquery_conn_id, bigquery_table, **kwargs):
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.gcp_bucket = gcp_bucket
        self.gcp_object = gcp_object
        self.bigquery_conn_id = bigquery_conn_id
        self.bigquery_table = bigquery_table

    def execute(self, context):
        uri = f"gs://{self.gcp_bucket}/{self.gcp_object}"
        schema = [
            {'name': 'artists', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'release_date', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]
        bigquery_insert_job = BigQueryInsertJobOperator(
            task_id='gcs_to_bigquery',
            project_id=project_id,
            configuration={
                'load': {
                    'sourceFormat': 'NEWLINE_DELIMITED_JSON',
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': self.bigquery_table.split('.')[0],
                        'tableId': self.bigquery_table.split('.')[1],
                    },
                    'sourceUris': [uri],
                    'schema': {'fields': schema},
                    'createDisposition': 'CREATE_IF_NEEDED',
                    'writeDisposition': 'WRITE_TRUNCATE',
                }
            },
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=None,
            impersonation_chain=None,
            location=None,
            project_id=None,
            dag=self.dag,
        )
        return bigquery_insert_job.execute(context=context)
