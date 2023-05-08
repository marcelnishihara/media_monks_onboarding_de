from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.contrib.operators.gcs_to_bigquery import GCSBigQueryInsertJobOperator

from spotify_plugin.operators.spotify_to_gcs_operator import SpotifyToGcsOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_new_releases',
    default_args=default_args,
    description='Fetch new releases from Spotify API and load to BigQuery',
    schedule_interval=timedelta(days=1),
)

spotify_conn_id = Variable.get('spotify_conn_id')
gcp_conn_id = Variable.get('gcp_conn_id')
gcs_bucket = Variable.get('gcs_bucket')

get_new_releases = SpotifyToGcsOperator(
    task_id='get_new_releases',
    spotify_conn_id=spotify_conn_id,
    gcp_conn_id=gcp_conn_id,
    gcs_bucket=gcs_bucket,
    object_name='new_releases.json',
    endpoint='/browse/new-releases',
    dag=dag,
)

load_new_releases = GCSBigQueryInsertJobOperator(
    task_id='load_new_releases',
    bucket=gcs_bucket,
    source_objects=['new_releases.json'],
    schema_fields=[
        {'name': 'artists', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'release_date', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    destination_project_dataset_table='my_project.my_dataset.new_releases',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=gcp_conn_id,
    google_cloud_storage_conn_id=gcp_conn_id,
    dag=dag
)

get_new_releases >> load_new_releases
