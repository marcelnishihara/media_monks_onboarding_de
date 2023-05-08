import json

from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from hooks.spotify_hook import SpotifyHook


class SpotifyToGCSOperator(BaseOperator):
    def __init__(
        self,
        spotify_conn_id,
        gcs_conn_id,
        gcs_bucket,
        gcs_prefix,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.spotify_conn_id = spotify_conn_id
        self.gcs_conn_id = gcs_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix

    def execute(self, context):
        spotify_hook = SpotifyHook(self.spotify_conn_id)
        response = spotify_hook.run('/browse/new-releases')
        new_releases = json.loads(response.content)['albums']['items']
        gcs_hook = GoogleCloudStorageHook(self.gcs_conn_id)
        with open('/tmp/new_releases.json', 'w') as f:
            for release in new_releases['albums']['items']:
                row = {'artists': ', '.join([artist['name'] for artist in release['artists']]),
                       'name': release['name'],
                       'release_date': release['release_date']}
                f.write(json.dumps(row) + '\n')
        gcs_hook.upload(bucket_name=self.gcs_bucket, object_name=self.gcs_object, filename='/tmp/new_releases.json')
        self.log.info(f"Uploaded new releases to gs://{self.gcs_bucket}/{self.gcs_object}")
