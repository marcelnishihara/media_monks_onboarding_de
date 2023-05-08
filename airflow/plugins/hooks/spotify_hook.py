from airflow.hooks.http_hook import HttpHook


class SpotifyHook(HttpHook):
    def __init__(self, spotify_conn_id='spotify_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_conn_id = spotify_conn_id
        self.method = 'GET'
        self.base_url = 'https://api.spotify.com/v1'
