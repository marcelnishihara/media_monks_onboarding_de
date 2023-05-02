"""Module for the Class Spotify
"""

from base64 import b64encode
from os import environ
from src.cloud_storage import CloudStorage

import json
import requests


class Spotify:
    """Class Spotify
    """

    def __init__(self) -> None:
        self.__client_id = environ['CLIENT_ID']
        self.__client_secret = environ['CLIENT_SECRET']
        self.__client_creds = f'{self.__client_id}:{self.__client_secret}'

        self.__headers_and_data = {}
        self.__spotify_api_response = {}


    def __compose_request_headers_and_data(self, endpoint: str) -> None:
        """Private method compose_request_headers_and_data
        """
        if endpoint == 'token':
            auth_header = b64encode(
                s=bytes(self.__client_creds, 'utf-8')
                ).decode(encoding='utf-8')

            self.__headers_and_data = {
                'spotify_api': f'https://accounts.spotify.com/api/{endpoint}',
                'request_method': 'POST',
                'request_headers': {'Authorization': f'Basic {auth_header}'},
                'request_data': {'grant_type': 'client_credentials'}
            }

        elif endpoint == 'browse/new-releases':
            client_token = self.__spotify_api_response["access_token"]

            self.__headers_and_data = {
                'spotify_api': f'https://api.spotify.com/v1/{endpoint}',
                'request_method': 'GET',
                'request_headers': {'Authorization': f'Bearer {client_token}'},
                'request_data': {}
            }

        else:
            raise ValueError(f'{endpoint} is not a valid value for endpoint')


    def __request_spotify_api(self) -> None:
        """Private method request_token
        """
        auth_response = requests.request(
            method=self.__headers_and_data['request_method'],
            url=self.__headers_and_data['spotify_api'],
            headers=self.__headers_and_data['request_headers'],
            data=self.__headers_and_data['request_data'],
            timeout=60)

        if auth_response.status_code == 200:
            self.__spotify_api_response = auth_response.json()

        else:
            connection_error_msg = {
                'statusCode': auth_response.status_code,
                'text': auth_response.text
            }

            raise ConnectionError(json.dumps(obj=connection_error_msg))


    def execute(self) -> None:
        """Public method execute
        """
        self.__compose_request_headers_and_data(endpoint='token')
        self.__request_spotify_api()

        self.__compose_request_headers_and_data(endpoint='browse/new-releases')
        self.__request_spotify_api()

        cloud_storage = CloudStorage()
        cloud_storage.execute(obj_to_log=self.__spotify_api_response)
