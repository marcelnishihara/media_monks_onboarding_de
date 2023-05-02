"""Module for the Class CloudStorage
"""

from datetime import datetime
from os import environ
from pytz import timezone
from google.cloud import storage

import json


class CloudStorage:
    """Class CloudStorage
    """
    def __init__(self) -> None:
        self.__project_id = 'marcelnishihara'
        self.__bucket_name = 'raccoon_monks'
        self.__credentials_path = './credentials/service_account.json'
        self.__client_bucket = ''


    def __authenticate(self) -> None:
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.__credentials_path
        storage_client = storage.Client(project=self.__project_id)

        self.__client_bucket = storage_client.get_bucket(
            bucket_or_name=self.__bucket_name)


    def __upload_file(self, blob_name: str, obj_to_log: dict) -> None:
        blob = self.__client_bucket.blob(
            blob_name=f'onboarding_de/{blob_name}')

        blob.upload_from_string(
            data=json.dumps(obj=obj_to_log, indent=4),
            content_type='application/json')


    @staticmethod
    def __datetime() -> str:
        return datetime.now(tz=timezone(zone='Brazil/East')).isoformat()


    def execute(self, obj_to_log: dict) -> None:
        now = self.__datetime()
        self.__authenticate()

        self.__upload_file(
            blob_name=f'spotify_{now}.json',
            obj_to_log=obj_to_log)
