from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.dataset_nm = dataset_nm
        self.path = path
        self.file_name = file_name
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        host = connection.host or 'openapi.seoul.go.kr'
        port = connection.port or 8088
        api_key = '6145736846656d753639425548776e'

        self.base_url = f'http://{host}:{port}/{api_key}/json/{self.dataset_nm}'
        self.log.info(f"API Base URL: {self.base_url}")

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            row_df = self.call_api(self.base_url, start_row, end_row)
            if row_df.empty:
                break
            total_row_df = pd.concat([total_row_df, row_df], ignore_index=True)
            if len(row_df) < 1000:
                break
            start_row = end_row + 1
            end_row += 1000

        os.makedirs(self.path, exist_ok=True)
        total_row_df.to_csv(f"{self.path}/{self.file_name}", encoding='utf-8', index=False)

    def call_api(self, base_url, start_row, end_row):
        import requests
        import json

        url = f"{base_url}/{start_row}/{end_row}"
        if self.base_dt:
            url += f"/{self.base_dt}"

        headers = {'Content-Type': 'application/json', 'Accept': '*/*'}

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            self.log.error(f"API 호출 실패: {response.status_code} - {response.text}")
            raise Exception(f"API 호출 실패: {response.status_code}")

        try:
            contents = json.loads(response.text)
            key_nm = list(contents.keys())[0]
            rows = contents[key_nm].get('row', [])
            return pd.DataFrame(rows)
        except Exception as e:
            self.log.error(f"JSON 파싱 오류: {str(e)}")
            raise
