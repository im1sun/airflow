from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '6145736846656d753639425548776e/json/TbOpendataFixedcctvSP/'
        self.base_dt = base_dt

    def execute(self, context):
    import os

    connection = BaseHook.get_connection(self.http_conn_id)
    if connection.port:
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'
    else:
        self.base_url = f'http://{connection.host}/{self.endpoint}'

    self.log.info(f'[DEBUG] Base URL: {self.base_url}')

    total_row_df = pd.DataFrame()
    start_row = 1
    end_row = 1000
    while True:
        row_df = self.call_api(self.base_url, start_row, end_row)
        if row_df is None:
            raise ValueError(f"API 호출 실패: {start_row}-{end_row}")
        total_row_df = pd.concat([total_row_df, row_df])
        if len(row_df) < 1000:
            break
        start_row = end_row + 1
        end_row += 1000

    os.makedirs(self.path, exist_ok=True)
    total_row_df.to_csv(f'{self.path}/{self.file_name}', encoding='utf-8', index=False)


def call_api(self, base_url, start_row, end_row):
    import requests
    import json

    headers = {'Content-Type': 'application/json', 'charset': 'utf-8', 'Accept': '*/*'}
    request_url = f'{base_url}{start_row}/{end_row}'
    if self.base_dt:
        request_url = f'{request_url}/{self.base_dt}'

    self.log.info(f'[DEBUG] API 요청 URL: {request_url}')
    response = requests.get(request_url, headers=headers)
    self.log.info(f'[DEBUG] 응답 코드: {response.status_code}')

    if response.status_code != 200 or not response.text:
        self.log.error(f'API 응답 오류 또는 빈 응답: {response.status_code}')
        return None

    try:
        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm, {}).get('row', [])
        return pd.DataFrame(row_data)
    except Exception as e:
        self.log.error(f'JSON 파싱 실패: {e}')
        return None

