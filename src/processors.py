from __future__ import annotations
from repo.postgres import Database
import json
import os
import requests

class ApiNodes:
    def __init__(
            self, db: Database, node_type: str,
            api_header: dict, api_endpoint: str, db_view: str):
        self.db = db
        self.node_type = node_type
        self.api_header = api_header
        self.api_endpoint_upsert = os.path.join(api_endpoint, "upsert")
        self.db_view = db_view
        self.rows: list 
        self.len_rows: int

    def _select(self) -> ApiNodes:
        qstr = f"SELECT * FROM {self.db_view};"
        rows = self.db.query(qstr)
        len_rows = len(rows) - 1 
        self.rows = rows
        self.len_rows = len_rows
        return self

    def _populate(self, main_dict: dict, row: dict) -> dict:
        data = {}
        data['id'] = row.get('id', None)
        data['parentId'] = row.get('parent_id', None)
        data['type'] = row.get('type', None)
        data['document'] = row.get('document', None)
        data['attributes'] = row.get('attributes', None)
        # data['createdAt'] = ''
        created_at = row.get('created_at', None)
        if created_at:
            data['createdAt'] = created_at.strftime("%m/%d/%Y %H:%M:%S.%f")
        main_dict[self.node_type].append(data)
        return main_dict

    def _validate(self, main_dict: dict) -> str:
        for row in main_dict[self.node_type]:
            has_id = row.get('id', None)
            if not has_id:
                return 'Each record must contain an id.'
            has_type = row.get('type', None)
            if not has_type:
                return 'Each record must contain a type.'
        return ''

    def _post(self, main_dict: dict) -> str:

        # validate data
        err = self._validate(main_dict)
        if err:
            return err

        # encode json
        jdata = json.dumps(main_dict)

        # post
        r = requests.post(
                self.api_endpoint_upsert, headers=self.api_header, data=jdata)
        status = f"Upload status {r.status_code}"

        if r.status_code != 200:
            status = f"## Houston, we have a problem {r.status_code}"
        return status

    def _upsert(self, chunk_by: int) -> None:
        """upsert to SvApi"""

        # chunk the data becuase an api request is limited by size
        # {'nodes': [data, data, data]}
        main_dict = {}
        main_dict[self.node_type] = []
        chunk = chunk_by - 1
        for row in self.rows:
            if self.len_rows == 0 or chunk == 0: # last one or chunk
                main_dict = self._populate(main_dict, row)
                status = self._post(main_dict)
                print(status)
                main_dict = {}
                main_dict[self.node_type] = []
                chunk = chunk_by - 1
                self.len_rows -= 1
            else:
                main_dict = self._populate(main_dict, row)
                chunk -= 1
                self.len_rows -= 1

    def upserter(self, chunk_by: int) -> None:
        self._select()
        self._upsert(chunk_by)


class ApiEdges:
    def __init__(
            self, db: Database, node_type: str,
            api_header: dict, api_endpoint: str, db_view: str):
        self.db = db
        self.node_type = node_type
        self.api_header = api_header
        self.api_endpoint_upsert = os.path.join(api_endpoint, "upsert")
        self.db_view = db_view
        self.rows: list 
        self.len_rows: int

    def _select(self) -> ApiEdges:
        qstr = f"SELECT * FROM {self.db_view};"
        rows = self.db.query(qstr)
        len_rows = len(rows) - 1 
        self.rows = rows
        self.len_rows = len_rows
        return self

    def _populate(self, main_dict: dict, row: dict) -> dict:
        data = {}
        data['fromId'] = row.get('from_id', None)
        data['toId'] = row.get('to_id', None)
        data['attributes'] = row.get('attributes', None)
        main_dict[self.node_type].append(data)
        return main_dict

    def _validate(self, main_dict: dict) -> str:
        for row in main_dict[self.node_type]:
            has_fid = row.get('fromId', None)
            if not has_fid:
                return 'Each record must contain a from_id.'
            has_tid = row.get('toId', None)
            if not has_tid:
                return 'Each record must contain a to_id.'
        return ''

    def _post(self, main_dict: dict) -> str:

        # validate data
        err = self._validate(main_dict)
        if err:
            return err

        # encode json
        jdata = json.dumps(main_dict)

        # post
        r = requests.post(
                self.api_endpoint_upsert, headers=self.api_header, data=jdata)
        status = f"Upload status {r.status_code}"

        if r.status_code != 200:
            status = f"## Houston, we have a problem {r.status_code}"
        return status

    def _upsert(self, chunk_by: int) -> None:
        """upsert to SvApi"""

        # chunk the data becuase an api request is limited by size
        # {'nodes': [data, data, data]}
        main_dict = {}
        main_dict[self.node_type] = []
        chunk = chunk_by - 1
        for row in self.rows:
            if self.len_rows == 0 or chunk == 0: # last one or chunk
                main_dict = self._populate(main_dict, row)
                status = self._post(main_dict)
                print(status)
                main_dict = {}
                main_dict[self.node_type] = []
                chunk = chunk_by - 1
                self.len_rows -= 1
            else:
                main_dict = self._populate(main_dict, row)
                chunk -= 1
                self.len_rows -= 1

    def upserter(self, chunk_by: int) -> None:
        self._select()
        self._upsert(chunk_by)
