import os
import hashlib
import pickle
import pandas as pd
from common_smart_dump import smart_dump
from collections.abc import Iterable, Mapping
from common_logging import dbg
from common_basic import use_cache_if_exists

class PostgresDataSaver():
    def __init__(self, base_dir="/NASQPS08.qpsdata/Postgres_data", dryrun=0, group_name="default_group", data_cache_dir=None):
        self.base_dir = use_cache_if_exists(base_dir, "/NASQPS08.qpsdata.cache/Postgres_data")
        self.pandas_data_types = (pd.DataFrame, pd.Series)
        self.dryrun = dryrun
        self.group_name = group_name
        self.data_cache_dir = data_cache_dir

    def is_pandas_data(self, data):
        isdf = isinstance(data, self.pandas_data_types)
        if isdf:
            return isdf
        if isinstance(data, dict):
            for v in data.values():
                isdf = self.is_pandas_data(v)
                if isdf:
                    return isdf
        elif isinstance(data, list):
            for v in data:
                isdf = self.is_pandas_data(v)
                if isdf:
                    return isdf
        return isdf

    def is_aggregate_type(self, obj):
        # if isinstance(obj, (str, bytes)):
        #     return False
        # return isinstance(obj, (list, tuple, set, frozenset, dict, Mapping, Iterable))
        return isinstance(obj, (list, tuple, set, frozenset, dict))

    def save(self, data_dict):
        result = {}
        for key, value in data_dict.items():
            if type(value) == memoryview:
                value = bytes(value)
            # try:
            #     value = pickle.loads(value)
            # except:
            #     pass
            result[key] = self._save_value(value)
        return result

    def _save_value(self, value):
        return_value = value
        if self.is_pandas_data(value):
            return_value = self._save_dataframe(value)
            # cache_disk_file(return_value, self.data_cache_dir)
        elif self.is_aggregate_type(value):
            return_value = self._save_aggregate_data(value)
            # cache_disk_file(return_value, self.data_cache_dir)
        elif isinstance(value, bytes):
            return_value = self._save_bytes(value)
            # cache_disk_file(return_value, self.data_cache_dir)
        return return_value

    def _save_dataframe(self, df):
        path = self._build_storage_path(df, "db")
        if not self.dryrun:
            smart_dump(df, path)
        return path

    def _save_aggregate_data(self, obj):
        path = self._build_storage_path(obj, "db")
        if not self.dryrun:
            smart_dump(obj, path)
        return path

    def _save_bytes(self, data):
        if data[:4] == b"%PDF":
            ext = "pdf"
        elif data[:8] == b"\x89PNG\r\n\x1a\n":
            ext = "png"
        else:
            ext = "bin"

        path = self._build_storage_path(data, ext)
        if not self.dryrun and not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(data)
                dbg(f"INFO: write bytes data to '{path}'")
        return path

    def _build_storage_path(self, data_bytes:bytes, original_name):
        if type(data_bytes) != bytes:
            data_bytes = pickle.dumps(data_bytes)
        hash = hashlib.md5(data_bytes).hexdigest()
        subdir = os.path.join(
            self.base_dir,
            hash[:2],
            # hash[2:4],
            # hash[4:6],
        )
        os.makedirs(subdir, exist_ok=True)
        return os.path.join(subdir, f"{hash}.{original_name}")

if __name__ == "__main__":
    saver = PostgresDataSaver(dryrun=0, group_name="test_group")
    import uuid
    from pathlib import Path
    import time
    from datetime import datetime
    uid = uuid.uuid1()
    ts = time.time_ns() // 1000000
    ts = int(datetime.strptime("20250701 150000", "%Y%m%d %H%M%S").timestamp() * 1000)
    data = {
        "id":ts,
        "rec_tm": pd.Timestamp(ts, unit="ms", tz="Asia/Shanghai"),
        "dict": {"a":"aa", "b":"bb"},
        "list": [1, 2, 3],
        "df": pickle.dumps(pd.DataFrame([{"a":"aa", "b":"bb"}, {"a":"aa", "b":"bb"}])),
        "df1": pd.DataFrame([{"a":"aa", "b":"bb"}, {"a":"aa", "b":"bb"}]),
        "png":  Path("/Fairedge_dev/app_regtests/lowopn01_cumret.png").read_bytes(),
        "pdf":  Path("/Fairedge_dev/app_regtests/lowopn01.pdf").read_bytes(),
        "str": "aaaaaa",
        "uuid": uid,
    }
    result_data = saver.save(data)
    # print(result_data)
    tmpl_result_data = {
        'id': 1751353200000, 
        'rec_tm': pd.Timestamp(ts, unit="ms", tz="Asia/Shanghai"),
        'dict': "/NASQPS08.qpsdata/Postgres_data/cb/cb8d84f0c8ede01e0e5156c69f92d1bf.db",
        'list': "/NASQPS08.qpsdata/Postgres_data/47/4795d80f956d95feab5f1dedc7b51792.db", 
        'df': '/NASQPS08.qpsdata/Postgres_data/44/442cae791cb186ecafd4063d67bec5a7.bin',
        'df1': '/NASQPS08.qpsdata/Postgres_data/44/442cae791cb186ecafd4063d67bec5a7.db',
        'png': '/NASQPS08.qpsdata/Postgres_data/46/4619e77bb9d33cf26bbdc4fc8a2d8b76.png',
        'pdf': '/NASQPS08.qpsdata/Postgres_data/22/2206f3da89399df9c41f2a45f7fe2da1.pdf',
        'str': 'aaaaaa',
        'uuid': uid,
    }
    assert result_data == tmpl_result_data

