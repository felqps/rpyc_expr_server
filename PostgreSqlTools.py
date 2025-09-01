import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values,RealDictCursor
from pathlib import Path
import json
import pickle
import logging
import time
import shutil
import re
import hashlib
import pandas as pd
import numpy as np
import traceback
import threading
import datetime
from pandas._libs.tslibs.timestamps import Timestamp
from sqlalchemy import create_engine, Column, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from queue import Queue
import sqlglot
from sqlglot.expressions import Column, Star
from sqlglot import parse_one, exp
from collections import defaultdict
import os
from PostgresDataSaver import PostgresDataSaver
from common_colors import GREEN,CYAN,NC
from common_logging import init_logger,logger
from qpstimeit import timeit_m_real

column_change_queue = Queue()
cached_table_columns = defaultdict(set)

class QueryKiller(threading.Thread):
    def __init__(self, db_cfg, max_age_sec=60, check_interval=10):
        super().__init__(daemon=True)
        self.db_cfg = db_cfg
        self.max_age_sec = max_age_sec
        self.check_interval = check_interval
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        while self._running:
            try:
                with psycopg2.connect(**self.db_cfg) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            SELECT pid
                            FROM pg_stat_activity
                            WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')
                                AND now() - query_start > interval '{self.max_age_sec} seconds'
                                AND query NOT ILIKE '%pg_stat_activity%'
                                AND datname = current_database();
                        """)
                        for row in cur.fetchall():
                            pid = row[0]
                            logging.warning(f"[QueryKiller] Terminating long query pid={pid}")
                            cur.execute(f"SELECT pg_terminate_backend({pid})")
            except Exception as e:
                logging.exception("[QueryKiller Exception] " + str(e))
            time.sleep(self.check_interval)

class PostgreSqlData():
    def __init__(self, data_cache_dir=None):
        # self.structure_thread = threading.Thread(target=self._structure_worker, daemon=True)
        # self.structure_thread.start()
        self.disk_dirs = None
        self.data_cache_dir = data_cache_dir
        # self.data_cache_dir = "/tmp/data_file_cache"
        self.saver = PostgresDataSaver(data_cache_dir=self.data_cache_dir)
    
    def _structure_worker(self):
        while True:
            try:
                table_name, new_columns = column_change_queue.get()
                self.ensure_connection()

                with self.conn.cursor() as cur:
                    # cur.execute(f"SET statement_timeout = 5000")  # 5 秒超时
                    cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                    existing_columns = [desc[0] for desc in cur.description]
                    cached_table_columns[table_name] = set(existing_columns)

                    for col_sql in new_columns:
                        col_name = col_sql.split()[0].strip('"')
                        if col_name not in cached_table_columns[table_name]:
                            logging.info(f"[DDL] ALTER TABLE {table_name} ADD COLUMN {col_sql}")
                            try:
                                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_sql}")
                                self.conn.commit()
                                cached_table_columns[table_name].add(col_name)
                                if "rec_tm" == col_name:
                                    cur.execute(f"UPDATE {table_name} SET rec_tm = to_timestamp(id / 1000.0);")
                                    self.conn.commit()
                            except psycopg2.errors.DuplicateColumn:
                                self.conn.rollback()
                                logging.warning(f"[SKIP] Column already exists: {col_name}")
                            except Exception as e:
                                self.conn.rollback()
                                logging.warning(f"[DDL Failed] {col_name}: {e}")

                time.sleep(0.1)
            except Exception as e:
                logging.exception("[StructureWorker Exception]" + str(e))

    def get_sqlcmd(self, table_name, cols_type):
        if len(cols_type) > 0:
            return f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {','.join(cols_type)}
                );
            """
        return None

    def create_table(self, table_name, cols_type):
        with self.conn.cursor() as cur:
            try:
                sqlcmd = self.get_sqlcmd(table_name, cols_type)
                assert sqlcmd is not None, f"Missing SQL for creating the '{table_name}' table."
                cur.execute(sqlcmd)
                self.conn.commit()
                logging.info(f"Create {table_name} success")
            except Exception as e:
                self.conn.rollback()
                logging.exception(e)

    def convert_np_value(self, value):
        if isinstance(value, (np.generic, np.ndarray)):
            return value.item()
        return value

    def preprocess_data(self, data):
        cols_type = {}
        if type(data) == dict:
            for k,v in data.items():
                if type(v) == str:
                    cols_type[k] =f"\"{k}\" VARCHAR DEFAULT ''"
                elif type(v) == bool:
                    cols_type[k] =f"\"{k}\" BOOLEAN DEFAULT FALSE"
                elif type(v) in [int, np.int64]:
                    cols_type[k] =f"\"{k}\" BIGINT DEFAULT 0"
                elif type(v) in [float, np.float64]:
                    cols_type[k] =f"\"{k}\" DOUBLE PRECISION DEFAULT 0.0"
                    data[k] = self.convert_np_value(v)
                elif type(v) == list or type(v) == dict:
                    try:
                        cols_type[k] =f"\"{k}\" JSONB DEFAULT '{{}}'"
                        data[k] = json.dumps(v)
                    except (TypeError, OverflowError):
                        cols_type[k] =f"\"{k}\" BYTEA DEFAULT '{b''}'"
                        data[k] = pickle.dumps(v)
                elif type(v) == bytes:
                    cols_type[k] =f"\"{k}\" BYTEA DEFAULT '{b''}'"
                elif isinstance(v, (pd.DataFrame, pd.Series)):
                    cols_type[k] =f"\"{k}\" BYTEA DEFAULT '{b''}'"
                    data[k] = pickle.dumps(v)
                elif type(v) == type(None):
                    cols_type[k] =f"\"{k}\" VARCHAR DEFAULT ''"
                    # data[k] = str(v)
                elif type(v) in [Timestamp, datetime.datetime]:
                    cols_type[k] =f"\"{k}\" TIMESTAMPTZ"
                else:
                    cols_type[k] =f"\"{k}\" TEXT DEFAULT ''"
                    data[k] = str(v)
                    print("====== Unknow data type:", k, type(v))
                if k == "id":
                    cols_type[k] = cols_type[k] + " PRIMARY KEY"
        return cols_type,data

    def add_columns(self, table_name, columns_type):
        with self.conn.cursor() as cur:
            try:
                for column_type in columns_type:
                    sqlcmd = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_type}"
                    cur.execute(sqlcmd)
                    self.conn.commit()
                    col_name = column_type.split()[0].strip('"')
                    if col_name not in cached_table_columns[table_name]:
                        cached_table_columns[table_name].add(col_name)

                    if "rec_tm" in column_type:
                        sqlcmd = f"UPDATE {table_name} SET rec_tm = to_timestamp(id / 1000.0);"
                        cur.execute(sqlcmd)
                        self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                import traceback
                traceback_info = traceback.format_exc()
                del traceback
                assert False,traceback_info


    def is_primary_key(self, table_name: str, pk_field: str) -> bool:
        with self.conn.cursor() as cur:
            query = """
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                AND tc.table_name = %s
                AND kcu.column_name = %s;
            """
            cur.execute(query, (table_name, pk_field))
            return cur.fetchone() is not None

    def save_list(self, table_name: str, data: list[dict], debug: int = 0) -> int:
        if not isinstance(data, list):
            raise TypeError(f"Argument 'data' must be of type list, but got {type(data)}")

        success_count = 0
        total_count = len(data)
        for idx, row in enumerate(data):
            try:
                if self.save_dict(table_name, row, debug):
                    success_count += 1
                else:
                    if debug > 0:
                        print(f"[DEBUG] Failed to save row {idx + 1}/{total_count}: {row}")
            except Exception as e:
                if debug > 0:
                    print(f"[DEBUG] Exception occurred while saving row {idx + 1}/{total_count}: {e}, data: {row}")
        print(f"Save completed: Total {total_count} rows, Success {success_count} rows, Failed {total_count - success_count} rows")
        return success_count

    #@timeit_m_real
    def save_dict(self, table_name, data:dict, debug=0):
        self.ensure_connection()
        assert type(data) == dict, "[save_dict] Input data is not a dict"
        try:
            data = self.saver.save(data)
            # data["id"] = time.time_ns() // 1000000
            ts = time.time_ns() // 1000000
            if True or "rec_tm" not in data:
                data = {"rec_tm": pd.Timestamp(ts, unit="ms", tz="Asia/Shanghai")} | data

            if "id" not in data:
                data = {"id":ts} | data

            # data = {"id":ts, "rec_tm": pd.Timestamp(ts, unit="ms")}.update(data)
            cols_type,processed_data = self.preprocess_data(data)
            

            new_cols_needed = []
            if table_name not in cached_table_columns:
                try:
                    with self.conn.cursor() as cur:
                        # cur.execute("SET statement_timeout = 5000")
                        cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
                        cached_table_columns[table_name] = {desc[0] for desc in cur.description}
                except psycopg2.errors.UndefinedTable:
                    logging.warning(f"[Create] Table {table_name} not found. Creating...")
                    try:
                        self.create_table(table_name, list(cols_type.values()))
                        cached_table_columns[table_name] = set(cols_type.keys())
                        self.conn.commit()
                    except Exception as ce:
                        self.conn.rollback()
                        logging.exception("[Create Failed] " + str(ce))
                        logging.exception(e)
                        return False
                except Exception as e:
                    self.conn.rollback()
                    logging.exception("[Check Table Failed] " + str(e))
                    return False

            for k, col_sql in cols_type.items():
                if k not in cached_table_columns[table_name]:
                    new_cols_needed.append(col_sql)

            if len(new_cols_needed) > 0:
                # column_change_queue.put((table_name, new_cols_needed))
                self.add_columns(table_name, new_cols_needed)

            col_names = cached_table_columns[table_name]
            col_names = set(col_names) & set(processed_data.keys())
            pk_field = "id"
            str_col_names = ','.join([f'\"{k}\"' for k in col_names])
            if self.is_primary_key(table_name, pk_field):
                updates = ', '.join([f"\"{k}\"=EXCLUDED.\"{k}\"" for k in col_names if k != pk_field])
                insert_sql = f"INSERT INTO {table_name} ({str_col_names}) VALUES ({','.join(['%s']*len(col_names))}) ON CONFLICT ({pk_field}) DO UPDATE SET {updates}"
            else:
                insert_sql = f"INSERT INTO {table_name} ({str_col_names}) VALUES ({','.join(['%s']*len(col_names))})"

            try:
                with self.conn.cursor() as cur:
                    # cur.execute("SET statement_timeout = 5000")
                    if debug:
                        print("execute_sql:", insert_sql)
                    # print([(k,cols_type[k],processed_data[k]) for k in col_names])
                    cur.execute(insert_sql, [processed_data[k] for k in col_names])
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logging.exception("[Insert Failed] " + str(e))
                logging.exception(e)
                return False
        except Exception as e:
            self.conn.rollback()
            logging.exception(e)
            return False
        return True
    
    def extract_table_name(self, sql):
        pattern = re.compile(r"FROM\s+([^\s;]+)", re.IGNORECASE)
        match = pattern.search(sql)
        if match:
            return match.group(1)
        return None

    def extract_columns(self, sqlcmd: str):
        try:
            parsed = sqlglot.parse_one(sqlcmd)
            select_exprs = parsed.expressions
            results = []
        
            for expr in select_exprs:
                if isinstance(expr, Column): # table.column
                    alias = expr.alias_or_name
                    results.append(alias)
                elif isinstance(expr, Star): # SELECT * or a.*
                    results.append(str(expr))
                else: # COUNT(*), id + 1
                    alias = expr.alias_or_name or expr.sql()
                    results.append(alias)
            return results
        except Exception as e:
            logging.exception(e)
        return None

    def remove_columns_from_sql(self, sql: str, columns_to_remove: list) -> str:
        expression = parse_one(sql)
        select_exprs = expression.expressions
        filtered_exprs = [e for e in select_exprs if e.alias_or_name not in columns_to_remove]
        expression.set("expressions", filtered_exprs)
        return expression.sql()

    def execute_sql(self, sqlcmd, fast_query=False):
        print("execute_sql:", sqlcmd)
        self.ensure_connection()
        with self.conn.cursor(cursor_factory=RealDictCursor) as dict_cur:
            try:
                sql_upper = sqlcmd.strip().upper()
                # if sql_upper.startswith("SELECT"):
                if "SELECT" in sql_upper:
                    return self.query_data(sqlcmd, fast_query=fast_query)
                else:
                    dict_cur.execute(sqlcmd)
                    self.conn.commit()

            except Exception as e:
                self.conn.rollback()
                logging.exception(e)
                import traceback
                traceback_info = traceback.format_exc()
                del traceback
                return traceback_info
            return {"status": "success", "message": f"SQL ({sqlcmd}) executed successfully."}

    def read_disk_file(self, filepath):
        # src_filepath,hash = parse_filename_with_hash(filepath)
        # cache_filepath = f"{self.data_cache_dir}/{os.path.basename(filepath)}"
        # if self.data_cache_dir:
        #     if not os.path.exists(cache_filepath):
        #         cache_disk_file(filepath, self.data_cache_dir)
        #     src_filepath = cache_filepath
        src_filepath = cache_disk_file(filepath, self.data_cache_dir)
        logger().info(f"{CYAN}Read data from '{src_filepath}'{NC}")
        fp = Path(src_filepath)
        if not fp.is_file():
            return filepath
        d = fp.read_bytes()
        try:
            d = pickle.loads(d)
        except pickle.UnpicklingError:
            pass
        except Exception as e:
            logging.exception(e)
        return d

    @timeit_m_real
    def query_data(self, sqlcmd, table_names=None, fast_query=False, fetchall=True):
        try:
            if fast_query:
                fast_query = not any(v in sqlcmd.upper() for v in ["JOIN", "COUNT"])
            if table_names is None:
                table_names = get_tables_from_sqlcmd(sqlcmd)
            column_types = {}
            [column_types.update(self.get_column_types(table_name)) for table_name in table_names]

            columns_to_remove = []
            if False and fast_query:
                # sqlcmd_columns = self.extract_columns(sqlcmd)
                if "*" in sqlcmd:
                    sqlcmd = sqlcmd.replace("*", ",".join([f"\"{k}\"" for k in column_types]))
                columns_to_remove = [k for k,v in column_types.items() if v in ["bytea", "json", "jsonb"]]
                # sqlcmd = self.remove_columns_from_sql(sqlcmd, columns_to_remove)
                
                temp_sqlcmd = sqlcmd
                from_index = sqlcmd.upper().find("FROM")
                for col_remove in columns_to_remove:
                    if col_remove in sqlcmd:
                        col_index = sqlcmd.find(col_remove)
                        if col_index < from_index:
                            temp_sqlcmd = temp_sqlcmd.replace(f"\"{col_remove}\"", f"E'{col_remove}'::bytea as \"{col_remove}\"")
                sqlcmd = temp_sqlcmd
            print(f"{GREEN}Processed sqlcmd:{NC} {sqlcmd}")
            with self.conn.cursor(cursor_factory=RealDictCursor) as dict_cur:
                dict_cur.execute(sqlcmd)
                if fetchall:
                    rows = dict_cur.fetchall()
                else:
                    return (None, None) #caller should not expect anything if fetchall=False

            return_rows = []
            for row in rows:
                return_row = {}
                # for k,v in column_types.items():
                #     if k in row:
                #         if v in ["json", "jsonb"]:
                #             return_row[k] = k if fast_query else row[k]
                #             # row[k] = json.loads(v)
                #         elif v in ["bytea"]:
                #             try:
                #                 return_row[k] = k if fast_query else pickle.loads(row[k])
                #             except pickle.UnpicklingError:
                #                 pass
                #             except Exception as e:
                #                 logging.exception(e)
                #     elif k in columns_to_remove:
                #         return_row[k] = k
                #     else:
                #         return_row[k] = row[k]
                

                for k,v in row.items():
                    column_type = column_types.get(k)
                    if self.is_postgres_disk_data(v) and not fast_query:
                        v = self.read_disk_file(v)

                    if column_type and column_type in ["json", "jsonb"]:
                        return_row[k] = k if fast_query else v
                        # row[k] = json.loads(v)
                    elif column_type and column_type in ["bytea"]:
                        try:
                            v = pickle.loads(v)
                        except pickle.UnpicklingError:
                            pass
                        except Exception as e:
                            logging.exception(e)
                        return_row[k] = k if fast_query else v
                    else:
                        return_row[k] = v

                # for col_remove in columns_to_remove:
                #     return_row[col_remove] = col_remove
                return_rows.append(return_row)
        except Exception as e:
            import traceback
            traceback_info = traceback.format_exc()
            self.conn.rollback()
            logging.exception(e)
            del traceback
            return traceback_info

        return (column_types, return_rows)

    def is_postgres_disk_data(self, filepath):
        if not self.disk_dirs:
            self.disk_dirs = eval(Path("/qpsdata/config/postgres_disk_dirs.cfg").read_text())
            self.disk_dirs = [Path(dir).resolve() for dir in self.disk_dirs]
        # return (isinstance(data, str) and "/NASQPS08.qpsdata/Postgres_data" in data) and os.path.exists(data)
        return any(str(filepath).startswith(str(dir)) for dir in self.disk_dirs)

class PostgreSqlTools(PostgreSqlData):
    def __init__(self, db_config_file, data_cache_dir=None):
        super().__init__(data_cache_dir)
        self.db_path = db_config_file
        self.db_cfg = eval(Path(db_config_file).read_text())
        self.connect()

        # self.query_killer = QueryKiller(self.db_cfg)
        # self.query_killer.start()

    def connect(self):
        self.conn = psycopg2.connect(**self.db_cfg)
        self.conn.autocommit = True
        # self.cursor = self.conn.cursor()
        # self.dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        self.engine = create_engine(f"postgresql+psycopg2://{self.db_cfg['user']}:{self.db_cfg['password']}@{self.db_cfg['host']}:{self.db_cfg['port']}/{self.db_cfg['dbname']}")
        self.session = sessionmaker(bind=self.engine)

    def execute_values(self, sql, data):
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                sql,
                data
            )
            self.conn.commit()

    def ensure_connection(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchall()
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            logging.warning("Lost DB connection, reconnecting...")
            self.connect()

    def check_table_exists(self, table_name, schema_name='public'):
        try:
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (schema_name, table_name))
                exists = cur.fetchone()[0]
                return exists
        except Exception as e:
            print(f"Error checking if table exists: {e}")
            return False
    
    def get_column_types(self, table_name):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT 1")
            desc = cur.description
            type_oids = [col.type_code for col in desc]

            cur.execute(
                "SELECT oid, typname FROM pg_type WHERE oid IN %s",
                (tuple(set(type_oids)),)
            )
            oid_name_map = dict(cur.fetchall())
            return {col.name: oid_name_map.get(col.type_code, 'UNKNOWN') for col in desc}

    def get_engine(self):
        return self.engine

    def close(self):
        self.conn.close()

_instance_cache = {}
data_cache_dir = "/tmp/data_file_cache" if os.path.exists("/tmp/data_file_cache") else None
def postgres_instance(db_config_file="/qpsdata/config/PostgreSql.cfg"):
    if db_config_file not in _instance_cache:
        _instance_cache[db_config_file] = PostgreSqlTools(db_config_file, data_cache_dir=data_cache_dir)
    return _instance_cache[db_config_file]

def new_postgres_instance(db_config_file="/qpsdata/config/PostgreSql.cfg"):
    return PostgreSqlTools(db_config_file, data_cache_dir=data_cache_dir)

def get_columns_before_tables_from_sqlcmd(sqlcmd):
    parsed = sqlglot.parse_one(sqlcmd)
    sql_str = parsed.sql()
    from_pos = sql_str.upper().find("FROM")
    if from_pos == -1:
        return []
    columns = []
    for column in parsed.find_all(exp.Column):
        col_str = column.sql()
        col_pos = sql_str.find(col_str)
        if col_pos != -1 and col_pos < from_pos:
            if col_str not in columns:
                columns.append(col_str)
    return columns

def get_columns_from_sqlcmd(sqlcmd):
    parsed = sqlglot.parse_one(sqlcmd)
    columns = []
    for col in parsed.find_all(exp.Column):
        col_str = col.sql()
        if col_str not in columns:
            columns.append(col_str)
    return columns

def get_tables_from_sqlcmd(sqlcmd):
    parsed = sqlglot.parse_one(sqlcmd)
    tables = []
    for table in parsed.find_all(exp.Table):
        table_str = table.sql()
        if table_str not in tables:
            tables.append(table_str)
    return tables

def format_postgres_sqlcmd(sqlcmd):
    columns = get_columns_from_sqlcmd(sqlcmd)
    tables = get_tables_from_sqlcmd(sqlcmd)
    for s in columns + tables:
        sqlcmd = sqlcmd.replace(s, f'"{s}"')
    return sqlcmd

def calculate_hash(filepath):
    if os.path.exists(filepath):
        data = Path(filepath).read_bytes()
    else:
        data = b""
    hash = hashlib.md5(data).hexdigest()
    return hash

def append_hash_to_filename(filepath):
    dirname, filename = os.path.split(filepath)
    name, ext = os.path.splitext(filename)
    # if re.fullmatch(r"[a-fA-F0-9]{16,64}", name):
    #     return filepath
    file_hash = calculate_hash(filepath)
    new_name = f"{name}__{file_hash}{ext}"
    return os.path.join(dirname, new_name)

def parse_filename_with_hash(filepath):
    dirname = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    name, ext = os.path.splitext(filename)
    match = re.match(r"(.+)__([a-f0-9]{8,64})$", name)
    if match:
        original_name, hash_part = match.groups()
        return f"{dirname}/" + original_name + ext, hash_part
    elif re.fullmatch(r"[a-fA-F0-9]{16,64}", name):
        return f"{dirname}/" + filename, name
    else:
        return f"{dirname}/" + filename, None

def cache_disk_file(filepath, data_cache_dir):
    src_filepath,hash = parse_filename_with_hash(filepath)
    if not (data_cache_dir and type(src_filepath) == str and src_filepath and hash is not None):
        return src_filepath

    data_cache_dir = f"{data_cache_dir}/{hash[:2]}"
    cache_file_path = f"{data_cache_dir}/{os.path.basename(filepath)}"
    if not os.path.exists(cache_file_path):
        if os.path.exists(src_filepath):
            os.makedirs(data_cache_dir, exist_ok=True)
            shutil.copy2(src_filepath, cache_file_path)
            logger().info(f"{CYAN}Cache '{src_filepath}' to '{cache_file_path}'{NC}")
        else:
           cache_file_path = src_filepath
    return cache_file_path

if __name__ == "__main__":
    init_logger(level="debug")
    cfg_file = "/qpsdata/config/PostgreSql.cfg"
    postgres = postgres_instance(cfg_file)
    assert postgres is postgres_instance(cfg_file)

    assert parse_filename_with_hash("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac5b06ed6915d6fcaff27d21a1__bf6b34309d2b8dd4d9ed123225a98967.fds") == ("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac5b06ed6915d6fcaff27d21a1.fds", "bf6b34309d2b8dd4d9ed123225a98967")
    assert parse_filename_with_hash("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac5b06ed6915d6fcaff27d21a1.fds") == ("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac5b06ed6915d6fcaff27d21a1.fds", "9eeaac5b06ed6915d6fcaff27d21a1")
    assert parse_filename_with_hash("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac_5b06ed_6915d6fca_ff27d21a1.fds") == ("/NASQPS04.qpsdata/fdf/cs_F/1d/77/9eeaac_5b06ed_6915d6fca_ff27d21a1.fds", None)
    # print(postgres.query_data(f'select * from performance_eval_details_new limit 1', fast_query = True))
    # exit()
    print("======test fun")
    sqlcmd = 'SELECT users.id, orders.amount, orders_amount, "orders-amount", name AS user_name FROM db1.public.users JOIN db2.sales.orders ON users.id = orders.user_id'
    assert get_columns_before_tables_from_sqlcmd(sqlcmd) == ['users.id', 'orders.amount', 'orders_amount', '"orders-amount"', 'name']
    assert get_columns_from_sqlcmd(sqlcmd) == ['users.id', 'orders.amount', 'orders_amount', '"orders-amount"', 'name', 'orders.user_id']
    assert get_tables_from_sqlcmd(f"SELECT id AS uid, COUNT(*) AS cnt FROM logs") == ['logs']
    assert get_columns_from_sqlcmd(f"SELECT id + 1 AS new_id, ROUND(score, 2) FROM grades") == ['score', 'id']
    assert get_columns_from_sqlcmd("SELECT * FROM test1 JOIN test2 ON test1.id = test2.id;") == ['test1.id', 'test2.id']
    assert get_tables_from_sqlcmd("SELECT * FROM test1 JOIN test2 ON test1.id = test2.id;") == ['test1', 'test2']
    assert format_postgres_sqlcmd("""
        SELECT 
            to_char(to_timestamp(id / 1000), 'YYYY-MM-DD HH24:00:00') AS hour,
            COUNT(*) AS count
        FROM expr_eval_by_corr where symset = 'CS_ALL'
        GROUP BY hour
        ORDER BY hour;
    """) == """
        SELECT 
            to_char(to_timestamp("id" / 1000), 'YYYY-MM-DD HH24:00:00') AS "hour",
            COUNT(*) AS count
        FROM "expr_eval_by_corr" where "symset" = 'CS_ALL'
        GROUP BY "hour"
        ORDER BY "hour";
    """

    print("====== save dict")
    postgres = new_postgres_instance(cfg_file)
    postgres.execute_sql(f"DELETE FROM z_regtest_1")
    postgres.execute_sql(f"DELETE FROM z_regtest_2")
    
    data_list = []
    for i in range(3):
        import uuid
        uid = uuid.uuid1()
        data = {
            "dict": {"a":"aa", "b":"bb"},
            "list": [1, 2, 3],
            "df": pd.DataFrame([{"a":"aa", "b":"bb"}, {"a":"aa", "b":"bb"}]),
            "png":  Path("/Fairedge_dev/app_regtests/lowopn01_cumret.png").read_bytes(),
            "pdf":  Path("/Fairedge_dev/app_regtests/lowopn01.pdf").read_bytes(),
            "str": "aaaaaa",
            "uuid": uid,
            "int": 1213,
            "float": 12.123,
            "bool":True,
            "None": None,
            "Timestamp":  pd.Timestamp(time.time_ns() // 1000000, unit="ms", tz="Asia/Shanghai")
        }
        postgres.save_dict("z_regtest_1", data)
        data_list.append(data)
    
    postgres.save_list("z_regtest_1", data_list)

    postgres.execute_sql(f"ALTER TABLE z_regtest_2 DROP COLUMN uuid")
    for i in range(3):
        data = {
            "dict": {"a":"aa", "b":"bb"},
            "list": [1, 2, 3],
            "df": pd.DataFrame([{"a":"aa", "b":"bb"}, {"a":"aa", "b":"bb"}]),
            "png":  Path("/Fairedge_dev/app_regtests/lowopn01_cumret.png").read_bytes(),
            "str": "aaaaaa",
        }
        postgres.save_dict("z_regtest_1", data)
        data = {
            "dict1": {"a":"aa", "b":"bb"},
            "list1": [1, 2, 3],
            "df1": pd.DataFrame([{"a":"aa", "b":"bb"}, {"a":"aa", "b":"bb"}]),
            "png1":  Path("/Fairedge_dev/app_regtests/lowopn01_cumret.png").read_bytes(),
            "str1": "aaaaaa",
            "uuid": uid
        }
        postgres.save_dict("z_regtest_2", data)
    # exit()
    print(postgres.execute_sql("SELECT z_regtest_1.id, z_regtest_2.rec_tm FROM z_regtest_1 JOIN z_regtest_2 ON z_regtest_1.uuid = z_regtest_2.uuid;", fast_query=False))
    print(postgres.execute_sql("SELECT z_regtest_1.id, z_regtest_2.rec_tm FROM z_regtest_1 JOIN z_regtest_2 ON z_regtest_1.uuid = z_regtest_2.uuid;", fast_query=True))
    print(postgres.execute_sql("select * from (SELECT z_regtest_1.id, z_regtest_2.rec_tm FROM z_regtest_1 JOIN z_regtest_2 ON z_regtest_1.uuid = z_regtest_2.uuid) as temp_table order by id", fast_query=True))
    # postgres.execute_sql("SELECT * FROM z_regtest_1", fast_query=False)

    print("====== connect wan")
    postgres = postgres_instance("/qpsdata/config/PostgreSql_wan.cfg")
    d = postgres.execute_sql("SELECT * FROM z_regtest_1", fast_query=True)
    print(d)
    print(pd.read_sql('select * from z_regtest_1', postgres.get_engine()))

    print("====== connect lan")
    postgres = postgres_instance(cfg_file)
    
    d = postgres.execute_sql("SELECT * FROM z_regtest_1", fast_query=True)
    print(d)
    print(pd.read_sql('select * from z_regtest_1', postgres.get_engine()))
    print(postgres.check_table_exists("z_regtest_1"))

    
