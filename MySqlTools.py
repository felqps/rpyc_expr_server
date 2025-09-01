import pymysql
import sqlalchemy
import QpsUtil
import logging
import pandas as pd
class MySqlTools():
    def __init__(self, host, user, passwd, db, port=3306):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            db=db,
            charset="gbk",
        )
        self.cursor = self.conn.cursor()

    def execute(self, str_sql):
        self.cursor.execute(str_sql)
        data = self.cursor.fetchall()
        if "SELECT" in str_sql.upper():
            return data
        self.conn.commit()
    
    def executemany(self, str_sql, data):
        data = self.cursor.executemany(str_sql, data)
        if "SELECT" in str_sql.upper():
            return data
        self.conn.commit()

    def query(self, str_sql):
        data = pd.read_sql(str_sql, self.get_engine())
        return data

    def get_engine(self):
        self.engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.user}:{self.passwd}@{self.host}:{self.port}/{self.db}')
        return self.engine

    def replace_many(self, table_name, raws:"[{}]", id_keys=None):
        if len(raws) <= 0:
            print("No date save to db")
            return
        column_name = list(raws[0].keys())
        column_name.append("id")
        column_values_name = ["%s" for _ in column_name]
        column_values = []
        for raw in raws:
            values = []
            for k in column_name:
                if k == "id":
                    continue
                v = raw[k]
                values.append(v)
            if id_keys:
                id_values = [raw[id_key] for id_key in id_keys]
            else:
                id_values = values
            id = QpsUtil.buf2md5("_".join([str(v) for v in id_values]))
            values.append(id)
            column_values.append(values)

        self.insert_to_db(table_name, ",".join(column_name), ",".join(column_values_name), column_values)
        
    def insert_to_db(self, table_name, column_name, column_values_name, column_values):
        try:
            rpl_cmd = f'''replace into {table_name} (
                    {column_name}
                ) values (
                    {column_values_name}
                );'''
            self.executemany(rpl_cmd, column_values)
        except Exception as e:
            print(column_name)
            print(column_values_name)
            print(column_values)
            logging.exception(e)

    def close(self):
        self.conn.close()

def get_instance(host, user="feadmin", passwd="feadmin000", db="sddb_cn", port=3306):
    return MySqlTools(host, user, passwd, db, port)

if __name__ == '__main__':
    # db = get_instance("192.168.20.56")
    db = get_instance("192.168.0.56", db="sddb_cf")
    print(db.execute("SELECT * FROM sec_daily_raw limit 10 "))
    db.close()
    db = get_instance("192.168.20.98", user="root", passwd="Sh654321", db="db_cn", port=3306)
    print(db.execute("SELECT * FROM filrpt limit 10 "))
    db.close()
    db = get_instance("58.246.73.60", user="root", passwd="Sh654321", db="db_cn", port=3306)
    print(db.execute("SELECT * FROM filrpt limit 10 "))
    db.close()



