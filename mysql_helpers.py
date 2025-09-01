#!/usr/bin/env python
import os,sys
from MySqlTools import MySqlTools
import pandas as pd
from common_options_helper import get_options
from fdf_helpers import print_df

DB_ENGINE = None

def db_engine():
    global DB_ENGINE
    if DB_ENGINE is None:
        DB_ENGINE = MySqlTools(host="192.168.20.98", port=3306, user="root", passwd="Sh654321", db="db_cn").get_engine()
        return DB_ENGINE
    return DB_ENGINE

def dbquery2fdf(df, refprc, fldnm, timedelta='15:00:00', debug=False):
    #assuming the presence of [date, sym, fldnm]
    if timedelta!='':
        df['date'] = pd.to_datetime(df['date']) + pd.Timedelta(timedelta)
        print_df(df, title="setdtm")
    df = df[['date', 'sym', fldnm]]
    #posFld = posFld[posFld['date']<'20231101']
    #print(posFld[posFld['sym']=='000789'])
    df.set_index(['date', 'sym'], inplace=True)
    #print(posFld.tail(100))
    # print(f"posFld shape= {posFld.shape}", file=sys.stderr)
    #posFld.index = posFld.index.drop_duplicates()
    #print("DEBUG_1124:", posFld.index[not posFld.index.duplicated()])
    df = df[fldnm].unstack('sym') 
    fdfRef = (refprc * 0.0).fillna(0)
    if debug:
        print_df(fdfRef, title="ref")
    df.rename(columns=sym_to_exch_map(df.columns, fdfRef.columns), inplace=True)
    if debug:
        print_df(df, title="bfore")
    df = (fdfRef + df).fillna(0)
    #posFld = posFld.dropna(axis=0, how='all')
    df = df.loc[fdfRef.index] #only select rows in the refprc dataframe
    if debug:
        print_df(df, title="after")

    return df

def sym_to_exch_map(insyms, mapsyms):
    #print(mapsyms)
    symmap = {}
    for x in insyms:
        xx = "%06d"%(int(x))
        #print(x, file=sys.stderr)
        if f'{xx}.XSHE' in mapsyms:
            symmap[x] = f'{xx}.XSHE'
        elif f'{xx}.XSHG' in mapsyms:
            symmap[x] = f'{xx}.XSHG'
        else:
            symmap[x] = xx
        #print(x, symmap[x])
    return symmap

def get_trade_records(bookid='strat001_prod'):
    #return pd.read_sql(f"SELECT * FROM filrpt AS rpt WHERE rpt.bookid = '{bookid}' AND rpt.date > 20221230 ORDER BY date", db_engine())
    sql = f"SELECT * FROM filrpt AS rpt WHERE rpt.bookid = '{bookid}' ORDER BY date"
    print(f"INFO: sql= {sql}")
    return pd.read_sql(sql, db_engine())  #must get all trades, otherwise daily position will be wrong.

def get_position_records(bookid='strat001_prod'):
    return pd.read_sql(f"SELECT * FROM open_position AS pos WHERE pos.bookid = '{bookid}' AND pos.date < 20231115 ORDER BY pos.date", db_engine())

def get_fund_netvalue_records(begdt='20230101'):
    #return pd.read_sql(f"SELECT * FROM netvalue limit 100", db_engine())
    return pd.read_sql(f"SELECT fund_id, fund_name, date, netvalue, net_assets FROM netvalue where date > {begdt} ORDER BY date, fund_id", db_engine())

if __name__ == "__main__":
    funcn = "mysql_helpers.main"
    opt, args = get_options(funcn)

    df = get_trade_records()
    print(set(df['bookid']))
    print(df)

