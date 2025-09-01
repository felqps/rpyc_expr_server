#!/usr/bin/env python

import sys
import os
import hashlib
from  fdf_colors import *
import pandas as pd
import numpy as np
from collections import Counter
import pickle
import datetime
from dateutil.parser import parse as dtparse


def zero2nan(df):
    df = df.replace({0: np.nan})
    return df

def tiny2zero(df):
    tiny = 1e-6
    res_neg = df.copy()
    res_pos = df.copy()
    res_neg[res_neg>-tiny] = 0.0
    res_pos[res_pos<tiny] = 0.0
    res_pos[res_pos==np.inf] = 0.0
    res_neg[res_neg==np.inf] = 0.0
    res_pos[res_pos==-np.inf] = 0.0
    res_neg[res_neg==-np.inf] = 0.0
    res = res_pos + res_neg
    return res

def tiny2nan(df):
    return zero2nan(tiny2zero(df))

def nonzero2one(df):
    res = df.copy()
    res[res!=0] = 1
    return res

def one2nan(df):
    df = df.replace({1: np.nan})
    return df

def reverse_bool(df):
    return df.replace({1:np.nan, np.nan:1, 0:1})

def bool2num(df):
    df = df.replace({True: 1.0, False: 0.0})
    return df  

def false2nan(df):
    df = df.replace({True: 1.0, False: np.nan})
    return df  

def bool2numinv(df):
    df = df.replace({True: 0.0, False: 1.0})
    return df  

def df_non_uniq_columns(df):
    if df is None:
        return None
    column_counts = Counter(df.columns)
    non_uniq_columns = [col for col,count in column_counts.items() if count>1]
    return non_uniq_columns

def df_uniq_columns(df):
    column_counts = Counter(df.columns)
    non_uniq_columns = [col for col,count in column_counts.items() if count<=1]
    return non_uniq_columns

def get_rightmost_non_nan(row):
    for item in row[::-1]:
        if not pd.isna(item):
            return item
    return np.nan

def df_merge_columns(df):
    return df.apply(get_rightmost_non_nan, axis=1)
    #return df.iloc[:,0]

def df_merge_non_uniq_columns(df, copy=False):
    if df is None:
        return df
    if len(df_non_uniq_columns(df))<=0:
        if copy:
            return df.copy()
        else:
            return df
    df_uniq = df[df_uniq_columns(df)].copy()
    for col in df_non_uniq_columns(df):
        df_uniq.loc[:,col] = df_merge_columns(df[col])
    return df_uniq

def demean_by_row(df):
    df = df.sub(df.mean(axis=1), axis=0)
    #print(df.sum(axis=1))
    return df

def df_get_top(x, n=2, axis=1):
    funcn = f"get_top({n})"
    x = x.replace({0: np.nan})
    x = demean_by_row(x)
    rank_hgh = x.fillna(-1e10).rank(axis=axis, ascending=False, method='min') 
    #df_inspect(rank_hgh, ch='rank_hgh')
    
    rank_hgh = rank_hgh[rank_hgh>n]
    #print_df(rank_hgh, title='rank_hgh2')
    result = x.copy()
    result[rank_hgh>n] = 0.0
    if 0:
        print(funcn, "\n", result.tail(3))
    return result

def df_get_bottom(x, n=2, axis=1):
    funcn = f"get_bottom({n})"
    x = x.replace({0: np.nan})
    x = demean_by_row(x)
    rank_low = x.fillna(1e10).rank(axis=axis, ascending=True, method='min') 
    
    rank_low = rank_low[rank_low>n]
    
    #print_df(rank_low, title='rank_low2')
    result = x.copy()
    result[rank_low>n] = 0.0
    if 0:
        print(funcn, "\n", result.tail(3))
    return result

def data_between_dates(df, begDt, endDt, delta_days=0):
    (begDtm, endDtm) = (dtparse(f'{begDt} 00:00:00') + datetime.timedelta(delta_days), dtparse(f'{endDt} 23:59:59') + datetime.timedelta(delta_days))
    if type(df) == type(pd.DataFrame()):    
        return df[begDtm:endDtm]
    elif type(df) == type(dict()):
        newdf = {}
        for k,v in df.items():
            newdf[k] = v[begDtm:endDtm]
        return newdf
    else:
        return df

def df_inspect(df, ch='o', rows=10, stop=True):
    if ch.find(':')<0:
        title='na'
    else:
        ch,title=ch.split(r':')

    print(f"-{ch}"*10, title, file=sys.stderr)
    print(df.tail(rows))
    if stop:
        exit(0)

def count_gt(df, value=1e-6):
    c = (df.fillna(0).to_numpy() > value).sum()
    return c
def count_ge(df, value=1e-6):
    c = (df.fillna(0).to_numpy() >= value).sum()
    return c
def count_lt(df, value=-1e-6):
    c = (df.fillna(0).to_numpy() < value).sum()
    return c
def count_le(df, value=-1e-6):
    c = (df.fillna(0).to_numpy() <= value).sum()
    return c

if __name__ == "__main__":
    funcn = "df_helpers.main"
    from options_helper import *
    opt, args = get_options(funcn)
    from pathlib import Path
    from fdf_helpers import print_df

    df = pickle.loads(Path(f"/Fairedge_dev/app_regtests/fdf/sample_factor_001.db").read_bytes())
    print_df(df, show_head=False, rows=20, title='v001')
    print_df(demean_by_row(df), show_head=False, rows=20, title='v001a')
    for n in [1,2]:
        print_df(zero2nan(df_get_top(df, n=n)), show_head=False, rows=20, title='v002')
        print_df(zero2nan(df_get_top(demean_by_row(df), n=n)), show_head=False, rows=20, title='v002a')
        print_df(zero2nan(df_get_bottom(df, n=n)), show_head=False, rows=20, title='v003')
        print_df(zero2nan(df_get_bottom(demean_by_row(df), n=n)), show_head=False, rows=20, title='v003')
