import os,sys

# from pathlib import Path
import pandas as pd 
import numpy as np 
from matplotlib import pyplot as plt

#from platform_helpers import *
import alpha101_code
from fdf_helpers import print_df
import sys
from df_helpers import *
from platform_helpers import *
from funcs_fld import *

def ma(f,d):
    if d<=1:
        return f
    rc = f.rolling(d).mean()
    return rc

def less_than(a, b):
    #b_p = b.loc[a.index,:] #force a b to the same dimention
    b_p = b.loc[a.index, a.columns] #force a b to the same dimention
    # print_df(a, show_head=False, title='a')
    # print_df(b_p, show_head=False, title='b')
    return(bool2num(a<b_p))

def z_score_standardization(series):
    return (series - series.mean()) / series.std()

def Z(df, axis=1):
    return df.apply(z_score_standardization, axis=axis)

def Winsorize(df, limits=[0.05, 0.95]):
    #quantiles = df.quantile(limits, axis=1)
    q_05 = df.quantile(limits[0], axis=1)
    q_05 = np.array([q_05]).T + np.zeros(df.shape)
    q_95 = df.quantile(limits[1], axis=1)
    q_95 = np.array([q_95]).T + np.zeros(df.shape)
    with np.errstate(invalid='ignore'):
        out = np.where(df.values <= q_05, q_05, np.where(df >= q_95, q_95, df))
    out = pd.DataFrame(out, index=df.index, columns=df.columns)
    return out

def ZW(df, axis=1, limits=[0.05, 0.95]):
    return Z(Winsorize(df, limits=limits), axis=axis)

def de_mean(series):
    return (series - series.mean())

def m(df, axis=1):
    return df.apply(de_mean, axis=axis)

def neg(x):
    return -x

def sqrt(f): 
    return np.sqrt(abs(f))

def inv(y):
    y[y==0] = 0.000001
    return 1/y

def pow2(x):
    return x.pow(2.0)

def pow3(x):
    return x.pow(3.0)

def sigmoid(f):
    return 1/(1 + np.exp(-f))

def iif(c,t,f): 
    return (bool2num(c)*t + bool2numinv(c)*f).astype(float)

# def rank(f): 
#     return f.rank(axis=1, pct=1, method='max')

# def rank(f): 
#     return f.rank(axis=1, pct=0, method='min')


def rank(f): 
    return f.rank(axis=1, pct=1)

def rank_sub(a,b):
    return (rank(a)-rank(b))

def rank_div(a,b):
    return (rank(a)/rank(b))

def signed_log(f):
    return np.sign(f)*np.log(f.abs()+1e-10)

def auto_scaling(f, autoscaling):
    if autoscaling==1:
        max_entry = f.max().max()  #find the max overall 
        f = f*10.0/max_entry #scale all to prevent overflow
    elif autoscaling==2:
        f = signed_log(f)
    #print_df(f, title=f"debug auto_scaling")
    return f

def regout(y, x, debug=True):
    #x = x + 0*y
    dx = Z(Winsorize(x[~y.isna()]))
    dy = Z(Winsorize(y[~x.isna()]))
    dxy = dy*dx
    dn = dxy.count(axis=1)
    #dx2 = dx*dx
    #beta = (dxy.sum(axis=1)/dx2.sum(axis=1)*dn/(dn-1))
    #beta = (dxy.sum(axis=1)/dn/dx.var(axis=1)*dn/(dn-1))
    beta = (dxy.sum(axis=1)/(dn-1))
    betax = pd.DataFrame(np.array([beta.values]).T + np.zeros(x.shape), index=x.index, columns=x.columns)
    out = (dy-betax*dx)
    # if debug:
    #     #display((dx2.sum(axis=1)/dn)[:10])
    #     #display((dx.var(axis=1))[:10])
    #     display(y.head())
    #     display(x.head())
    #     display(betax.head())
    #     display(out.head())
        
    return [out, dy, dx]

# def mdvrank(c,v):
#     dv=c*v
#     mdv=apply_by_mkt([dv], func=lambda x: x[0].rolling(21).median())
#     mdvrank=mdv.rank(axis=1, ascending=False)
#     return mdvrank

def toprank(df, cutoff=1800):
    return (df<=cutoff)

def lnret(df,days=1):
    return np.log(df.fillna(method='bfill', axis=0)/df.shift(days))

def ref(f,d):
    return f.shift(d)

def delay(f,d):
    return f.shift(d)

def delta(f,d):
    return f.diff(d)

def sum(f,d):
    return f.rolling(d).sum()

def sign(f):
    return np.sign(f)

# def signedpower(f,d):
#     return np.sign(f)*(f.abs() ** d)

def signedpower(f,d): 
    if d == 0:
        return f*0.0 + 1.0
    elif d == 1:
        return f
    return np.sign(f)*(f.abs() ** d)

def stddev(f,d):
    return f.rolling(d).std()

def ema(f,d):
    f = f.fillna(method='ffill', axis=0)
    f = f.fillna(0.0)
    return f.apply(lambda x: talib.EMA(x, d) if not np.isnan(x).all() else x, axis=0)

def wma(f,d):
    f = f.fillna(method='ffill', axis=0)
    f = f.fillna(0.0)
    return f.apply(lambda x: talib.WMA(x, d) if not np.isnan(x).all() else x, axis=0)

def product(f,d):
    return alpha101_code.product(f,d)

def covariance(a,b,d): 
    return a.rolling(d).cov(b)

def correlation(a,b,d): 
    # print(a['000509.XSHE'])
    # print(df_non_uniq_columns(a))
    a = df_merge_non_uniq_columns(a)
    b = df_merge_non_uniq_columns(b)
    return  a.rolling(d).corr(b)

### from register_script_funtions
def add(x,y):
    return x.add(y)

def sub(x,y):
    return x.sub(y)

def mul(x,y):
    return x.mul(y)

def div(x,y):
    #print("**************", x, y)
    if y is None:
        if x is None:
            return 1.0
        else:
            return x

    y[y==0] = 0.000001
    return x.div(y)

def inv(y):
    y[y==0] = 0.000001
    return 1/y

def pow2(x):
    return x.pow(2.0)

def pow3(x):
    return x.pow(3.0)

def z(x):
    return Z(x)

def w(x):
    return Winsorize(x)

def zw(x):
    return ZW(x)

def m(x):
    return M(x)

def neg(x):
    return -x

def abs(f):
    return np.abs(f)

def sign(f):
    return np.sign(f)

def log(f): 
    f[f==0] = 0.000001
    return np.log(f)

def sqrt(f): 
    return np.sqrt(abs(f))

def iif(c,t,f): 
    return bool2num(c)*t + bool2numinv(c)*f

def rank(f): 
    return f.rank(axis=1, pct=1)

def rank_sub(a,b):
    return (rank(a)-rank(b))

def rank_div(a,b):
    return (rank(a)/rank(b))

# def scale(f,s=1): 
#     return alpha101_code.scale(f,s)
def scale(f,d=1):
    return alpha101_code.scale(f,d)

def sigmoid(f):
    return 1/(1 + np.exp(-f))

# for i in [1,2,3]:
#     #Arity 1
#     for funcn in ['signedpower']:
#         exec(f"""
# def {funcn}{i}(f,d={i}): 
#     return {funcn}(f,d)
# """)

####TimeSeries
def ts_sft(f,d):
    return f.shift(d)

# def ts_sft(f,d):
#     return f.shift(d)

def ts_delta(f,d):
    return f.diff(d)

# def ts_delta(f,d):
#     return f.diff(d)

def ts_sum(f,d):
    if d<=1:
        return f
    return f.rolling(d).sum()

# def ts_sum(f,d,autoscaling=0):
#     if d<=1:
#         return f
#     f = auto_scaling(f, autoscaling)
#     return f.rolling(d).sum()

def ts_prod(f,d):
    if d<=1:
        return f
    return f.rolling(d).agg(lambda x : x.prod())

# def ts_prod(f,d,autoscaling=2):
#     if d<=1:
#         return f
#     f = auto_scaling(f, autoscaling)
#     return f.rolling(d).agg(lambda x : x.prod())

def ts_stddev(f,d):
    if d<=1:
        return f*0.0
    return f.rolling(d).std()

# def ts_stddev(f,d):
#     if d<=1:
#         return f*0.0
#     return f.rolling(d).std()

def ts_zscore(f,d):
    if d<=1:
        return f*0.0
    #print(f.rolling(d).std())
    return f.rolling(d).mean()/(f.rolling(d).std()+1e-10)

# def ts_zscore(f,d):
#     if d<=1:
#         return f*0.0
#     return f.rolling(d).mean()/f.rolling(d).std()

def ts_rank(f,d): 
    return alpha101_code.ts_rank(f,d)

# def ts_rank(f,d): 
#     return alpha101_code.ts_rank(f,d)

def ts_corr(a,b,d): 
    if d<=1:
        return a*0.0 + 1.0
    return a.fillna(0).rolling(d).corr(b.fillna(0))

# def ts_corr(a,b,d): 
#     if d<=2:
#         d = 3
#         #return a*0.0 + 1.0
#     return a.fillna(0).rolling(d).corr(b.fillna(0))

def ts_cov(a,b,d): 
    if d<=1:
        return a*0.0
    return a.fillna(0).rolling(d).cov(b.fillna(0))

# def ts_cov(a,b,d): 
#     if d<=1:
#         return a*0.0
#     return a.fillna(0).rolling(d).cov(b.fillna(0))

def decay_linear(f,d): 
    if d<=1:
        return f
    return f.apply(lambda x: talib.WMA(x, d) if not np.isnan(x).all() else x, axis=0) #x timeseries could be all nan for a stock

# def decay_linear(f,d): 
#     if d<=1:
#         return f
#     return wma(f,d)
#     #return f.apply(lambda x: talib.WMA(x, d) if not np.isnan(x).all() else x, axis=0) #x timeseries could be all nan for a stock

def ts_min(f,d): 
    return f.rolling(d).min()

# def ts_min(f,d): 
#     return f.rolling(d).min()

def ts_max(f,d):
    return f.rolling(d).max() if d>1 else f

# def ts_max(f,d):
#     return f.rolling(d).max() if d>1 else f

def ts_argmax(f,d):
    return f.rolling(d).apply(np.argmax) + 1

# def ts_argmax(f,d):
#     return f.rolling(d).apply(np.argmax) + 1

def ts_argmin(f,d):
    return f.rolling(d).apply(np.argmin) + 1

# def ts_argmin(f,d):
#     return f.rolling(d).apply(np.argmin) + 1

def ts_avg(f,d):
    return f.rolling(d).mean()

def ma(f,d):
    if d<=1:
        return f
    rc = f.rolling(d).mean()
    return rc

def mdvrank(c,v):
    dv=c*v
    mdv=apply_by_mkt([dv], func=lambda x: x[0].rolling(21).median())
    mdvrank=mdv.rank(axis=1, ascending=False)
    return mdvrank

def toprank(df, cutoff=1800):
    return (df<=cutoff)

def lnret(df,days=1):
    return np.log(df.fillna(method='bfill', axis=0)/df.shift(days))

def ts_days():
    return [1,2,3,4,5,6,7,8,9,10,20,60,90,120]

for i in ts_days():
    #Arity 1
    for funcn in ['ts_sum', 
                  'ts_stddev', 
                  'ts_rank', 
                  'ts_delta', 
                  'ts_sft', 
                  'decay_linear', 
                  'ts_min', 
                  'ts_max', 
                  'ts_argmax', 
                  'ts_argmin', 
                  'ts_prod', 
                  'ma',
                  'signedpower',
                  'ts_zscore']:
        exec(f"""
def {funcn}{i}(f,d={i}): 
    return {funcn}(f,d)
""")

    #Arity 2
    for funcn in ['ts_corr', 
                  'ts_cov']:
        exec(f"""
def {funcn}{i}(a,b,d={i}): 
    return {funcn}(a,b,d)
""")
        
def ts_func_periods():
    return [*range(1,31), 60, 90, 120]

def gen_adv_threshold_report(fw, adv_threshold=[70000], adv_period=20):
    res = {}
    adv = eval(f"adv({adv_period})", fw.ctx()) #note fw is needed to get the context where all other functions are defined.
    for th in adv_threshold:
        advDf = adv.copy()
        advDf[advDf<th] = 0.0
        advDf[advDf>=th] = 1.0
        print_df(advDf, show_head=False, title="gen_mr_liq_report")
        res[f"{th}"] = advDf.sum(axis=1)
    resDf = pd.DataFrame.from_dict(res, orient='columns')
    if 0:
        resDf.plot(figsize=(20, 15), title="# of markets with 20 day ADV over threshold", xlabel='date', ylabel='count')
        #resDf.plot(figsize=(30, 20))
        plt.savefig(f"gen_futures_adv{adv_period}_report.png")
    return advDf

