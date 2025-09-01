#!/home/shuser/anaconda3/bin/python

import sys

import timeit
from itertools import product
from common_basic import *
from sklearn.linear_model import LinearRegression
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
import numpy as np


def sortino_ratio(series, N, rf):
    mean = series.mean() * N -rf
    std_neg = series[series<0].std()*np.sqrt(N)
    return mean/std_neg
# sortinos = df.apply(sortino_ratio, args=(N,rf,), axis=0 )
# sortinos.plot.bar()
# plt.ylabel('Sortino Ratio')

def max_drawdown(return_series):
    #networth = (return_series+1).cumprod()
    #networth = return_series.cumsum()
    networth = np.exp(return_series).cumprod()
    peak = networth.expanding(min_periods=1).max()
    dd = (networth/peak)-1
    return dd.min()

def max_drawdown2(return_series, mult=1.0, debug=False, title=""):
    funcn = "max_drawdown2"
    #print(return_series)
    networth = np.exp(return_series * mult).cumprod() 
    df = pd.Series(networth, name="nw").to_frame()
    max_peaks_idx = df.nw.expanding(min_periods=1).apply(lambda x: x.argmax()).fillna(0).astype(int)
    df['max_peaks_idx'] = pd.Series(max_peaks_idx).to_frame()
    nw_peaks = pd.Series(df.nw.iloc[max_peaks_idx.values].values, index=df.nw.index)
    df['dd'] = ((df.nw-nw_peaks)/nw_peaks)
    df['mdd'] = df.groupby('max_peaks_idx').dd.apply(lambda x: x.expanding(min_periods=1).apply(lambda y: y.min())).fillna(0)

    maxDD = df['mdd'].min()
    maxDDStr = "%.6f"%(maxDD)
    peakIdxs =  df[df['mdd']==0].index.tolist()
    peakIdxs.append(df.index[-1])
    rcyDays = -1
    for (b, e) in zip(peakIdxs[:-1], peakIdxs[1:]):
        periodMin = df['mdd'].loc[b:e].min()
        periodMinStr = "%.6f"%(periodMin)
        if periodMinStr == maxDDStr:
            #rcyDays = (e-b).days
            rcyDays =  df.index.get_loc(e) - df.index.get_loc(b)
            if debug:
                print(f"INFO: {funcn} title= {title}, b={b}, e={e}, min= {periodMinStr}, maxDD= {maxDDStr}, rcyDays= {rcyDays}")
    if debug:
        print(f"DEBUG: peakIdxs {peakIdxs}")
    return (df['mdd'].min(), rcyDays, df)

def CalcRecDays(return_series):
    navs = np.exp(return_series).cumprod() 
    max_val = navs[0]
    count = 0
    max_count = 0  #记录最大回撤天数
    for i in range(1, navs.size):
        if navs[i] > max_val:
            max_val = navs[i]
            count = 0
        else:
            count = count + 1
        if count > max_count:
            max_count = count
    return max_count
# max_drawdowns = df.apply(max_drawdown,axis=0)
# max_drawdowns.plot.bar()
# plt.yabel('Max Drawdown')

def lm(y, x, title="", debug=False, draw=False):
    xy = pd.DataFrame.from_dict({"x": x.copy(), "y": y.copy()})
    # print(f"INFO: lm({title}) input(head)", xy.head())
    # print(f"INFO: lm({title}) input(tail)", xy.tail())
    #print(f"DEBUG: xy before dropna shape= {xy.shape}, {xy}")
    with pd.option_context('mode.use_inf_as_na', True):
        xy = xy.dropna(how='any')
    if debug:
        print(f"DEBUG: xy after dropna shape= {xy.shape}, {xy}")
    xy['y_x'] = xy['y'] - xy['x']
    # print(f"DEBUG: xy after dropna {xy.shape}")

    #xy.cumsum().plot(figsize=(12,10))
    x = np.array(xy['x'])
    y = np.array(xy['y'])
    y_x = np.array(xy['y_x'])
    
    if draw:
        xy.plot.scatter(x='x', y='y')

    x = x.reshape(-1, 1)
    # print(f"INFO: {title} lm x=", x)
    # print(f"INFO: {title} lm y=", y)
    if len(y)<=0:
        return None

    model = LinearRegression().fit(x, y)

    # if title == 'rf': #all zeros
    #     model.intercept_ = y.mean()
    if debug:
        print(f"DEBUG: lm({title}) beta= %8.6f, alpha= %8.6f, mean= %8.6f"%(model.coef_[0], model.intercept_, y.mean()))

    bmNm = title.split(r':')[0]
    rc = {}
    rc[f'{bmNm}.Beta'] = model.coef_[0]
    rc[f'{bmNm}.Alpha'] = model.intercept_
    rc[f'{bmNm}.ExRet'] = y_x.mean()
    rc[f'{bmNm}.RetBM'] = x.mean()
    rc[f'{bmNm}.ExRetCum'] = y_x.sum()
    rc[f'{bmNm}.ExRetStd'] = y_x.std()
    rc[f'{bmNm}.IR'] = rc[f'{bmNm}.ExRet']/rc[f'{bmNm}.ExRetStd']*16 #annulaized
    rc[f'{bmNm}.Sortino'] = rc[f'{bmNm}.ExRet']/(y_x[y_x<0].std())*16 
    rc[f'{bmNm}.Drawdown'] = max_drawdown(xy['y_x']) 
    rc[f'{bmNm}.Calmar'] = rc[f'{bmNm}.ExRet']/abs(rc[f'{bmNm}.Drawdown'])*256

    return rc
    #return(model)
    #return(model.coef_[0], model.intercept_)


