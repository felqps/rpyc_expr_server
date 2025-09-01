#!/home/che/anaconda3/bin/python

import sys
import os

from common import *
from shared_func import *
import numpy as np
import pandas as pd
import alpha101_code
from CTX import *

#from platform_helpers import *

def apply_by_mkt(flds, func):
    out = None
    isTuple = False
    for pmid in flds[0].columns:
        input = [x[pmid] for x in flds]
        funcResult = func(input)

        if out is None:
            if type(funcResult) == type(tuple()):
                isTuple=True
                out = list( {} for i in range(len(funcResult)))
            else:
                out = [{}]
        if isTuple:
            for i in range(len(funcResult)):
                out[i][pmid] = funcResult[i]
        else:
            out[0][pmid] = funcResult
    #print("DEBUG: ===", len(out), type(funcResult))
    if isTuple: 
        return([pd.DataFrame.from_dict(x) for x in out])
    else:
        return(pd.DataFrame.from_dict(out[0]))

# def apply_by_mkt(flds, func):
#     out = {}
#     for pmid in flds[0].columns:
#         input = [x[pmid] for x in flds]
#         out[pmid] = func(input)

#     return(pd.DataFrame.from_dict(out))

#apply_by_mkt([dta['h'], dta['l'], dta['c']], func=lambda x: talib.ATR(*x, timeperiod=5))

def zero2nan(df):
    df = df.replace({0: np.nan})
    return df

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

def M(df, axis=1):
    return df.apply(de_mean, axis=axis)

def stack(df):
    return(df.stack().reset_index().sort_values(['Mkt', "Dtm"]).set_index(['Mkt', "Dtm"]))

def calc_mean_values(df):
    rc = {}
    for col in df.columns:
        rc[col] = df[col].mean()
    return rc

def get_mkts_with_data(df, min_valid_cnt=100, return_first=4):
    validCnt = df.count(axis=0)
    mkts = list(validCnt[validCnt>min_valid_cnt].index)
    return mkts[:(return_first if return_first<=len(mkts) else len(mkts))]

def filter_range(dfIn, llmt, ulmt, threshold=0.0):
    # df = dfIn.duplicated()
    df = dfIn.copy()
    df[df<=llmt+threshold] = np.nan
    df[df>=ulmt-threshold] = np.nan

    df = df * 0.0
    df[df==0] = 1.0
    #df = df.fillna(0.0)
    return df

def data_between_dates(df, begDt, endDt, delta_days=0):
    (begDtm, endDtm) = (dtparse(f'{begDt} 00:00:00') + timedelta(delta_days), dtparse(f'{endDt} 23:59:59') + timedelta(delta_days))
    return df[begDtm:endDtm]

def filter_like(df, ref_mkt, begdt, enddt, tgtdf, debug=False):
    df = df.replace(np.nan,0).replace(-np.inf, 0).replace(np.inf, 0) #.fillna(0, inplace=True)
    
    dfRef = data_between_dates(df, begdt, enddt, delta_days=0)
    #print(df.iloc[:,:5])
    print('===', df.columns, f"find {ref_mkt} {ref_mkt in df.columns}", file=sys.stderr)
    ref = dfRef[ref_mkt]

    index = df.index[df.index<=dfRef.index[-1]]
    print(index)
    insampleTgtRet = np.array([])
    outsampleTgtRet = np.array([])
    for sft in range(120):
        dfTst = data_between_dates(df.shift(sft), begdt, enddt)

        if debug:
            print(type(ref), ref[:5], ref[-5:])
            print(dfTst.iloc[:5,:5])

        corrs = dfTst.corrwith(ref, axis=0).sort_values().dropna()
        corrsSelected = corrs[corrs>0.40]
        if len(corrsSelected)<=0:
            continue

        if tgtdf is not None:
            for mkt in corrsSelected.index:
                tgtRet = tgtdf.loc[index[-sft-1], mkt]
                if np.isnan(tgtRet):
                    continue
                if sft == 0:
                    insampleTgtRet = np.append(insampleTgtRet, [tgtRet])
                else:
                    outsampleTgtRet = np.append(outsampleTgtRet, [tgtRet])
                print("%3d %s %s %6.4f : %7.4f"%(sft, index[-sft-1], mkt, corrsSelected[mkt], tgtRet))
        else:
            print("sft=", sft, index[-sft-1], corrsSelected)

        #print(dfTst['301153.XSHE'])
    print(f"TOTAL: ins= {insampleTgtRet.sum()}/{insampleTgtRet.mean()}, outs= {outsampleTgtRet.sum()}/{outsampleTgtRet.mean()}")
    return df

def __filter_like(df, ref_mkt, begdt, enddt):
    df = df.shift()
    df = data_between_dates(df, begdt, enddt)
    df = df.replace(np.nan,0).replace(-np.inf, 0).replace(np.inf, 0) #.fillna(0, inplace=True)
    #print(df.iloc[:,:5])
    print('===', df.columns, f"find {ref_mkt} {ref_mkt in df.columns}", file=sys.stderr)
    ref = df[ref_mkt]
    ref = ref.to_numpy()
    print(type(ref), ref)

    #print(df["000004.XSHE"])
    #print((df.corrwith(df[ref_mkt], axis=0)).sort_values()[-10:])
    df[ref_mkt] = ref
    print((df.corrwith(ref, axis=0)).sort_values()[-10:])
    return df

# from numpy.lib.stride_tricks import as_strided
# from numpy.lib import pad
# def rolling_spearman(seqa, seqb, window):
#     stridea = seqa.strides[0]
#     ssa = as_strided(seqa, shape=[len(seqa) - window + 1, window], strides=[stridea, stridea])
#     strideb = seqa.strides[0]
#     ssb = as_strided(seqb, shape=[len(seqb) - window + 1, window], strides =[strideb, strideb])
#     ar = pd.DataFrame(ssa)
#     br = pd.DataFrame(ssb)
#     ar = ar.rank(1)
#     br = br.rank(1)
#     corrs = ar.corrwith(br, 1)
#     return pad(corrs, (window - 1, 0), 'constant', constant_values=np.nan)

__BMRETS__ = {}

def getBmRets(opt, symset, dfSS=None, debug=True):
    funcn = "getBmRets"
    debug=True
    global __BMRETS__
    if symset not in __BMRETS__:
        scn = Scn(opt, "Indices", opt.dts_cfg, 'SN_CS_DAY', asofdate='download')
        retsFn = (scn.qdfRoot + "1d" + "Resp_lnret_C2C_1d_1" + f'Indices.db').fp()
        # retsFn = f"{dd('raw')}/indices/index_returns_CS.db"
        if ctx_verbose(1):
            print(f"INFO: {funcn} reading file {retsFn}")
        bmRets = QpsUtil.smart_load(retsFn)
        # bmRets = bmRets.shift(-1) #align with response col
        bmRets = bmRets[['000300.XSHG', '000905.XSHG']]
        bmRets.columns = ['3h', '5h']
        #bmRets.columns = ['2k', '1k', '5h', '3h', '2h', '1h']
        bmRets['rf'] = 0 #assume risk free 0 for now
        #bmRets.set_index(bmRets.index + pd.Timedelta('15:00:00'), inplace=True)
        if ctx_verbose(1):
            print(f"INFO: {funcn} symset= {symset}, bmRets=", bmRets.tail(10))
        __BMRETS__[symset] = bmRets

    if dfSS is not None: #only symset-returns are changed each time
        __BMRETS__[symset]['ss'] = dfSS
    return __BMRETS__[symset]

def mktdtmIdx2IndexReturn(bmRets, mktdtmIdx, bm):
    return dtm2IndexReturn(bmRets, mktdtmIdx[1], bm)

def dtm2IndexReturn(bmRets, dtm, bm):
    try:
        #print(id[1], bmRets.loc[id[1], bm])
        return bmRets.loc[dtm, bm]
    except Exception as e:
        return np.nan

__MKTCLASSIFICATION__ = {}
def getMktClassification(dts_cfg, clsname="sector", debug=False):
    funcn = "getMktClassification"
    global __MKTCLASSIFICATION__
    if clsname not in __MKTCLASSIFICATION__:
        d = {}
        dbFns = []
        if clsname == "sector":
            dbFns = glob.glob(f"/qpsdata/config/symsets/{dts_cfg}/{clsname}_*.pkl")
        elif clsname == "rqindu":
            dbFns = glob.glob(f"/qpsdata/config/symsets/{dts_cfg}/???.pkl")
        elif clsname == "citicsindu":
            dbFns = glob.glob(f"/qpsdata/config/symsets/{dts_cfg}/citics_??.pkl")
        elif clsname == "swsindu":
            dbFns = glob.glob(f"/qpsdata/config/symsets/{dts_cfg}/sws_*.pkl")
        else:
            assert False,  f"ERROR: invalid class {funcn} {clsname}"
        for fn in dbFns:
            if debug:
                print(fn)
            db = smart_load(fn, debug=False, title=funcn)
            classification = os.path.basename(fn).split(r'.')[0]
            for indu in db.keys():
                for x in db[indu]:
                    if x in d:
                        if debug:
                            print(f"WARNING: {x} multiple classifications: {d[x]}, {classification}")
                            continue
                    d[x]=classification
        __MKTCLASSIFICATION__[clsname] = d
    return __MKTCLASSIFICATION__[clsname]

def mktdtmIdx2classification(mktclsDb, mktdtmIdx):
    return mkt2classification(mktclsDb, mktdtmIdx[0])

def mkt2classification(mktclsDb, mkt):
    try:
        return mktclsDb[mkt]
    except Exception as e:
        return "unknown"    

def calc_classification_code(fld, dts_cfg='W', clsname='sector'):
    funcn = "calc_classification_code"
    mktclsDb = getMktClassification(dts_cfg, clsname)
    codeLookup = {}
    if clsname == 'sector':
        codeLookup = {
            "sector_Unknown": 0,
            "sector_ConsumerDiscretionary": 1,
            "sector_ConsumerStaples": 2,
            "sector_Energy": 3,
            "sector_Financials": 4,
            "sector_HealthCare": 5,
            "sector_Industrials": 6,
            "sector_InformationTechnology": 7,
            "sector_Materials": 8,
            "sector_RealEstate": 9,
            "sector_TelecommunicationServices": 10,
            "sector_Utilities": 11,
        }
    elif clsname in ['rqindu']:
        code = 0
        for ln in QpsUtil.open_readlines(f"/Fairedge_dev/app_QpsData/classification_{clsname}.txt"):
            ln = ln.strip()
            codeLookup[ln] = int(code)
            code += 1

    elif clsname in  ['citicsindu', 'swsindu']:
        for ln in QpsUtil.open_readlines(f"/Fairedge_dev/app_QpsData/classification_{clsname}.txt"):
            ln = ln.strip().split()[0] #use the first column
            codeLookup[ln] = int(ln.split(r'_')[-1])


    newFld = fld.copy()
    for mkt in newFld.columns:
        cls = mkt2classification(mktclsDb, mkt)
        if cls in codeLookup:
            newFld.loc[:,mkt] = codeLookup[cls] + 0.100
        else:
            print(f"INFO: calc_classification_code {clsname} missing {mkt}")
            newFld.loc[:,mkt] = 0.100
    return newFld

#def industry_neutralize(opt, df, clsname='citicsindu', debug=False):
def industry_neutralize(dts_cfg, df, clsname='swsindu', debug=False):
    funcn = "industry_neutralize"
    # ndf = df.copy()
    # ndf = ndf.T.stack()
    # ndf.index.rename(['Mkt', 'Dtm'], inplace=True)
    if debug:
        print_df(df, rows=30, show_head=True, title=f'{funcn} input')
    ndf = pd.DataFrame()
    ndf['raw'] = df.dropna(how='all', axis=1).T.stack()
    ndf.index.rename(['Mkt', 'Dtm'], inplace=True)

    ndf['cls'] = ndf.index.map(lambda mktdtmIdx: mktdtmIdx2classification(getMktClassification(dts_cfg, clsname), mktdtmIdx))
    ndf.reset_index(inplace=True)
    if debug:
        print_df(ndf, title="ndf.reset_index")

    mdf = ndf.groupby(['Dtm', 'cls'])['raw'].mean()
    if debug:
        print_df(mdf, rows=50, show_head=True, title='mdf.mean') 

    # ndf['mean'] = ndf[['Dtm','cls']].map(lambda dtm,cls: mdf.loc[dtm,cls])
    ndf['mean'] = ndf.apply(lambda row: mdf.loc[row['Dtm'],row['cls']], axis=1)

    if debug:
        print_df(ndf, title='before')

    ndf['demean'] =  ndf['raw'] - ndf['mean'] 

    if debug:
        print_df(ndf, title='after')
    rc = ndf.sort_values(['Mkt', "Dtm"]).set_index(['Mkt', "Dtm"])['demean'].unstack(level=0)
    if debug:
        print_df(rc, title='final')
    return(rc)

def less_than(a, b):
    #b_p = b.loc[a.index,:] #force a b to the same dimention
    b_p = b.loc[a.index, a.columns] #force a b to the same dimention
    # print_df(a, show_head=False, title='a')
    # print_df(b_p, show_head=False, title='b')
    return(bool2num(a<b_p))

def _calc_beta(returns, index_returns, n_beta_days=60, min_days=20, debug=False):
    funcn = "_calc_beta"
    returns = returns.shift(-1) #returns is pred, index_returns is resp
    returns_intersecton_index = index_returns.index.intersection(returns.index) #returns and index_returns may have different start time

    if ctx_debug(2):
        print(f"DEBUG_4561: {funcn}", type(returns), type(index_returns))
        print_df(returns, show_head=True, title='returns')
        print(index_returns.head())
        print(index_returns.tail())
        print(returns_intersecton_index[:10])

    # betas=pd.DataFrame(np.ones_like(returns),index=returns.index,columns=returns.columns)
    # vol=pd.DataFrame(np.zeros_like(returns),index=returns.index,columns=returns.columns)
    #returns = returns.loc[index_returns.index,:] #make time-length the same
    returns = returns.loc[returns_intersecton_index,:] 
    if ctx_debug(1):
        print(f"DEBUG_2110: {funcn} returns= {returns.shape}, index= {index_returns.shape}")
        print(f"DEBUG_2111: market returns(head)=", returns.iloc[:,:5].head())
        print(f"DEBUG_2112: index  returns(head)=", index_returns.head())
        print(f"DEBUG_2111: market returns(tail)=", returns.iloc[:,:5].tail())
        print(f"DEBUG_2112: index  returns(tail)=", index_returns.tail())

    betas=pd.DataFrame(np.full_like(returns, np.nan, dtype=np.double),index=returns.index,columns=returns.columns)
    vol=pd.DataFrame(np.full_like(returns, np.nan, dtype=np.double),index=returns.index,columns=returns.columns)
    
    for i in range(n_beta_days-1, returns.shape[0]):
        x=index_returns.iloc[i-n_beta_days+1:i+1]
        y=returns.iloc[i-n_beta_days+1:i+1,:]
        x_bar = np.array([x-x.mean()]).T
        y_bar = y.sub(y.mean(axis=0), axis=1)
        cv=pd.DataFrame(x_bar*y_bar.values,index=y.index,columns=y.columns).mean()
        n=len(x)
        beta=(cv/x.var())*(n/(n-1))
        beta.where(y.isna().sum()<min_days,other=np.nan,inplace=True)
        betas.iloc[i,:]=beta
        vol.iloc[i,:]=np.sqrt(y.var())
    return (betas, vol)

def calc_beta(opt, returns, index_name, n_beta_days=60, min_days=20):
    return _calc_beta(returns, getBmRets(opt, index_name)[index_name])

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

def re_index(tgt, ref):
    if not tgt.index.is_unique:
        tgt = tgt[~tgt.index.duplicated(keep='last')]
        print(f"DEBUG_4533:", print_df(tgt, title='ERROR: factor index duplicated'))
    
    if type(ref) == type(pd.Series()):
        tgt=tgt.reindex(index=ref.index)
    else:
        tgt=tgt.reindex(index=ref.index,columns=ref.columns)

    if type(tgt) == type(pd.Series()):
        tgt = tgt.dropna(how='all')
    else:
        tgt = tgt.dropna(how='all', axis=0)
        tgt = tgt.dropna(how='all', axis=1)        
    return tgt

if __name__ == '__main__':
    (opt, args) = get_options_sgen(list_cmds)

    # s = getMktClassification(opt, "sector")
    # c = getMktClassification(opt, "citics")

    for pmid in ['000780', '002071', '300362']:
        for clsname in ['sector', 'citics']:
            lbd = lambda mkt: mkt2classification(getMktClassification(opt, clsname), mkt)
            print(f"INFO: classification {pmid} {clsname} {lbd(QpsUtil.sym2rqsym(pmid))}")
