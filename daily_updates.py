
import matplotlib as mpl
mpl.rcParams['figure.max_open_warning'] = 0
            
import sys

import os
from EgenNew import *
from FldNamespace import *
from EgenStudy import *
from shared_cmn import *
import timeit
import QpsUtil
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from common_basic import *
from EgenStudy import get_classifier_flds,analyze_qSummary
import itertools

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 30)
##pd.set_option('display.max_colwidth', -1) # or 199

#plt.style.use('seaborn-white')
figsizeHalf=(18,6)
figsize=(18,13)
figsize2=(18,26)

def addSymset(params, ss, ssn, dta, calc_all = False):
    for indunm, indu in dta.groupby('category'):
        #print(indu)
        ss[ssn][indunm] = list(indu['permid'])
        
        #select a column to include all stocks, so we do not calculate multiple times for each column
        if calc_all:
            ss[params['SYMSET']][indunm] = list(indu['permid'])
            
def gen_symset_from_column_value(params, ss, df):
    #dataframe column name to classification name
    for col_cls in ['category:indu', 'citics_industry_code:citics', 'sector_code:sector', 'board_type:board'][:]:
        (col,cls) = col_cls.split(r':')
        grpByCol = df.groupby(col)
        #grpByCol.count().sort_values('permid', ascending=False)

        for grpnm, grp in grpByCol:
            ssn = grpnm #symset name
            if col != 'category':
                ssn = f"{cls}_{grpnm}"
            addSymset(params, ss, ssn, grp, calc_all = (col == 'category'))
                
def gen_symset_random_samples(params, ss, df):
    #Random sample symsets
    import random
    idx = list(range(len(df['permid'])))
    thirdIdx = int(len(df['permid'])/3)

    random.seed('a')
    random.shuffle(idx)
    allMkts = df['permid']
    # allMkts[idx[:thirdIdx*2]]
    inIdx = idx[:int(thirdIdx*2)]
    outIdx = idx[int(thirdIdx*2):]

    sampleIn = df[df['permid'].isin(allMkts.iloc[inIdx])]
    sampleOut = df[df['permid'].isin(allMkts.iloc[outIdx])]
    # sampleIn.head(10)
    addSymset(params, ss, "RandIn_01", sampleIn)
    addSymset(params, ss, "RandOut_01", sampleOut)    
    
def gen_symset_super_used(params, ss, df):
    import glob
    mkts = {}
    for fn in glob.glob(f"{rootdata()}/Quanpass/reports/*/SLO*.cfg"):
        for ln in QpsUtil.open_readlines(fn):
            (dt, mkt) = ln.split(r',')
            mkts[mkt] = dt
    rqmkts = []
    for pmid in df['permid']:
        mkt = f"{pmid.split(r'.')[0]}"
        if (f"sh{mkt}" in mkts) or (f"sz{mkt}" in mkts):
            rqmkts.append(pmid)
    
    addSymset(params, ss, "super_SLO", df[df['permid'].isin(rqmkts)])
            
             
def write_symsets(opt, params, ss):
    outDir = f"{rootdata()}/config/symsets/{params['dts_cfg']}"
    QpsUtil.mkdir([outDir])
    info = {}
    for ssn in ss.keys():
        mktList = ([y for x in ss[ssn].values() for y in x])
        info[ssn] = [len(ss[ssn].keys()), len(mktList)]
        QpsUtil.smart_dump(ss[ssn], f"{outDir}/{ssn}.pkl", verbose=1, title=opt.do)

    infoDf = pd.DataFrame.from_dict(info, orient='index', columns=["InduCnt", "MktCnt"])
    #infoDf.shape
    infoDf.sort_values('MktCnt', ascending=False, inplace=True)

    infoDf.to_csv(f"{outDir}/summary.csv")
    return infoDf

def fix_pmid_indu(row, pmid2indu):
    #print(row)
    if row.permid in pmid2indu:
        if row.category != pmid2indu[row.permid]:
            if False:
                print(f"INFO: fix_pmid_indu {row.permid} {row.category} => {pmid2indu[row.permid]}")
            return pmid2indu[row.permid]
    return row.category

def main_gen_symsets(opt, paramsL, dfLst, infoLst):
    funcn = "main_gen_symsets"
    for params in paramsL:
        studyName="study_symsets"
        scn = Scn(opt, params['SYMSET'], params['dts_cfg'], params['snn'], asofdate=opt.asofdate)
        instrudbFn = f"{dd('raw')}/{scn.symset}/{scn.snn}/{scn.dts_cfg_expanded}/vps_instru.{scn.symset}.db"
        
        if scn.symset.find("CF_")>=0:
            instrudbFn = instrudbFn.replace(".db", ".main.db")

        print(f"DEBUG_4524: {funcn}, instrudbFn= {instrudbFn}")

        df = smart_load(instrudbFn, debug=True)
        if df is not None:
            print(f"INFO: {funcn}", instrudbFn, df.shape)
            print_df(df, title=f"{funcn} init")
        else:
            print(f"ERROR: {funcn} empty file {instrudbFn}")
        dfLst.append(df)

        from collections import defaultdict
        ss = defaultdict(dict) #symset dict, contains of a dict of list keyed by RqIndu

        get_symset_sws(params, ss)     

        #pmid2indu = get_pmid2indu_for_symset(opt.dts_cfg, "CS_ALL") #xxx ERROR, recursive
        pmid2indu = {}
        for ln in open(f"{rootdata()}/config/all_instru_CS.txt", 'r').readlines():
            (pmid, indu) = ln.split()[:2]
            pmid2indu[pmid] = indu
        df.category = df.apply(lambda r: fix_pmid_indu(r, pmid2indu), axis=1)
        print_df(df, title=f"{funcn} add category")


        gen_symset_from_column_value(params, ss, df)
        gen_symset_random_samples(params, ss, df)
        gen_symset_super_used(params, ss, df)

        infoLst.append(write_symsets(opt, params, ss))

def get_symset_sws(params, ss):
    swsindu2ssn = {}
    for (ssn, indu) in [x.split() for x in QpsUtil.open_readlines("/Fairedge_dev/app_QpsData/classification_swsindu.txt", verbose=True)]:
        swsindu2ssn[indu] = ssn
    #print(swsindu2ssn)
    for fn in sorted(glob.glob(f"{rootdata()}/data_rq.20100101_uptodate/*/factor_expusure.db")):
        df = smart_load(fn, debug=True, title="get_symset_sws")
        for k,v in df.items():
            if v is not None:
                v['mkt'] = k
        df = pd.concat([x for x in df.values() if x is not None])
        #print_df(df)
        rqindu = fn.split(r'/')[-2]
        for swsindu, ssn in swsindu2ssn.items():
            if swsindu not in df.columns:
                print(f"ERROR: can not find {swsindu}")
            else:
                members = list(set(df[df[swsindu]>0]['mkt']))
                if len(members)>0:
                    ss[ssn][rqindu] = members
                #print(ssn, ss[ssn][rqindu]

def download_dta(DataCfg):
    funcn = 'daily_updates.download_dta'
    import rqdatac as rc
    assert has_quota(), f"ERROR: {funcn} has no quota"

    dwldDta = {}
    missing = []
    for pmid in ctx_cnstk_indices():
        df = rc.get_price(pmid, start_date=DataCfg.qryBegDt.replace('2021-08-10', '2021-08-01'), end_date=DataCfg.qryEndDt) #hack for '2021-08-10'
        if ctx_verbose(5):
            print_df(df, show_head=True, rows=3, title='funcn')
        if df is None:
            print(f"WARNING: {funcn} no data form {pmid}")
            missing.append(pmid)
        else:
            dwldDta[pmid] = df
    return dwldDta

def process_return(dwldDta, DataCfg):
    funcn = 'daily_updates.process_close'
    scn = ctx_scn()
    dta = {}
    for pmid in ctx_cnstk_indices(): 
        if pmid in dwldDta: 
            dta[pmid] = np.log(dwldDta[pmid]['close'].pct_change()+1)
    df = pd.concat(dta.values()).unstack(level=0)
    # for pmid in missing:
    #     df[pmid] = np.nan
    #print_df(df)
    outFp = f"{rootdata()}/data_rq.{DataCfg.begDt}_{DataCfg.endDt}/indices/index_returns_CS.csv".replace('-','')
    #df.to_csv(outFp)
    QpsUtil.smart_dump(df, outFp, debug=True, verbose=0)
    QpsUtil.smart_dump(df, outFp.replace(".csv", ".db"), debug=True, verbose=0)
    df.set_index(df.index + pd.Timedelta('15:00:00'), inplace=True)
    fldFp = (scn.qdfRoot + "1d" + "Pred_lnret_C2C_1d_1" + f'Indices.db').fp()
    QpsUtil.smart_dump(df, fldFp, debug=True, title=funcn, verbose=0)
    fldFp = (scn.qdfRoot + "1d" + "Resp_lnret_C2C_1d_1" + f'Indices.db').fp()
    QpsUtil.smart_dump(df.shift(-1), fldFp, debug=True, title=funcn)

def process_ohlcv(dwldDta, DataCfg, colnm='close', fldnm='C'):
    funcn = f'daily_updates.process_ohlcv{fldnm}'
    scn = ctx_scn()
    dta = {}
    for pmid in ctx_cnstk_indices():
        if pmid in dwldDta: 
            dta[pmid] = dwldDta[pmid][colnm]
    df = pd.concat(dta.values()).unstack(level=0)
    # for pmid in missing:
    #     df[pmid] = np.nan
    #print_df(df)
    df.set_index(df.index + pd.Timedelta('15:00:00'), inplace=True)
    if fldnm.find("Pred_")>=0:
        fldFp = (scn.qdfRoot + "1d" + fldnm + f'Indices.db').fp()
    else:
        fldFp = (scn.qdfRoot + "1d" + f"Pred_OHLCV_{fldnm}_1d_1" + f'Indices.db').fp()
    QpsUtil.smart_dump(df, fldFp, debug=True, title=funcn, verbose=0)


def main_gen_indices(opt, paramsL):
    funcn = "main_gen_indices"
    for params in paramsL:  
        print(params)
        if params['dts_cfg'] in ["prod", "P"]:
            print(f"INFO: skipping {funcn} for {params['dts_cfg']}, not needed...")
            continue
        (QDF, DataCfg) = config_qdf(params['dts_cfg'])
        #print('====', DataCfg)
        scn = Scn(opt, "Indices", params['dts_cfg'], 'SN_CS_DAY', asofdate='download')

        print(f"INFO: {funcn} {DataCfg.qryBegDt} {DataCfg.qryEndDt}")
        dwldDta = download_dta(DataCfg)
        process_return(dwldDta, DataCfg)
        process_ohlcv(dwldDta, DataCfg, 'open', 'O')
        process_ohlcv(dwldDta, DataCfg, 'high', 'H')
        process_ohlcv(dwldDta, DataCfg, 'low', 'L')
        process_ohlcv(dwldDta, DataCfg, 'close', 'C')
        process_ohlcv(dwldDta, DataCfg, 'volume', 'V')
        process_ohlcv(dwldDta, DataCfg, 'total_turnover', 'Pred_Rqraw_RqTotalTurnover_1d_1')

    
def gen_fld_info_db():
    funcn = "gen_fld_info_db"
    infoDict = {}
    fps = glob.glob(f"{rootdata()}/egen_study/flds_summary/*.dict")
    for fp in fps:
        fldNm = os.path.basename(fp).replace("_summary.dict", "")
        info = smart_load(fp)
        infoDict[fldNm] = info
        
    for ln in QpsUtil.open_readlines(f"{rootdata()}/config/fgen_flds.CS.cfg"):
        eles = ln.split(r';')
        freq_name = eles[0]
        (freq, fldNm) = freq_name.split(r'/')

        if freq_name.find('mysql')>=0: #super factors        
            nm = '_'.join(fldNm.split(r'_')[2:4])
            src = "super" 
        else:
            #print(ln)
            src = [x for x in eles[1:] if x.find("rq=")>=0]
            src = "QPS" if len(src)==0 else src[0]
            nm = [x for x in eles[1:] if x.find("nm=")>=0]
            nm = fldNm if len(nm)==0 else nm[0]
        
        if fldNm in infoDict:
            infoDict[fldNm]['freq'] = freq
            infoDict[fldNm]['category'] = (src.split(r'=')[-1].replace('factor_rq_', '').replace('.db','')).replace("_factors", "").replace("factor_","").replace("_alphas", "")
            infoDict[fldNm]['nm'] = nm.split(r'=')[-1]
            
    fldInfoDf = pd.DataFrame.from_dict(infoDict, orient='index')
    fldInfoDf = fldInfoDf.loc[:, ~fldInfoDf.columns.isin(['fldDir'])]
    fldInfoDf.sort_index(level=0, ascending=True, inplace=True)
    for dbFn in [f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db", "/Fairedge_dev/proj_JVrc/fldInfoDf.db"]:
        QpsUtil.smart_dump(fldInfoDf, dbFn, debug=True, title=funcn)
        QpsUtil.smart_dump(fldInfoDf, dbFn.replace('.db', '.csv'), fmt='csv', debug=True, title=funcn)
    return fldInfoDf

def get_params():
    paramsL = []
    for x in "prod1w,prod,rschhist,W,P,R,E,F,G,T".split(r','):
        paramsL.append({
            "SYMSET": "CS_ALL",
            "dts_cfg": x,
            "snn": "SN_CS_DAY"
        })
    return paramsL

if __name__ == '__main__':
    (opt, args) = get_options_jobgraph(list_cmds = lambda x: "")

    paramsL = get_params()

    dfLst = []
    infoLst = []        
        
    if opt.do == "gen_symsets":
        if opt.dts_cfg == "all":
            cfgs = "prod1w,F,G,R,P,W".split(r',')
        else:
            cfgs = [opt.dts_cfg]

        for dts_cfg in cfgs:
            opt.dts_cfg = dts_cfg
            #opt.dts_cfg = "rschhist,prod1w,prod,E,F,G,R,P,W"
            #print(f"INFO: dts_cfg= {opt.dts_cfg}")

            filteredParamsL = [x for x in paramsL if x['dts_cfg'] in [dts_cfg]]
            print(f"DEBUG_9871: dts_cfg= {dts_cfg}, filteredParamsL= {filteredParamsL}")
            
            main_gen_symsets(opt, filteredParamsL, dfLst, infoLst)

    elif opt.do == "gen_indices":
        if opt.dts_cfg == "all":
            opt.dts_cfg = "R,P,W,E,F,G"
        print(f"INFO: dts_cfg= {opt.dts_cfg}")
        filteredParamsL = [x for x in paramsL if x['dts_cfg'] in opt.dts_cfg.split(r',')]
        main_gen_indices(opt, filteredParamsL)           
    elif opt.do == "gen_flds_db":
        gen_fld_info_db()



