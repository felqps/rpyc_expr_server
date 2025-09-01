#from doctest import OutputChecker
import sys
#from webbrowser import get

import os
import time
import datetime
import re
import glob
import traceback
import math
import json
import pickle
import pandas as pd
import numpy as np
from common_basic import *
import QpsUtil
from qdb_options import *
#from CTX import *
from shared_cmn import *
from Scn import Scn
#import FldNamespace_egen as ns
from common_env import env
from concat_factors import *
from common_options_helper import get_options
from cmdline_opt import *

def fldnm2timedelta(fldnm):
    # if fldnm.find("O_1d")>=0:
    #     dta.index = dta.index +  pd.Timedelta('09:30:00')
    # elif fldnm.find("H_1d")>=0 or fldnm.find("L_1d")>=0 or fldnm.find("C_1d")>=0 or fldnm.find("V_1d")>=0:
    #     dta.index = dta.index +  pd.Timedelta('15:00:00')
    if fldnm.find("2O")>=0 or fldnm.find("_O_")>=0:
        return pd.Timedelta('09:30:00')
    if fldnm.find("2A")>=0:
        return pd.Timedelta('13:00:00')
    else:
        return pd.Timedelta('15:00:00')

def try_use_local(fldnm):
    if fldnm.find("/qpsdata")>=0:
        return False #full back specified
    elif fldnm.find("Resp_") >= 0:
        return True
    elif fldnm.find("Pred_OHLCV_") >= 0:
        return True
    return False

def load_fld_new(opt, scn, fldnm, debug=True, use_local=False, align_to="NA", cachefn_found=None):
    funcn = f"load_fld_new(scn={scn}, fldnm={fldnm})"
    if opt.verbose:
        print(f"{RED}{funcn}{NC}")
    df = get_fld_FGW(opt, fldnm, opt.symset, scn.dts_cfg, default_merge=False)
    if ctx_debug(1):
        print_df(df, title=f"{funcn} {fldnm}")
    return df

#@timeit_real
def load_fld_impl(scn, fldnm, symset, debug=False, use_local=False, align_to="NA", cachefn_found=None, error_if_not_found=True):
    funcn = f'load_fld_impl(scn.cfg= {scn.dts_cfg:<6s}; fldnm={fldnm:<36s}; symset= {symset}; debug= {debug})'
    if ctx_debug(5):
        print(f"INFO: {NC}{funcn}{NC}")
    debug=False

    if ctx_debug(5):
        print(f"DEBUG_2234: {funcn} {scn}")
        print(f"DEBUG_2235: {scn.qdfRoot}")

    show_smart_load = 5
    use_local = try_use_local(fldnm)
    use_local = False

    #sign = get_predictor_sign(fldnm, symset)
    sign = None
    if use_local:
        tgtFp = f"/qpsuser/che/fld_cache/{scn.dts_cfg}/{symset}.{studyResp()}.{fldnm}.pkl" #Different resp are aligned differently
        if scn.dts_cfg != "prod1w" and use_local: #prod1w will not use cache, as it keeps changing
            if os.path.exists(tgtFp):
                print(f"DEBUG_xx76: {funcn}, {tgtFp}")
                dta = smart_load(tgtFp, debug=True, title=f"{'<'*100} {funcn}:cache", cachefn_found=cachefn_found)
                return dta if sign is None else dta * sign

    bar = fldnm2bar(fldnm)

    fldFp = (scn.qdfRoot + bar + fldnm + f'{symset}.db').fp()

    if ctx_debug(5):
        print(f"{DBG}DEBUG_3454: {funcn} {fldFp}{NC}")

    dta = None
    mkts = []
    dfL = []
    if QpsUtil.smart_find(fldFp):
        dta = smart_load(fldFp, title=funcn, debug=show_smart_load, align_to=align_to, cachefn_found=cachefn_found, error_if_not_found=error_if_not_found)
        mkts = dta.columns
    elif fldFp.find("A_")>0:
        pass
    else:
        print(f"{RED}ERROR: {funcn} cannot find {fldFp}{NC}")
        symsetFn = fn_symset(symset)
        if symsetFn.find("Indices")<0:
            symsetDict = QpsUtil.smart_load(symsetFn, use_smart_find=True, title=funcn, debug=False)  
        else:
            symsetDict = {"Indices": []}

        if len(symsetDict.keys())>1 or True:      
            for indu in symsetDict.keys():
                if fldnm.find("/qpsdata")<0:
                    fldFp = (scn.qdfRoot + bar + fldnm + f'{indu}.db').fp()
                else:
                    fldFp = fldnm

                mkts.extend(symsetDict[indu])

                show_smart_load = show_smart_load - 1
                df = QpsUtil.smart_load(fldFp, debug=show_smart_load>0, use_smart_find=True, title=f"{funcn} show:{show_smart_load} use_smart_find: True", error_if_not_found = error_if_not_found)

                if df is not None:
                    dfL.append(df)
                    if debug:
                        print(f"INFO: {funcn} found {fldFpFound}")
                else:
                    if error_if_not_found and fldFp.find("Unknown.db")<0:
                        print(f"WARNING: {funcn} cannot find {fldFp}")

            if True:
                print(f"INFO: {funcn} symset= {symset}, tgt= {fldFp}, indu= {list(symsetDict.keys())}, mkt_cnt= {len(mkts)}")  

        try:
            if len(dfL)>0:
                dta = pd.concat(dfL, axis=1)
        except Exception as e:
            print(f"ERROR: {funcn} for {fldFp}, exception=", e)

    if dta is None:
        return None

    dta = dta[sorted(dta.columns)]
    # print_df(dta)
    # print(f"DEBUG_9990: {type(dta.index)}")
    # print(dta.index.date)
    # print(ctx_cutoffdt(), dtparse(ctx_cutoffdt()))
    # if str(type(dta.index)).find("DatatimeIndex")<=0:
    #     dta.index = [dtparse(x) for x in dta.index]
    #     print(type(dta.index))

    #dta = dta[dta.index.date<=dtparse(ctx_cutoffdt()).date()]
    dta = dta[dta.index<=ctx_cutoffdt()]
    if ctx_debug(7655):
        print_df(dta, title=f"DEBUG_7655 {funcn} {fldnm}")
    if ctx_debug(8249):
        print_df(dta, title=f"{funcn} {fldnm} cutoffdt={ctx_cutoffdt()}")

    dta = QpsUtil.df_fix_duplicate_columns(dta)
    last_valid_index = dta.last_valid_index()
    if last_valid_index is None:
        if fldFp.find("_univ_")>=0 or fldFp.find("Unknown")>=0:
            print(f"{BROWN}WARNING: {funcn} data file contains NaN only, {fldFp}{NC}")
        else:
            print(f"{ERR}ERROR: {funcn} data file contains NaN only, {fldFp}{NC}")
        return None

    # for fp in fldFps:
    #    QpsUtil.smart_load(fp).tail()

    if debug:
        print_df(dta, title=f"INFO: {funcn} data {fldnm}")
        print(f"INFO: {funcn} columns", QpsUtil.format_arr(dta.columns))
        print(f"INFO: {funcn} mkts", QpsUtil.format_arr(mkts))
        print(f"INFO: {funcn} shape", dta.shape)
    
    mkts = set(mkts).intersection(dta.columns)
    mkts = sorted(mkts)
    if debug:
        print(f"DEBUG: {funcn} {sorted(mkts)[:5]}")

    if datetime.time(0, 0) == last_valid_index.time(): #not set time
        dta.index = dta.index +  fldnm2timedelta(fldnm)

    dta = dta[mkts]   #This to support custom symsets with might be a partial collection of different indus

    if use_local:
        QpsUtil.mkdir([os.path.dirname(tgtFp)])
        QpsUtil.smart_dump(dta, tgtFp, debug=True, title=f"{'>'*100} {funcn}")

    return dta if sign is None else dta * sign

SCN_DICT = {}
def get_fld_scn(opt, symset, dts_cfg):
    global SCN_DICT
    scn_name = f"{symset}.{dts_cfg}"
    if scn_name not in SCN_DICT:
        scn = Scn(opt, symset, dts_cfg, 'SN_CS_DAY', asofdate=opt.asofdate)
        if ctx_debug(5):
            print("DEBUG_8345:", scn)
        SCN_DICT[scn_name] = scn
    else:
        scn = SCN_DICT[scn_name]
    return(scn)

def get_fld_FGW(opt, fldnm, symset, dts_cfg='W', default_merge=False, debug=True):
    if dts_cfg == "prod":
        dts_cfg = "prod1w"

    debug=True
    # if debug is None:
    #     debug = ctx_verbose(1 + env().print_get_fld_fgw())

    opt_asofdate = opt.asofdate #need to hack date here
    #opt.asofdate='uptodate' #XXX

    dts_cfg_extended = [dts_cfg]
    #dts_cfg_extended = ['F','G','W']
    if dts_cfg in ['A']:
        dts_cfg_extended = ['A','F','G','W'] #XXX added 'A', if 'A' already exist, use it
    if dts_cfg in ['U']:
        dts_cfg_extended = ['F','G','W','T'] #append means we merge F/G/W/T into long time series, takes longer to run
    if dts_cfg in ['T']:
        dts_cfg_extended = ['W','T'] #for T, some fld need to smart_load from W

    funcn = f"get_fld_FGW(fldnm= {fldnm}, symset={symset}, dts_cfg={dts_cfg}, dts_cfg_extended= {dts_cfg_extended})"
    if opt.verbose:
        print(f"{BROWN}INFO: {funcn}{NC}")

    # if debug:
    #     print(f"DEBUG_65xx: {funcn}({fldnm}, {symset}, {dts_cfg}) dts_cfg_extended= {dts_cfg_extended}")

    # For U, if U fldfn already exists, then donot need to load FGWT 
    dfs = []
    # if dts_cfg not in dts_cfg_extended:
    #     scn = get_fld_scn(opt, symset, dts_cfg)
    #     df = load_fld_impl(scn, fldnm, symset, debug=debug, error_if_not_found=False)
    #     if df is not None:
    #         dfs.append(df)

    if len(dfs) <= 0:
        for dc in dts_cfg_extended: 
            scn = get_fld_scn(opt, symset, dc)
            df = load_fld_impl(scn, fldnm, symset, debug=debug)
            if df is None:
                if dc != 'A': #virtual scn like A are normal to be missing
                    print(f"WARNING: {funcn} can not find file for {dc}:{fldnm}:{symset}")
            else:
                dfs.append(df)
                if dc == 'A':
                    #no concat needed
                    break

                # if fldnm == "Pred_SRCH8f7ffc_RqTAPI_neg_ma5_1d_1" and False:
                #     print_df(df, show_head=True)

    if len(dfs)<=0:
        dfFGW = None
        prev_df_last_dtm = None
    else:
        dfFGW = concat_factors(fldnm, dfs)
        prev_df_last_dtm = dfFGW.index[-1]
        
    if display(100201):
        print(f"INFO: {funcn} fldnm= {fldnm:<24}; symset= {symset}; scn= {dts_cfg}; shape= {dfFGW.shape if dfFGW is not None else 'NA'}; prev_df_last_dtm= {prev_df_last_dtm}; dts_cfg_extended= {dts_cfg_extended}", file=sys.stderr)
    
    opt.asofdate = opt_asofdate
    return dfFGW

if __name__ == "__main__":
    funcn = "get_fld.main"
    opt, args = get_options(funcn)
    print_opt(opt)

    symset="C39"
    for fldnm in [
        "Resp_lnret_C2C_1d_1",
        "Pred_OHLCV_C_1d_1",
        "Pred_SRCH8f7ffc_DNA_d38fe5_1d_1",
        "Pred_mysql_sbp_1d_1",
        "Pred_SRCH8f7ffc_FmtlEp_1d_1"
    ]:
        df = get_fld_FGW(opt, fldnm=fldnm, symset=symset, dts_cfg="T")
        if type(df) == type(pd.DataFrame()):
            print_df(df, title=f"shape= {df.shape}")
