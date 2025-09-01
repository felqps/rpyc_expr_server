from doctest import OutputChecker
import sys
from webbrowser import get

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
from scipy import stats
#import pyfolio as pf
#from optparse import OptionParser
import pcComm
from common_basic import *
import QpsUtil

#from qdb_options import *
from common_options_helper import *

from CTX import *
from shared_cmn import *
from Scn import Scn
import FldNamespace as ns
from get_fld import get_fld_FGW
from bgen_helpers import get_strat_ids
from rank_select_cfg import get_rank_select_factors,get_rank_select_cfgs_for_symset
from FldNamespace import __lookup_membership_file__
from RpycExprSync import *
from df_helpers import zero2nan
from common_debug import *

#display config
#pd.set_option("display.max_rows", 50)
pd.set_option("display.max_columns", 20)
#pd.set_option("display.max_columns", None)
pd.set_option("display.width", 180)

def list_cmds(opt):
    pass

def qtile_to_one(df, alphaQPct=0.20):
    funcn = f"qtile_to_one({alphaQPct})"
    df = zero2nan(df) #XXX

    qtiles = df.quantile(1-alphaQPct, axis=1) #rank low to high
    # print_df(qtiles)
    # print_df(df.ge(qtiles, axis=0), title='gt')
    # print_df(df.lt(qtiles, axis=0), title='lt')
    df[df.ge(qtiles, axis=0)] = 1.0
    df[df.lt(qtiles, axis=0)] = 0.0
    df.fillna(0, inplace = True)
    print(f"INFO: {funcn} daily_mean= {df.sum(axis=1).sum(axis=0)/df.shape[0]}")
    return df

IDXMEM_CACHE={}
#@timeit
def do_symset_filter(opt, dfFGW, symset_filter):
    funcn = f"do_symset_filter({symset_filter})"
    global IDXMEM_CACHE
    if symset_filter in ['univ_cn500', 'univ_cn300']: #filter on cn500
        index_sym = "all"
        if symset_filter.find("500")>=0:
            index_sym = '000905'
        elif symset_filter.find("300")>=0:
            index_sym = '000300'

        idxmem = None

        if True:
            #idxmem_expr = f"@{opt.scn}:CS_ALL:index_membership('{index_sym}', OPEN)"
            #idxmem_expr = f"cs_1d_{opt.scn}:CS_ALL:index_membership('{index_sym}', OPEN)"
            idxmem_expr = f"@{opt.scn}:CS_ALL:index_membership('{index_sym}', OPEN)"
            if idxmem_expr in IDXMEM_CACHE:
                idxmem = IDXMEM_CACHE[idxmem_expr]
            else:
                force=0 #XXX
                rpycExpr = RpycExprSync(opt, expr=idxmem_expr, model='@basic', force=force) #Only do once a day, seperately in another program
                idxmem = rpycExpr.get()
                if False:
                    debug_dump(opt, idxmem, title=f"idxmem.{index_sym}.raw", note=f"{funcn} idxmem.{index_sym} {idxmem.shape}")
                #idxmem.set_index(pd.to_datetime(idxmem.index.date) + pd.Timedelta('15:00:00'), inplace=True)
                #idxmem.set_index(pd.to_datetime(idxmem.index) - pd.Timedelta('9:30:00'), inplace=True)
                if False:
                    debug_dump(opt, idxmem, title=f"idxmem.{index_sym}", note=f"{funcn} idxmem.{index_sym} {idxmem.shape}")
                for col in dfFGW.columns:
                    if col not in idxmem.columns:
                        idxmem.loc[:,col] = np.nan
                idxmem = idxmem[dfFGW.columns]
                # print("=================", dfFGW.tail(5))


                beg_dtm = idxmem.index[0]
                end_dtm = idxmem.index[-1]
                
                
                #print_df(idxmem, title=f"{funcn}:{rpycExpr}:before_padding:{idxmem.shape}") 
                idxmem = idxmem.reindex(dfFGW.index)
                #padding data outside range
                for dtm in dfFGW.index:
                    if dtm > end_dtm:
                        idxmem.loc[dtm,:] = idxmem.loc[end_dtm,:]
                    elif dtm < beg_dtm:
                        idxmem.loc[dtm,:] = idxmem.loc[beg_dtm,:]

                # idxmem.fillna(method='ffill', inplace=True)
                # idxmem.fillna(method='bfill', inplace=True)
                #idxmem.loc[dfFGW.index[~dfFGW.index.isin(idxmem.index)], :] = np.nan
                #print_df(idxmem, title=f"{funcn}:{rpycExpr}:post_padding:{idxmem.shape}")
                if False:
                    debug_dump(opt, idxmem, title=f"idxmem.{index_sym}.post_padding", note=f"{funcn} idxmem.{index_sym}.post_padding {idxmem.shape}")
                    debug_dump(opt, idxmem.fillna(-1).cumsum(), title=f"idxmem.{index_sym}.post_padding.cumsum", note=f"{funcn} idxmem.{index_sym}.post_padding {idxmem.shape}")
                    debug_dump(opt, idxmem.sum(axis=1), title=f"idxmem.{index_sym}.count", note=f"{funcn} idxmem.{index_sym}.post_padding count", data_type='se')

                IDXMEM_CACHE[idxmem_expr] = idxmem
        
        if idxmem is not None and True:
            dfFGW = dfFGW * idxmem
        else:           
            for col in dfFGW.columns:
                in_filter = __lookup_membership_file__(col, '20230118', symset_filter, debug=False)
                #print(col, in_filter)
                if not in_filter:
                    dfFGW.loc[:,col] = np.nan

        if ctx_verbose(0) and False:
            print_df(dfFGW, show_head=True, cols=30, rows=10, title=f"{funcn}:dfFGW:after:{dfFGW.shape}")
    return dfFGW

@timeit
def qtile_combine(opt, comb_db, fldnms, alphaQPct=0.20, symset_filter="univ_all", debug=False): #
    funcn = f"qtile_combine()"
    dfComb = None

    # print(opt)
    # fds = FDFCfg("cs_1d_T").ss(opt.symset).fdf("index_membership('000905')")
    # print(fds)
    # exit(0)


    for fldnm in fldnms:
        dfFGW = get_fld_FGW(opt, fldnm, opt.symset, dts_cfg=opt.scn)
        if False:
            debug_dump(opt, dfFGW, title=f"dfFGW.{fldnm}", note=f"{funcn} dfFGW fldnm={fldnm}; shape= {dfFGW.shape}")

        if dfFGW is None:
            print(f"{RED}ERROR: {funcn} dfFGW is None for fldnm= {fldnm}{NC}")
            continue

        print(f"{NC}INFO: {funcn} do_symset_filter for fldnm= {fldnm}, symset_filter= {symset_filter}{NC}")
        dfFGW = do_symset_filter(opt, dfFGW, symset_filter)
        if False:
            debug_dump(opt, dfFGW, title=f"dfFGW.{fldnm}.post_symset_filter", note=f"{funcn} dfFGW fldnm={fldnm}; shape= {dfFGW.shape}; symset_filter= {symset_filter}")
        if dfFGW is None:
            continue

        if debug:
            print_df(dfFGW, show_head=1, rows=10, title="dfFGW: after symset filter")

        if ctx_verbose(1):
            print_df(dfFGW, show_head=True, show_body=True, rows=3, title=f"{funcn} fldnm= {fldnm}")

        #print(dfFGW.tail(20))
        dfFGW = qtile_to_one(dfFGW, alphaQPct=alphaQPct)
        #print(dfFGW.tail(20))

        if debug:
            print(f"DEBUG_3453: {fldnm}, shape= {dfFGW.shape}")
            print_df(dfFGW, show_head=1, rows=10, cols=30, title="dfFGW: after qtile_to_one")
            print_df(dfFGW.sum(axis=1)/dfFGW.shape[1], show_head=1, rows=10, cols=30, title="dfFGW.sum(axis=1)")
            #print(dfFGW.sum(axis=1).tail(10)/dfFGW.shape[1])
        
        if ctx_verbose(1):
            print_df(dfFGW, show_head=True, show_body=True, rows=3, title=f"{funcn} qantile fldnm= {fldnm}")

        if dfComb is None:
            dfComb = dfFGW
        else:
            #dfComb = dfComb + dfFGW
            dfComb = dfComb.add(dfFGW, fill_value=0.0)

        if ctx_debug(1):
            print(f"DEBUG_4343: adding {fldnm} to comb_db")
            comb_db[fldnm] = dfFGW.stack()

    if ctx_verbose(1):
        print_df(dfComb, show_head=True, show_body=True, rows=3, title=f"{funcn} dfComb")
    
    if False:
        debug_dump(opt, dfComb.sum(axis=1), title=f"dfComb.sum", note=f"{funcn} dfComb.sum", data_type="se")
        debug_dump(opt, dfComb.sum(axis=1)/len(fldnms), title=f"dfComb.avg", note=f"{funcn} dfComb.avg", data_type="se")

    return(dfComb)

def get_daily_pos_units(df, unitFldnm="Pred_CombEqWgt"):
    daily_units = df[unitFldnm].unstack().sum(axis=1)
    if ctx_verbose(1):
        print_df(daily_units, title="daily_pos_units")
    return daily_units

def get_daily_trade_units(df, unitFldnm="Pred_CombEqWgt"):
    daily_pos = df[unitFldnm].unstack()
    daily_trd = daily_pos - daily_pos.shift(1)
    if False:
        print_df(daily_pos, title="daily_pos")
        print_df(daily_trd, title="daily_trd")
    daily_trade_units = daily_trd.abs().sum(axis=1)
    if ctx_verbose(1):
        print_df(daily_trade_units, title="daily_trade_units")
    return daily_trade_units

def get_daily_pnl(df, unitFldnm="Pred_CombEqWgt", retFldnm="Resp_lnret_C2C_1d_1"):
    daily_pnl_df = df[unitFldnm].unstack()*df[retFldnm].unstack()
    #print_df(daily_pnl_df)
    daily_pnl = daily_pnl_df.sum(axis=1)
    #print(daily_pnl)
    return daily_pnl

@timeit
def dict2df_slow(dictin):
    dfout = pd.DataFrame.from_dict(dictin)
    print("slow", dfout.shape)
    return dfout

@timeit
def dict2df(dictin):
    #dfout = pd.DataFrame.from_dict(dictin)
    dfout = pd.DataFrame()
    keys = list(dictin.keys())
    keys.remove('Pred_OHLCV_C_1d_1')  
    keys = ['Pred_OHLCV_C_1d_1', *keys]
    for k in keys:
        print(f"INFO: dict2df {k:<36s}: {k.shape if isinstance(dictin[k], pd.DataFrame) else len(dictin[k])}")
        dfout[k] = dictin[k]
    print("fast", dfout.shape)
    return dfout

@timeit
def gen_factcomb(opt, select_cfg, stratid, symset_filter):
    funcn = f"gen_factcomb(sef_cfg={select_cfg}, si={stratid}, sf={symset_filter})"

    comb_db = {}
    outDir = f"/qpsdata/egen_study/factcomb"
    if opt.scn in ['T', 'U']:
        outDir = f"/qpsdata/egen_study/factcomb.{opt.scn}"
    outDir = f"{outDir}/{opt.symset}/{select_cfg['sel_name']}"

    if opt.outdir != "":
        outDir = opt.outdir
    else:
        opt.outdir = outDir
        
    outDir = f"{outDir}.{stratid}.{symset_filter}"

    QpsUtil.mkdir([outDir])
    if os.path.exists(f"{outDir}/factcomb.db") and opt.force <= 0:
        print(f"INFO: tgt {outDir}/factcomb.db already exists, skipping...")
        return

    for fldnm in ["Resp_lnret_C2C_1d_1", "Pred_OHLCV_C_1d_1", "Pred_OHLCV_CFlag_1d_1", "Pred_inc_AgePrc_1d_1", "Pred_univ_NonStSts_1d_1", "Pred_OHLCV_V_1d_1"]:
        fld = get_fld_FGW(opt, fldnm, opt.symset, opt.scn)
        comb_db[fldnm] = fld.stack()

    fldnms=get_rank_select_factors(opt.symset, select_cfg)
    fldnms = [fldnm for fldnm in fldnms if "Rq" not in fldnm and "FmtlEp2" not in fldnm]
    print(f"INFO: {funcn} fldnms_count=  {len(fldnms)}")
    if True:
        for fldnm in fldnms:
            print(f"selected factor: fldnm= {fldnm}")

    pred_eq_wgt = qtile_combine(opt, comb_db, fldnms=fldnms, symset_filter=symset_filter, alphaQPct=0.20)

    #print_df(pred_eq_wgt, rows=20, cols=30, title=f"{funcn}")
    if False: 
        debug_dump(opt, pred_eq_wgt, title=f"pred_eq_wgt", note=f"{funcn}", data_type='df')
        debug_dump(opt, pred_eq_wgt.sum(axis=1), title=f"pred_eq_wgt.sum", note=f"{funcn}", data_type='se')

    comb_db[f"Pred_CombEqWgt"] = pred_eq_wgt.stack()

    if ctx_debug(1):
        print(f"DEBUG_2342: {comb_db.keys()}")

    #dfFactComb = dict2df_slow(comb_db)
    dfFactComb = dict2df(comb_db)

    if ctx_verbose(1):
        print_df(dfFactComb, title='dfFactComb')

    for fldnm in comb_db.keys():
        if ctx_verbose(1):
            print_df(dfFactComb[fldnm].unstack(), show_head=True, show_body=True, rows=3, title=fldnm)

    smart_dump(select_cfg, f"{outDir}/select_cfg.db", verbose=True, force=True)
    smart_dump(dfFactComb, f"{outDir}/factcomb.db", verbose=True, force=True)
    smart_dump(dfFactComb, f"{outDir}/factcomb.csv", verbose=True, force=True)

    daily_units = get_daily_pos_units(dfFactComb)
    daily_trd = get_daily_trade_units(dfFactComb)
    #round-trip cost is 13 per 10000, we assume slippage the same, so charge it on both buys and sells.
    daily_trd_cost = daily_trd * 13.0 / 10000.0
    daily_gross_pnl = get_daily_pnl(dfFactComb)
    daily_gross_ret = daily_gross_pnl/daily_units

    daily_net_pnl = daily_gross_pnl - daily_trd_cost
    daily_net_ret = daily_net_pnl/daily_units
    if ctx_verbose(1):
        print("INFO: gross_ret=", daily_gross_ret[daily_gross_ret.index.year == 2022].cumsum().tail(10))
        print("INFO: net_ret=", daily_net_ret[daily_gross_ret.index.year == 2022].cumsum().tail(10))

if __name__=='__main__':
    funcn = 'factcomb.main'
    #(opt, args) = get_options_sgen(list_cmds, customize_options=None)
    (opt, args) = get_options(funcn)
    # print(f"opt.outdir= {opt.outdir}")
    # if ctx_verbose(1):
    #     print(opt, file=sys.stderr)
    ctx_set_opt(opt)
    if not opt.symset_filter:
        opt.symset_filter = 'univ_all,univ_cn500,univ_cn300'
    opt_dict = opt2dict(opt)
    print_opt(opt_dict)

    try:
        stratids = get_strat_ids(opt.scn)
        if opt.stratid:
            stradids = opt.stratid.split(r',')
        for stratid in stradids:
            for symset_filter in opt.symset_filter.split(r','): #['univ_all', 'univ_cn500', 'univ_cn300']:
                print(f"INFO: stratid= {stratid}, symset_filter= {symset_filter}")
                for select_cfg in get_rank_select_cfgs_for_symset(opt, opt.scn, opt.symset, stratid):
                    if select_cfg['sel_name'] == opt.grep and opt.dryrun == 0:
                        if opt.debug:
                            print(f"DEBUG_8708: {funcn} sel_name= {opt.grep}, rank_select_cfg= {select_cfg}")
                        gen_factcomb(opt, select_cfg, stratid, symset_filter)
                    elif opt.grep == ".*":
                        print(f"python /Fairedge_dev/app_factrank/factcomb.py --symset {opt.symset} --scn {opt.scn} --asofdate {opt.asofdate} --grep {select_cfg['sel_name']}")

    except Exception as ex:
        print(f"Error: {ex}")
        traceback.print_exc()

    print_opt(opt_dict)
        

