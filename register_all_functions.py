import os,sys

from pathlib import Path
import pandas as pd 
import numpy as np 

#from platform_helpers import *
import alpha101_code
from fdf_helpers import print_df
import sys
import math
import pickle
from pathlib import Path

from get_fld import get_fld_FGW
from FldNamespace import calc_prc_flag, calc_StSts
from df_helpers import reverse_bool,bool2num,nonzero2one
from qpstimeit import *
from funcs_fld import filter_range


import funcs_fld as ffld
#from register_script_functions import *
#from alpha101_code import decay_linear as decay_linear_alpha
from common_smart_dump import smart_dump
from cmdline_opt import cmdline_opt
from ExprHelper import *
from performance_eval_server import print_out_mixed_type_dict
from PostgreSqlTools import postgres_instance
from dynamic_modules import load_dynamic_modules,register_function,register_factor_worker,print_registered_functions,cf,cs

def calculate_corr(x, y, trading_days=243, formula_worker=None): #expr_eval_by_corr"):
    if formula_worker is not None:
        print(f"{DBG}Postgres{NC}: {formula_worker.pld()['EXPR_FULL']}", file=sys.stderr)
    x_bar = x.sub(x.mean(axis=1), axis=0)
    y_bar = y.sub(y.mean(axis=1), axis=0)
    corr = (x_bar * y_bar).mean(axis=1) / (np.std(x, axis=1) * np.std(y, axis=1))
    corr_mean = corr.mean()
    corr_std = corr.std()
    if corr_std > 0:
        ir = math.sqrt(trading_days) * corr_mean / corr_std
    else:
        ir = 0
    corr_mean = float(corr_mean)
    corr_std = float(corr_std)
    ir = float(ir)    
    return [corr_mean, corr_std, ir]

def eval_by_corr(x, y, trading_days=243, formula_worker=None):
    [corr_mean, corr_std, ir] = calculate_corr(x,y,trading_days,formula_worker)
    #smart_dump(x, "/Fairedge_dev/app_qpsrpyc/test_predictor_dump.db", verbose=1)
    postgres_data = {'corr_mean': corr_mean, 'corr_std': corr_std, 'ir': ir, 'factor_value': x}
    postgres_data.update(ExprHelper(formula_worker.pld()['EXPR_FULL']).to_dict())
    if formula_worker is not None:
        print_out_mixed_type_dict(postgres_data)
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    postgres.save_dict("expr_eval_by_corr", postgres_data)
    return [corr_mean, corr_std, ir]

def pretty_mean_std_ir(mean, std, ir, expr, mean_threshold=0.0250, pass_threshold_only=False):
    ln = f"[{mean: 8.6f}, {std: 8.6f}, {ir: 8.6f}]"
    if not pass_threshold_only:
        mean_threshold = 0.0
    if abs(mean) > mean_threshold or not pass_threshold_only:
        COLOR = RED
        if mean < -mean_threshold:
            COLOR = GREEN
        print(f"{COLOR}corr= {ln}{NC}, formula= {expr}", file=sys.stderr)
    else:
        if pass_threshold_only and False:
            ln = None
    return ln

@timeit
def fld_fgw(fw, fldnm, refdf=None, debug=False):
    funcn = f"fld_fgw(fw= {fw}, fldnm= {fldnm})"
    print(f"{BROWN}INFO: {funcn}{NC}")
    if refdf is None:
        refdf = fw.ctx()['CLOSE']
    fw.opt().qdb_cfg = 'all'
    fw.opt().asofdate = cmdline_opt().asofdate #'uptodate'
    dts_cfg = fw.opt().cfg.split(r'_')[-1]
    #Currently always get the longest combined history, may be fixed later to improve speed
    if fldnm.find("Pred")<0:
        fldnm = f"Pred_Rqraw_{fldnm}_1d_1"

    fld = get_fld_FGW(fw.opt(), fldnm, fw.opt().symset, dts_cfg=dts_cfg, default_merge=False, debug=None)
    if fldnm.find("OHLCV_O_")>=0:
        fld.set_index(pd.to_datetime(fld.index.date) + pd.Timedelta('15:00:00'), inplace=True)

    if debug:
        print("debug_3234:")
        print_df(fld, title=f"{fldnm} before")

    missing_columns = [x for x in refdf.columns if x not in fld.columns]
    missing_rows = [x for x in refdf.index if x not in fld.index]
    if len(missing_columns)>0:
        print(f"INFO: {funcn} compare fld({fld.shape}) to refdf({refdf.shape}), missing_columns_count= {len(missing_columns)}; missing_rows_count= {len(missing_rows)}")
        #print(f"INFO: {funcn} missing_columns= {missing_columns}; missing_rows= {missing_rows}")

    for col in missing_columns:
        #fld.loc[:,col] = np.nan
        fld.loc[col] = np.nan


    # for row in missing_rows:
    #     fld.loc[row,:] = np.nan
    fld = fld.reindex(index=refdf.index, fill_value=np.nan)


    # fld = fld.loc[refdf.index,refdf.columns]

    if debug:
        print("debug_3235:")
        print_df(refdf, title='refdf')
        print_df(fld, title=f"{fldnm} after")
    return fld

def set_var(fw, varname, expr):
    fw.ctx()[varname] = fw.eval_expr(expr)
    #print(fw.ctx())
    return fw.ctx()[varname]

def save_as(v,fn):
    Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)
    pickle.dump(v, open(fn, 'wb'))
    return v.shape

def init_ctx(fw, debug=False):
    register_function(fw, 'save_as', save_as, 2, 'ds')
    register_function(fw, 'set_var', lambda v,e: set_var(fw, v, e), 2, 'ss')
    register_function(fw, 'calc_prc_flag', calc_prc_flag, 4, 'dddd')
    register_function(fw, 'calc_ststs', calc_StSts, 1, 'd')
    register_function(fw, 'reverse_bool', cf.reverse_bool, 1, 'dddd')
    register_function(fw, 'fld_fgw', lambda fldnm, refdf: fld_fgw(fw, fldnm, refdf), 2, 'sd')
    register_function(fw, 'F', lambda fldnm: fld_fgw(fw, fldnm, refdf=None), 1, 's')
    register_function(fw, 'print_df', print_df, None, None)
    register_function(fw, 'add', lambda x,y: x+y, 2, None)
    register_function(fw, 'sub', lambda x,y: x-y, 2, None)
    register_function(fw, 'mul',lambda x,y: x*y, 2, None)
    register_function(fw, 'div',lambda x,y: x/y, 2, None)
    register_function(fw, 'regout',lambda y,x: cf.regout(y, x)[0], 2, 'dd')
    register_function(fw, 'z', cf.Z, 1, None)
    register_function(fw, 'zz',lambda d: cf.Z(d).fillna(0.0), 1, None)
    register_function(fw, 'w', cf.Winsorize, 1, None)
    register_function(fw, 'm', cf.m, 1, None)
    register_function(fw, 'de_mean', cf.m, 1, None)
    register_function(fw, 'zw', cf.ZW, 1, None)
    register_function(fw, 'ref', cf.ref, 2, 'di')
    register_function(fw, 'delay', cf.delay, 2, 'di')
    register_function(fw, 'delta', cf.delta, 2, 'di')
    register_function(fw, 'sum', cf.sum, 2, 'di')
    register_function(fw, 'stddev', cf.stddev, 2, 'di')
    register_function(fw, 'iif', cf.iif, 3, None) #iif=if, but if is reserved
    #register_function(fw, 'CORRELATION', lambda a,b,d: rolling_spearman(a,b,d)
    register_function(fw, 'correlation', cf.correlation, 3, 'ddi')
    register_function(fw, 'covariance', cf.covariance, 3, 'ddi')
    register_function(fw, 'ts_rank', cf.ts_rank, 2, 'di')
    register_function(fw, 'ts_sum', cf.ts_sum, 2, 'di')
    register_function(fw, 'ts_sft', cf.ts_sft, 2, 'di')
    register_function(fw, 'ts_prod', cf.ts_prod, 2, 'di')
    register_function(fw, 'ts_delta', cf.ts_delta, 2, 'di')
    register_function(fw, 'ts_stddev', cf.ts_stddev, 2, 'di')
    register_function(fw, 'ts_stddev', cf.ts_stddev, 2, 'di')
    register_function(fw, 'ts_zscore', cf.ts_zscore, 2, 'di')
    register_function(fw, 'ts_corr', cf.ts_corr, 3, 'ddi')
    register_function(fw, 'ts_cov', cf.ts_cov, 3, 'ddi')
    register_function(fw, 'ts_argmax', cf.ts_argmax, 2, 'di')
    register_function(fw, 'ts_argmin', cf.ts_argmin, 2, 'di')
    register_function(fw, 'ts_avg', cf.ts_avg, 2, 'di')
    register_function(fw, 'ts_min', cf.ts_min, 2, 'di')
    register_function(fw, 'ts_max', cf.ts_max, 2, 'di')
    register_function(fw, 'rank', cf.rank, 1, "d")
    register_function(fw, 'rank_sub', cf.rank_sub, 2, "dd")
    register_function(fw, 'rank_div', cf.rank_div, 2, "dd")
    register_function(fw, 'sign', cf.sign, 1, None)
    register_function(fw, 'neg', cf.neg, 1, None)
    register_function(fw, 'sqrt', cf.sqrt, 1, None)
    register_function(fw, 'inv', cf.inv, 1, None)
    register_function(fw, 'pow2', cf.pow2, 1, None)
    register_function(fw, 'pow3', cf.pow3, 1, None)
    register_function(fw, 'sigmoid', cf.sigmoid, 1, None)
    register_function(fw, 'm', cf.m, 1, None)
    register_function(fw, 'false2nan', cf.false2nan, 1, None)
    register_function(fw, 'toprank', cf.toprank, 1, None)
    register_function(fw, 'lnret', cf.lnret, 1, None)
    register_function(fw, 'ffill', lambda f: f.fillna(method='ffill', inplace=False) if f is not None else f, 1, None)
    register_function(fw, 'abs', lambda f: np.abs(f), 1, None)
    register_function(fw, 'max', lambda a,b: a.where(a > b, b).fillna(a), 2, None)
    register_function(fw, 'min', lambda a,b: a.where(a < b, b).fillna(a), 2, None)
    register_function(fw, 'lt', lambda a,b: cf.less_than(a,b), 2, None)
    register_function(fw, 'log', lambda f: np.sign(f)*np.log(f.abs() + 1e-10), 1, None)
    
    register_function(fw, 'adv', lambda d: fw.var('VOLUME').rolling(d).mean(), 1, 'i')
    #register_function(fw, 'adv', lambda d: fw.F('V_1d').rolling(d).mean()
    #register_function(fw, 'decay_linear', lambda f,d: alpha101_code.decay_linear(f, d)
    register_function(fw, 'decay_linear', cf.decay_linear, 2, 'di') #x timeseries could be all nan for a stock
    register_function(fw, 'scale', cf.scale, 2, 'di')

    #register_function(fw, 'ema', lambda f,d: f.rolling(d).mean() if d>1 else f
    register_function(fw, 'ema', cf.ema, 2, 'di')
    #register_function(fw, 'ema', lambda f,d: f.ewm(halflife=d, axis=0) if d>1 else f

    register_function(fw, 'ma', cf.ma, 2, 'di')

    #register_function(fw, 'signedpower', lambda f,d: pow(f,d)
    register_function(fw, 'signedpower', cf.signedpower, 2, 'di')

    register_function(fw, 'as_float', lambda f: bool2num(f), 1, 'd')
    register_function(fw, 'product', cf.product, 2, 'di')

    register_function(fw, 'industry_neutralize', lambda f: ffld.industry_neutralize('A', f), 1, 'd')
    # register_function(fw, 'SECTOR_CODE', lambda f: calc_classification_code(f, dts_cfg=fw._opt.dts_cfg, clsname='sector')
    # register_function(fw, 'RQINDU_CODE', lambda f: calc_classification_code(f, dts_cfg=fw._opt.dts_cfg, clsname='rqindu')
    # register_function(fw, 'CITICSINDU_CODE', lambda f: calc_classification_code(f, dts_cfg=fw._opt.dts_cfg, clsname='citicsindu')
    # register_function(fw, 'SWSINDU_CODE', lambda f: calc_classification_code(f, dts_cfg=fw._opt.dts_cfg, clsname='swsindu')
    # register_function(fw, 'Beta3h_60', lambda f: calc_beta(fw._opt, f, '3h', n_beta_days=60, min_days=20)
    # register_function(fw, 'Beta5h_60', lambda f: calc_beta(fw._opt, f, '5h', n_beta_days=60, min_days=20)
    # register_function(fw, 'FFILL', lambda f: f.fillna(method='ffill', inplace=False) if f is not None else f
    register_function(fw, 'filter_range', lambda f,llmt,ulmt,threshold: filter_range(f, llmt, ulmt, threshold), 4, 'dddf')
    # fw._ctx['FILTER_LIKE', lambda f,ref_mkt,begdt,enddt,tgtdf: filter_like(f, ref_mkt, begdt, enddt, tgtdf)

    register_function(fw, 'get_trd_qty', cs.get_trd_qty, 2, 'bd') #b=bookid
    register_function(fw, 'get_trd_prc', cs.get_trd_prc, 2, 'bd')
    register_function(fw, 'get_trd_amt', cs.get_trd_amt, 2, 'bd')
    register_function(fw, 'get_trd_pnl', cs.get_trd_pnl, 2, 'bd')
    register_function(fw, 'get_open_qty', cs.get_open_qty, 2, 'bd')
    register_function(fw, 'get_fund_stats', cs.get_fund_stats, 1, 'F') #F=fund name

    register_function(fw, 'index_membership', cs.index_membership, 2, 'Bd') #B=Benchmark Index 
    register_function(fw, 'industry_membership', cs.industry_membership, 2, 'Id') #I=industry
    register_function(fw, 'plot_line', cs.plot_line, 1, 'd')

    #register_function(fw, 'gen_mr_liq_report', lambda th: gen_mr_liq_report(fw.var('VOLUME').rolling(20).mean(), th), 1, 'i')
    register_function(fw, 'gen_mr_liq_report', lambda adv_threshold=[70000],adv_period=20: gen_adv_threshold_report(fw,adv_threshold,adv_period), 2, 'wi')

    register_function(fw, 'eval_by_corr', lambda x,y: {'eval_by_corr': eval_by_corr(x,y,formula_worker=fw)}, 2, 'dd')

    for funcFn in fw.work_case()["function_files"]:
        if Path(funcFn).exists():
            exec(Path(funcFn).read_text(), {'ctx': fw._ctx})

    register_function(fw, 'nonzero2one', nonzero2one, 1, None)
    register_function(fw, 'reset_pld', fw.reset_pld, 0, None)

    register_factor_worker(fw)
    register_function(fw, 'load_dynamic_modules', load_dynamic_modules, 1, 's')
    load_dynamic_modules("/qpsdata/config/dynamic_modules/modules_list.txt")

    print_registered_functions(fw)

    return fw._ctx

def test_func(a,b):
    return a*b

if __name__ == "__main__":
    funcn = "register_cf_function.main"
    funcs = {}
    funcn = "test_func"
    for x in range(1,4):
        exec(f"""
def {funcn}{x}(f,d={x}): 
    return {funcn}(f,d)
funcs[f"test_func{x}"] = lambda a: {funcn}{x}(a)
""")
        
    for k,v in funcs.items():
        print(k, v(4))
