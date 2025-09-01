#!/usr/bin/env python

import warnings
warnings.simplefilter("ignore", category=UserWarning)


#For faster import, only should put small/fast basic functions here
from optparse import OptionParser
from functools import wraps
from pdb import pm
import re
import datetime
import os
import pickle
import subprocess
import sys
import glob
import hashlib
from unittest.mock import sentinel
import pandas as pd
from itertools import product
from common_smart_load import *
from common_smart_dump import *
from common_colors import *
# from CTX import *
from str_minmax import str_minmax
from common_env import env
#from common_paths import *
from qpstimeit import *
import traceback
from QpsSys import running_remotely

TINY=1e-6
MICRO=1e-16

def func_called(my_func):
    @wraps(my_func)
    def func_called(*args, **kw):
        print(f"{GREEN}#####FUNC_CALL_BEGIN{NC} {my_func.__name__}")
        #traceback.print_stack()
        output = my_func(*args, **kw)
        print(f"{GREEN}#####FUNC_CALL_END{NC} {my_func.__name__}")
        return output
    return func_called

def func_m_called(my_func):
    @wraps(my_func)
    def func_m_called(ref, *args, **kw):
        print(f"{GREEN}#####FUNC_CALL_BEGIN{NC} {ref.__class__.__name__}.{my_func.__name__}")
        #traceback.print_stack()
        output = my_func(ref, *args, **kw)
        print(f"{GREEN}#####FUNC_CALL_END{NC} {ref.__class__.__name__}.{my_func.__name__}")
        return output
    return func_m_called

def deprecated(my_func):
    @wraps(my_func)
    def deprecated(*args, **kw):
        print(f"{ERR}#####DEPRECATED_BEGIN{NC}")
        print(f"{ERR}#####DEPRECATED_BEGIN{NC} {my_func.__name__}")
        print(f"{ERR}#####DEPRECATED_BEGIN{NC}")
        output = my_func(*args, **kw)
        print(f"{ERR}#####DEPRECATED_END{NC}")
        print(f"{ERR}#####DEPRECATED_END{NC} {my_func.__name__}")
        print(f"{ERR}#####DEPRECATED_END{NC}")
        return output
    return deprecated

def deprecated_m(my_func):
    @wraps(my_func)
    def deprecated_m(ref, *args, **kw):    
        output = my_func(ref, *args, **kw)
        print(f"{ERR}#####DEPRECATED_BEGIN{NC}")
        print(f"{ERR}#####DEPRECATED_BEGIN{NC} {ref.__class__.__name__}.{my_func.__name__}")
        print(f"{ERR}#####DEPRECATED_BEGIN{NC}")
        output = my_func(ref, *args, **kw)
        print(f"{ERR}#####DEPRECATED_END{NC}")
        print(f"{ERR}#####DEPRECATED_END{NC} {ref.__class__.__name__}.{my_func.__name__}")
        print(f"{ERR}#####DEPRECATED_END{NC}")
        return output
    return deprecated_m

def deprecated_disabled(my_func):
    return(my_func)

def deprecated_m_disabled(my_func):
    return(my_func)

def symset2sectype(ssn):
    if ssn.find('CF_')>=0:
        return 'CF'
    else:
        return 'CS'

def str_minmax_without_status(cmd):
    idx_beg_status = cmd.find("--status")
    idx_end_status = cmd.find(".status")+8
    cmd = f"{cmd[:idx_beg_status]}{cmd[idx_end_status:]}"
    return(str_minmax(cmd))


def use_if_exists(sf, fnTry, force=False):
    sf.path = fnTry
    if sf.exists() and not force:
        raise Exception(f"INFO: found existing {sf.path}")
    print(f"INFO: use_if_exists creating {sf.path}")


def format_qsummary(qSummary):
    fmtCols = ['Symset', 'BegDt', 'EndDt', 'Days', 'Mkts'] #, 'RetMean', 'RetStd', 'RetCum', 'IR']
    allCols = sorted(qSummary.columns)
    for bmNm in ['Pred', 'rf', 'ss', '2k', '1k', '5h', '3h', '2h', '1h']:
        fmtCols.extend([x for x in allCols if x.find(bmNm)>=0])
    #print(fmtCols, file=sys.stderr)
    #self._qSummary = self._qSummary[fmtCols]
    return qSummary[fmtCols]

def get_pmid2indu_for_symset(dts_cfg, symset):
    symsetFn = f"{rootdata()}/config/symsets/{dts_cfg}/{symset}.pkl"
    #symsetDb = smart_load(symsetFn, title='symset2InduPmid', debug=True)
    symsetDb = pickle.load(open(symsetFn, 'rb'))
    #print(symset[opt.symset])
    #print(symsetFn)
    rc = {}
    for indu in symsetDb.keys():
        for pmid in symsetDb[indu]:
            # rc.append((indu, pmid))
            rc[pmid] = indu
    return rc

def get_pmid2indu_from_ohlcv():
    pmid2indu = {}
    pmid2lastdate = {}
    ohlc_ld_fns = glob.glob(f"{rootdata()}/data_rq.20210810_uptodate/???/ohlcv_1d_pre.db")
    ohlc_ld_fns.append(f"{rootdata()}/data_rq.20210810_uptodate/Unknown/ohlcv_1d_pre.db")
    for fn in ohlc_ld_fns:
        indu = fn.split("/")[-2]
        ohlc = pickle.load(open(fn, 'rb'))
        #print(type(df), df.keys())
        for pmid in ohlc.keys():
            if ohlc[pmid] is None or ohlc[pmid].empty:
                continue

            currlastdt = ohlc[pmid].index[-1]
            if pmid in pmid2lastdate and str(currlastdt)<str(pmid2lastdate[pmid]):
                continue
            pmid2indu[pmid] = indu
            pmid2lastdate[pmid] = currlastdt

            # if pmid == "688303.XSHG":
            #     print(f"DEBUG_1123: {pmid} {pmid2indu[pmid]} {fn}", file=sys.stderr)
            #     # print(ohlc[pmid].index[-1])
    return pmid2indu

def find_first_available(fileList, begat=0, default='NA', debug=0):
    for fn in fileList[begat:]: 
        if (os.path.exists(fn)):
            if debug:
                print("INFO: find_first_available %s" % (fn))
            return fn
        if debug:
            print(f"DEBUG: can not find {fn}")
    return default

def done_version():
    return "done01"

def nm2fldnm(nm):
    if nm.find("Pred")>=0 or nm.find("Resp")>=0:
        return nm
    #db = smart_load(f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db", title='nm2fldnm')
    db = pickle.load(open(f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db", 'rb'))
    if nm.find("_1d")<0 and nm.find("_1m")<0 and nm.find("_5m")<0:
        nm = f"{nm}_1d"
    return(db[db['nm']==nm].index[0])

def getPredFldPrefix():
    return "Rq,Alpha,Qps,WQ,Srch,Fmtl,DNA"

def make_fldnm(fldnm, prefix="", md5=""):
    #<type>_<ops>_<nm>_<transform?>_<period>_<version>
    #Pred_SRCH8f7ffc_RqWQ042_ma5_1d_1
    #Resp_lnret_C2C_1d_1
    tokens = fldnm.split(r'_')
    if tokens[0] in ['Resp', 'Pred']: #asssume fully specified
        return fldnm
    
    return '_'.join(["Pred", f"{prefix}{md5}", fldnm, "1d", "1"])

def standardize_fldnm(fldnm):
    hasNeg = ""
    if fldnm.find("_neg")>=0:
        hasNeg = "_neg"
        fldnm = fldnm.replace("_neg", "")
    hasMA = ""
    if fldnm.find("_ma")>=0:
        hasMA = fldnm[fldnm.find("_ma"):]
        fldnm = fldnm[:fldnm.find("_ma")]
    return f"{fldnm}{hasNeg}{hasMA}"

def fldnm_root(fldnm):
    hasNeg = ""
    if fldnm.find("_neg")>=0:
        hasNeg = "_neg"
        fldnm = fldnm.replace("_neg", "")
    hasMA = ""
    if fldnm.find("_ma")>=0:
        hasMA = fldnm[fldnm.find("_ma"):]
        fldnm = fldnm[:fldnm.find("_ma")]
    
    fldnmRoot = fldnm.split(r'_')[0]
    fldnmRoot = fldnmRoot.replace("Alpha6", "Alpha0") #Alpha6xx is variant of Alpha0xx
    fldnmRoot = fldnmRoot.replace("RqWQ0", "Alpha0")
    return fldnmRoot

def list_modules():
    for k,v in sys.modules.items():
        vstr = f"{v}"
        #print(v)
        if vstr.find("/Fairedge_dev")>=0:
            print(vstr)

def qps_glob(globstr, title="NA"):
    frmfns = glob.glob(globstr)
    if len(frmfns)<=0:
        print(f"{RED}ERROR({title}): can not find glob files, globstr= {globstr}{NC}")
    return frmfns


def fld_debug_info(varN, fld, md5=True):
    funcn = "fld_debug_info"
    if md5:
        md5str=f", md5={hashlib.md5(pickle.dumps(fld)).hexdigest()[-6:]}"
    else:
        md5str=""

    if fld.shape[0]<=0:
        return f"{varN: <40} shape={fld.shape}"
    else:
        return f"{varN: <40} shape={fld.shape}; begtm={fld.index[0]}; endtm={fld.index[-1]}{md5str}"

def fld_index_drop_tm(fld):
    try:
        dates = list(map(lambda x: datetime.date(x.year, x.month, x.day), fld.index.to_pydatetime()))
        fld.index = pd.to_datetime(dates)
    except:
        pass
    return fld

def print_fn(title, fn):
    print(f"{BROWN}INFO: {title} file://{fn}", file=sys.stdout)

def str_list_search(l, pattern):
    if  pattern.find("$")>=0 or pattern.find("*")>=0 or pattern.find("?")>=0 or pattern.find(".")>=0:
        patternRe = re.compile(pattern)
        selected = [x for x in l if len(patternRe.findall(x))>0]
    else:
        selected = [x for x in l if x.find(pattern)>=0]
    return selected

def regtest_002():
    for env in ["rschhist", "prod", "prod1w", "R", "P", "W"]:
        setDataEnv(env)
        print(f"{env: <8}: raw= {dd('raw')}, usr= {dd('usr')}")

    # cnt = 0
    # for dta in [pd.DataFrame(), pd.DataFrame().reset_index(), {}]:
    #     for fmt in ["pkl", "ftr"]:
    #         fp = f"/tmp/test_smart_dump.{cnt}.{fmt}"
            
    #         try:
    #             smart_dump(dta, fp, fmt)
    #         except Exception as e:
    #             fp = fp.replace('ftr', 'pkl')
    #             smart_dump(dta, fp, "pkl")
    #             print(e)
    #         print(f"INFO: testing type= {type(dta)}, fp= {fp}, fmt= {fmt}")
    #         cnt = cnt + 1

    # print(nm2fldnm('WQ004_1d'))

    # for scn in ['F', 'G', 'W']:
    #     print(get_lastest_qpsuser_dir(scn))

def regtest_03():
    print(str_minmax_without_status("success python  ~A/calc_corr.py ~j/predictors/FactorCorr/Pred_SRCH8f7ffc_DNA_defadb_1d_1.Pred_SRCH8f7ffc_DNA_c48b3b_1d_1.CS_ALL.pkl,~c/~Q.~x20210101/CS/~N/F_~x20210101/1d/Pred_SRCH8f7ffc_DNA_defadb_1d_1/CS_ALL.egen,~c/~Q.~x20210101/CS/~N/F_~x20210101/1d/Pred_SRCH8f7ffc_DNA_c48b3b_1d_1/CS_ALL.egen --status_file ~T/FC_EGEN_STUDY.80.01_eb161cf6.status --force 0 --verbose 0 --dryrun 0 "))

def get_indices_file(opt):
    found = ""
    pattern = "prod1w"
    (ex, freq, scn) = opt.cfg.split('_')
    if scn in "EFGW":
        pattern = f"/{scn}_"
    for fn in glob.glob("/qpsuser/che/data_rq.*/CS/SN_CS_DAY/*/1d/Resp_lnret_C2C_1d_1/Indices.db"):
        if fn.find(pattern) >= 0:
            found = fn
    return found

def get_resp_file_impl(cfg, symset):
    found = ""
    pattern = "prod1w"
    (ex, freq, scn) = cfg.split('_')
    if scn in "EFGW":
        pattern = f"/{scn}_"
    for fn in glob.glob(f"/qpsuser/che/data_rq.*/CS/SN_CS_DAY/*/1d/Resp_lnret_C2C_1d_1/{symset}.db"):
        if fn.find(pattern) >= 0:
            found = fn
    return found

def get_resp_file(opt):
    return get_resp_file_impl(opt.cfg, opt.symset)

def get_limit_up_file(cfg, symset):
    found = ""
    pattern = "prod1w"
    (ex, freq, scn) = cfg.split('_')
    if scn in "EFGW":
        pattern = f"/{scn}_"
    for fn in glob.glob(f"/qpsuser/che/data_rq.*/CS/SN_CS_DAY/*/1d/Pred_Rqraw_RqLimitUp_1d_1/{symset}.db"):
        if fn.find(pattern) >= 0:
            found = fn
    return found

def get_limit_down_file(cfg, symset):
    found = ""
    pattern = "prod1w"
    (ex, freq, scn) = cfg.split('_')
    if scn in "EFGW":
        pattern = f"/{scn}_"
    for fn in glob.glob(f"/qpsuser/che/data_rq.*/CS/SN_CS_DAY/*/1d/Pred_Rqraw_RqLimitDown_1d_1/{symset}.db"):
        if fn.find(pattern) >= 0:
            found = fn
    return found

def align_index(ref, tgt):
    if ref.shape == tgt.shape:
        return (ref,tgt)
    # tgt.dropna(axis=0, how='all', inplace=True)
    # ref.dropna(axis=0, how='all', inplace=True)

    # print_df(ref, title=f"align_index(ref)")
    # print_df(tgt, title=f"align_index(tgt)")

    if ref.shape[0] != tgt.shape[0]:
        cmnidx = ref.index.intersection(tgt.index)
        ref = ref.loc[cmnidx]
        tgt = tgt.loc[cmnidx]

    if ref.shape[1] != tgt.shape[1]:
        cmncol = ref.columns.intersection(tgt.columns)
        ref = ref[cmncol]
        tgt = tgt[cmncol]

    return(ref,tgt)

def use_cache_if_exists(fn, cache_dir="/NASQPS08.qpsdata.cache/research/performance_eval_server/"):
    if fn.find(cache_dir.replace(".cache",""))>=0:
        fn_cache = fn.replace(cache_dir.replace(".cache",""), cache_dir)
        if os.path.exists(fn_cache):
            print(f"{CYAN}INFO: Using CACHE {fn_cache}{NC}")
            return(fn_cache)
    return(fn)

CMDLINE_FN=None
CMDLINE_FP=None
def print_opt(opt, title="NONE"):   
    global CMDLINE_FN
    global CMDLINE_FP

    if not isinstance(opt, dict):
        optDict = opt2dict(opt)
    else:
        optDict = opt

    print(f"{CYAN}")
    cmdline_fn_dir = use_cache_if_exists("/mdrive/temp/rpts/cmdlines/uptodate", "/mdrive.cache/temp/rpts/cmdlines/uptodate")
    if os.path.exists(cmdline_fn_dir):
        if not CMDLINE_FN:
            now = datetime.datetime.today()
            CMDLINE_FN = f"{now.strftime('%Y%m%d_%H%M%S')}_{os.path.basename(sys.argv[0])}".replace('.py', f".{os.getpid()}.txt")
            CMDLINE_FN = f"{cmdline_fn_dir}/{CMDLINE_FN}"
            CMDLINE_FP = open(CMDLINE_FN, 'w')
        print_fn("cmdline_file", CMDLINE_FN)

    if CMDLINE_FP is None:
        CMDLINE_FP = sys.stdout

    print(f"{'='*20} {title} {'='*20}", file=CMDLINE_FP)
    print(f"cmdline: {' '.join(sys.argv)}", file=CMDLINE_FP)
    for k in sorted(list(optDict.keys())):
        print(f"{k:<32}= {optDict[k]}", file=CMDLINE_FP)
    #print(pd.DataFrame.from_dict(optDict, orient='index', columns=['opt']))
    CMDLINE_FP.flush()
    print(f"{NC}")

if __name__ == '__main__':
    funcn = sys.argv[1]
    eval(funcn)

    # parser.add_option("--regtest",
    #                   dest="regtest",
    #                   type="int",
    #                   help="regtest (default: %default)",
    #                   metavar="regtest",
    #                   default=0)




