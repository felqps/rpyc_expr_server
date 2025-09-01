#!/usr/bin/env python
import sys

import time,sys,os,datetime
import pandas as pd
from functools import wraps
from fdf_colors import *
from qpstimeit import *
#from cmdline_opt import *
from common_basic import print_opt


os.environ['NUMEXPR_MAX_THREADS'] = '16'
os.environ['NUMEXPR_NUM_THREADS'] = '16'


def list_modules():
    for k,v in sys.modules.items():
        vstr = f"{v}"
        #print(v)
        if vstr.find("/Fairedge_dev")>=0:
            print(vstr)

def regtest():
    funcn = "fdf_basic.regtest()"
    print(f"INFO: {funcn}")

def smart_path(fpIn, optional=False):
    fnTry = fpIn
    if not os.path.exists(fnTry):
        path_remap = {
            # "/qpsdata/config/fdf_cfgs": "/Fairedge_dev/app_fdf/cfgs"      #axiong
            "/Fairedge_dev/app_fdf/cfgs": "/qpsdata/config/fdf_cfgs"
        }
        
        for k,v in path_remap.items():
            if fnTry.find(k)>=0:
                fnTry = fnTry.replace(k, v)

    if not optional:
        assert os.path.exists(fnTry), f"{RED}ERROR: cannot find {fpIn}{NC}"

    return fnTry

def is_empty_line(ln):
    tmp = ln.replace(' ', '')
    tmp = tmp.replace('\t', '')
    tmp = tmp.replace('\r', '')
    tmp = tmp.replace('\n', '')
    return tmp == "" or (len(tmp)>0 and tmp[0] == '#')

def open_readlines(fn, verbose=False, warning=False, note=os.path.basename(sys.argv[0]).split(".py")[0]):
    lines = []
    fn = fn.replace('\\', '/')
    if not os.path.exists(fn):
        if warning:
            print("WARNING: cannot find %s"%(fn))
        return lines
    
    if verbose:
        print(f"INFO: open_readlines({note}, {verbose}) file {fn}", file=sys.stderr)

    for ln in open(fn, 'r').readlines():
        ln = ln.strip()
        if ln.find("#include")>=0:
            inclFilter = ""
            cols = ln.split()
            inclFn = cols[1]
            if len(cols)>=3:
                inclFilter = cols[2]
            if (inclFilter == ""):
                lines.extend(open(inclFn, 'r').readlines())
            else:
                lines.extend([ln.strip() for ln in open(inclFn, 'r').readlines() if ln.find(inclFilter)>=0])
        else:
            if not is_empty_line(ln):
                lines.append(ln)
    return lines

def revise_data(df_left, df_right):
    try:
        for col in df_left:
            if col not in df_right:
                continue

            if df_left.index[-1] not in df_right[col]:
                continue
            
            if  abs(df_left[col][-1] - df_right[col][df_left.index[-1]]) > 0.000001: # > 1 tics
                percentage = df_right[col][df_left.index[-1]]/df_left[col][-1]
                df_left[col] = df_left[col].map(lambda x: round(x*percentage, 6))
    except Exception as err:
        print(f"ERROR: {err}")

def merger_fdf(dfs, revise=True):
    if len(dfs) == 0:
        print(f"WARNING: no date to merger")
        return None
    
    df_left = dfs[0]
    if len(dfs) == 1:
        return df_left

    for df_right in dfs[1:]:
        if df_right is None:
            print(f"WARNING: data is None")
            continue
        
        if df_left is None:
            print(f"WARNING: data is None")
            data_left = df_right
            continue
        
        if df_right.shape[0]<=0:
            continue

        revise_data(df_left, df_right)
        df_left = df_left[df_left.index<df_right.index.values[0]]
        df_left = pd.concat([df_left, df_right])
    return df_left

def gethostname():
    import socket
    return socket.gethostname()

def get_dates(dates_cfg_file="/qpsdata/config/MktOMSDates.uptodate.cfg"):
    dates = {}
    for line in open(dates_cfg_file, 'r').readlines():
        [key, value] = line.strip().split("=")[:2]
        dates[key] = value
    return dates

# CMDLINE_FN=None
# CMDLINE_FP=None
# def print_opt(opt, title="NONE"):   
#     global CMDLINE_FN
#     global CMDLINE_FP

#     optDict = {k:getattr(opt,k) for k in dir(opt) if k.find("__")<0 and k.find("read_")<0 and k.find("_update")<0 and k.find("ensure")<0}
#     #optDict = eval(f"{opt}") 

#     print(f"{CYAN}")
#     cmdline_fn_dir = "/mdrive/temp/rpts/cmdlines/uptodate"
#     if os.path.exists(cmdline_fn_dir):
#         if not CMDLINE_FN:
#             now = datetime.datetime.today()
#             CMDLINE_FN = f"{now.strftime('%Y%m%d_%H%M%S')}_{os.path.basename(sys.argv[0])}".replace('.py', f".{os.getpid()}.txt")
#             CMDLINE_FN = f"{cmdline_fn_dir}/{CMDLINE_FN}"
#             CMDLINE_FP = open(CMDLINE_FN, 'w')
#         print_fn("cmdline_file", CMDLINE_FN)

#     if CMDLINE_FP is None:
#         CMDLINE_FP = sys.stdout

#     print(f"{'='*20} {title} {'='*20}", file=CMDLINE_FP)
#     print(f"cmdline: {' '.join(sys.argv)}", file=CMDLINE_FP)
#     for k in sorted(list(optDict.keys())):
#         print(f"{k:<32}= {optDict[k]}", file=CMDLINE_FP)
#     #print(pd.DataFrame.from_dict(optDict, orient='index', columns=['opt']))
#     CMDLINE_FP.flush()
#     print(f"{NC}")

if __name__ == '__main__':
    funcn = "fdf_basic.main"
    # opt, args = get_options(funcn)

    if len(sys.argv)<=1:
        funcn = "regtest()"
    else:
        funcn = sys.argv[-1]
    eval(funcn)

    # parser.add_option("--regtest",
    #                   dest="regtest",
    #                   type="int",
    #                   help="regtest (default: %default)",
    #                   metavar="regtest",
    #                   default=0)
    # import pickle
    # df1 = pickle.load(open("/NASQPS06.qpsdata/fdf/cs_F/1d/76/ed46f10ecd0e0206dbf05afc725225.fdf", "rb"))
    # df2 = pickle.load(open("/NASQPS06.qpsdata/fdf/cs_G/1d/71/6aa7bf238adcab7ecba8980232461b.fdf", "rb"))
    # df3 = pickle.load(open("/NASQPS06.qpsdata/fdf/cs_prod/1d/43/73b2af8588527c7cc5a42365ef5b41.fdf", "rb"))
    # df = merger_fdf([df1,df2,df3])
    # print(df)
    # pickle.dump(df, open("/temp/test.db", "wb"))
