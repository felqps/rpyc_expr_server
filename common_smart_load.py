#!/usr/bin/env python

import sys


import time
import datetime
import os
import pickle
import pandas as pd
import glob
import subprocess
import hashlib
from itertools import product
from common_colors import *
from dateutil.parser import parse as dtparse
from common_env import env
from CTX import *
#from common_options_helper import get_options

from cmdline_opt import *
from common_logging import *

def mkdir(dirs):
    for dir in dirs:
        if not os.path.exists(dir):
            #print(f"DEBUIG_2342: mkdir {dir}")
            run_command("mkdir -p %s"%(dir), waitFinish=1)
            #os.makedirs(dir)
    return(dirs)

LATEST_QPSUSER_DIR_MAP = None
def get_latest_qpsuser_dir_map(asofdate=None, debug=False):
    funcn = f"get_latest_qpsuser_dir_map(asofdate={asofdate})"
    global LATEST_QPSUSER_DIR_MAP
    if LATEST_QPSUSER_DIR_MAP is None:
        LATEST_QPSUSER_DIR_MAP = {}
        sentinelFn = "Pred_OHLCV_C_1d_1"
        sentinelFn = "Resp_lnret_C2C_1d_1"
        #sentinelFn = "Pred_SRCH8f7ffc_DNA_3fd575_1d_1"
        globstr = f"/qpsuser/che/data_rq.*/CS/SN_CS_DAY/*_????????_????????/1d/{sentinelFn}"
        #print(f'DEBUG_2320: {globstr}')
        for ln in sorted(glob.glob(globstr)):
            if ln.find("test")>=0:
                continue
            if ctx_debug(5):
                print(f"DEBUG_7772: {ln}, {asofdate}")
            dts_cfg_str = ln.split(r'/')[-3]
            scn = dts_cfg_str.split(r'_')[0]

            if debug:
                print(f"{RED}{funcn}: {ln} {dts_cfg_str} {scn} asofdate= {asofdate}{NC}")

            # if asofdate is not None and dts_cfg_str.split(r'_')[-1] > asofdate:
            #     continue
            
            LATEST_QPSUSER_DIR_MAP[scn] = ln.replace(sentinelFn, '')
            if scn in 'prod1w':
                LATEST_QPSUSER_DIR_MAP['W'] = ln.replace(sentinelFn, '')
                LATEST_QPSUSER_DIR_MAP['prod'] = ln.replace(sentinelFn, '')
                
        if debug or True:
            for k,v in LATEST_QPSUSER_DIR_MAP.items():
                if ctx_debug(5):
                    print(f"DEBUG_2342: LATEST_QPSUSER_DIR_MAP {k:<8} {v}")

        if 'T' in LATEST_QPSUSER_DIR_MAP:
            #LATEST_QPSUSER_DIR_MAP['U'] = LATEST_QPSUSER_DIR_MAP['T'].replace("/T_", "/U_")
            LATEST_QPSUSER_DIR_MAP['A'] = LATEST_QPSUSER_DIR_MAP['T'].replace("/T_", "/A_")

        if ctx_verbose(0):
            print(f"{CYAN}INFO: {funcn} sentinel files=", globstr, NC)
            for k,v in LATEST_QPSUSER_DIR_MAP.items():
                print(f"{funcn}: {k: >8}: {v}")


    return LATEST_QPSUSER_DIR_MAP

def get_lastest_qpsuser_dir(scn, asofdate=None):    
    if scn == 'W':
        scn = 'prod1w'
    return get_latest_qpsuser_dir_map(asofdate)[scn]

def get_lastest_qpsuser_dts_cfg_str_list(scn, asofdate=None):
    if scn in ['prod1w', 'prod']:
        scn = 'W'
    lst = [get_latest_qpsuser_dir_map(asofdate=asofdate)[scn]]
    if scn in ["T"]:
        lst.append(get_latest_qpsuser_dir_map(asofdate)['W'])
    return [x.split(r'/')[-3] for x in lst]

def fake_tradedate_row(fn, fld, tradedate, val=None):
    if val is None:
        if fn.find("Resp_lnret")>=0:
            val = 0.0
    funcn = f"fake_tradedate_row(fn={fn}, tradedate={tradedate}, val={val})"

    #print(funcn)
    if tradedate in fld.index:
        print(f"INFO: {tradedate} already exists, skip {funcn}...")
        return fld

    index_v = dtparse(str(dtparse(tradedate).date()) + str(fld.index[0])[10:])
    #print("DEBUG_1128:", '%'*200, index_v, val)
    if val is None:
        fld.loc[index_v] = fld.iloc[-1]
    else:
        fld.loc[index_v] = val
    # last_row = pd.DataFrame(fld.iloc[-1]).T
    # data_v = last_row.to_dict("records")
    # index_v = tradedate + str(last_row.index[0])[10:]
    # df_raw = pd.DataFrame(data_v, index=pd.DatetimeIndex([index_v]))
    # #print(df_raw)
    # fld = fld.append([df_raw])
    return fld

def fnlst_search_config(fp="/qpsdata/config/symsets/T/CS_ALL.pkl", debug=False):
    fnlst = [fp]
    fnlst.append(fp.replace("/T/", "/W/"))
    if debug:
        print("\n".join(fnlst))
    return fnlst

def fnlst_search_rkan(fp="/NASQPS08.qpsdata/research/performance_eval_server/rkan5/Pred_Beta_Daily_CN500_C20T.pkl", debug=False):
    fnlst = [fp]
    if (fp.find("rkan")>=0):
        segs = fp.split("/")
        (basename, ext) = segs[-1].split(r'.')
        basenameSegs = basename.split(r'_')
        symset_scn = basenameSegs[-1]

        if basename.find("CS_ALL")>=0:
            symset = "CS_ALL"
        else:
            symset = basenameSegs[-1][:-1]

        if symset_scn.find("prod1w")>=0:
            scn = "prod1w"
            symset = symset_scn.replace(scn, '')
        else:
            scn = symset_scn[-1]

        #print("--------", symset_scn, symset, scn)

        if scn=='T' or scn=='prod1w':
            scn = "W"
            if fp.find("CS_ALL")>=0:
                fnlst.append(f"{'/'.join(segs[:-1])}/{'_'.join(basenameSegs[:-2])}_{symset}{scn}.{ext}")
            else:
                fnlst.append(f"{'/'.join(segs[:-1])}/{'_'.join(basenameSegs[:-1])}_{symset}{scn}.{ext}")

        if symset != 'CS_ALL':
            symset = "CS_ALL"
            if fp.find("CS_ALL")>=0:
                fnlst.append(f"{'/'.join(segs[:-1])}/{'_'.join(basenameSegs[:-2])}_{symset}{scn}.{ext}")
            else:
                fnlst.append(f"{'/'.join(segs[:-1])}/{'_'.join(basenameSegs[:-1])}_{symset}{scn}.{ext}")
    if debug:
        print("DEBUG_2342: fnlst_search_rkan file candidates for fp= {fp}: ")
        print("\n".join(fnlst))
    return(fnlst)

def ordered_uniq(fnlst):
    tmp = {}
    rc = []
    for fn in fnlst:
        if fn not in tmp.keys():
            rc.append(fn)
            tmp[fn] = 1
    return rc

def fnlst_search_fld(fp="/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_20220715/1d/Resp_lnret_C2C_1d_1/C20.egen", debug=False):
    funcn = "fnlst_search_fld(fp= {fp})"
    #debug=1
    fp = fp.replace("//", "/")

    if fp.find("rkan5")>=0:
        fp = fp.replace("prod", "W")

    fnlst = [fp]
    segs = fp.split(r'/')
    if len(segs)>=10:
        basename = segs[-1]
        #print(segs[2:])
        (usr, data_rq, CS, sesn, dts_cfg_str, day, fldnm, basename) = segs[2:]
        els = basename.split(r'.')
        (ssn, ext) = (els[0], els[-1])  
        if cmdline_opt_debug_on():
            print(f"DEBUG: {funcn} ctx_dcsndt()= {ctx_dcsndt()}, cmdline_opt().asofdate= {cmdline_opt().asofdate}")
        lst = list(product(
            get_lastest_qpsuser_dts_cfg_str_list(dts_cfg_str.split(r'_')[0], asofdate=cmdline_opt().asofdate), #get_lastest_qpsuser_dts_cfg_str_list(dts_cfg_str.split(r'_')[0], asofdate=dts_cfg_str.split(r'_')[2])
            ["db", "pkl", "egen", "dbx"],
            [ssn],
        ))

        for (dts_cfg_str, ext, ssn) in lst:
            newBasename = '.'.join([ssn, *els[1:-1], ext])
            newfn = "/".join([*segs[:2], usr, data_rq, CS, sesn, dts_cfg_str, day, fldnm, newBasename])
            fnlst.extend([newfn, newfn.replace("CS_ALLT", "CS_ALLW").replace("IndicesT", "IndicesW").replace("/T/", "/W/")])

    if debug:
        print(f"DEBUG_2350: fnlst_search_fld candidates for fp= {fp}: ")
        print("\n".join(ordered_uniq(fnlst)))
    return(fnlst)

FDS_LOOKUP_INFO=None
def fnlst_search_new_fds(fp="/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_20220715/1d/Resp_lnret_C2C_1d_1/C20.egen", debug=False):
    funcn = f"fnlst_search_new_fds(fp={fp})"

    fp = fp.replace("//", "/")
    fnlst = []
    segs = fp.split(r'/')
    #print(f"{RED} ---------------------------------------------- {funcn} {len(segs)}; {segs[2:]}{NC}")
    
    global FDS_LOOKUP_INFO
    if FDS_LOOKUP_INFO is None:
        backward_lookup_fn = f"/qpsdata/config/formula_id_backward_lookup.{cmdline_opt().scn}.pkl"
        print(f"{BROWN}IMPORTANT: backward_lookup_fn= {backward_lookup_fn}{NC}")
        FDS_LOOKUP_INFO = pickle.load(open(backward_lookup_fn, 'rb'))
        #print(f"INFO: FDS_LOOKUP_INFO keys: {FDS_LOOKUP_INFO.keys()}")
        #print(FDS_LOOKUP_INFO)

    if len(segs)>=10:
        basename = segs[-1]
        #print(segs[2:])
        (usr, data_rq, CS, sesn, dts_cfg_str, day, fldnm, basename) = segs[2:]
        els = basename.split(r'.')
        (ssn, ext) = (els[0], els[-1]) 
        dmn = dts_cfg_str.split(r'_')[0]

        if dmn in ['T','A'] or True:
            if ssn in FDS_LOOKUP_INFO:
                #print(f"{RED} ---------------------------------------------- {funcn} {ssn}")
                for k,v in FDS_LOOKUP_INFO[ssn].items():
                    if fldnm.find(k)>=0:
                        if os.path.exists(v['payload_data_fn']) and os.path.getsize(v['payload_data_fn'])>200:
                            if debug:
                                print(f"{GREEN}{funcn}: {dts_cfg_str}, {dmn}, {ssn}, {fldnm}, {v['payload_data_fn']}{NC}")
                            fnlst.append(v['payload_data_fn'])
                        #print(f"{RED} ---------------------------------------------- {v['payload_data_fn']} {NC}")

    return(fnlst)

SMART_FIND_DB = {}

def smart_find(fp, debug=False):
    funcn = f"smart_find({fp})"

    try:
        USE_FDS=cmdline_opt().use_fds
    except Exception as e:
        USE_FDS=False

    fp_found = None
    if not USE_FDS:
        if os.path.exists(fp):
            fp_found = fp

    if fp_found is None:
        global SMART_FIND_DB
        if fp in SMART_FIND_DB and (SMART_FIND_DB[fp] is not None):
            fp_found = SMART_FIND_DB[fp]
        else:
            fpTrys = []
            if USE_FDS:
                fpTrys.extend(fnlst_search_new_fds(fp))
            if os.path.exists(fp):
                fpTrys.extend([fp])

            #will not smart find T
            if fp.find("T_")<0 or True:
                fpTrys.extend(fnlst_search_rkan(fp))
                fpTrys.extend(fnlst_search_fld(fp))
                fpTrys.extend(fnlst_search_config(fp))
                #fpTrys = list(set(fpTrys))

            if cmdline_opt_debug_on() or False:
                print(f"DEBUG_5724: {funcn}")
                print('\t', '\n\t'.join(fpTrys), sep='')

            for fpTry in fpTrys:
                if debug and False:
                    print(f"DEBUG_4459: {funcn} trying file: {fpTry}", file=sys.stderr)

                if os.path.exists(fpTry):
                    SMART_FIND_DB[fp] = fpTry
                    fp_found = fpTry
                    break

    remapped = (fp!=fp_found)
    if remapped:
        remapped = f"{RED}remapped= {fp_found}{NC}"
    else:
        remapped = f"{GREEN}remapped= {remapped}{NC}"

    found = fp_found
    if found is None:
        found= f"{RED}{found}{NC}"

    if debug:
        dbg(f"INFO: {GREEN}{funcn:<128}{NC} {remapped}")
        if remapped.find("True")>=0:
            print(f"\tremap_to= {found}, {remapped}")
    return fp_found

def buf2md5(buf):
    md5 = hashlib.md5()
    buf = buf.encode('utf-8')
    md5.update(buf)
    return md5.hexdigest()

def is_newer(tgt, chks, usecache=1, debug=False):
    if usecache>=1:
        if(not os.path.isfile(tgt)):
            return False
        tgtTime = os.stat(tgt).st_mtime
        for chk in chks:
            if(not os.path.isfile(chk)):
                continue
            chkTime = os.stat(chk).st_mtime
            if tgtTime < chkTime:
                if ctx_debug(5) or debug:
                    print(f"DEBUG_2342: tgt needs update: {tgt} {tgtTime}\n chk: {chk} {chkTime}")
                return False
        if ctx_debug(5) or debug:
            print(f"DEBUG_2341: tgt update-to-date: {tgt} {tgtTime}\n chks: ", '\n'.join(chks))
        return True
    else:
        return False

def smart_load(fp, use_smart_find=True, second_try=False, use_fgen=False, title='title', debug=False, 
    verbose=False, protocol=None, show_shape=False, align_to="NA", cachefn_found=None, error_if_not_found=True):
    # if not os.path.exists(fp):
    #     fp = fp.replace("CS_ALLT", "CS_ALLW")
    #     fp = fp.replace("IndicesT", "IndicesW")
    #     fp = fp.replace("/T/", "/W/")

    if use_smart_find == True:
        fpTry = smart_find(fp, debug=use_smart_find)
    else:
        fpTry = fp
    
    if fpTry != None:
        cachefn = "NA"
        load_cachefn = "NA"
        # or fpTry.find("_Rqraw_")<0: # or fpTry.find("instruments")>=0: #these files should be cached when possible
        if (fpTry.find("Resp_")>=0 or fpTry.find("_OHLCV_")>=0) and (fpTry.find("genetic_func_cache")<0): 
            cachefn =f"/tmp/fld_cache/{fpTry.split(r'/')[-2]}.{buf2md5(fpTry)[-8:]}.db"

        if ctx_use_fld_cache():
            if align_to != "NA":
                cachefn_aligned = f"{cachefn}.{align_to}"
                if os.path.exists(cachefn_aligned) and is_newer(cachefn_aligned, [fpTry]):
                    load_cachefn = cachefn_aligned

            if load_cachefn == "NA" and os.path.exists(cachefn) and is_newer(cachefn, [fpTry]) and (os.path.getsize(cachefn) > 100):
                load_cachefn = cachefn

            if cachefn_found is not None:
                cachefn_found['fn'] = load_cachefn

        if ctx_verbose(1+env().print_smart_load()) or True:
            print(f"{BLUE}INFO: smart_load({title}) fpTry= {fpTry}; load_cachefn= {load_cachefn}{NC}", file=sys.stderr)

        ext = fp.split(".")[-1]
        if ext == "ftr": #feather file
            return None
            # dta = pd.read_feather(fpTry)
            # if 'index' in dta.columns:
            #     dta.set_index(['index'], inplace=True)
            # elif 'date' in dta.columns:
            #     dta.set_index(['date'], inplace=True)
            # return dta
        else:
            try:
                fn = fpTry
                if load_cachefn != "NA":
                    fn = load_cachefn
                #     pld = pickle.load(open(load_cachefn, 'rb'))
                # else:
                #     pld = pickle.load(open(fpTry, 'rb'))
                pld = pickle.load(open(fn, 'rb'))
            except Exception as e:
                print(f"{ERR}EXCEPTION: smart_load fn= {fn}, e= {e}{NC}")
                if f"{e}".find("new_block")>=0:
                    cmd = f"mv {fn} {fn}.wrong_pandas_version"
                    os.system(cmd)
                    pld = None
                else:
                    time.sleep(15)
                    print(f"{ERR}EXCEPTION: smart_load try again after 15 sec, fn= {fn}, e= {e}{NC}")
                    try: 
                        pld = pickle.load(open(fn, 'rb'))
                    except Exception as e:
                        print(f"{ERR}EXCEPTION: smart_load try again fn= {fn}, e= {e}{NC}")
                    pld = None

            # if fp.find("Pred_lnret")>=0 or fp.find("Resp_lnret")>=0:
            #     if fp.find("_univ_")<0:
            #         if type(pld) == pd.DataFrame:
            #             pld.dropna(axis=0, how='all', inplace=True)
            #             #print("drop_all_na", "+"*300, fp)

            if fpTry != fp and fp.find("T_")>0 and fpTry.find("prod1w_")>0: #T file that was converted from W on the flight
                if ctx_debug(1) or True:
                    print(f"DEBUG_1110: smart_load converting W=>T, {fpTry} => {fp}")
                pld.fillna(method='ffill', inplace=True)
                pld = fake_tradedate_row(fpTry, pld, ctx_trddt())
                pld = pld[sorted(pld.columns)]
                if ctx_debug(1):
                    #XXX
                    print(f"DEBUG_9870", pld.iloc[-2:,:10])

            if False: #XXX Do this in roll_db.py
                if fpTry.find("prod1w_")>=0 and fpTry.find("Resp_lnret")>=0 and fpTry.find("/CS/")>=0: #we need to fake last day so pred will calculate the last day, which is needed for T.
                    if ctx_debug(1) or True:
                        print(f"DEBUG_1112: smart_load fake_tradedate_row {fpTry}", file=sys.stderr)
                        from shared_cmn import print_df
                        print_df(pld.iloc[-2:,:10], title=f"DEBUG_9871", file=sys.stderr)
                    pld = fake_tradedate_row(fpTry, pld, ctx_dcsndt(), val=0.0)

            if ctx_use_fld_cache() and load_cachefn == "NA" and cachefn != "NA":
                #smart_dump(pld, cachefn)
                if ctx_debug(2):
                    print(f"DEBUG_1129: smart_load fp={fp}, write cache= {cachefn}")
                pickle.dump(pld, open(cachefn, 'wb'))
            return pld
    else:
        if ctx_debug(1):
            print(f"INFO: smart_load({title}) missing {fp}")

    #print(f"INFO: smart_load({title}) cannot find {fp}")
    if use_fgen or fp.find("ptret") >=0:
        if second_try == False:
            mkdir([os.path.dirname(fp)])
            #os.system(f"/Fairedge_dev/app_fgen/fgen_ss.py --force 0 --fp {fp}")
            os.system(f"bash /Fairedge_dev/app_fgen/fgen_ss.bash --force 0 {fp}")
            return(smart_load(fp, second_try=True, use_fgen=use_fgen))
    else:
        if error_if_not_found and fp.find("Unknown.db")<0 and fp.find("A_")<0: #Unknown indu is possible normally
            print(f"{RED}ERROR: smart_load cannot find {fp}{NC}")
        return None

def run_command(cmd, waitFinish=1, debug=0, stdout=subprocess.PIPE):
    if debug:
        print("INFO: %s"%(cmd))
    p = subprocess.Popen(cmd,
                        stdout=stdout,
                        stderr=subprocess.STDOUT,shell=(sys.platform!="win32"))
    s = iter(p.stdout.readline, r'')
    if (waitFinish):
        p.wait()
    return s

def run_command_new(cmd, waitFinish=1, debug=0, stdout=subprocess.PIPE):
    if debug:
        print >> sys.stderr, "INFO: %s"%(cmd)
    p = subprocess.Popen(cmd,
                        stdout=stdout,
                        stderr=subprocess.STDOUT,shell=(sys.platform!="win32"))
    if (waitFinish):
        p.wait()

    if p.stdout:
        return p.returncode, p.stdout.read()
    else:
        return p.returncode, ""

if __name__ == '__main__':
    funcn = "common_smart_load.main"
    # opt, args = get_options(funcn)
    
    expr = sys.argv[1]
    if funcn:
        print(eval(expr))
    else:
        print(smart_load("/tmp/test_smart_dump.db"))
    # test_case = sys.argv[2]
    # if int(test_case) == 1:
    #     globals()[funcn]()
    # if int(test_case) == 2:
    #     globals()[funcn](fp="/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20220715/1d/Resp_lnret_C2C_1d_1/C20.egen", debug=True)
