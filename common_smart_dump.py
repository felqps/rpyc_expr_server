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
from CTX import *
from dateutil.parser import parse as dtparse
from common_smart_load import mkdir
from QpsVts import buf2md5
import hashlib
from RedisTools import my_redis
from common_env import env
from SymsetMM import SymsetMM
from cmdline_opt import *

def smart_hash(dta):
    str = pickle.dump(dta)

    return rc

class HashedFile(object):
    def __init__(self, fn=None):
        self.data = []
        self.fn = fn
    def write(self, stuff):
        self.data.append(stuff)
    def bytes(self):
        return b''.join(self.data)
    def md5(self):
        return hashlib.md5(self.bytes()).hexdigest()
    def sha1(self):
        return hashlib.sha1(self.bytes()).hexdigest()
    def save(self):
        #print(f"DEBUG_2121: HasedFile save {self.fn}, {self.md5()}")
        open(self.fn, 'wb').write(self.bytes())

def smart_dump_cmn(dta, fp, fmt='pkl', sep=',', max_rows=10, title="title", debug=0, index=True, skip_if_exists=False, verbose=0, force=False):
    #funcn = f"smart_dump_cmn(fp= {fp}, note= {note})"
    funcn = f"smart_dump_cmn"
    
    #verbose = 0
    if skip_if_exists and os.path.exists(fp):
        return fp

    mkdir([os.path.dirname(fp)])
    ext = fp.split(".")[-1]
    note = f"({title},{fmt},{ext}):" 

    if fmt == 'df':
        with pd.option_context('display.max_rows', max_rows):
            print(df, file=open(fp, 'w'))

    # elif ext == "ftr":
    #     # dta.reset_index(inplace=True)
    #     # print(dta.tail())
    #     dta.reset_index().to_feather(fp)

    elif ext == 'csv':
        #dta.to_csv(fp, columns=dta.columns, sep=sep, index=True)
        dta.round(8).to_csv(fp, sep=sep, index=index)

    elif ext == 'txt':
        # with pd.option_context('display.max_rows', None):
        #     with pd.option_context('display.max_cols', None):
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)
        pd.options.display.float_format = '{:10.8f}'.format
        print(dta.to_string(index=index), file=open(fp, 'w'))        

    else:
        hf = HashedFile(fp)
        pickle.dump(dta, hf, protocol = 4)
        
        cur_hashvalue = hf.md5()
        redis_hkey = f"fld_cache:{hf.fn}"

        existing_hashvalue = my_redis.get_value(redis_hkey)
        #existing_hashvalue = None

        if existing_hashvalue is not None:
            if os.path.exists(fp):# and not ctx_force():
                if fp.find(".XSH")>=0: #For some reason check does not work as different machine may generate different chksum, so do not overwrite unless empty
                    try:
                        curDf = pickle.load(open(fp, 'rb'))
                        if curDf is not None:
                            # if type(curDf) curDf.shape[1]>0:
                            return fp
                    except Exception as e:
                        print(f"{BROWN}EXCEPTION: {funcn} failed loading existing fn= {fp}, e= {e}{NC}")

                if (existing_hashvalue == cur_hashvalue) and (not force): # and False: #XXX disable
                    # if not ctx_force():
                    #     print(f"{BROWN}INFO: {funcn} ({title},{fmt},{ext}) {hf.fn} redis_hkey= {redis_hkey}, redis_hval= {existing_hashvalue} exists, force write ...{NC}")
                    if cmdline_opt_verbose_on() or True: 
                        #print(f"{BROWN}INFO: {funcn} {note:<18} {fp:<136} #redis_hkey exists, skip dump again ...{NC}")
                        print(f"{BROWN}INFO: {funcn} fp= {fp:<136} redis_hkey exists(skip dump) ...{NC}")
                        if False:
                            print(f"{BROWN}INFO: {funcn} {note:<18}\n\tredisK= {redis_hkey:<138} redisV= {existing_hashvalue} exists, skip dump...{NC}")
                    return fp
                else:
                    print(f"{BROWN}INFO: {funcn} fp= {fp:<136} redis_hkey changed(overwriting) ...{NC}")
                    if ctx_verbose(5) and False:
                        #print(f"{BROWN}INFO: {funcn} {note:<18}\n\tredisK= {redis_hkey:<138}, redistVOld= {existing_hashvalue}, redisVNew= {cur_hashvalue}, overwriting ...{NC}")
                        print(f"{BROWN}INFO: fp= {fp:<136}\n\tredisK= {redis_hkey:<138}, redistVOld= {existing_hashvalue}, redisVNew= {cur_hashvalue}, overwriting ...{NC}")
        hf.save()

        cachefn =f"/tmp/fld_cache/{fp.split(r'/')[-2]}.{buf2md5(fp)[-8:]}.db"
        if os.path.exists(cachefn):
            print(f"INFO: {funcn}, orig file change, remove local_cache={cachefn}")
            os.unlink(cachefn) #need to wipe local copy if master file changed.

        #pickle.dump(dta, open(fp, 'wb'), protocol = 4)
        #print(f"DEBUG_3452: HashedFile {hf.fn}: {cur_hashvalue}")
        my_redis.set_value(f"fld_cache:{hf.fn}", cur_hashvalue)

        if fp.find("CS_ALL.db")>=0 and (
            fp.find("Pred_univ_")>=0 or
            fp.find("Pred_lnret_")>=0 or
            fp.find("Pred_OHLCV_")>=0 
        ):
            #generate by indu output
            #print(f"INFO: dump by indus", file=sys.stderr)
            sm = SymsetMM(ctx_dts_cfg())
            for indu in sm.all_groups():
                cmnMkts = set(dta.columns).intersection(sm.ssn2mkts(indu))
                induDf = dta[cmnMkts]
                induFn = fp.replace("CS_ALL.db", f"{indu}.db")
                if ctx_verbose(0):
                    print(f"{BROWN}INFO: {funcn}(by_indu) {induFn}, indu= {indu}, shape= {induDf.shape}{NC}")
                pickle.dump(induDf, open(induFn, 'wb'), protocol = 4)
                open(f"{induFn}.cmd", 'w').write(' '.join(sys.argv)+'\n')

    if cmdline_opt_verbose_on() or True:
        #if fp.find(".desc") < 0 and fp.find(".XSH") < 0:
        print(f"{BROWN}INFO: {funcn} fp= {fp:<136} shape= {dta.shape if type(dta)==type(pd.DataFrame()) else {type(dta)}}{NC}")
        open(f"{fp}.cmd", 'w').write(' '.join(sys.argv)+'\n')
        #print(f"{BROWN}INFO: {funcn} ({title}): {fp}{NC}")

    return fp

def smart_hash(dta):
    str = pickle.dump(dta)

    return rc

class HashedFile(object):
    def __init__(self, fn=None):
        self.data = []
        self.fn = fn
    def write(self, stuff):
        self.data.append(stuff)
    def bytes(self):
        return b''.join(self.data)
    def md5(self):
        return hashlib.md5(self.bytes()).hexdigest()
    def sha1(self):
        return hashlib.sha1(self.bytes()).hexdigest()  
    def save(self):
        #print(f"DEBUG_2121: HasedFile save {self.fn}, {self.md5()}")
        open(self.fn, 'wb').write(self.bytes())

def smart_dump_redis(dta, fp, fmt='pkl', sep=',', max_rows=10, title="title", debug=0, index=True, skip_if_exists=False, verbose=0):
    funcn = f"smart_dump_redis(fp={fp})"
    #verbose = 0
    if skip_if_exists and os.path.exists(fp):
        return fp
    
    #print(f"DEBUG_1232: {fp} {type(dta)} {hashlib.sha1(b''.join(mf.data)).hexdigest()}", file=sys.stderr)
    #XXX print(f"DEBUG_1232: {fp} {type(dta)} {hashlib.md5(b''.join(mf.data)).hexdigest()}", file=sys.stderr)

    mkdir([os.path.dirname(fp)])
    ext = fp.split(".")[-1]

    if fmt == 'df':
        with pd.option_context('display.max_rows', max_rows):
            print(df, file=open(fp, 'w'))

    # elif ext == "ftr":
    #     # dta.reset_index(inplace=True)
    #     # print(dta.tail())
    #     dta.reset_index().to_feather(fp)

    elif ext == 'csv':
        #dta.to_csv(fp, columns=dta.columns, sep=sep, index=True)
        dta.round(8).to_csv(fp, sep=sep, index=index)

    elif ext == 'txt':
        # with pd.option_context('display.max_rows', None):
        #     with pd.option_context('display.max_cols', None):
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_rows', None)
        pd.options.display.float_format = '{:10.8f}'.format
        print(dta.to_string(index=index), file=open(fp, 'w'))        

    else:
        hf = HashedFile(fp)

        # if fp.find("Pred_")>=0 or fp.find("Resp_")>=0:
        #     #dta.dropna(axis=0, how='all', inplace=True)
        #     # dta = dta[dta.index.time==dta.last_valid_index().time()]
        #     # #print(dta.tail(5))
        #     # print("-"*300)
        #     pass
        pickle.dump(dta, hf, protocol = 4)
        
        cur_hashvalue = hf.md5()
        redis_hkey = f"fld_cache:{hf.fn}"
        existing_hashvalue = my_redis.get_value(redis_hkey)
        if existing_hashvalue is not None:
            if os.path.exists(fp):# and not ctx_force():
                if fp.find(".XSH")>=0: #For some reason check does not work as different machine may generate different chksum, so do not overwrite unless empty
                    try:
                        curDf = pickle.load(open(fp, 'rb'))
                        if curDf is not None:
                            # if type(curDf) curDf.shape[1]>0:
                            return fp
                    except Exception as e:
                        print(f"{BROWN}EXCEPTION: {funcn} failed loading existing fn= {fp}, e= {e}{NC}")


                note = f"{title},{fmt},{ext}" 
                if existing_hashvalue == cur_hashvalue:
                    # if not ctx_force():
                    #     print(f"{BROWN}INFO: smart_dump ({title},{fmt},{ext}) {hf.fn} redis_hkey= {redis_hkey}, redis_hval= {existing_hashvalue} exists, force write ...{NC}")
                    if ctx_verbose(1 + env().print_smart_dump(fp)) or True: 
                        print(f"{BROWN}INFO: smart_dump ({note: <60}) redisK= {redis_hkey: <160} redisV= {existing_hashvalue} exists, skip dump...{NC}")
                    return fp
                else:
                    if ctx_verbose(1 + env().print_smart_dump(fp)) or True: 
                        print(f"{BROWN}INFO: smart_dump ({note: <60}) redisK= {redis_hkey: <160} redisV old= {existing_hashvalue}, new= {cur_hashvalue}, overwriting ...{NC}")
                
        hf.save()

        cachefn =f"/tmp/fld_cache/{fp.split(r'/')[-2]}.{buf2md5(fp)[-8:]}.db"
        if os.path.exists(cachefn):
            print(f"INFO: {funcn}, orig file change, remove local_cache={cachefn}")
            os.unlink(cachefn) #need to wipe local copy if master file changed.

        #pickle.dump(dta, open(fp, 'wb'), protocol = 4)
        #print(f"DEBUG_3452: HashedFile {hf.fn}: {cur_hashvalue}")
        my_redis.set_value(f"fld_cache:{hf.fn}", cur_hashvalue)

        if fp.find("CS_ALL.db")>=0 and (
            fp.find("Pred_univ_")>=0 or
            fp.find("Pred_lnret_")>=0 or
            fp.find("Pred_OHLCV_")>=0 
        ):
            #generate by indu output
            #print(f"INFO: dump by indus", file=sys.stderr)
            sm = SymsetMM(ctx_dts_cfg())
            for indu in sm.all_groups():
                cmnMkts = set(dta.columns).intersection(sm.ssn2mkts(indu))
                induDf = dta[cmnMkts]
                induFn = fp.replace("CS_ALL.db", f"{indu}.db")
                if ctx_verbose(2) or True:
                    print(f"{BROWN}INFO: smart_dump(by_indu) {induFn}, indu= {indu}, shape= {induDf.shape}{NC}")
                pickle.dump(induDf, open(induFn, 'wb'), protocol = 4)

    if ctx_verbose(1 + env().print_smart_dump(fp)) or True:
        #if fp.find(".desc") < 0 and fp.find(".XSH") < 0:
        note = f"{title},{fmt},{ext}" 
        print(f"{BROWN}INFO: smart_dump ({note: <60}) redisK= {redis_hkey: <160}{NC}")
        #print(f"{BROWN}INFO: smart_dump ({title}): {fp}{NC}")

    return fp

smart_dump = smart_dump_cmn

if __name__ == '__main__':
    # funcn = sys.argv[1]
    # eval(funcn)
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)
    smart_dump({"test": "smart_dump"}, "/mdrive/temp/public/test_smart_dump.db", debug=1)
    smart_dump(df, "/mdrive/temp/public/test_smart_dump_02.db", debug=1)

    f = pd.DataFrame([
        [1, 2],
        (3, 4)
    ])
    print(f)
    # test_case = sys.argv[2]
    # if int(test_case) == 1:
    #     globals()[funcn]()
    # if int(test_case) == 2:
    #     globals()[funcn](fp="/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20220715/1d/Resp_lnret_C2C_1d_1/C20.egen", debug=True)
