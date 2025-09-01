import os,sys
import pickle
import pandas as pd
import numpy as np
import random
import math
import time
import glob
import traceback
import rpyc
import re
from macro_helper import fdf_expand_macros
from RpycCfgFile import RpycCfgFile
from qpstimeit import timeit_m

class RpycExprSync:
    cache={}
    def __init__(self, opt, expr, model='@basic', force=0):
        expr = fdf_expand_macros(expr)
        # print(expr)
        # exit(0)
        self._opt = opt
        self._params = {}
        self._params['expr'] = expr.replace('RETURNS', 'RETURN').replace('_W:', '_prod:') #do some backward compatible transforms
        self._params['model'] = fdf_expand_macros(model)
        self._params['force'] = force

    def __str__(self):
        return f"RpycExprSync[expr]={self._params['expr']}"

    def init_rpyc_servers(self):
        funcn = "RpycExprSync.init_rpc_servers()"
        if not hasattr(self._opt, 'rpyc_servers'):
            print(funcn)
            self._opt.rpyc_servers = {}
            for ln in RpycCfgFile(self._opt).readlines():
                (svr_id,ip,port,workspace,models,expr_re) = ln.split(r',')
                try:
                    conn = rpyc.connect(ip, int(port))
                    conn._config['sync_request_timeout'] = 10*60 #seconds
                    conn._config['sync_request_timeout'] = None #no timeout
                    if models not in self._opt.rpyc_servers:
                        self._opt.rpyc_servers[models] = []
                    self._opt.rpyc_servers[models].append((re.compile(expr_re), conn))
                except Exception as e:
                    print(f"RpycExprSync.init() Exception: {e}")
                    continue

    def params(self):
        return self._params
    
    @timeit_m
    def get(self):  
        self.init_rpyc_servers()
        if self._params['expr'] in RpycExprSync.cache:
            return RpycExprSync.cache[self._params['expr']]
        for (regex, conn) in self._opt.rpyc_servers[self.params()['model']]:
            #try all available servers that can handle the expr, and return the first sucess.
            if regex.match(self.params()['expr']):
                try:
                    rec = pickle.loads(conn.root.get_answer(self.params()))
                    result = rec.get('payload', None)
                    if result is None and 'fpath' in rec:
                        result = pickle.load(open(rec['fpath'], 'rb'))
                    RpycExprSync.cache[self._params['expr']] = result
                    return result
                except Exception as e:
                    print(f"RpycExprSync.get() Exception: {e}")
                    continue
       
        print(f"ERROR: cannot find server for params= {self.params()}")
        return None
