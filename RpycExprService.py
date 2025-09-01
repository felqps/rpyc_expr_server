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
import datetime
import copy

from factor_worker import FEWorkerBase
from ExprHelper import ExprHelper
from cmdline_opt import set_cmdline_opt
from common_logging import *
from common_smart_dump import smart_dump

class RpycExprService(rpyc.Service):
    def __init__(self, opt):
        super().__init__()
        self._opt = opt
        #self._wkr = formula_worker

    def exposed_get_answer(self, params):
        funcn = "RpycExprService.exposed_get_answer"
        #params = eval(params_str)
        
        eh = ExprHelper(params['expr'])
        expr = eh.expr_server_side() #params['expr'].split(r':')[-1]
        symset = eh.symset() #params['expr'].split(r':')[-2]
        cfg = eh.cfg() #params['expr'].split(r':')[-3]
        wkr_id = eh.wkr_id() #f"{cfg}.{symset}"
        print(f"wkr_id= {wkr_id}", file=sys.stderr)

        if wkr_id not in self._opt.symset2wkr.keys():
            self._opt.symset = symset
            self._opt.cfg = cfg
            worker = FEWorkerBase(copy.copy(self._opt), f"{self._opt.workspace_name}", skip_build_formula_list=True, skip_load_result_db=True)            
            self._opt.symset2wkr[wkr_id] = worker
            set_cmdline_opt(self._opt)
        wkr = self._opt.symset2wkr[wkr_id]
        wkr.pld(now=datetime.datetime.today())

        print(f"{funcn}: expr= {expr}", file=sys.stderr)
        outfn = eh.rpyc_expr_status_file()
        info(f"{funcn}: params= {params}, expr= {eh.expr_full()}, symset= {eh.symset()}, outfn= {outfn}")

        result=None
        if not os.path.exists(outfn) or params['force']: # or True: #or fileOlderThan(gettoday(), outfn, now=datetime.datetime.today()):
            #result = eval(expr, wkr.ctx())
            try:
                result = wkr.eval_expr_only(expr)
            except Exception as err:
                print(err)
                result = np.nan

            if result is not None:
                if self._opt.debug or self._opt.verbose:
                    print(f"INFO: pickle.dump {outfn}")
                #pickle.dump(result, open(outfn, 'bw'))
                smart_dump(result, outfn)
            if result is None:
                print(f"INFO: eval_expr({expr}) return None")
            elif (type(result) != type(dict())):
                if self._opt.verbose:
                    # print(result.tail(60), file=outfp)
                    print_df(result, title=f"{funcn} expr= {expr}, outfn= {outfn}, shape= {result.shape}")
        else:
            print(f"{funcn} {BLUE}cached result{NC} at {outfn}")
            #result = pickle.load(open(outfn, 'rb'))  
        
        return pickle.dumps({"fpath": outfn, "server_id": self._opt.server_id, "payload": result}) #return serialized data, always a dictionary


    def jump_start(self, do=False):
        if not do:
            return
        #for expr in [f"cs_1d_prod:{x}:ma(CLOSE,5)" for x in symsets_str_paper().split() if x not in ['NA']]:
        # dmn='prod'
        # dmn='G'
        dmn='T'
        # symset='CS_ALL'
        symsets=['C25', 'CS_ALL']
        for expr in [f"cs_1d_{dmn}:{x}:ma(CLOSE,5)" for x in symsets]:
            print(f"{RED} INFO: jump_start {expr}{NC}")
            self.exposed_get_answer({'expr': expr, 'force': 1})
