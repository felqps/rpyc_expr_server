import os,sys
import pickle
import pandas as pd
import numpy as np
import random
import math
import time
import glob
import traceback
import datetime

from options_helper import get_options
from FDF import *
from formula_helpers import *
from data_by_runid import runidRe
from register_all_functions import *
from factor_evalulators import *
from fdf_logging import *
from df_helpers import *
from FdfExpr import *
from platform_helpers import *
from concat_factors import *
from fdf_helpers import print_df
from remap_expr_new_style import remap_expr_new_style
import funcs_fld
from PostgreSqlTools import postgres_instance
from common_basic import align_index,running_remotely
from gen_fdd_A import merge_fdf_for_cfgs

def make_runid(opt, expr):
    return buf2md5(f"{opt.symset};{expr}")[:16]

class FEWorkerBase:
    def __init__(self, opt, workspaceFn, skip_build_formula_list=False, skip_load_result_db=False, debug=True):
        funcn = "FEWorkerBase.init()"
        self._opt = opt
        self._beg_time = time.strftime('%Y%m%d_%H%M%S')
        self._workspace_root = os.path.dirname(workspaceFn) #workspaceFn.replace(".wk","")
        mkdir([self._workspace_root])
        if running_remotely():
            workspaceFn = "/Fairedge_dev/app_qpshub/config.wk"
        print(f"INFO: eval workspaceFn {workspaceFn}")
        self._work_case = eval(Path(workspaceFn).read_text(), globals())
        #self._work_case = {}
        self._work_case["formulas"] = []
        self._ctx = None
        self._pld = None
        self._outputfp = None
        self._input_names = None
        self._result_db = None
        self._respL = None
        self.build_formula_list(skip_build_formula_list)
        self.build_result_db(skip_load_result_db)
        if "factor_evaluators" not in self._work_case:
            self._work_case["factor_evaluators"] = "Corr"
        self._factor_evaluators = get_factor_evaluators(self.workspace_root(), self._work_case["factor_evaluators"])
        if opt.verbose or opt.debug:
            print(funcn, opt, file=sys.stderr)
        self._pld_time = None
        self._last_expr_eval_time=datetime.datetime.today()

    def workspace_root(self):
        return self._workspace_root

    def print_funcn_supported(self):
        func_names = [x for x in self.func_supported() if (x.find("_ordinal")<0 and x.find("_dimension")<0)]
        for funcn in func_names:
            if f'{funcn}_ordinal' in self.ctx():
                print(f"funcn_supported: {funcn} {self.ctx()[f'{funcn}_ordinal']} {self.ctx()[f'{funcn}_dimension']}")
            else:
                print(f"funcn_supported: {funcn}")

    def var(self, varname):
        #return the value for var varname
        if varname not in self.ctx():
            return None
        return self.ctx()[varname]

    def ctx(self):
        if self._ctx is None:
            self._ctx = init_ctx(self)
            self._ctx['FEWorkerBase'] = self
            if self.do_regtest():
                self.print_funcn_supported()
        return self._ctx
    
    def __str__(self):
        return f"FEWorkerBase: cfg= {self.opt().cfg}"

    def regtest_expr(self, expr, outdir):
        funcn = f"regtest_expr()"
        fdfexpr = FdfExpr(expr)
        outfn = f"{outdir}/{fdfexpr.md5()}.output.txt"
        res = eval(expr, self.ctx())

        # print(res.tail(), file=sys.stderr)
        pd.set_option('display.max_columns', 8)
        pd.set_option('display.width', 200)

        fdfexpr.save_definition(outfn.replace('.output.txt', '.expr.txt'))
        print(f"{funcn}: expr={expr}, output= {outfn}", file=sys.stderr)
        outfp = open(outfn, 'w')
        print(f"{funcn}: {expr}", file=outfp)
        if(type(res) == type(pd.DataFrame())):
            print(res.iloc[-5:,:8], file=outfp)
        else:
            print(res, file=outfp)
        outfp.close()
        return res

    def regtest(self, opt=None):
        if not self.do_regtest():
            return

        print(f"{RED}INFO: do_regtest= {self.do_regtest()}{NC}, workspace_root= {self.workspace_root()}", file=sys.stderr)
        for k in list(self.ctx().keys()):
            if k.find("ordinal")>=0:
                if self.ctx()[k] is None:
                    continue
                funcn = k.replace("_ordinal", "")

                numbers = re.findall(r'\d+', funcn)
                #print(numbers)
                if len(numbers)>0 and (int(numbers[0])<5 or int(numbers[0])>7) :
                    continue #only test 3 different days
                
                expr = f"{funcn}({','.join(self.input_names()[:self.ctx()[k]])})"
                if self.ctx()[f"{funcn}_dimension"] == 'di':
                    expr = f"{funcn}({self.input_names()[0]}, 5)"
                elif self.ctx()[f"{funcn}_dimension"] == 'ddi':
                    expr = f"{funcn}({self.input_names()[0]}, {self.input_names()[1]}, 5)"
                elif self.ctx()[f"{funcn}_dimension"] == 'bd':
                    expr = f"{funcn}('strat001_prod', {','.join(self.input_names()[:self.ctx()[k]-1])})"
                elif self.ctx()[f"{funcn}_dimension"] == 'Id':
                    expr = f"{funcn}('A01', {','.join(self.input_names()[:self.ctx()[k]-1])})"
                elif self.ctx()[f"{funcn}_dimension"] == 'Bd':
                    expr = f"{funcn}('000300', {','.join(self.input_names()[:self.ctx()[k]-1])})"
                elif self.ctx()[f"{funcn}_dimension"] == 'F':
                    expr = f"{funcn}('zq500')"
                elif self.ctx()[f"{funcn}_dimension"] == 'i':
                    expr = f"{funcn}(10)"

                print(f"{GREEN}regtest {funcn}: {expr}{NC}", file=sys.stderr)

                # if expr.find("get_trd")<0:
                #     continue
                # if expr.find("membership")<0:
                #     continue

                if opt.dryrun:
                    print(f'@W:S90:{expr}')
                else:
                    self.regtest_expr(expr=expr, outdir=f"{self.workspace_root().replace('/qpsdata/FactorWorker', '/Fairedge_dev/app_regtests/factor_worker')}")
                                

    def opt(self):
        return self._opt
            
    def do_regtest(self):
        return self.opt().do in ['regtest']

    def build_formula_list(self, skip_build_formula_list=False, debug=False):
        funcn = f"build_formula_list(regtest={self.opt().regtest})"

        if self.opt().expr != "" or skip_build_formula_list:
            return

        expr_lists = self._work_case['expr_lists']
        # if do_regtest:
        #     expr_lists = self._work_case['expr_lists'][:1]
        for fn in expr_lists:
            if self.do_regtest() and fn.find("test")<0:
                continue
            elif self.do_regtest() and fn.find("test")>=0:
                continue 

            print(f"INFO: {funcn} reading {fn}")
            for ln in open_readlines(fn):
                if ln.find("expr:")>=0:
                    expr = ln.replace("expr:","")
                    expr = expr.replace(' ','')
                    if self.can_eval(expr):
                        self._work_case["formulas"].append(expr)
                        if debug:
                            print(f"INFO: add expr candidate {expr}", file=sys.stderr)
        print(f"INFO: added expr count {len(self._work_case['formulas'])}", file=sys.stderr)
        check_func_completeness = False
        if check_func_completeness:
            exit(0)

        # self._work_case["formulas"] = self._work_case["formulas"][:10]

    def build_result_db(self, skip_load_result_db=False):
        funcn = f"build_result_db({self.workspace_root()})"
        self._result_db = {}
        if skip_load_result_db:
            return

        if self.opt().expr != "":
            return
        for fn in sorted(glob.glob(f"{self.workspace_root()}/corr/corr.*.txt")):
            dbg(f"INFO: build_result_db file {fn}")
            for ln in open_readlines(fn):
                runid = runidRe.findall(ln)
                if runid:
                    runid = runid[0]
                else:
                    dbg(f"{funcn} missing runid {ln}")
                    continue
                dbg(f"{funcn} runid ln={ln}; runid= {runid}")
                self._result_db[runid] = ln

        info(f"{BROWN}{funcn} loaded runid count {len(self._result_db)}{NC}")

    def result_db(self):
        if self._result_db is None:
            self.build_result_db()
        return self._result_db

    def can_eval(self, expr):
        funcsInExpr = new_func_list_in(expr)
        if len(funcsInExpr)<=0:
            return False
        can = True
        #print(funcsInExpr)
        for func in funcsInExpr:
            if func not in self.func_supported():
                print(f"INFO: missing func {BROWN}{func}{NC}, expr= {expr}", file=sys.stderr)
                can = False
        return can

    def output_fp(self):
        if self.opt().runid:
            print(f"DEBUG_8999: {self.opt().runid}", file=sys.stderr)
            return sys.stderr
        if self._outputfp is None:
            fn = f"{self.workspace_root()}/corr/corr.{self._beg_time}.txt"
            # mkdir([os.path.dirname(fn)])
            # print(f"INFO: output {fn}", file=sys.stderr)
            # self._outputfp = open(fn, 'w')
            self._outputfp = open(Path(fn, create_dir_if_missing=True), 'w')
        return self._outputfp

    def work_case(self):
        return self._work_case

    def func_supported(self):
        return sorted([x for x in self.ctx().keys() if newFuncRe.match(x)])

    def build_responses(self, key='RETURN', debug=False):
        funcn = f"build_responses({key})"
        
        #from fdd_maker import fdd_summary
        #print(fdd_summary(self.pld(), rows=10))
        r = self.pld()[key]

        #df_inspect(self.pld()['CLOSE'].tail(3), ch='c:CLOSE', stop=False)
        #print_df(data_between_dates(r[['NI168', 'C168']], '20220306', '20220315', delta_days=0), title='watch', loglevel='info')
        #exit(0)

        self._respL = {}
        self._respL['Resp1_1']=(r.shift(-1).copy())
        self._respL['Resp2_1']=(r.shift(-2).copy())
        self._respL['Resp3_1']=(r.shift(-3).copy())  #shift=3, duration=1
        self._respL['Resp1_2']=self._respL['Resp1_1']+self._respL['Resp2_1']
        self._respL['Resp1_3']=self._respL['Resp1_1']+self._respL['Resp2_1']+self._respL['Resp3_1']


        for i in sorted(list(self._respL.keys())):
            # self.ctx()[i] = df_merge_non_uniq_columns(self._respL[i], copy=True) #expose flds to local namespace
            # info(f"{funcn} add {i} to ctx, shape= {self._respL[i].shape}")
            if debug or self.opt().debug or self.opt().verbose:
                print_df(self._respL[i].tail(10), rows=10, title=f"resp {i}")

    def respL(self, key='RETURN', debug=False):
        if self._respL is None:
            if key: #self.work_case()["resp_flds"]:
                self.build_responses(key, debug=debug)
            else:
                wrn(f"no resp_flds specified")
                self._respL = {}
        return self._respL

    def already_calculated(self, runid):
        return runid in self.result_db()

    def eval_factor(self, x, expr, ynames=None, debug=False, runid=None):
        if ynames is None:
            ynames = sorted(list(self.respL(debug).keys()))
        funcn = f"FEWorkerBase.eval_factor(expr={expr}, ynames={ynames}, runid={runid})"
        dbg(funcn)
        if x is None:
            err(f"{funcn} x= None")
            return None
        valid_rate=self.calc_valid_data_rate(x)
        rc = {}
        x.replace([np.inf, np.nan, -np.inf], 0.0, inplace=True)
        for yname in ynames:
            y = self.respL(debug)[yname]
            #runid = buf2md5(f"{yname};{expr}")[:16]
            if runid is None:
                runid = make_runid(self.opt(), expr)

            normal_run = not self.do_regtest() and not runid and not self.opt().debug and not self.opt().force
            if self.already_calculated(runid) and normal_run:
                continue

            res = {}
            for factor_evaluator in self._factor_evaluators:
                dbg(f"{funcn} factor_evalulator= {factor_evaluator.name()}")
                
                res[factor_evaluator.name()] = factor_evaluator.do(runid, x, y, normal_run, valid_rate=valid_rate, opt=self.opt())
                if 1: #not needed
                    (mean, std, ir) = calculate_corr(x,y)
                    res['corr'] = (mean*243, std*15.59, ir)
                    #xy = f"{yname};{expr};{runid}"
                    #ln = pretty_mean_std_ir(mean, std, ir, xy, pass_threshold_only=normal_run)
            rc[yname] = res
        rc['expr'] = expr
        rc['runid'] = runid
        rc['valid_rate'] = valid_rate
        #ln = f"expr= {expr}; runid= {runid}"
        if valid_rate > 0.9:
            # self.output_fp().write_text(f"{rc}")
            # self.output_fp().flush()
            print(f"{rc}", file=self.output_fp())
            self.output_fp().flush()
        else:
            info(f"low valid_rate= {valid_rate}")

        return rc
    
    def need_eval(self, expr, debug=False):
        if self.do_regtest():
            return True
        rc = False
        runid = None
        for yname,y in self.respL(debug).items():
            runid = buf2md5(f"{yname};{expr}")[:16]
            if not self.already_calculated(runid):
                rc = True

        if not rc:
            dbg(f"DEBUG_9834: skipping expr= {expr}, runid= {runid}")
        else:
            dbg(f"DEBUG_9835: need_eval expr= {expr}, runid= {runid}")
        return rc

    def adjust_contract_roll(self, pld):
        loglevel='info'
        loglevel='debug'
        unadjusted_close_return = np.log(self._pld['CLOSE'] / self._pld['CLOSE'].shift(1))
        
        print_df(unadjusted_close_return, show_head=0, rows=60, title="unadjusted_close_return", loglevel=loglevel)
        print_df(self._pld['RETURN'], show_head=0, rows=60, title="adjusted_close_return", loglevel=loglevel)
        return_jumps = unadjusted_close_return - self._pld['RETURN']
        return_jumps = tiny2zero(return_jumps)
        return_jumps = nonzero2one(return_jumps)        
        for fldnm in "OPEN,HIGH,LOW,CLOSE".split(r',')[:]:
            unadjusted_ratio = self._pld[fldnm] / self._pld[fldnm].shift(1)*(1 - self._pld['RETURN'])
            multiplier = unadjusted_ratio[return_jumps!=0].shift(-1) #shift the ratio to previous day
            #multiplier.fillna(method='bfill', inplace=True)
            multiplier.fillna(value=1.0, inplace=True)
            multiplier = multiplier[::-1].cumprod()[::-1]
            print_df(multiplier, show_head=0, rows=120, title="jump-ratio", loglevel=loglevel)
            #loglevel = 'info'
            print_df(self._pld[fldnm], show_head=0, rows=20, title=f"before roll-adj {fldnm}", loglevel=loglevel)
            print_df(self._pld['RETURN'], show_head=0, rows=20, title=f"returns {fldnm}", loglevel=loglevel)
            print_df(multiplier, show_head=0, rows=20, title=f"multiplier {fldnm}", loglevel=loglevel)
            self._pld[fldnm] = self._pld[fldnm] * multiplier
            print_df(self._pld[fldnm], show_head=0, rows=20, title=f"after roll-adj {fldnm}", loglevel=loglevel)
    
    def reset_pld(self):
        print("+"*200)
        self._pld = None
        print("-"*200)
        return None

    def pld(self, now=datetime.datetime.today(), debug=True, adjust_contract_roll=True):
        refresh_duration = 60*5 #Greater than 5 min lull will trigger an refresh data
        if (self._pld_time is not None and 
            (now-self._pld_time).total_seconds() > refresh_duration and 
                (now-self._last_expr_eval_time).total_seconds() > refresh_duration):
            self._pld = None
            print(f"{DBG}INFO: periodic pld refresh, refresh_duration {refresh_duration} seconds{NC}")
        if self._pld is not None:
            return self._pld

        use_postgres = True
        if self._pld is None:
            if use_postgres and not self._opt.gen_fdd_A:
                known_cache = {}
                # known_cache[f"cs_1d_A:C35"]    = "/tmp/data_file_cache/1b/1b94f375af59789192058b454e20b7ae.db"
                # known_cache[f"cs_1d_A:CS_ALL"] = "/tmp/data_file_cache/5c/5cb670f68a16909ed450ea6516830409.db"
                known_cache[f"cs_1d_A:CS_ALL"] = "/tmp/data_file_cache/50/505cda693f71db326abe3ff51d101c57.db"
                # known_cache[f"cs_1d_A:I65"]    = "/tmp/data_file_cache/b0/b0cf0d440b3cedbcb7c7cf4e9beb0eb9.db"
                known_cache[f"cs_1d_A:I65"] = "/tmp/data_file_cache/c3/c39c8f20415986b76e5a8733f73f999c.db"

                k = f"{self._opt.cfg}:{self._opt.symset}"
                pld = None
                if k in known_cache and os.path.exists(known_cache[k]):
                    print(f"INFO: loading known_cache for {k}: {known_cache[k]}")
                    pld = pickle.load(open(known_cache[k], 'rb'))

                if pld is None:
                    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
                    #(column_types, rows) = postgres.query_data(f"select fpath from fdf_info where \"symOut\"='{self._opt.symset}' and cfgnm='{self._opt.cfg}' and ftype='fdd' and DATE_TRUNC('day', rec_tm) = '20250818' order by rec_tm desc limit 1")
                    (column_types, rows) = postgres.query_data(f"select fpath from fdf_info where \"symOut\"='{self._opt.symset}' and cfgnm='{self._opt.cfg}' and ftype='fdd' order by rec_tm desc limit 1") #get the latest
                    if len(rows)>=1:
                        pld = rows[0]['fpath']
                    assert pld is not None, f"ERROR: cannot find the data in postgres"
                    if type(pld) ==  type(dict()):
                        print(f"INFO: fdf_info fdd keys= {pld.keys()}") 
                    else:
                        print(type(pld))
                        exit(0)
            else:
                pld = merge_fdf_for_cfgs(self._opt)

            if 'RETURN' not in pld.keys(): #cs data
                pld['RETURN'] = np.log(pld['CLOSE']/pld['CLOSE'].shift(1))
                pld['RETURNS'] = pld['RETURN']

            if 'TRADE_AT_CLOSE' not in pld.keys():
                pld['TRADE_AT_CLOSE'] = funcs_fld.filter_range(pld['CLOSE'],pld['LIMIT_DOWN'],pld['LIMIT_UP'],0.02)

            if 'TRADE_AT_OPEN' not in pld.keys():
                pld['TRADE_AT_OPEN'] = funcs_fld.filter_range(pld['OPEN'],pld['LIMIT_DOWN'],pld['LIMIT_UP'],0.02)    

            if 'CAP' not in pld.keys():
                pld['CAP'] = pld['MARKET_CAP']
            
            self._pld = pld

        self.ctx()['pld'] = self._pld


        if self._opt.gen_fdd_A:
            if self._opt.cfg.split(r'_')[-1] in ['A']:
                dta = {
                    'cfgnm': self._opt.cfg,
                    'symIn': self._opt.symset,
                    'symOut': self._opt.symset,
                    'rootdir': '',
                    'begdt': '20100101',
                    'enddt': 'uptodate',
                    'fldnm': '',
                    'ftype': 'fdd',
                    'file_size': '10 MB',
                    'fpath': self._pld
                }
                postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
                postgres.save_dict("fdf_info", dta)
            else:
                print(f"ERROR: gen_fdd_A cfg= {self._opt.cfg}, must be cs_1d_A")
            exit(0)

    
        self._pld.update(self.respL())

        if 'TOTAL_TURNOVER' in self._pld.keys():
            self._pld['VWAP'] = self._pld['TOTAL_TURNOVER']/self._pld['VOLUME']
        
        for k in self._pld.keys():
            self.ctx()[k] = self._pld[k]  #expose flds to local namespace
            print(f"{BROWN}INFO: FEWorkerBase.pld() predefined cfg= {self._opt.cfg}; symset= {self._opt.symset}; k= {k:<16}; shape= {self._pld[k].shape}{NC}")

        if adjust_contract_roll:
            self.adjust_contract_roll(self._pld)

        self._pld_time = datetime.datetime.today()

        return self._pld

    def input_names(self):
        funcn = f"input_names()"
        if self._input_names is None:
            self._input_names = []
            col_names = list(self.pld().keys())
            for k in "CONTRACT,LMTUP,LMTDOWN,RETURN_T_RQ".split(r','):
                if k in col_names:
                    col_names.remove(k)
            for _ in range(20):
                self._input_names.extend(col_names)
            info(f"{funcn}: input_names= {col_names}, var_list= {len(self._input_names)}")
        return self._input_names

    def run(self, debug=True):
        random.seed(self.work_case()["random_seed"])
        #self.build_responses(self.work_case()["resp_flds"], debug=debug)
        epoch_max = self.work_case()["epoch_max"]
        if self.do_regtest():
            epoch_max = 1

        evaluations_done = 0
        for epoch in range(epoch_max):
            input_names = self.input_names()
            for expr in self.work_case()["formulas"]:
                random.shuffle(input_names)
                expr = instrumenting_expr(expr, input_names)
                result = self.eval_expr(expr, debug=self.opt().debug)
                if result is None:
                    if self.need_eval(expr):
                        err(f"FEWorkerBase.run eval_expr({expr}) return None")
                    continue
                self.eval_factor(result, expr, debug=self.opt().debug)
                evaluations_done = evaluations_done + 1
                if evaluations_done >= opt.max_eval_num:
                    exit(0)
                if evaluations_done%200 == 0:
                    info(f"INFO: evaluations_done epoch= {epoch}, evaluations= {evaluations_done}")


    def eval_expr(self, expr, ynames=None, debug=False):
        funcn = "FEWorkerBase.eval_expr"
        try: 
            if not self.need_eval(expr) and ynames is None and not debug and False:
                return None
            
            debug_exprs = []
            expr = remap_expr_new_style(expr, debug_exprs)
            self.pld()['EXPR_FULL'] = f"{self._opt.cfg}:{self._opt.symset}:{expr}"
            self.pld()['EXPR'] = f"{expr}"

            for debug_expr in debug_exprs:
                print(f"{RED}debug_expr= {debug_expr}{NC}")
                print_df(eval(debug_expr, self.ctx()))

            try:
                result = eval(expr, self.ctx())
                #print_df(result, title='result')
            except Exception as e:
                print(e)
                result = None               
            
            if expr.find("ts_stddev21(RETURN)")!=0:
                pass
                # print(self.pld().keys())
                # df_inspect(self.pld()['HIGH'], ch=f'w:{expr}', rows=50, stop=False)
                # df_inspect(self.pld()['HIGH'], ch=f'w:{expr}', rows=50, stop=False)
                # df_inspect(result, ch=f'w:{expr}', rows=50, stop=False)

            if (result is not None) and (type(result) == type(pd.DataFrame())) and False:
                result.fillna(0.0, inplace=True)

            if debug:
                print_df(result, show_head=0, title=expr)

            # print(np.sign(result))
            return result
            
        except Exception as e:
            print(f"{RED}ERROR:  occurred in {funcn},  expr= {expr}, e= {e}{NC}", file=sys.stderr)
            print(traceback.print_exc(), file=sys.stderr)

    def eval_expr_only(self, expr):
        self._last_expr_eval_time=datetime.datetime.today()
        #return eval(expr, self.ctx())
        return self.eval_expr(expr)
    
    def calc_valid_data_rate(self, res):
        c_valid = count_gt(self._pld['CLOSE'], value=1e-6)
        r_valid = count_gt(self._pld['RETURN'], value=1e-6) + count_lt(self._pld['RETURN'], value=-1e-6)
        f_valid = count_gt(res, value=1e-6) + count_lt(res, value=-1e-6)
        return (max(f_valid/c_valid, f_valid/r_valid))


if __name__ == "__main__":
    funcn = "factor_worker.main"
    opt, args = get_options(funcn)

    dbg(f"{funcn}: {opt}")

    #workspaceFn = f"{opt.workspace_home}/{opt.workspace_name}"
    workspaceFn = f"{opt.workspace_name}"
    
    if workspaceFn.find(".wk")<0:
        print("WARN: improper named workspace {workspaceFn}")

    if opt.do in ["regtest"]:
        wkr = FEWorkerBase(opt, workspaceFn, skip_build_formula_list=True, skip_load_result_db=True)
        wkr.regtest(opt)
        wkr.run(opt.debug)

    elif opt.do in ['expr']:
        wkr = FEWorkerBase(opt, workspaceFn, skip_build_formula_list=True, skip_load_result_db=True)
        wkr.pld() #get fdd payload
        for expr in args:
            print(f"eval_expr {expr}", file=sys.stderr)
            if not os.path.exists(f"{wkr.workspace_root()}/regtests"):
                mkdir([f"{wkr.workspace_root()}/regtests"])

            result = wkr.regtest_expr(expr=expr, outdir=f"{wkr.workspace_root()}/regtests")
            # fdfexpr = FdfExpr(expr)
            # outfp = open(f"{wkr.workspace_root()}/regtests/{expr}.txt".replace('/','$'), 'w')
            # result = wkr.eval_expr(expr, ynames=['Resp1_1'])
            if result is None:
                err(f"eval_expr({expr}) return None")
            elif (type(result) != type(dict())):
                # print(result.tail(60), file=outfp)
                print_df(result, title=f"{funcn} expr={expr}, shape={result.shape}")

    elif opt.runid != "":
        wkr = FEWorkerBase(opt, workspaceFn)
        print(wkr.result_db()[opt.runid])
        (yname, expr, runid) = wkr.result_db()[opt.runid].split()[-1].split(";")
        wkr.pld()
        result = wkr.eval_expr(expr, ynames=[yname])
        result.replace([np.inf, np.nan, -np.inf], 0.0, inplace=True)
        print(result.iloc[-20:,:8])
        # resultNonezero = result[result != 0.0].dropna(axis=0, how='all')
        # print(resultNonezero.tail(20).dropna(axis=1, how='all'))
        ev = wkr.eval_factor(result, expr, ynames=[yname])

        from factor_return import FactorReturn
        fr = FactorReturn({'factor_data': res, 'return_data': wkr.respL()[yname]}, flip_sign=ev[yname][0]<0)
    elif opt.do in ['search']:
        wkr = FEWorkerBase(opt, workspaceFn)
        wkr.pld()
        wkr.run(opt.debug)
    elif opt.do in ['gen_workspace']:
        pass
    else:
        err(f"{funcn} must specifiy valid --do [do]")
            

            




