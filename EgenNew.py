#!/usr/bin/env python
import sys
from ast import operator
import sys

import numpy as np

from EgenBase import EgenBase
from FldNamespace import *
from common_basic import *
from shared_func import *
from funcs_fld import *
from common import *
from dateutil.parser import parse as dtparse
#from platform_helpers import *
from platform_helpers import *
import operator
import traceback
from genetic_geppy import *
from QDMergeQpsuser import merger_data
from common_smart_load import *
from remap_expr_old_style import remap_expr_old_style

#@timeit_real
def initNsByMkt(scn, respNm, respFld, debug=False):
    if debug:
        print(f"DEBUG_1104: initNsByMkt {respNm}")

    nsByMkt = FldNamespace(scn, bymkt=1)
    if respFld is None:
        return nsByMkt

    mkts = list(respFld.columns)

    # mkts.remove('002124.XSHE')
    # mkts.remove('000691.XSHE')
    # mkts.remove('002157.XSHE')
    # mkts.remove('600965.XSHG')
    for mkt in mkts:
        if mkt not in nsByMkt.raw:
            nsByMkt[mkt] = {}
        nsByMkt[mkt][respNm] = respFld[mkt] 

    return nsByMkt

def compile_expr_list(exprList):
    compiledList = []
    for x in exprList:
        if x.find(';')>=0:
            (v,tm) = x.split(";")
        else:
            (v,tm) = (x, None)

        if v.find('=')<0:
            compiledList.append(v)
            # self.compiledList.append(f"nsByVar.raw['{v}'] = nsByVar.get_fld(scn, nsByVar, nsByVar.varname2fldname('{v}'))")
        else:
            [v,*s] = v.split("=") #Need this to support default argument value in python
            s = "=".join(s)
            #Backward compatible, var on the left can be without $ if only one var.
            if v.find('$')>=0:
                stmt = f"{compile_expr(v)} = {compile_expr(s)}".replace("nsByVar.raw", "nsByVar").replace("nsByVar", "ns")
            else:
                stmt = f"nsByVar['{v}'] = {compile_expr(s)}".replace("nsByVar.raw", "nsByVar").replace("nsByVar", "ns")
            compiledList.append(stmt) #stmt.replace(' ',''))
        if tm is not None:
            stmt = f"nsByVar['{v}'].set_index(pd.to_datetime(nsByVar['{v}'].index.date) + pd.Timedelta('{tm}'), inplace=True)".replace("nsByVar", "ns")
            compiledList.append(stmt)


    return compiledList

def nms_like(pattern):
    nms = []
    for ln in QpsUtil.open_readlines("/Fairedge_dev/app_fgen/fgen_flds.CS.cfg"):
        ln = ln[ln.find("nm=")+3:]
        nm = ln.split(";")[0]
        if nm.find(pattern)>=0:
            if nm not in nms:
                nms.append(nm)
    print(f"INFO: nms_like({pattern}): {nms}", file=sys.stderr)
    return nms

CACHED_FLDNM = None
def get_cached_fldnm(include, symset):
    global CACHED_FLDNM
    if CACHED_FLDNM is None:
        CACHED_FLDNM = {}
        for ln in QpsUtil.open_readlines("/Fairedge_dev/proj_JVrc/Resp_C2C.{symset}/formula_include.txt"):
            fldnm = ln.split("=")[0].strip()
            CACHED_FLDNM[fldnm] = 1
    if include is not None:
        CACHED_FLDNM[include] = 1
    return CACHED_FLDNM

class EgenNew(EgenBase):
    respStacked = None
    def lines(self, debug=True):
        linesOut = []
        linesIn = []
        if self._linesIn is not None and self._linesIn != "":
            linesIn = self._linesIn.split(r'\n')
        else:
            linesIn = open(self._fldlstFn, 'r').readlines()

        linesIn = self.pre_processing(linesIn)

        for ln in linesIn:
            #ln = ln.strip()
            if ln.strip() == "":
                continue
            if ln.find('@')>=0:
                includeFn = ln.replace("@", "") + ".py"
                #includeFn = f"{os.getcwd()}/{includeFn.strip()}"
                includeFn = f"/Fairedge_dev/app_egen/studies/{includeFn.strip()}"
                print(f"INFO: {ln.strip()} => {includeFn}", file=sys.stderr)
                assert os.path.exists(includeFn), f"ERROR: can not find {includeFn}"

                for inclLn in open(includeFn, 'r').readlines():
                    inclLn = inclLn.strip()
                    #print(inclLn, file=sys.stderr)
                    linesOut.append(inclLn)

            else:
                linesOut.append(ln)

        linesOut = [x for x in linesOut if x.find("#")<0]

        if debug:
            print(f"INFO: EgenNew.lines", '\n'.join(linesOut))
        return '\n'.join(linesOut)

    def parse_predictor_selection_impl(self, scn, exprList, resp, predictor_selecton_file, debug=False):
        funcn = "parse_predictor_selection_impl"
        #print(f"DEBUG: {funcn}, selection_file= {predictor_selecton_file}, resp= {resp}, exprList= {exprList}")
        for ln in QpsUtil.open_readlines(predictor_selecton_file):
            if ln.find('class')>=0:
                continue
            predNm = ln.split(r',')[1]
            predSpec = f"{resp}.{predNm.replace('^','/')}"
            predGlob = f"{rootdata()}/egen_study/*_*/{predSpec}.{scn}/q9/*.raw.db"
            found = glob.glob(predGlob)
            if len(found)>0:
                fldFn = found[0]
                #print(fldFn)
                exprList.append(f"${predNm.replace('^','')}=F('{fldFn}')")
            else:
                print(f"ERROR: {funcn} can not find file for glob= {predGlob}")
        if debug:
            print(f"DEBUG_1105: {funcn}, selection_file= {predictor_selecton_file}, resp= {resp}, exprList= {exprList}")

    def F(self, varNm, debug=False):
        funcn = f"EgenNew.F(varNm='{varNm}')"
        if varNm.find('_')<0: #If not specify freq, assume 1d
            varNm = f"{varNm}_1d"
        if varNm == 'Full_1d':
            return 1
        if debug:
            print(f"DEBUG_9785: {funcn}")

    
        dfPrevScn = None
        if (self._scn.get_prev_scn() is not None) and True:
            if ctx_verbose(1):
                print(f"DEBUG_9786: {self._scn.get_prev_scn()})")
            #print(f"**** {self._nsByVar.varname2fldname(varNm)}")
            dfPrevScn = self._nsByVar.get_pre_calculated(self._scn.get_prev_scn(), self._nsByVar.varname2fldname(varNm), debug=debug, toplevel=False) 
            if ctx_verbose(1):
                print_df(dfPrevScn, show_head=True, title='df_prev_scn')
                print(dfPrevScn.shape)

        if ctx_verbose(1):
            print(f"DEBUG_9787: {self._scn})")
        df = self._nsByVar.get_pre_calculated(self._scn, self._nsByVar.varname2fldname(varNm), debug=debug)

        if dfPrevScn is not None:
            if ctx_verbose(1):
                print_df(df, show_head=True, title='df_cur_scn')
                print(df.shape)
            df = merger_data(dfPrevScn, df)
            if ctx_verbose(0):
                print_df(df, show_head=True, title=f"post_merge, varNm={varNm}")
                print(df.shape)

        return df

    def get_ctx(self, loc, debug=False):
        loc.update(EgenBase._script_ctx)
        loc['bar'] = self._bar
        loc['BAR'] = self._bar
        loc['fldlstFn'] = ""
        loc['RR']="Resp_lnret" #Predefined macros
        loc['unitTestMkts'] = self._unitTestMkts
        loc['unitTestDts'] = self._unitTestDts
        loc['debug'] = debug
        loc['genFlds'] = self._genFlds

        # def F(fldNm):
        #     return self.nsByVar.get_pre_calculated(self.scn, fldNm, debug=debug)
        def N(varNm):
            return self._nsByVar.varname2fldname(varNm)
        # def F(varNm):
        #     return self._nsByVar.get_pre_calculated(self._scn, self._nsByVar.varname2fldname(varNm), debug=debug)
        def parse_predictor_selection(exprList, resp, predictor_selecton_file):
            return self.parse_predictor_selection_impl(self._opt._scn, exprList, resp, predictor_selecton_file)
        # loc['F'] = lambda fldNm: self.nsByVar.get_pre_calculated(self.scn, fldNm, debug=debug)
        # loc['N'] = lambda varNm: self.nsByVar.varname2fldname(varNm)

        loc['C'] = lambda c: c #constant
        loc['F'] = lambda fldNm: self.F(fldNm)
        loc['N'] = N
        loc['Z'] = Z
        loc['ZZ'] = lambda d: Z(d).fillna(0.0) #Zero on Z
        loc['W'] = Winsorize
        loc['ZW'] = ZW
        loc['T'] = lambda f: self._nsByVar.T(f)
        loc['parse_predictor_selection'] = parse_predictor_selection
        loc['REF'] = lambda f,d: f.shift(d)
        loc['DELAY'] = lambda f,d: f.shift(d)
        loc['DELTA'] = lambda f,d: f.diff(d)
        loc['SUM'] = lambda f,d: f.rolling(d).sum()
        loc['STDDEV'] = lambda f,d: f.rolling(d).std()
        loc['IF'] = lambda c,t,f: bool2num(c)*t + bool2numinv(c)*f
        #loc['CORRELATION'] = lambda a,b,d: rolling_spearman(a,b,d)
        loc['CORRELATION'] = lambda a,b,d: a.rolling(d).corr(b)
        loc['COVARIANCE'] = lambda a,b,d: a.rolling(d).cov(b)
        # def rank(array):
        #     s = pd.Series(array)
        #     return s.rank(ascending=False)[len(s)-1]
        # loc['TS_RANK'] = lambda f,d: f.rolling(d).apply(rank)
        loc['TS_RANK'] = lambda f,d: alpha101_code.ts_rank(f,d)
        #loc['RANK'] = lambda f: rank_by_row(f)
        loc['RANK'] = lambda f: f.rank(axis=1, pct=1)
        loc['SIGN'] = lambda f: np.sign(f)
        loc['ABS'] = lambda f: np.abs(f)
        loc['MAX'] = lambda a,b: a.where(a > b, b).fillna(a)
        def DEBUG_MIN(a,b):
            (a,b) = align_index(a, b)
            return a.where(a < b, b).fillna(a)
        #loc['MIN'] = lambda a,b: a.where(a < b, b).fillna(a)
        loc['MIN'] = lambda a,b: DEBUG_MIN(a,b)
        loc['LT'] = lambda a,b: less_than(a,b)
        loc['LOG'] = lambda f: np.log(f)
        #loc['ADV'] = lambda d: self._nsByVar['VOLUME'].rolling(d).mean()
        loc['ADV'] = lambda d: self.F('V_1d').rolling(d).mean()
        #loc['DECAY_LINEAR'] = lambda f,d: alpha101_code.decay_linear(f, d)
        loc['DECAY_LINEAR'] = lambda f,d: f.apply(lambda x: talib.WMA(x, d) if not np.isnan(x).all() else x, axis=0) #x timeseries could be all nan for a stock
        loc['SCALE'] = lambda f,d=1: alpha101_code.scale(f,d)
        loc['TS_ARGMAX'] = lambda f,d: f.rolling(d).apply(np.argmax) + 1
        loc['TS_ARGMIN'] = lambda f,d: f.rolling(d).apply(np.argmin) + 1
        loc['TS_AVG'] = lambda f,d: f.rolling(d).mean()
        #loc['MA'] = lambda f,d: f.rolling(d).mean() if d>1 else f
        loc['MA'] = lambda f,d: ma(f,d)
        #loc['EMA'] = lambda f,d: f.rolling(d).mean() if d>1 else f
        loc['EMA'] = lambda f,d: f.apply(lambda x: talib.EMA(x, d) if not np.isnan(x).all() else x, axis=0)
        #loc['EMA'] = lambda f,d: f.ewm(halflife=d, axis=0) if d>1 else f

        #loc['SIGNEDPOWER'] = lambda f,d: pow(f,d)
        loc['SIGNEDPOWER'] = lambda f,e: np.sign(f)*(f.abs() ** e)

        loc['TS_MIN'] = lambda f,d: f.rolling(d).min()
        loc['TS_MAX'] = lambda f,d: f.rolling(d).max() if d>1 else f
        loc['AS_FLOAT'] = lambda f: bool2num(f)
        loc['PRODUCT'] = lambda f,d=10: alpha101_code.product(f,d)
        loc['INDUSTRY_NEUTRALIZE'] = lambda f: industry_neutralize(self._opt, f)
        loc['SECTOR_CODE'] = lambda f: calc_classification_code(f, dts_cfg=self._opt.dts_cfg, clsname='sector')
        loc['RQINDU_CODE'] = lambda f: calc_classification_code(f, dts_cfg=self._opt.dts_cfg, clsname='rqindu')
        loc['CITICSINDU_CODE'] = lambda f: calc_classification_code(f, dts_cfg=self._opt.dts_cfg, clsname='citicsindu')
        loc['SWSINDU_CODE'] = lambda f: calc_classification_code(f, dts_cfg=self._opt.dts_cfg, clsname='swsindu')
        loc['Beta3h_60'] = lambda f: calc_beta(self._opt, f, '3h', n_beta_days=60, min_days=20)
        loc['Beta5h_60'] = lambda f: calc_beta(self._opt, f, '5h', n_beta_days=60, min_days=20)
        loc['REGOUT'] = lambda y,x: regout(y, x)[0]
        loc['FFILL'] = lambda f: f.fillna(method='ffill', inplace=False) if f is not None else f
        loc['FILTER_RANGE'] = lambda f,llmt,ulmt: filter_range(f, llmt, ulmt)
        loc['FILTER_LIKE'] = lambda f,ref_mkt,begdt,enddt,tgtdf: filter_like(f, ref_mkt, begdt, enddt, tgtdf)

        EgenBase.register_script_func('genetic_geppy', 
            lambda *x, randseed=0, mode='normal', func_select='all', fld_select='all': genetic_basic_01(self, *x, randseed=randseed, mode=mode, func_select=func_select, fld_select=fld_select)) #variable arg list

        EgenBase.register_script_func('genetic_geppy', lambda *x: genetic_geppy_basic(self, *x)) #variable arg list

        #loc['genetic_basic'] = lambda a,b,c: 1.0
        # loc['add'] = operator.add
        # loc['sub'] = operator.sub
        # loc['mul'] = operator.mul
        # loc['div'] = lambda a,b: a/b
        # loc['protected_div'] = lambda a,b: a/b

        if self._params is not None:
            loc.update(self._params)

        loc['respList'] = []
        loc['exprList'] = []
        loc['modelList'] = []
        return loc

    @timeit_m
    def init(self, debug=False):
        #debug = True
        funcn = "EgenNew.init"
        self._nsByVar = FldNamespace(self._scn, outDir=self.cacheDir())

        loc = self.get_ctx(locals())
        exec(self.lines(debug))
        self._script_ctx.update(loc)

        if debug:
            #self._genFlds.update(loc['genFlds'])
            print(f"DEBUG_1106: {funcn} genFlds", loc['genFlds'])
            print(f"DEBUG_1106: {funcn} Namespace", loc, file=sys.stderr)
            print(f"DEBUG_1106: {funcn} {loc['bar']} fldlstFn= {loc['fldlstFn']}", file=sys.stderr)

        if loc['fldlstFn'] != "":
            if self._fldlstFn == "":
                self._fldlstFn = loc['fldlstFn']
            if self._outDir == "":
                self._outDir = os.path.dirname(loc['fldlstFn']) 
                if not os.path.exists(self._outDir):
                    QpsUtil.mkdir([self._outDir])
            #When writing fldlst file, remove the fillstFn line.
            open(self._fldlstFn, 'w').write(re.sub(r'fldlstFn.*?\n', '', self.lines()))

        #self.exprList = [*respList, *exprList]
        self._exprList = [*loc['respList'], *loc['exprList']]
        self._compiledList = compile_expr_list(self._exprList)
        if debug:
            self.display(what='compiled')

    @timeit_m
    def run_egen(self, runDir=None, force=False, write_event_file=False, debug=False, gen_flds=True, genetic_algor=False):
        funcn = "EgenNew.run_egen"

        self.init(debug)

        if not self.need_gen_flds(funcn, force):
            return None

        try:
            self.exec_compiled_statements(debug, server_mode=(self.opt().server_mode!="NA"))
        except Exception as e:
            if f"{e}".find("Pred_univ_CN")>=0:
                return None
            elif (self.opt().server_mode!="NA"):
                raise e
            elif f"{e}".find("fld_contains_nan_only")>=0:
                if ctx_symset() not in ['Unknown']:
                    print(f"{BROWN}INFO: rerun exec_compiled_statements with prev_scn set to G, {e}{NC}", file=sys.stderr)

                #some factors need longer history than W, so add append G
                self._scn.set_prev_scn('G')
                try:
                    #self._scn.get_prev_scn()
                    self.exec_compiled_statements(debug=True)
                except Exception as e:
                    if ctx_symset() in ['R88', 'Unknown']:
                        print(f"INFO: symset= {ctx_symset()}, exception= {e}, rerun exec_compiled_statements failed")
                        exit(0)
                    else:
                        print(f"{RED}INFO: symset= {ctx_symset()}, exception= {e}, rerun exec_compiled_statements failed{NC}")
                        traceback.print_stack()
                        exit(1)
            else:
                raise e

        # self.respNm(debug=True)
        # if not self.need_gen_flds(funcn, force):
        #     return self

        self.display('flds')

        self.tidy_up_nsvars(funcn)

        self.set_ns_by_mkt()

        #dtaBlob = pd.concat(self._dfByMkt.values(), keys=self._dfByMkt.keys(), names=['mkt', 'datetime'])
        if debug and False:
            #print(f"DEBUG: {funcn} df.tail()", self.df().tail())
            print_df(self.dfMktDtmByFlds(title=f"{funcn} debug_print").tail(), title=f"DEBUG_1107: {funcn}")

        if gen_flds:
            self.gen_flds(self._scn.symset, self.dfMktDtmByFlds(self._genFlds.keys(), f"{funcn} gen_flds"))

        if not genetic_algor:
            if ctx_symset() in ['E47']:
                self.write_regtests(self.df(f"{funcn} regtests"))
                self.write_event_output(funcn, write_event_file, self.df(f"{funcn} event_output"))

        return self

    def set_respNm(self, varNm):
        funcn = "set_respNm"
        print(f"{NC}INFO: {funcn} setting respNm= {varNm}{NC}")
        self._respNm = varNm
        self._nsByVar.respNm = varNm
        self._nsByVar.respFld = self._nsByVar[varNm]
        #print(fld_debug_info(varNm, self._nsByVar[varNm]))
        #self._nsByVar.respFld.set_index(self._nsByVar.respFld.index + pd.Timedelta('15:00:00'), inplace=True)
        #print_df(self._nsByVar.respFld )

    @timeit_m
    def exec_compiled_statements(self, debug=False, server_mode=True):
        funcn = "EgenNew.exec_compiled_statements"
        loc = self.get_ctx(locals())
        loc['ns'] = self._nsByVar

        print(f"DEBUG_8731: {funcn} compiledList=", '\n'.join(self._compiledList))

        for stmt in self._compiledList[:]:
            if stmt.find("=") < 0:  # varNm only
                varNm = stmt

                if self._scn.get_prev_scn() is not None:
                    assert True, "NEED implementation"

                if varNm in self._nsByVar and self._nsByVar[varNm] is not None and server_mode:
                    print(f"{CYAN}INFO: {funcn}(expression): ({stmt}), skipping...{NC}", file=sys.stderr)
                else:
                    print(f"{CYAN}INFO: {funcn}(expression): ({stmt}), running...{NC}", file=sys.stderr)
                    self._nsByVar[varNm] = self._nsByVar.get_fld(self._scn, self._nsByVar, self._nsByVar.varname2fldname(varNm), debug=debug)
                    
                if self._nsByVar[varNm] is None:
                    if ctx_symset() in ['R88', 'Unknown']:
                        exit(0)
                    assert False, f"ERROR: {funcn} cannot find field {varNm} {ctx_symset()}"
                    self._nsByVar[varNm] = 1

                if ctx_debug(1):
                    #print("DEBUG: EgenNew.run()", stmt, self._nsByVar.raw[stmt].tail())
                    print_df(self._nsByVar.raw[stmt].tail(), title=f"DEBUG_1108: {funcn}")

            else:
                (varNs, *fldExpr) = stmt.split(r'=')

                cacheFns = {}
                if varNs.find("ns[")>=0:
                    #assignment left can have left-list like: (ns['Pred_daily_Beta3H60_1d_1'],ns['Pred_daily_Vol60_1d_1'])
                    #varNs = [x for x in varNs.split("'") if x.find("_")>=0]
                    varNs = re.findall(r"'(\S+?)'", varNs)
                    varNs = [x for x in varNs if x.find(":")<0] #remove things like '09:30:00'
                else:
                    varNs = [varNs]

                if len(varNs)>0: #if not all left found cache, we need to evaluate the statement
                    if ctx_debug(5):
                        print(f"{DBG}DEBUG_3246: {funcn} stmt= {stmt}{NC}", file=sys.stderr)

                    need_exec = len(varNs)
                    # for varNm in varNs:
                    #     if varNm in self._nsByVar:
                    #         if self._nsByVar[varNm] is not None:
                    #             #print(f"{PURPLE}===============", varNm, need_exec, self._nsByVar[varNm].shape, f"{NC}")
                    #             need_exec -= 1
                    leftoverVarNms = []
                    for varNm in self._nsByVar.keys():
                        if varNm in varNs:
                            if self._nsByVar[varNm] is not None:
                                #print(f"{PURPLE}===============", varNm, need_exec, self._nsByVar[varNm].shape, f"{NC}")
                                need_exec -= 1
                        else:
                            if varNm.find("Pred_")>=0:
                                leftoverVarNms.append(varNm) #can not del during iteration
                            
                    for varNm in leftoverVarNms:
                        print(f"{PURPLE}remove leftover fldNm=", varNm, self._nsByVar[varNm].shape, f"{NC}")
                        del self._nsByVar[varNm]

                    if need_exec <=0 and server_mode:
                        print(f"{BROWN}INFO: {funcn}(assignment): ({stmt}) skipping...{NC}", file=sys.stderr)
                        if self._respNm is None:
                            self.set_respNm(varNs[0])
                    else:
                        print(f"{BROWN}INFO: {funcn}(assignment): ({stmt}) running...{NC}", file=sys.stdout)

                        debug_stmts = []
                        stmt = remap_expr_old_style(stmt, debug_stmts)
                        for debug_stmt in debug_stmts:
                            print(f"{RED}debug_stmt= {debug_stmt}{NC}")
                            exec(debug_stmt)
                        exec(stmt)

                        #CHE
                        FIX_MISSING_DSCNDT=1
                        if FIX_MISSING_DSCNDT:
                            if 'Resp_C2C' in self._nsByVar and self._nsByVar['Resp_C2C'] is not None:
                                dcsndtm = pd.to_datetime(f'{ctx_dcsndt()} 15:00:00')
                                if dcsndtm not in self._nsByVar['Resp_C2C'].index: #insert decision date data
                                    if stmt.find("Resp_lnret"):
                                        #print("DEBUG_4352:", stmt) 
                                        #print(">>>", self._nsByVar['Resp_C2C'].tail())
                                        self._nsByVar['Resp_C2C'].loc[dcsndtm] = 0
                                        self._nsByVar['Resp_C2C'].sort_index(inplace=True)
                                        #print("<<<", self._nsByVar['Resp_C2C'].tail())
                        #print(f"{PURPLE}INFO: {funcn}(assignment): ({stmt}) {varNm}.shape={self._nsByVar[varNm].shape}{NC}")

                if ctx_debug(5):
                    for varNm in varNs:
                        print(f"DEBUG_4563: {funcn} {varNm: <16} {self._nsByVar[varNm].shape}", file=sys.stderr)

                if len(cacheFns)>0:
                    for varN in varNs:
                        if varN in cacheFns:
                            if varN.find("Pred_")<0:
                                smart_dump(self._nsByVar[varN], cacheFns[varN], debug=True, title=f"{funcn} {varN}")

                if ctx_dts_cfg() in ['prod1w', 'W', 'T']:
                    for k in varNs:
                        if k not in self._nsByVar:
                            continue
                        v = self._nsByVar[k]
                        if type(v) == type(pd.DataFrame()) and v.dropna(axis=0, how='all').shape[0] == 0:
                            if debug or True:
                                loc['ns'].print()
                            exceptionStr = f"ERROR: {funcn} fld_contains_nan_only scn={ctx_dts_cfg()}, fldnm={k}, expr= {fldExpr}, shape={v.dropna(axis=0, how='all').shape}"
                            raise Exception(exceptionStr)

        if ctx_debug(5):
            print(f"{DBG}DEBUG_3245: nsByVar {list(self._nsByVar.keys())},  {list(loc['ns'].keys())}{NC}")
            #print_df(self._nsByVar[varNs[-1]])

        #check if a field is all NA
        #print(f"============= {ctx_scn()}")
        # if ctx_dts_cfg() in ['prod1w', 'W', 'T']:
        #     for k,v in self._nsByVar.items():
        #         if type(v) == type(pd.DataFrame()) and v.dropna(axis=0, how='all').shape[0] == 0:
        #             if debug or True:
        #                 loc['ns'].print()
        #             exceptionStr = f"ERROR: {funcn} fld_contains_nan_only scn={ctx_dts_cfg()}, fldnm={k}, expr= {fldExpr}, shape={v.dropna(axis=0, how='all').shape}"
        #             raise Exception(exceptionStr)

    @timeit_m
    def set_ns_by_mkt(self, debug=False):
        funcn = "EgenNew.set_ns_by_mkt"
        self._nsByMkt = initNsByMkt(self._scn, self.respNm(), self._nsByVar[self.respNm()])
        for varN in self._nsByVar.keys():
            if type(self._nsByVar[varN]) != type(pd.DataFrame()):
                continue
            for mkt in self.mkts():
                dtaByVar = self._nsByVar[varN]
                if dtaByVar is None:
                    continue
                if type(dtaByVar) == type(tuple()):
                    dtaByVar = dtaByVar[1]
                # print(f"self.opt.debug: self.nsByVar[{varN}] {type(self.nsByVar[varN])}, {self.nsByVar[varN]}")
                if debug:
                    print(f"DEBUG:{funcn} self.nsByVar[{varN}] mkt={mkt} {type(self._nsByVar[varN])}")  # , {dtaByVar.tail()}
                if mkt not in dtaByVar:
                    print(f'ERROR: {mkt} not in {varN}')
                    mkts.remove(mkt)
                else:
                    # print(mkt)
                    self._nsByMkt[mkt][varN] = dtaByVar[mkt]  # varD[mkt]

    @timeit_m
    def tidy_up_nsvars(self, funcn, trim_dtabegdtm=False):
        funcn = "tidy_up_nsvars"
        if ctx_debug(1):
            print(f"DEBUG_4327: {funcn} {self._scn.dtaBegDtm}")
        #print(f"DEBUG_9876: tidy_up_nsvars({funcn}) ...")
        # after all flds generated, trim the data to remove leading dates data (they do not necessary starts at the same time)
        
        for v in self._nsByVar.keys():
            df = self._nsByVar[v]
            if type(df) == type(pd.DataFrame()):
                df.dropna(axis=0, how='all', inplace=True) #XXX
            
            if df is None:
                if v.find("Pred_") >= 0 and v.find("niv") < 0 and ctx_symset().find("Unknown") < 0:
                    print(f"{ERR}ERROR: {funcn} no data for fldnm= {v}, symset= {ctx_symset()}, existing ... {NC}")
                    exit(1)
                else:
                    print(f"WARNING: {funcn} no data for fldnm= {v}, symset= {ctx_symset()}", file=sys.stderr)
            else:
                if type(df) == type(pd.DataFrame()) and trim_dtabegdtm:
                    self._nsByVar[v] = df[df.index >= self._scn.dtaBegDtm]
                else:
                    self._nsByVar[v] = df
        # If a mkt is in Resp, but not in varD for some reason (frequent occurs), sometimes caused strange issues. So patch with NA
        for v in self._nsByVar.keys():
            if type(self._nsByVar[v]) != type(pd.DataFrame()):
                continue
            for mkt in self.mkts():
                if v not in self._nsByVar or self._nsByVar[v] is None:
                    print(f"ERROR: nsByVar does not contain var= {v}")
                    continue

                if mkt not in self._nsByVar[v].columns:
                    if self._opt.debug:
                        print(f"WARNING: data(varD) missing for {v} {mkt}, patching with NaNs")
                    self._nsByVar[v].loc[:,mkt] = np.nan
                    # self.nsByVar[v].loc[:, mkt] = np.nan
        #print(f"done {funcn}")
        #print(f"DEBUG_9877: tidy_up_nsvars({funcn}), done")

    def need_gen_flds(self, funcn, force):
        needGenFld = False
        if (self._genFlds is not None) and (len(self._genFlds) > 0):
            for fldNm in self._genFlds:
                fp = f'{self._scn.qdfRoot}/{self._bar}/{fldNm}/{self._scn.symset}.db'
                if not os.path.exists(fp):
                    needGenFld = True
                elif force:
                    needGenFld = True
                    #Path(fp).unlink()
                    if ctx_verbose(1) or True:
                        print(f"INFO: {funcn} found field file {fp}, force regenerating...")
                else:
                    self.show_fld(self._opt, fp)
                    print(f"INFO: {funcn} found field file {fp}, skipping...")
        else:
            #This is not a field generation, but a model-running task, set needGenFld so program can continue
            needGenFld = True
        return needGenFld

    def show_fld(self, opt, fp):
        if opt and hasattr(opt, 'show') and "fld" in opt.show.split(r','):
            cmd = f"python /Fairedge_dev/app_QpsData/run_jupyter_tmpl.py --tmpl fld {fp}"
            print(cmd)
            os.system(f"{cmd} > /dev/null")

    @timeit_m
    def gen_flds(self, symset, df, debug=False):
        funcn = "EgenNew.gen_flds"
        if ctx_verbose(1):
            print(f"INFO: {funcn} genFlds= {self.genFldNms()}, columns= {df.columns}")
        if ctx_debug(8239):
            print_df(df, title=funcn)
        fldPaths = []
        for fldNm in self._genFlds.keys():
            if self._genFlds[fldNm].find(";")>=0:
                (fldTm, fldVarNm) = self._genFlds[fldNm].split(";")
            else:
                fldTm = self._genFlds[fldNm]
                fldVarNm = fldNm

            if fldVarNm not in df:
                if True or fldVarNm.find('_trd')<0:
                    print(f"WARNING: {funcn} can not find fld to dump {fldNm}~{fldVarNm}", file=sys.stderr)
                continue

            fp = f'{self._scn.qdfRoot}/{self._bar}/{fldNm}/{self._scn.symset}.db'
            pld = df[fldVarNm].unstack(level=0)
            pld.dropna(axis=0, how='all', inplace=True) #XXX
            # pld.dropna(axis=1, how='all', inplace=True)
            pld.set_index(pd.to_datetime(pld.index.date) + pd.Timedelta(fldTm), inplace=True)
            #print(pld.tail())
            QpsUtil.mkdir([os.path.dirname(fp)])
            if debug and ctx_regtest():
                print(f"INFO: {funcn} dump fldNm= {fldNm},", str_minmax(fp, disable=True), pld.shape)
                print_df(pld.tail(10), title=f"{funcn} fldNm={fldNm}")
            smart_dump(pld, fp, debug=True, title=funcn)
            fldPaths.append(fp)
            fldDesc = self._params
            smart_dump(fldDesc, fp.replace('.db', '.desc'), debug=debug, title=funcn)

            # md5 = QpsUtil.buf2md5(fp)[-6:]
            # show_data = {"fldPath": fp, "dtaDir": self.dta_dir()}
            # #smart_dump(show_data, f"/Fairedge/run_jupyter/gen_data.{md5}.pkl")
            # smart_dump(show_data, f"{self.dta_dir()}/gen_data.{md5}.pkl")
            
            self.show_fld(self._opt, fp) 

        if len(fldPaths):
            if self._task_name != "NA":
                smart_dump(fldPaths, f"{self.task_dir()}/{symset}.fldPaths.pkl", title=funcn, debug=True, verbose=1)

    @timeit_m
    def display(self, what='all'):
        funcn = f"EgenNew.display(what={what})"
        if what in ['flds']:
            for var,df in self._nsByVar.raw.items():
                if isinstance(df, pd.DataFrame):
                    if ctx_verbose(1):
                        print(f"INFO: {funcn} fld var= {var:<16}, shape= {df.shape}, last row valid cnt= {df.tail(1).count(axis=1).iloc[0]}, index uniq= {df.index.is_unique}")
                    if ctx_verbose(5):
                        print_df(df, title=f"{DBG}{var}{NC}")    
                    # print(df.tail().count(axis=1))
                    # print(df.tail().iloc[:,:10])

        # exit(0)

        if what in ["all"]:
            print('\n'.join([f"EgenNew.expr: {x}" for x in self._exprList]))
            print('\n'.join([f"EgenNew.flds: {x}" for x in self._genFlds.keys()]))
            print(f"Egen evtFn: {self.evtFn()}")

        if what in ["compiled", "all"]:
            print('\n'.join([f"EgenNew.cmpl: {x}" for x in self._compiledList]))

        if what in ['value']:
            for var,df in self._nsByVar.raw.items():
                if isinstance(df, pd.DataFrame):
                    print(f"Egen var {var}: {df.tail()}")

    def export(self, tag, dir=".", bunch_name=None):
        funcn = 'EgenNew.export'
        if bunch_name is not None:
            #return smart_dump(self._nsByVar.raw, f"{dir}/{bunch_name}_{tag}.pkl", debug=True, title=funcn)
            print_df(self.to_dataframe().head(5), title=funcn)
            #print(self.to_numpy()[:5])
            return smart_dump(self.to_dataframe(), f"{dir}/{bunch_name}_{tag}.pkl", debug=True, title=funcn)
        else:
            if self._nsByVar is not None:
                for var,df in self._nsByVar.raw.items():
                    if isinstance(df, pd.DataFrame):
                        smart_dump(df, f"{dir}/{var}_{tag}.pkl", debug=True, title=f"{funcn} {var} {df.shape}")
        return None        

    def to_dataframe(self):
        if self._to_dataframe is None:
            self._to_dataframe = pd.DataFrame.from_dict({k:v.stack() for k,v in self._nsByVar.raw.items() if type(v) == type(pd.DataFrame())})
        return self._to_dataframe

    def to_numpy(self):
        return self.to_dataframe().to_numpy()

    def tag(self):
        #print('tag: fldlstFn', self.fldlstFn)
        rc = ('='.join(self._fldlstFn.split(r'/')[3:6]))
        if rc == "":
            return "egentag"
        else:
            return rc

    def outFnTmpl(self):
        outFnTmpl = f'{self._fldlstFn.replace(".fldlst", "")}.{self._scn.symset}.{self._scn.snn}.{self._opt.asofdate}.{self._scn.dts_cfg}.{self._bar}.MKT.FMT'
        #print(outFnTmpl)
        return outFnTmpl

    @deprecated
    def getPlotData(self, mkt):
        src = self._dfByMkt[mkt]
        #print(f"{self.getPlotData.__name__}", src.tail())
        tgt = deepcopy(src)
        for v in src.columns:
            if self._nsByVar.fldType(v) == 'ret':
                #tgt.loc[:,v] = src[v].cumsum()
                tgt.loc[:,v] = src[v]

                #print(f"{v} {self.fldType(v)}, src={src[v]}, tgt= {tgt[v]}")
            else:
                tgt.drop(columns=[v], inplace=True)

        #print(f"tgt=", tgt.tail())

        return(tgt)

    def dfForMkt(self, mkt):
        if mkt in self._nsByMkt:
            return pd.DataFrame.from_dict(self._nsByMkt[mkt])

    def __dfMktDtmByFlds(self, fldNms, title="NA"):
        funcn = "EgenNew._dfMktDtmByFlds"
        print(funcn, fldNms)
        if self._df is None:
            if ctx_verbose(1):
                print(f"INFO: {funcn}", list(self._nsByVar.keys()))
            unstackDict = {}
            scalarDict = {}
            for (k, v) in self._nsByVar.items():
                if v is None:
                    continue
                if k not in fldNms and k.find("Resp")<0:
                    print(f"{PURPLE}", funcn, "skiping", k, f"{NC}", file=sys.stderr)
                    continue

                #print(f"DEBUG_7434: ===", k, type(v), type(1)) 
                if type(v) == type(pd.DataFrame()):
                    if not v.index.is_unique:
                        print(f"{RED}ERROR_3409: {funcn} index not uniq for {k}{NC}", file=sys.stderr)  
                        print(v.index[v.index.duplicated()])   
                        continue

                    if v.shape[0] == 0:
                        print(f"{RED}ERROR_3409: {funcn} symset {ctx_symset()} has no data for {k}, exiting...{NC}", file=sys.stderr)
                        exit(0)

                    if ctx_verbose(1):
                        print_df(v, title=f"{funcn} {k}")
                    
                    if k.find("Resp")>=0:
                        if EgenNew.respStacked is None:
                            print(f"{PURPLE}", funcn, "transform_to_stackform", k, f"{NC}", file=sys.stderr)
                            EgenNew.respStacked = v.unstack()
                        else:
                            print(f"{PURPLE}", funcn, "arleady_stacked_form", k, f"{NC}", file=sys.stderr)
                        unstackDict[k] = EgenNew.respStacked
                    else:
                        print(f"{PURPLE}", funcn, "transform", k, f"{NC}", file=sys.stderr)
                        unstackDict[k] = v.unstack()
                    #print_df(unstackDict[k], title=f"{funcn} {k}")
                    
                elif type(v) != type(1) and type(v) != type(1.0):
                    unstackDict[k] = v
                else:
                    scalarDict[k] = v

                if k == self.respNm():
                    self._respBegDtm = v.index[0]
                    self._respEndDtm = v.index[-1]

            if False:
                for k,v in unstackDict.items():
                    print("****", k, v[:5])

            if len(unstackDict)>0:
                if False:
                    self._df = pd.DataFrame.from_dict(unstackDict)
                else:
                    @timeit
                    def speedup_from_dict(self, cols):
                        if ctx_debug(1):
                            print("DEBUG_6112", cols)
                        self._df = pd.DataFrame.from_dict({x: unstackDict[x] for x in cols})

                    speedup_from_dict(self, [x for x in unstackDict.keys() if x.find("Resp_")>=0 or x.find("Pred_")>=0])
                    #speedup_from_dict(self, [x for x in unstackDict.keys()])

                self._df.index.set_names(['Mkt', 'Dtm'], inplace=True)
                for (k,v) in scalarDict.items():
                    self._df[k] = v
                #print_df(self._df)
                if False:
                    resp = self._df[self.respNm()]
                    #resp = resp.drop_duplicates()
                    resp = resp.unstack('Mkt')
                    # print(f"DEBUG: resp duplicated {resp.columns.duplicated()}")
                    # resp = resp.loc[:, ~resp.columns.duplicated()]
                    resp.dropna(axis=0, how='all', inplace=True)
                    self._respBegDtm = resp.index[0]
                    self._respEndDtm = resp.index[-1]
                self._df = self._df.loc[(self._df.index.get_level_values('Dtm') >= self._respBegDtm) & (self._df.index.get_level_values('Dtm') <= self._respEndDtm)]
            else:
                self._df = pd.DataFrame()

        #print(f"INFO: done {funcn}, title={title}")
        #sys.stdout.flush()

        return self._df

    @timeit_m
    def dfMktDtmByFlds(self, fldNms, title="NA"):
        try:
            return self.__dfMktDtmByFlds(fldNms, title=f"EgenNew.df {title}")
        except Exception as e:
            print(f"Exception: EgenNew.df {e}")
            traceback.print_exc()
            return None



