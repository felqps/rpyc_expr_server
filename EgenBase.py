import os
import sys

import pandas
from common_basic import *
from common_paths import *
import QpsUtil
from dateutil.parser import parse as dtparse

class EgenBase:
    _script_ctx = {}
    _script_funcs = None
    def __init__(self, opt, scn="", fldlstFn="", outDir="", lines="", params={}, task_name="taskname", run_dir="", sty=None):
        funcn = "Egen.init()"
        self._scn = scn
        self._bar = opt.bar
        self._fldlstFn = fldlstFn
        self._opt = opt
        self._outDir = outDir
        if self._outDir == "":
            self._outDir = os.path.dirname(fldlstFn)
        
        self._nsByVar = None
        self._nsByMkt = None
        #self._dfByMkt = None
        self._task_name = task_name
        self._exprList = []
        self._genFlds = {}
        self._linesIn = lines
        self._params = params
        self._unitTestMkts = ['000628.XSHE', '600939.XSHG']
        self._unitTestDts = ['20201127', '20211209']
        #self._mkts = None
        self._df = None
        self._run_dir = run_dir
        if self._run_dir == "":
            self._run_dir = "/temp"
        
        self._sty = sty
        self._respNm = None
        self._respBegDtm = None
        self._respEndDtm = None
        self._compiledList = None
        self._to_dataframe = None

    def script_ctx():
        return EgenBase._script_ctx
    
    #@timeit
    def script_funcs():
        if EgenBase._script_funcs is None:
            EgenBase._script_funcs = {
                "RANK": "RANK",
                "TS_RANK": "TS_RANK",
                "CORRELATION": "CORRELATION",
                "DELTA": "DELTA",
                "DELAY": "DELAY",
                "ADV": "ADV",
                "REF": "REF",
                "DECAY_LINEAR": "DECAY_LINEAR",
                "SUM": "SUM",
                "SCALE": "SCALE",
                "INDUSTRY_NEUTRALIZE": "NA",
                "TS_ARGMIN": "TS_ARGMIN",
                "PRODUCT": "PRODUCT",
                "LOG": "LOG",
                "TS_MIN": "TS_MIN",
                "TS_AVG": "TS_AVG",
                "AS_FLOAT": "AS_FLOAT",
                "SIGNEDPOWER": "SIGNEDPOWER",
                "TS_MAX": "TS_MAX",
                "TS_ARGMAX": "TS_ARGMAX",
                "Factor": "F",
                "IF": "IF",
                "STDDEV": "STDDEV",
                "SIGN": "SIGN",
                "ABS": "ABS",
                "MAX": "MAX",
                "MIN": "MIN",
                "LT": "LT",
                "GT": "GT",
                "EQ": "EQ",
                "COVARIANCE": "COVARIANCE",
                "REGOUT": "REGOUT",
                "F": "F",
                "C": "C",
                "Z": "Z",
                "ZZ": "ZZ",
                "W": "W",
                "ZW": "ZW",
                "MA": "MA",
                "EMA": "EMA",
                "FFILL": "FFILL",
                "FILTER_RANGE": "FILTER_RANGE",
                "FILTER_LIKE": "FILTER_LIKE",
            }

        return EgenBase._script_funcs
    
    def register_script_func(funcn, func):
        EgenBase.script_ctx()[funcn] = func
        EgenBase.script_funcs()[funcn] = funcn
        #print(EgenBase.script_funcs())

    def register_script_func_name(funcn):
        EgenBase.script_funcs()[funcn] = funcn

    def pre_processing(self, linesIn):
        #Currently only pre-process "INCLUDE"
        loc = {}
        loc.update(self._params)
        linesOut = []
        for ln in linesIn:
            if ln.find("INCLUDE")>=0:
                ln = eval(ln, loc)
            linesOut.append(ln)
        return linesOut

    def study_name(self):
        if self._sty is None:
            return "NA"
        else:
            return self._sty.study_name()
        
    #@deprecated_m
    @func_m_called
    def save(self, what="setup"):
        funcn = f"EgenBase.save(what={what}"
        QpsUtil.smart_dump(self._genFlds, f"{self.dta_dir()}/genFlds.pkl", debug=True, title=funcn)
        if self._sty is None:
            return
        else:
            return self._sty.save(what)

    def respNm(self, debug=False):
        #The first column is the reference column
        if self._respNm is None:
            self._respNm = list(self._nsByVar.keys())[0]
            self._nsByVar._respNm = self._respNm
            #self._mkts = self._nsByVar[self._respNm].columns
            if debug:
                print(f"INFO: referenceFld(respN)= {self._respNm}, mkts= {self.mkts()}")
                print(f"DEBUG: vars= {list(self._nsByVar.keys())}, {self._nsByVar.raw[self.respNm()].iloc[:, :2].tail()}")
        return self._respNm

    def mkts(self):
        return self._nsByVar[self.respNm()].columns

    def nsByVar(self):
        return self._nsByVar

    def opt(self):
        return self._opt

    def getGenFlds(self):
        return self._genFlds

    def genFldNms(self):
        return sorted(list(self.getGenFlds().keys()))

    def __str__(self):
        return f"Egen: (task_dir= {self.task_dir()})"

    def show(self):
        pass
        # cmd = f"python /Fairedge_dev/app_QpsData/run_jupyter_tmpl.py --tmpl cx {self.dta_dir()}"
        # print(cmd)
        # os.system(f"{cmd} > /dev/null")

    def write_event_output(self, funcn, write_event_file, dtaBlob):
        if write_event_file:
            assert self._outDir != "", f"ERROR: {funcn} write_event_file without specifying outDir"
            QpsUtil.mkdir([os.path.dirname(self.evtFn())])
            if True or self._opt.debug:
                print(f'INFO: egen dump evt data {self.evtFn()} {dtaBlob.shape}', file=sys.stderr)

            if self.evtFn().find("/sel") < 0:
                if self._opt.debug:
                    print("DEBUG: columns order", [*self._exprList, 'RqInduCode'])
                smart_dump(dtaBlob, self.evtFn(), debug=True)
                # dtaBlob.to_csv(evtFn.replace('.db', '.txt'), sep='\t')
            else:  # /sel<date> files are for Super to read by R
                # print("DEBUG: columns order", [*exprList, 'RqInduCode'])
                # pickle.dump(dtaBlob[[*exprList, 'RqInduCode']], open(evtFn, 'wb'))
                dtaBlob.reset_index(inplace=True)
                smart_dump(dtaBlob, self.evtFn(), debug=True)

    def write_regtests(self, dtaBlob):
        if self._opt.dts_cfg == 'prod1w' and \
                self._scn.symset in ['E47'] and \
                self._fldlstFn != "":
            tag = '='.join(self._fldlstFn.split(r'/')[3:6])
            regtestFn = f'/Fairedge_dev/app_egen/regtests/{os.path.basename(self._fldlstFn)}.{self._scn.symset}.{tag}.txt'
            print(f'INFO: egen regtest output to {regtestFn}, index type {type(dtaBlob.index.get_level_values(1))}')
            print(dtaBlob.loc[(dtaBlob.index.get_level_values(1) >= dtparse("2021-09-13 15:00:00")) & (
                        dtaBlob.index.get_level_values(1) <= dtparse("2021-10-13 15:00:00"))],
                  file=open(regtestFn, 'w'))
            # print(dtaBlob, file=open(regtestFn, 'w'))

    def cacheDir(self):
        return f"{self._outDir}/{self._scn.snn}/{self._scn.dts_cfg}_{self._scn.begDt}_{self._scn.endDt}/{self._bar}"

    def task_dir(self):
        return f"{self._run_dir}/{self._task_name}"

    def dta_dir(self, cache=False):
        #contains predictor transformations
        #d = f"{self.task_dir()}/{self._sty._symset}.{dts_cfg_shortform(self._scn.dts_cfg)}"
        d = f"{self.task_dir()}/{self._scn.symset}.{dts_cfg_shortform(self._scn.dts_cfg)}"
        if not os.path.exists(d):
            QpsUtil.mkdir([d])
        return d

    def rpt_dir(self):
        #return f"{rootdata()}/egen_study/{self.study_name()}/{self.run_name()}/{self.task_name()}/{self.symset()}.{self.scn()}/q10"
        return f"{self.dta_dir()}/q10"

    def evtFn(self):
        evtFn = f'{self._outDir}/{self._scn.snn}/{self._scn.dts_cfg}_{self._scn.begDt}_{self._scn.endDt}/{self._bar}/{self._scn.symset}.db'
        if self._outDir.find("/Fairedge_dev")>=0: #If use fldlst from src dir(instead of egen/ dir), same symset will overwrite each other, so attach basename to the output
            baseFn = os.path.basename(self._fldlstFn).split(r'.')[0]
            evtFn = f'{self._outDir}/{self._scn.snn}/{self._scn.dts_cfg}_{self._scn.begDt}_{self._scn.endDt}/{self._bar}/{self._scn.symset}.{baseFn}.db'
        return evtFn

    def respBegDtm(self):
        return self._respBegDtm

    def respEndDtm(self):
        return self._respEndDtm

    def symset(self):
        return self._scn.symset

    def scn(self):
        return self._scn

    @timeit
    def save_unit_tests(self, unit_tests_dir):
        funcn = "Egen.save_unit_tests"
        for mkt in self._unitTestMkts:
            if mkt not in self.mkts():
                continue
            df = self.dfMktDtmByFlds(title=f"{funcn} {mkt}")
            pd.set_option('display.width', 1000)
            for dt in self._unitTestDts:
                if dt in df.loc[mkt].index:
                    QpsUtil.mkdir([unit_tests_dir])
                    fn = f"{unit_tests_dir}/{mkt}.{dt}.txt"
                    print(f"INFO: {funcn} writing {fn}")
                    open(fn, 'w').write(f"{df.loc[mkt].loc[dt]}")

