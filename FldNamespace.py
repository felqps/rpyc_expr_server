#!/home/che/anaconda3/bin/python

import sys

from tabnanny import verbose

import matplotlib.pyplot as plt
import pandas as pd
import re
import os
import traceback
from copy import deepcopy
from dateutil.parser import parse as dtparse
import QpsUtil
from Fld import Fld
from shared_cmn import fldnm2bar
from common import *
from Scn import Scn
import json
from bgen_helpers import *
from get_fld import *
from CTX import *
from common_options_helper import get_options
from cmdline_opt import *

from collections.abc import MutableMapping

def sft_by(pld, bar, num=0):
    #print(f'DEBUG: {bar}')
    barLen = int("".join(bar[:-1]))
    barUnit = bar[-1]
    #print(f"INFO: sft_by({barLen}, {barUnit})")
    tmDelta = pd.Timedelta(barLen*num, unit=barUnit)
    if num == 0:
        return(pld)
    def func(tm, tmDelta=tmDelta):
        tm += tmDelta
        return(tm)
    pld.index = pld.index.map(func)
    return(pld)

def sft_daily(pld, bar):
    def func(tm):
        if bar == '5m':
            return(pd.to_datetime(f'{tm.date()} 14:55:00'))
            #return(pd.to_datetime(f'{tm.date()} 15:00:00'))
        if bar == '1m':
            return(pd.to_datetime(f'{tm.date()} 14:59:00'))
            #return(pd.to_datetime(f'{tm.date()} 15:00:00'))
        else:
            #return(pd.to_datetime(f'{tm.date()} 14:59:00'))
            return(pd.to_datetime(f'{tm.date()} 15:00:00'))
    pld.index = pld.index.map(func)
    return(pld)

def _calc_limit_updown_flag_helper(prc, lmtUp, lmtDown, prcDelta):
    flag = "LH"
    if(prc >= lmtUp - prcDelta):
        flag="H"
    if(prc <= lmtDown + prcDelta):
        flag="L"
    return flag

def _calc_limit_updown_flag(row, debug=False):
    funcn = '_calc_limit_updown_flag'
    (iniPrc, iniLmtUp, iniLmtDown, liqPrc, liqLmtUp, liqLmtDown) = row
    if debug:
        print("DEBUG:", funcn, row.name, iniPrc, iniLmtUp, iniLmtDown, liqPrc, liqLmtUp, liqLmtDown)
    prcDelta = 0.02
    iFlag = _calc_limit_updown_flag_helper(iniPrc, iniLmtUp, iniLmtDown, prcDelta)
    lFlag = _calc_limit_updown_flag_helper(liqPrc, liqLmtUp, liqLmtDown, prcDelta)

    return(f"i={iFlag};l={lFlag}")

def calc_limit_updown_flag(iniPrc, iniLmtUp, iniLmtDown, liqPrc, liqLmtUp, liqLmtDown, debug=False):
    funcn = 'calc_limit_updown_flag'

    #print(type(iniPrc), type(liqPrc), type(lmtUp), type(lmtDown))
    # foo = iniPrc.stack()
    # bar = foo.unstack()
    # print(bar)

    df = pd.DataFrame.from_dict({
        'iniPrc': iniPrc.stack(),
        'iniLmtUp': iniLmtUp.stack(), 
        'iniLmtDown': iniLmtDown.stack(),
        'liqPrc': liqPrc.stack(), 
        'liqLmtUp': liqLmtUp.stack(), 
        'liqLmtDown': liqLmtDown.stack()})
    df['rc'] = df.apply(_calc_limit_updown_flag, axis=1)
    
    if debug:
        print("DEBUG:", funcn, df.tail())
    return(df['rc'].unstack())

@deprecated
def nan_if_condition_equal(val, condfld, equal=0, debug=False):
    return nan_if_condition_fld_equal(val, condfld, equal, debug)

def calc_prc_flag(prc, lmtUp, lmtDown, V, return_chk=0.005):
    # Flags: 
    # 0 = tradable prc
    # nan = stock halted (no lmtup/down info)
    # 1 = lmtUp
    # -1 = lmtDown
    flag = deepcopy(prc)
    flag.loc[:] = np.nan
    flag[lmtUp>0] = 0

    flag[abs(prc/lmtDown-1.0)<return_chk] = -1
    flag[np.abs(prc/lmtUp-1.0)<return_chk] = 1
    # print_df(V, title=f"DEBUG_2333")
    # print_df(flag[V!=0], title=f"DEBUG_2334")
    flag = flag[V!=0]
    return flag

def calc_prc_tradable(prc, lmtUp, lmtDown, V, return_chk=0.005):
    flag = calc_prc_flag(prc, lmtUp, lmtDown, V, return_chk)
    prc = deepcopy(prc)
    prc[flag!=0] = np.nan
    return(prc)

def nan_if_condition_fld_equal(val, condfld, equal=0, debug=False):
    funcn = 'nan_if_condition_equal'
    val[condfld == equal] = np.nan
    return(val)

def overwrite_if_condition_fld_nan(val, condfld, value=0, debug=False):
    funcn = 'nan_if_condition_equal'
    val[condfld.isnull()] = value
    return(val)

def set_const(val, const_val):
    valNew = deepcopy(val)
    valNew[:] = const_val
    return(valNew)


#E47 testing use lmtDelta=0.27, default=0.02
def calc_point_return_tradable(prcFrm, prcTo, v, lmtUp, lmtDown, lags, lmtDelta=0.02, debug=False):
    lags = 1 if lags == "" else int(lags)+1
    funcn = f'calc_point_return_tradable(lags={lags})'
    rcs = {}
    for sym in prcFrm.columns:
        df = pd.DataFrame.from_dict({
            "prcFrm": prcFrm[sym],
            "prcTo": prcTo[sym],
            "v": v[sym],
            "lmtUp": lmtUp[sym],
            "lmtDown": lmtDown[sym]
        }, orient='columns')

        for col in ['prcFrm', 'prcTo']:
            df[col][df[col]>=df["lmtUp"]-lmtDelta] = np.nan
            df[col][df[col]<=df["lmtDown"]+lmtDelta] = np.nan

        df['prcTo'] = df['prcTo'].shift(-lags)
        df['prcTo'] = df['prcTo'].fillna(method='bfill', axis=0)
        # If prcFrm is nan, then nan for the day. If prcFrm is valid but prcTo is not valid, then find next valid prcTo and use it
        
        #df['rc'] = df['prcTo']/df['prcFrm'] - 1.0
        df['rc'] = np.log(df['prcTo']/df['prcFrm'])

        if debug:
            print_df(df, rows=20, title=f"{funcn} {sym}")

        rcs[sym] = df['rc']

    df = pd.DataFrame.from_dict(rcs, orient='columns')
    if debug:
        print(df.tail(20))

    return df

def calc_value_streak(valIn):
    valOut = pd.DataFrame()
    cum = None
    for i, row in valIn.iterrows():
        if cum is None:
            cum = row
        else:
            prod = cum*row
            #changed sign set to 0
            prod = (prod>=0).replace({True: 1, False:0})
            cum = cum*prod + row
        valOut[i] = cum
    return(valOut.transpose())

def calc_over_under(valIn, threshold= 0.0010):
    O = (valIn>threshold).replace({True:1, False:0})
    U = (valIn<-threshold).replace({True:-1, False:0})
    OU = O + U
    return(OU)

def calc_val2indic(valIn, lookup = {3: 1, -4: -1}):
    O = (valIn==5).replace({True:0, False:0})
    U = (valIn==-9).replace({True:1, False:0})

    OU = O + U
    return(OU)

def binary_fac(varN = "smarg1_1d"):
    class Fac:
        def __init__(self):
            pld = smart_load(f"{rootdata()}/config/flds_binary.dict", debug=True)
            assert varN in pld, f"ERROR: binary_fac can not find {varN}"
            self.dta = pld[varN]
            pass
        def mean(self, debug=False):
            if debug:
                print(f"DEBUG: binary_fac {varN} mean {self.dta['mean']}")
            return self.dta['mean']

    return Fac()

MEMBERSHIP_DB = {}
def lookup_membership_file(row, fnGlobs, debug=False):
    #print(type(row), str(row.index[0]), '\n')
    sym = (row.index[0]).split(r'.')[0]
    dt = str(row.name.date())
    dt = dt.replace('-','')
    return __lookup_membership_file__(sym, dt, fnGlobs, debug)
    
def __lookup_membership_file__(sym, dt, fnGlobs, debug=False):
    funcn = "__lookup_membership_file__"
    sym = sym.split(r'.')[0]
    global MEMBERSHIP_DB
    if fnGlobs not in MEMBERSHIP_DB:
        MEMBERSHIP_INFO = {}
        MEMBERSHIP_DB[fnGlobs] = MEMBERSHIP_INFO
        for fnGlob in fnGlobs.split(","):
            fn = sorted(glob.glob(f"{rootdata()}/config/{fnGlob}/????????/*.csv"))[-1]
            if ctx_verbose(1):
                print(f"INFO: {funcn} lookup file {fn}", file=sys.stderr)
            for ln in QpsUtil.open_readlines(fn):
                (symbol,sym_set_name,permid,alive_date,reset_dt,dead_date) = ln.split(r',')
                symbol = symbol.strip()
                sym_set_name = sym_set_name.strip()
                if symbol not in MEMBERSHIP_INFO:
                    MEMBERSHIP_INFO[symbol] = []
                MEMBERSHIP_INFO[symbol].append((alive_date, dead_date))

    isMember = 0
    #print('DEBUG:', sym, dt)
    MEMBERSHIP_INFO = MEMBERSHIP_DB[fnGlobs]
    if sym in MEMBERSHIP_INFO:
        if debug:
            print("DEBUG:", dt, sym, MEMBERSHIP_INFO[sym])
        for (begDt, endDt) in MEMBERSHIP_INFO[sym]:
            if dt >= begDt and dt <= endDt:
                isMember = 1
                if debug:
                    print(f"DEBUG: {funcn} {sym} {dt} MEMBERSHIP= {isMember}")
                break   
    return(isMember)

def calc_st_flag(cPrc, debug=False):
    funcn = 'calc_st_flag'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "st_file"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def calc_StSts(cPrc, debug=False):
    funcn = 'calc_StSts'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "st_file"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def calc_CN300(cPrc, debug=False):
    funcn = 'calc_CN300'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "univ_cn300"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def calc_CN500(cPrc, debug=False):
    funcn = 'calc_CN500'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "univ_cn500"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def calc_CN800(cPrc, debug=False):
    funcn = 'calc_CN800'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "univ_cn300,univ_cn500"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def calc_CN1000(cPrc, debug=False):
    funcn = 'calc_CN1000'
    rc = pd.DataFrame()
    for sym in cPrc.columns[:]:
        rc.loc[:,sym] = cPrc[[sym]].apply(lambda r: lookup_membership_file(r, "univ_cn1000"), axis=1)
        if debug:
            print(f"DEBUG: {funcn}", rc[[sym]].tail())
    return(rc)

def compile_expr(expr):
    formula = re.sub(r"\$(\w+)", r"nsByVar.raw['\1']", expr)
    #formula = formula.replace('T(', 'nsByVar.T(')
    return formula

# def fldnm2timedelta(fldNm):
#     # if fldNm.find("O_1d")>=0:
#     #     dta.index = dta.index +  pd.Timedelta('09:30:00')
#     # elif fldNm.find("H_1d")>=0 or fldNm.find("L_1d")>=0 or fldNm.find("C_1d")>=0 or fldNm.find("V_1d")>=0:
#     #     dta.index = dta.index +  pd.Timedelta('15:00:00')
#     if fldNm.find("2O")>=0 or fldNm.find("_O_")>=0:
#         return pd.Timedelta('09:30:00')
#     if fldNm.find("2A")>=0:
#         return pd.Timedelta('13:00:00')
#     else:
#         return pd.Timedelta('15:00:00')

def get_predictor_sign(fldNm, symset, resp='C2C'):
    fp  = f"{rootdata()}/egen_study/predictors/sign/{resp}/{fldNm}.txt"
    rc = None
    if os.path.exists(fp):
        si = json.load(open(fp, 'r'))
        if symset in si:
            rc =  si[symset]
        elif "CS_ALL" in si:
            rc = si["CS_ALL"]
    if rc is not None:
        print(f"=============================================DEBUG: predictor sign {fldNm} {symset} {rc}, {fp}")   
    return rc

def dedup(fld, note):
    #if type(fld) != type(pd.Series(dtype='float64')):
    if not isinstance(fld, pd.Series):
        print(f"WARNING: duplicate columns: {note}")
        return fld.iloc[:,0]
    else:
        return fld
    
#@timeit_real
def align(refld, infld, fldType="ret", debug=False):
    funcn = 'FldNamespace.align'
    if debug:
        print(f"DEBUG: refld.shape= {refld.shape}; infld.shape= {infld.shape};")
        print_df(refld.tail(5), title="refld")
        print_df(infld.tail(5), title="infld")

    if refld.shape == infld.shape:
        if refld.last_valid_index().time() == infld.last_valid_index().time():
            if debug:
                print("DEBUG_1223: Assume already aligned. Maybe need to further compare columns and indices if want to be really sure.")
            return infld

    newX = {}
    cmnCols = list(set(refld.columns) & set(infld.columns))
    for mkt in sorted(cmnCols): #set(refld.columns)   
        x = pd.DataFrame.from_dict(
            {'refld': dedup(refld[mkt], note=f'{funcn} refld {mkt}'), 
             'infld': dedup(infld[mkt], note=f'{funcn} infld {mkt}')}
        )

        if fldType in ["ret", "univ"]: #For returns, should fill missing with 0.0
            if refld.last_valid_index().time() != infld.last_valid_index().time():
                if debug:
                    print(f"DEBUG_1222: {funcn} mkt= {mkt} fldType={fldType} reftm= {refld.last_valid_index().time()}, fldtm= {infld.last_valid_index().time()}", file=sys.stderr)
                x['infld'] = x['infld'].fillna(method='ffill', limit=1, axis=0) #Note only fill-forward at most 1-step to align time
            if fldType == "ret":
                pass
                #x['infld'] = x['infld'].fillna(0.0)   

        else: #for "prc", etc
            x['infld'] = x['infld'].fillna(method='ffill', axis=0)

        #We need to keep the last few input rows for generating production forecast, even though resp is Nan
        x = x[x.index.time == refld.last_valid_index().time()]
        if ctx_debug(5) and False:
            print(f"{RED}DEBUG_1224{NC}: ", funcn, mkt, x.tail(3), "\n", x['infld'].tail(3))
        newX[mkt] = x['infld']
    #print("refld", len(refld.columns), "infld", len(list(newX.keys())))

    pldX = pd.DataFrame.from_dict(newX)

    mkts_in_ref_only = list(set(refld.columns) - set(cmnCols))
    if len(mkts_in_ref_only)>0:
        print(f"DEBUG_1244: pre_adj refcol = {len(refld.columns)}, cmncols= {len(cmnCols)}, only_ref= {len(mkts_in_ref_only)} ")
    for mkt in mkts_in_ref_only:
        pldX[mkt] = np.nan
    #pldX.loc[:, mkts_in_ref_only] = np.nan #XXX not working, threw exception
    # if len(mkts_in_ref_only)>0:
    #     print(f"DEBUG_1244: post_adj refcol = {len(refld.columns)}, cmncols= {len(cmnCols)}, only_ref= {len(mkts_in_ref_only)} ")

    if debug:
        print_df(pldX, title="DEBUG_5445")

    pldX = pldX[pldX.index.isin(refld.index)]
    pldX = pldX[refld.columns]
    return(pldX)


def fldType(fldNm):
    rc = "ret"
    if fldNm.find("lnret")>=0 and fldNm.find("lnret_trd")<0:
        rc = "ret"
    elif  re.match(r'.?2.*_', fldNm):
        rc =  "ret"
    elif fldNm.find('univ_')>=0:
        rc = "univ"
    elif fldNm.find("OHLCV")>=0:
        rc = "prc"

    if False:
        print(f"INFO: ***** fldType {fldNm} => {rc}")
    return rc

PRECALCULATED_DB = {}
DICT_DB = {}

def get_dict(sid, name, bymkt):
    global DICT_DB
    k = f"{sid}_{name}_bymkt{bymkt}"
    if k not in DICT_DB:
        if cmdline_opt_debug_on():
            print(f"{PURPLE}INFO: FldNamespace get_dict add k= {k}{NC}", file=sys.stderr)
        DICT_DB[k] = dict()
    else:
        if cmdline_opt_debug_on():
            print(f"{PURPLE}INFO: FldNamespace get_dict existing k= {k}{NC}", file=sys.stderr)
    return DICT_DB[k]

class FldNamespace(MutableMapping):
    """A dictionary that applies an arbitrary key-altering
       function before accessing the keys"""

    def __init__(self, scn=None, outDir=None, bymkt=0, *args, **kwargs):
        global DICT_DB
        
        self.sid = scn.sid()
        #print(f"{PURPLE}INFO: FldNamespace.init() FldNamespace.scn= {scn}{NC}")
        self.scn = scn
        self.outDir = outDir
        #values are stored both in 'raw' and 'net'.
        #Statements are evaluated with 'raw' data so daily data is shifted properly
        #Output uses 'net' values which as reindexed to bar for daily data
        self.respNm = None
        self.bymkt = bymkt
        self.raw = get_dict(self.sid, "raw", bymkt)
        self.varN2fldN = get_dict(self.sid, "varN2fldN", bymkt)
        self.fldN2varN = get_dict(self.sid, "fldN2varN", bymkt)
        self.cfgFn = "NA"
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        #self.fldDefDir = "/Fairedge_dev/app_sgen/simulations/flds"
        #QpsUtil.mkdir([self.fldDefDir])
        self.init()
        #self.respIndexT = None

    def defined(self, varN):
        return (varN in self.raw)

    def __getitem__(self, varN):
        if varN in self.raw:
            if ctx_debug(7):
                if type(self.raw[varN]) == type(pd.DataFrame()):
                    if varN.find("Resp_C2C")>=0 or varN.find("VOLUME")>=0:
                        print(f"{PURPLE}getitem {fld_debug_info(varN, self.raw[varN])}{NC}", file=sys.stderr)
            return self.raw[varN]
        else:
            return None

    def __setitem__(self, varN, value):
        if type(value) == type(pd.DataFrame()):
            if ctx_debug(5):
                # value = value[(value.index >= self.scn.dtaBegDtm) & (value.index <= dtparse("2023-03-08 15:00:00"))]
                # value.dropna(how='all', axis=0, inplace=True) 
                #print("=============", type(value.index))
                #value.set_index(value.index.date() + pd.Timedelta('15:00:00'), inplace=True)
                print(f"{PURPLE}setitem {fld_debug_info(varN, value)}{NC}")
            #print_df(value)
        self.raw[varN] = value

    def __delitem__(self, varN):
        del self.raw[varN]

    def __iter__(self):
        return iter(self.raw)
    
    def __len__(self):
        return len(self.raw)
    
    def print(self, file=sys.stderr):
        funcn = "FldNamespace.print()"
        for varN,fldN in self.varN2fldN.items():
            if self[varN] is not None:
                print(funcn, varN, fldN, self[varN].shape, file=file)
            else:
                pass
                #print(funcn, varN, fldN, None, file=file)

    def varname2fldname(self, varN):
        funcn = 'varname2fldname'
        #print(f"DEBUG_3258: {funcn}({varN})")
        if varN in self.varN2fldN:
            fldN = self.varN2fldN[varN]
        elif len(varN.split(r'_')) >= 5: #already full fld name
            fldN = varN
        elif varN.find('Resp_')<0 and varN.find('Pred_')<0:
            if varN.find("00_1d")>=0: #assume TOP1800,TOP1200,CN300/500/800
                fldN = f"Pred_univ_{varN}_1"
            else:
                fldN = f"Pred_lnret_{varN}_1"
        elif varN.find('_lnret')<0:
            if varN.find('Resp')>=0:
                fldN = (f"{varN.replace('Resp', 'Resp_lnret')}_1")
            else:
                if len(varN.split("_"))<5:
                    fldN = (f"{varN.replace('Pred', 'Pred_lnret')}_1")
                else:
                    fldN = varN
        else:
            fldN = varN

        if False:
            print(f"INFO: {funcn} {varN} => {fldN}")

        #Hack fldnm remap
        if fldN == "Pred_lnret_StFlag_1d_1":
            fldN = "Pred_mysql_StFlag_1d_1"
        elif fldN == "Pred_Rqraw_RqAMOUNT_1d_1":
            fldN = "Pred_Rqraw_RqTotalTurnover_1d_1"
        elif fldN == "Pred_Rqraw_RqCAPITAL_1d_1":
            fldN = "Pred_Rqraw_RqMarketCap_1d_1"
        
        return fldN

    def init(self):
        self.cfgFn = fn_fgen_flds_cfg('CS')
        if self.scn.ss.name.find('CF_')>=0:
            self.cfgFn = fn_fgen_flds_cfg('CF')

        self.varN2fldN = QpsUtil.getVarN2FldN(fn = self.cfgFn)
        self.fldN2varN = QpsUtil.getVarN2FldN(fn = self.cfgFn, reverse_lookup=True)

    def fld2var(self, fldN):
        if fldN in self.varN2fldN: #already var name
            return fldN
        else:
            return self.fldN2varN[fldN]

    @deprecated
    def T(self, pld):
        return(pld)
        # newPld = deepcopy(pld)
        # newPld.fillna(method='bfill', inplace=True)
        # return(newPld)

    #@timeit
    def get_pre_calculated(self, scn, fldNm, mkt=None, sft=0, debug=True, toplevel=True): 
        funcn = "FldNamespace.get_pre_calculated"
        if ctx_debug(1):
            print(f"DEBUG_6412: {funcn} symset= {scn.symset}, scn={scn.dts_cfg}, respNm={self.respNm}, fldNm={fldNm}")
        global PRECALCULATED_DB
        fldNmScn = f"{fldNm}.{scn.dts_cfg}" #support multiple scn

        already_aligned = False
        cachefn_found={"fn": "NA"}
        if fldNm in PRECALCULATED_DB:
            already_aligned = True
            pldX = PRECALCULATED_DB[fldNmScn]
        else:
            pldX = load_fld_new(scn.opt, scn, fldNm, debug=False, align_to="NA" if self.respNm is None else self.respNm, cachefn_found=cachefn_found)
            if cachefn_found['fn'].find("db.Resp")>=0:
                already_aligned = True

        if pldX is None:
            return pldX
        else:
            print(funcn, type(pldX), pldX.shape)

        if len(pldX.index)<=0:
            print(f"ERROR: no data for {fldX.fp()}", file=sys.stderr)
            assert(False), funcn

        if toplevel: #support multiple scn merge, with latest scn as toplevel=True
            if self.respNm is None:
                self.respNm  = fldNm
                self.respFld = pldX
    
            if self.respNm != "" and fldNm != self.respNm: # and fldNm != self.varname2fldname(self.respNm):
                if ctx_verbose(1):
                    print(f"INFO: {funcn} ref= {self.respNm}, input= {fldNm}", file=sys.stderr)
                #pldX = align(self.raw[self.respNm], pldX, fldType=self.fldType(fldNm))

                if not already_aligned:
                    pldX = align(self.respFld, pldX, fldType=fldType(fldNm))

            if ctx_debug(1):
                print_df(pldX, title=f"DEBUG_3229: {funcn} fldNm= {fldNm},  pldX shape= {pldX.shape}")

        PRECALCULATED_DB[fldNmScn] = pldX
        #print("="*100, already_aligned, cachefn_found['fn'], fldNm, self.respNm)
        if already_aligned == False and cachefn_found['fn'].find(".db")>=0 and fldNm != self.respNm:
            smart_dump(pldX, f"{cachefn_found['fn']}.{self.respNm}", title=funcn, debug=0, verbose=0)
        
        return(pldX)

    def eval_statement(self, scn, nsByVar, fldExpr, mkt, sft=0):
        assert fldExpr.find('=')>=0, f"ERROR: invalid expression {fldExpr}"
        
        (varN, expr) = fldExpr.split(r'=')

        if expr.find('$') >= 0:
            formula = compile_expr(expr)
            if not scn.opt.quiet:
                print(f"INFO: {varN} = eval_statement({expr}), formula= {formula}")

            #varN = f"{varN}.{QpsUtil.buf2md5(formula)[-4:]}"
            #open(f'{self.fldDefDir}/{varN}.def', 'w').write(expr)
            self.raw[varN] = eval(formula)
            if ctx_debug(1):
                print_df(self.raw[varN], title=f'DEBUG_4591: raw[{varN}]')
            # if False:
            #     if fldnm2bar(self.respNm) != fldnm2bar(varN):
            #         #print(f'INFO: net reindexing {varN}')
            #         self.net[varN] = self.T(self.raw[varN])
            return (varN, self[varN])
        else:
            assert False, f"ERROR: unsupported expression"
                
    def get_fld(self, scn, nsByVar, fldExpr, mkt=None, debug=False):
        funcn = "FldNamespace.get_fld"
        #A fldExpr can be 1) varN like C2C5m; 2) fldNm like Resp_lnret_O2C_5m_1; 3) statement like C2C5m.mmm=C2C5m*2;

        if scn.opt.debug:
            print(f"DEBUG: {funcn}", fldExpr)

        if mkt is None:
            mkt = scn.symset

        if fldExpr.find('=')>=0:
            (varN, varD) = self.eval_statement(scn, nsByVar, fldExpr, mkt)
            return(varD)

        elif fldExpr.find("Resp_")>=0:
            fldN = fldExpr
            #return self.get_resp(scn, fldN, mkt)
            return self.get_pre_calculated(scn, fldN, mkt, debug=debug)

        elif fldExpr.find("Pred_")>=0:
            fldN = fldExpr
            #return self.get_pred(scn, fldN, mkt)
            return self.get_pre_calculated(scn, fldN, mkt, debug=debug)

        else: 
            varN = fldExpr
            if False or scn.opt.debug:
                print(f"DEBUG: get_fld({fldExpr}) {varN} {self.varname2fldname(varN)}")
            varD = self.get_pre_calculated(scn, self.varname2fldname(varN), mkt, debug=debug)

            return (varN, varD)
            #return (varN, ns[varN])

def __regtest(regtest):
    (opt, args) = get_options_sgen(lambda x: print("NA"))
    set_cmdline_opt(opt)
    if int(regtest) == 0:
        scn = Scn(opt, 'C20', "F", "SN_CS_DAY", asofdate=opt.asofdate)
    elif regtest == 1:
        scn = Scn(opt, 'C20', "prod1w", "SN_CS_DAY", asofdate=opt.asofdate)
    elif regtest == 2:
        scn = Scn(opt, 'C20', "G", "SN_CS_DAY", asofdate=opt.asofdate)
    print(scn)
    opt.symset = 'C20'
    
    refFlds = {}
    inFlds = {}
    for fldNm in [f"Resp_lnret_{x}_1d_1" for x in getRespFldnms()]:
        print("FldNamespace.main()", fldNm, file=sys.stderr)
        refFlds[fldNm] = load_fld_new(opt, scn, fldNm, debug=False, use_local=(int(regtest)!=1))

    for fldNm in ["Pred_OHLCV_C_1d_1", "Pred_OHLCV_O_1d_1", "Pred_lnret_C2C_1d_1"]:
        inFlds[fldNm] = load_fld_new(opt, scn, fldNm, debug=False, use_local=(int(regtest)!=1))

    from itertools import product
    for (refNm,inNm) in product(refFlds.keys(), inFlds.keys()):
    #for (refNm,inNm) in [("Resp_lnret_O2C_1d_1", "Pred_OHLCV_C_1d_1")]:
        print("="*30, f"{inNm}->{refNm}", "="*30)        
        print(f"Align: refFld {refNm}\n", refFlds[refNm].tail().iloc[:,0])
        print(f"Align: inFld {inNm}\n", inFlds[inNm].tail().iloc[:,0])
        print(f"Align: outFld {inNm}->{refNm}\n", align(refFlds[refNm], inFlds[inNm]).tail().iloc[:,0])

def get_regtest_fldfn(globstr):
    funcn = "get_regtest_fldfn"
    fn = glob.glob(globstr)[-1]
    if ctx_debug(1):
        print(f"DEBUG_4511: {funcn} {fn}", file=sys.stderr)
    return fn

# def __regtest_align(refnm=get_regtest_fldfn(f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_*/1d/Resp_lnret_C2C_1d_1/C20.db"),
#     inpnm=get_regtest_fldfn(f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_*/1d/Pred_OHLCV_V_1d_1/C20.db"),
#     debug=True):
def __regtest_align(refnm=f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20221118/1d/Resp_lnret_C2C_1d_1/C38.db",
    inpnm=f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20221118/1d/Pred_OHLCV_V_1d_1/C20.db",
    debug=True):
    ref = smart_load(refnm)
    print_df(ref)
    inp = smart_load(inpnm)
    print_df(inp)
    pldX = align(ref, inp, fldType=fldType(inpnm), debug=debug)
    print_df(pldX)

# def __regtest_align_01(refnm=get_regtest_fldfn(f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_*/1d/Resp_lnret_C2C_1d_1/C38.db"),
#     inpnm=get_regtest_fldfn(f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_*/1d/Pred_Rqraw_RqTotalTurnover_1d_1/C38.db"),
#     debug=True):
def __regtest_align_01(refnm=f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20221118/1d/Resp_lnret_C2C_1d_1/C38.db",
    inpnm=f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/T_20210810_20221118/1d/Pred_Rqraw_RqTotalTurnover_1d_1/C38.db",
    debug=True):
    __regtest_align(refnm, inpnm, debug)

if __name__ == '__main__':
    funcn = "common_smart_load.main"
    opt, args = get_options(funcn)
    set_cmdline_opt(opt)
    print_opt(opt)
    
    expr = sys.argv[1]
    if funcn:
        print(eval(expr))

