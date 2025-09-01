#!/usr/bin/env python

import sys

import sys
from optparse import OptionParser
import datetime

from common_colors import *
from common_ctx import *
#from Scn import ctx_scn
#from cmdline_opt import *

def fldnm2bar(fldNm):
    bar = '1d'
    if fldNm.find('5m')>=0:
        bar = '5m'
    elif fldNm.find('1m')>=0:
        bar = '1m'
    return(bar)

def ctx_scn(customize_options = None, warn=False):
    global CTX
    if 'scn' not in CTX:
        #global CUSTOMIZE_OPTIONS
        #print("WARNING: ctx_scn not specified, using 'opt' as fake scn", file=sys.stderr)

        if 'opt' in CTX:
            if warn:
                print(f"{WRN}WARNING: scn not initialized, using existing CTX['opt']= {CTX['opt']}{NC}", file=sys.stderr)
            opt = CTX['opt']
            #set_cmdline_opt(opt)
        else:
            if warn:
                print(f"{WRN}WARNING: scn not initialized, using default opt values{NC}", file=sys.stderr)
            #from qdb_options import get_options_sgen,get_options_defaults
            #(opt, args) = get_options_sgen(list_cmds=None, customize_options = get_customize_options())
            parser = OptionParser(description="GraphEngine")
            (opt, args) = parser.parse_args(args=[])
            opt.opt = opt
            #set_cmdline_opt(opt)

        from QpsUtil import gettoday
        if not hasattr(opt,"dcsnDt"):
            opt.dcsnDt = gettoday()
        if not hasattr(opt,"dts_cfg"):
            opt.dts_cfg = 'W'
        if not hasattr(opt,"qdb_cfg"):
            opt.qdb_cfg = 'all'
        if not hasattr(opt,"asofdate"):
            opt.asofdate = 'uptodate'
        if not hasattr(opt,"symset"):
            opt.symset = 'CS_ALL'
        if not hasattr(opt,"snn"):
            opt.snn = 'SN_CS_DAY'
        if not hasattr(opt,"verbose"):
            opt.verbose = 0
        if not hasattr(opt,"debug"):
            opt.debug = 0
        if not hasattr(opt,"fld_cache"):
            opt.fld_cache = 0
            #CTX['scn'] = opt

        from Scn import Scn
        CTX['scn'] = Scn(opt, opt.symset, opt.dts_cfg, opt.snn)
    
    return CTX['scn']

def ctx_dts_cfg():
    return ctx_scn().dts_cfg

def ctx_dts_FGW():
    return ctx_dts_cfg().replace('prod1w', 'W')

def ctx_begdt():
    return ctx_scn().begDt

def ctx_enddt():
    return ctx_scn().endDt

def ctx_trddt():
    return ctx_scn().trdDt

def ctx_prevdt():
    return ctx_scn().prevDt

def ctx_pre2dt():
    return ctx_scn().pre2Dt

def ctx_dcsndt():
    return ctx_scn().dcsnDt

def ctx_rundt():
    return ctx_scn().runDt

def ctx_nextdt():
    return ctx_scn().nextDt

def ctx_snn():
    return ctx_scn().snn
    
def ctx_last_datadt():
    if ctx_dts_cfg() in ['T']:
        return ctx_trddt()
    else:
        return ctx_dcsndt()

def ctx_cutoffdt():
    if ctx_dts_cfg() in ["T"]:
        return ctx_nextdt()
    else:
        return ctx_trddt()

def ctx_dts_cfg_expanded():
    return ctx_scn().dts_cfg_expanded

def ctx_qdfroot():
    return ctx_scn().qdfRoot

def ctx_symset():
    return ctx_scn().symset

def ctx_opt():
    #return cmdline_opt()
    global CTX
    if 'scn' in CTX and hasattr(ctx_scn(), 'opt'):
        return ctx_scn().opt
    elif 'opt' in CTX:
        return CTX['opt']
    else:
        return None

def ctx_asofdate():
    return ctx_opt().asofdate

# def formula_search_ver():
#     return ctx_opt().formula_search_ver

# def formula_build_ver():
#     return ctx_opt().formula_build_ver

def select_predictors_ver():
    #return cmdline_opt().select_predictors_ver
    return ctx_opt().select_predictors_ver

def ctx_force():
    return ctx_opt().force

def ctx_debug(code):
    if ctx_opt() is None:
        return True
    elif ctx_opt().debug >= code%10:
        #print(f"DEBUG_{code}:", file=sys.stderr)
        return True
    return False

def ctx_verbose(code):
    if ctx_opt() is None:
        return True
    elif code<0 or ctx_opt().verbose >= code%10:
        return True
    return False

def ctx_use_fld_cache():
    if ctx_opt() is None or not hasattr(ctx_opt(), "fld_cache"):
        return False #default not use cache
    return ctx_opt().fld_cache

def ctx_cnstk_indices():
    return ['399303.XSHE', '399311.XSHE', '000905.XSHG', '000300.XSHG', '000904.XSHG', '000903.XSHG', '159629.XSHE', '000016.XSHG', '000906.XSHG', '399005.XSHE', '399006.XSHE']
