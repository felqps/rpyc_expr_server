#!/usr/bin/env python

from ast import operator
import sys


from FldNamespace import *
from QpsNotebookShared import *
import os
from collections import defaultdict
from qdb_options import get_options_sgen
import traceback
import re
from run_study import eval_predictor,get_runtime
from shared_func import *
from itertools import product
import QpsUtil
from EgenBase import EgenBase
from bgen_helpers import *
from register_script_functions import *
from CTX import *

#dict of global
FB_VARS = {}
VARS_USED = {}
FB_ARGS = None  #dict of valid func argument 
CANNED_PARAMS = {}

@timeit
def _formula_builder_func_args(opt):
    funcn = "_formula_builder_func_args"
    rc = {
            "close": "C_1d",
            "open": "O_1d",
            "high": "H_1d",
            "low": "L_1d",
            "volume": "V_1d",
            "Full_1d": "Full_1d",
            "univ": opt.univ,
            "unittest": "unittest",
            "normal": "normal",
            "all": "all",
            "apple": "apple",
            "bear": "bear",
            "macd": "macd",
            "C_1d": "C_1d",
            "O_1d": "O_1d",
            "H_1d": "H_1d",
            "L_1d": "L_1d",
            "V_1d": "V_1d",
            "uC_1d": "uC_1d",
            "uO_1d": "uO_1d",
            "uH_1d": "uH_1d",
            "uL_1d": "uL_1d",
            "uV_1d": "uV_1d",
        }

    for x in getRespFldnms():
        rc[f"Resp_{x}"] = f"Resp_lnret_{x}_1d_1"

    fn = f"{rootdata()}/config/formula_builder_func_args.txt"
    if ctx_verbose(1):
        print(f"INFO: {funcn} reading {fn}")
    if os.path.exists(fn):
        for ln in open(fn).readlines():
            (x,y) = ln.split()
            rc[x] = y
    else: #This step takes 2s
        fldInfoDf = QpsUtil.smart_load(f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db")
        fp = open(fn, "w")
        for nm in fldInfoDf['nm']:
            if type(nm) == type(str()):
                print(nm, nm, file=fp)
                rc[nm] = nm
    return rc

def formula_builder_func_args(opt):
    global FB_ARGS
    if FB_ARGS is None:
        FB_ARGS = _formula_builder_func_args(opt)
    return FB_ARGS

def canned_params():
    global CANNED_PARAMS
    for ln in QpsUtil.open_readlines("/Fairedge_dev/app_QpsData/canned_params.txt"):
        #examples: 'par0;1;range(1,5,3)', 'par1;"close";["open", "high", "low", "close"]'
        #print(ln)
        par = ln.split(";")[0]
        par = f"{par}'"
        CANNED_PARAMS[par] = ln
    return CANNED_PARAMS

#@timeit
def tokenize_expr(expr, debug=False):
    funcn = "tokenize_expr"
    token_list = []
    # macros = re.findall(r"(\w+|\{\w+\})", expr)
    # print("macros", macros)
    #tokens = re.findall(r"(\{\w+\}|\w+|\(|\)|\'|\+|\-|\*|\/|\,|\.|\>|\<|\=|\?|\|)", expr)
    tokens = re.findall(r"(\{\w+\}|\w+|\(|\)|\+|\-|\*|\/|\,|\.|\>|\<|\=|\?|\||\'\S*?\'|\')", expr)
    for idx,token in enumerate(tokens):
        type = None

        if token in EgenBase.script_funcs():
            type = "fun"
        elif token[0] == "{":
            type = "macro"  #macro will be expanded by params dict via the string format "{}" syntax
        elif re.findall(r"[A-Za-z_]+", token):
            # if idx == 0 or tokens[idx-1] not in ['"', "'"]:
            if token[0] not in ['"', "'"]:
                type = "var" #variable name
            else: # wrapped with quotes
                type = "arg" #argument to functions such F('close')
                if token.find("par")>=0:
                    type = "argpar"
                    if token in canned_params():
                        token = canned_params()[token] #some parXXXs are specified in canned_params file
        else:
            type = "nop"
        token_list.append((token, type))
    if debug:
        print(f"DEBUG_4522: {funcn}", token_list, file=sys.stderr)
    return token_list

#@timeit
def parse_formula_rq(expr, argpars, debug=False, opt=None):  
    global FB_VARS
    mapTo = []
    # if expr.find("=")<0:
    #     return(expr)

    tokens = tokenize_expr(expr)
    vars_used = []

    # print("len(tokens)", len(tokens))
    # print(time.time() - tstart)

    # tstart = time.time()
    for (token, type) in tokens:
        #print("DEBUG_2011:", type, token, print(time.time() - tstart))
        if type == "var":
            if token not in FB_VARS:
                FB_VARS[token] = f"${token}"
            if debug:
                print(f"token: {token:<32} -> {type} -> {FB_VARS[token]}", file=sys.stderr)
            mapTo.append(FB_VARS[token])
            vars_used.append(token)

        elif type == "macro":
            if token not in FB_VARS:
                FB_VARS[token] = f"{token}"
            if debug:
                print(f"token: {token:<32} -> {type} -> {FB_VARS[token]}", file=sys.stderr)
            mapTo.append(FB_VARS[token])

        elif type == "arg":
            token = token.strip("'")
            if token not in formula_builder_func_args(opt):
                if token.find('.')>=0:
                    FB_ARGS[token] = token
                else:
                    FB_ARGS[token] = f"{QpsUtil.fldnmRq2Qps(token)}"
                    if FB_ARGS[token].find("1d") < 0:
                        FB_ARGS[token] = FB_ARGS[token] + "_1d"
                    elif FB_ARGS[token].find("_1d") < 0:
                        FB_ARGS[token] = FB_ARGS[token].replace("1d", "_1d")
            if debug:
                print(f"DEBUG_1887: token: {token:<32} -> {type} -> {FB_ARGS[token]}", file=sys.stderr)
            mapTo.append(f"'{FB_ARGS[token]}'")

        elif type == "argpar":
            parNm = token.split(r';')[0][1:] #start at 1 because it's quoted
            if argpars is not None:
                argpars[parNm] = token.strip("'")
            mapTo.append("{%s}"%(parNm)) 

        elif type == "fun":
            if debug:
                print(f"token: {token:<32} -> {type} -> {EgenBase.script_funcs()[token]}", file=sys.stderr)
            if EgenBase.script_funcs()[token] == "NA":
                mapTo.append(token)
            else:
                mapTo.append(EgenBase.script_funcs()[token])

        elif type == "nop": #number or operator
            if debug:
                print(f"token: {token:<32} -> {type})", file=sys.stderr)
            mapTo.append(token)

    # print(time.time() - tstart)
    vars_used = list(set(vars_used))

    if debug or False:
        print(f"INFO: compile \"{expr}\" ----> \"{''.join(mapTo)}\", vars_used {vars_used})", file=sys.stderr)

    return(''.join(mapTo), vars_used)

#@timeit
def parse_formula(lines, var, expr, argpars, style = 'rq', is_pred=False, opt=None, debug=False):
    funcn = "parse_formula"
    global VARS_USED
    if style == 'rq':
        (mapTo, vars_used) = parse_formula_rq(expr, argpars, opt=opt)
        if is_pred:
            lines.append("{PRED}="+f"{mapTo}")
        elif var:
            lines.append(f"{var}={mapTo}")
            VARS_USED[var] = vars_used
            if debug:
                print(f"DEBUG: {funcn} VARS_USED: {var} => {vars_used}")
        else:
            lines.append(mapTo)
    else:
        assert False, f"ERROR: formula style {style} not supported"
    return vars_used

#@timeit
def gen_fldlist(linesIn):
    #         script = """
    # exprList.extend([
    # f"CLOSE=f('C_1d')",
    # f"HIGH=f('H_1d')",
    # ])
    # """

    linesOut = []
    linesOut.append("exprList.extend([")
    for ln in linesIn:
        linesOut.append(f'f"{ln}",')
    linesOut.append('])')
    linesOut.append("genFlds.update({f'{PRED}': '15:00:00'})")

    linesOut = '\n'.join(linesOut)
    #print(linesOut)
    return linesOut

def get_vars_used_recursive(vars_used, debug=True):
    global VARS_USED
    vars_used_recursive = []
    for var in vars_used:
        vars_used_recursive.append(var)
        if var in VARS_USED:
            vars_used_recursive.extend(get_vars_used_recursive(VARS_USED[var], debug=False))
    if ctx_verbose(1):
        print("vars_used_recursive", sorted(list(set(vars_used_recursive))))
    return vars_used_recursive

@timeit
def gen_script(opt, calcFacNm, formulaFn, argpars, debug=False):
    funcn = "formula_builder.gen_script"
    geppy_register_script_functions(pset=None, func_select='all', funcn="gen_script.register_script_functions")

    lines_vars = [] #These vars are shared by all formular in the file
    # def ClearVars():
    #     print('+'*300)
    #     global lines_vars
    #     lines_vars = []
    #vars_used = ['Resp_C2C']
    vars_used = [studyResp()]
    nm_refed = []
    linesIn = []

    for ln in QpsUtil.open_readlines(formulaFn):
        if re.findall(r"^#", ln):
            continue
        elif re.findall(r"^@once", ln):
            eval(ln.replace('@', ''))
            continue
        elif re.findall(r"^@include", ln):
            linesIn.extend(eval(ln.replace('@', '')))
            continue

        linesIn.append(ln)

    lines_formula = []
    for ln in linesIn:
        foundMatch = False
        isClassifier = False
        if ln.find("classifier")>=0:
            isClassifier = True

        #first split out the description part
        if ln.find('#')>=0:
            (ln, desc) = ln.split(r'#') #allow commments on-line
        else:
            desc = None

        #print("formular input:", ln)
        if ln.find("=")>=0:
            (facNm, facExpr) = ln.split(r' = ')
            # if False and facExpr.find("INDUSTRY_NEUTRALIZE")>=0:
            #     print(f"WARNING: skipping {facNm}, f{facExpr}")
            #     continue
            #print(f"INFO: FACTOR= {facNm}")
        else:
            facNm = None
            facExpr = ln 

        if facNm is None:
            print(rf"#\#\#\#DEPRECATED forumla format: {ln}")
            parse_formula(lines_vars, None, facExpr, None, style='rq', opt=opt) 
        else:       
            #if facNm.find(calcFacNm)>=0: #predictor line
            if facNm == calcFacNm: #predictor line
                #only the current calcFacNm add to argpars for param expanding
                vars_used.extend(parse_formula(lines_formula, facNm, facExpr, argpars, style='rq', is_pred=True, opt=opt)) #output factor uses MACRO FACTOR
                if desc and desc.find("ref=")>=0 and opt.mode == 'cmp': #For supporting #ref=<ref>
                    nm_refed.extend(re.findall(r"ref=([\-\w]+)", desc))
                foundMatch = True
            elif desc is not None and f"cmp={calcFacNm}" in desc.split(";"): #For supportting #cmp=<tgt>
                vars_used.extend(parse_formula(lines_formula, facNm, facExpr, None, style='rq', is_pred=False, opt=opt))
            else:
                isPred = False
                # for oneTimeName in getOneTimeNames():
                #     if facNm.find(oneTimeName)>=0:
                #         isPred = True
                if not isPred: #parse all formula lines as we might need to use them as vars in subsequent formulas
                    parse_formula(lines_vars, facNm, facExpr, None, style='rq', opt=opt)

        if not foundMatch:
            continue

        if ctx_debug(1) or ctx_verbose(1):
            print(f"\nINFO: {funcn} {calcFacNm} ...")

        
        vars_used_recursive = get_vars_used_recursive(vars_used)

        #The order of vars maintain the same as input, so input need to have correct dependencies in the right order
        lines_script = [ln for ln in lines_vars if ln.split(r'=')[0].strip() in vars_used_recursive]  
        for nm in nm_refed:
            if nm.find('-')>=0:
                neg = '-'
            else:
                neg = ''
            nm = nm.replace('-', '')
            #lines_script.append(f"{nm}={neg}$U*F('{nm2fldnm(nm)}')") #Donot use U as it might be unresolved if U is only used here
            lines_script.append(f"{nm}=F('{nm}')")
        lines_script.extend(lines_formula)
        linesOut = gen_fldlist(lines_script)

        if ctx_debug(1) or ctx_verbose(1):
            print(f"INFO: {funcn} linesOut:\n", "\n".join([f"{'>'*40} {x}" for x in linesOut.split("\n")]))

        return (facNm, linesOut, isClassifier)

    return (None, None, None)

def customize_formula_builder_options(parser): 
    parser.add_option("--show",
                    dest = "show",
                    type = "str",
                    help = "--show (default: %default)",
                    metavar = "--show",
                    default = 'none')

    parser.add_option("--force_step",
                    dest = "force_step",
                    type = "str",
                    help = "--force_step (default: %default)",
                    metavar = "--force_step",
                    default = 'fld,cx')

    parser.add_option("--eval",
                    dest = "eval",
                    type = "str",
                    help = "--eval (default: %default)",
                    metavar = "--eval",
                    default = 'cx_fast')

    parser.add_option("--export_tag",
                    dest = "export_tag",
                    type = "str",
                    help = "--export_tag (default: %default)",
                    metavar = "--export_tag",
                    default = 'NA')
                
    parser.add_option("--export_dir",
                    dest = "export_dir",
                    type = "str",
                    help = "--export_dir (default: %default)",
                    metavar = "--export_dir",
                    default = f'/NASQPS08.qpsdata/research/performance_eval_server/rkan5')    

    parser.add_option("--results_dir",
                    dest = "results_dir",
                    type = "str",
                    help = "--results_dir (default: %default)",
                    metavar = "--results_dir",
                    default = 'NA')                

    parser.add_option("--detailed",
                    dest = "detailed",
                    type = "int",
                    help = "--detailed (default: %default)",
                    metavar = "--detailed",
                    default = 0)

    parser.add_option("--use_fds",
                    dest = "use_fds",
                    type = "int",
                    help = "--use_fds (default: %default)",
                    metavar = "--use_fds",
                    default = 0)
    
    parser.add_option("--par_set_per_run",
                    dest = "par_set_per_run",
                    type = "int",
                    help = "--par_set_per_run (default: %default)",
                    metavar = "--par_set_per_run",
                    default = 3)

    parser.add_option("--argpar_use_default",
                    dest = "argpar_use_default",
                    type = "int",
                    help = "--argpar_use_default (default: %default)",
                    metavar = "--argpar_use_default",
                    default = 0)

    parser.add_option("--facnm",
                    dest = "facnm",
                    type = "str",
                    help = "--facnm (default: %default)",
                    metavar = "--facnm",
                    default = "NA")

    parser.add_option("--mode",
                    dest = "mode",
                    type = "str",
                    help = "--mode (default: %default)",
                    metavar = "--mode",
                    default = "cmp")

    parser.add_option("--parmd5",
                    dest = "parmd5",
                    type = "str",
                    help = "--parmd5 (default: %default)",
                    metavar = "--parmd5",
                    default = "NA")

    parser.add_option("--formula_file",
                    dest = "formula_file",
                    type = "str",
                    help = "--formula_file (default: %default)",
                    metavar = "--formula_file",
                    default = None)

    parser.add_option("--server_mode",
                    dest = "server_mode",
                    type = "str",
                    help = "--server_mode (default: %default)",
                    metavar = "--server_mode",
                    default = "")



__fmDta = None
def formulaGroup2metainfo():
    global __fmDta
    if __fmDta is None:
        __fmDta = {}
        __fmDta['alpha101'] = {"faccat0": 'PV', "faccat1": "NA"}
        __fmDta['tests'] = {"faccat0": 'PV', "faccat1": "NA"}
        __fmDta['Rqoverbought'] = {"faccat0": 'PV', "faccat1": "MeanRev"}
        __fmDta['RqWorldQuant'] = {"faccat0": 'PV', "faccat1": "NA"}
        __fmDta['Qpssuper'] = {"faccat0": 'Fmtl', "faccat1": "NA"}
        __fmDta['QpsFmtl'] = {"faccat0": 'Fmtl', "faccat1": "NA"}
        __fmDta['QpsSrch'] = {"faccat0": 'Fmtl', "faccat1": "NA"}
        __fmDta['Rqderived_cashflow'] = {"faccat0": 'Fmtl', "faccat1": "Cashflow"}
        __fmDta['Rqderived_finance'] = {"faccat0": 'Fmtl', "faccat1": "Finance"}
        __fmDta['Rqderived_growth'] = {"faccat0": 'Fmtl', "faccat1": "Growth"}
        __fmDta['Rqderived_valuation'] = {"faccat0": 'Fmtl', "faccat1": "Valuation"}
        __fmDta['Rqmomentum'] = {"faccat0": 'PV', "faccat1": "Momentum"}
        __fmDta['Rqmoving_averages'] = {"faccat0": 'PV', "faccat1": "Momentum"}
        __fmDta['Port'] = {"faccat0": 'PV', "faccat1": "NA"}    
        __fmDta['genetic'] = {"faccat0": 'PV', "faccat1": "NA"}
        __fmDta['egenflds'] = {"faccat0": 'PV', "faccat1": "NA"}
        for k in __fmDta.keys():
            __fmDta[k]['facgrp'] = k
    return __fmDta

@timeit
def get_formula_files(opt):
    funcn = "formula_builder.get_forumla_files"
    rc = []
    if opt.formula_file is not None:
        rc = [opt.formula_file]
    elif opt.study_name.find("formula_")>=0:
        for globStr in [f"/Fairedge_dev/proj_JVrc/{opt.resp}.{opt.symset}/formula_{x}*.99.txt" for x in formulaGroup2metainfo().keys() if x.find("_include")<0]:
            rc.extend(glob.glob(globStr))
    else:
        assert False, f"ERROR: must specify valid study name"
    if ctx_verbose(1):
        print(f"INFO: {funcn}:", "\n".join(rc))
    return rc

__facnm2meta = None
def facnm2metainfo(facnm):
    global __facnm2meta
    if __facnm2meta is None:
        __facnm2meta = {}
        grp2meta = formulaGroup2metainfo()
        for grp in grp2meta.keys():
            for ln in QpsUtil.open_readlines(f"/Fairedge_dev/proj_JVrc/formula_{grp}.99.txt"):
                if ln.find('=')>=0:
                    facnm = ln.split(r'=')[0].strip()
                    __facnm2meta[facnm] = grp2meta[grp]

    if facnm in __facnm2meta:
        return __facnm2meta[facnm]
    else:
        return {"faccat0": 'NA', "faccat1": "NA"}

OneTimeNameList = []
def once(names):
    global OneTimeNameList
    OneTimeNameList.extend(names.split(r','))
    #print("$$$", OneTimeNameList)

def include(fn):
    return QpsUtil.open_readlines(fn)

def getOneTimeNames():
    global OneTimeNameList
    return OneTimeNameList

#lines_vars = [] #These vars are shared by all formular in the file
def clear_vars():
    #print('+'*300)
    global lines_vars
    lines_vars = []

def make_params(paramsTmpl, facNm):
    funcn = 'make_params'
    paramsL = [{}]
    for k,vL in paramsTmpl.items():
        tmpL = []
        for params in paramsL:
            for v in vL.split(r','):
                params.update({k:v})
                tmpL.append(params.copy())
        paramsL = tmpL.copy()
    #print(paramsL)

    for params in paramsL:
        md5 = QpsUtil.params2md5(QpsUtil.kv2params(params))[-6:]
        params['TASK_NAME'] = md5
        params['PRED'] = make_fldnm(facNm, prefix=params['PREFIX'], md5=md5)

    return paramsL

def eval_predictor_with_with_transform(opt, rt, params, transform, is_binary_factor=False):
    funcn = "eval_predictor_with_with_transform"
    params['PREFIX'] = transform
    params['TASK_NAME'] = f"{transform}"
    mdlTasks = eval_predictor(opt, rt, params, opt.study_name, models=[opt.eval], detailed=opt.detailed, is_binary_factor=is_binary_factor)
    for (mdltask_dir, mdltask) in mdlTasks:
        if opt.export_tag != "" and opt.export_tag != "NA":
            mdltask.export(f"{opt.export_tag}.{transform}")
        else:
            if ctx_debug(1):
                print(f"DEBUG_3232: {funcn} skip exporting with tag {opt.export_tag}")
        for task in mdltask.egens().values():
            if 'cx' in opt.show.split(r','):
                cmd = f"python /Fairedge_dev/app_QpsData/run_jupyter_tmpl.py --jupyter_port {opt.jupyter_port} --tmpl cx {task.dta_dir()}/{opt.eval}"
                print(cmd)
                os.system(f"{cmd} > /dev/null")
            return smart_load(f"{task.dta_dir()}/{opt.eval}/cx.summary.pkl")
    return None

if __name__ == '__main__':
    funcn = 'formula_builder.main'
    (opt, args) = get_options_sgen(list_cmds, customize_options=customize_formula_builder_options)

    if opt.do == "NA":
        opt.do = "build"
    if opt.univ.find('_1d')<0:
        opt.univ = f"{opt.univ}_1d"

    rt = get_runtime(opt, opt.study_name, default="rq_factors_02")
    if ctx_verbose(1):
        print(f"INFO: rt= {rt}", file=sys.stderr)

    #global OneTimeNameList
    #for calcFacNm in calcFacNmList:
    for (calcFacNm, formulaFn) in list(itertools.product([opt.facnm], get_formula_files(opt))):
        print("INFO: doing", calcFacNm, formulaFn, file=sys.stderr)
        argpars = {}
        (facNm, script_lines, isClassifier) = gen_script(opt, calcFacNm, formulaFn, argpars)
        if opt.dryrun or facNm is None:
            continue

        #for params in make_params({'PREFIX': 'TEST', 'UNIV': "Full_1d,TOP1800_1d"}):
        for params in make_params({'PREFIX': 'BLDR', 'UNIV': opt.univ, 'STUDY_NAME': opt.study_name}, facNm):
            print(f"DEBUG: params {params}")
            #continue
            task = EgenStudy(opt, study_name = opt.study_name, symsets=opt.symset, scn=opt.scn, 
                lines=script_lines, run_name=facNm, params=params, models=[])
            task.run(force='fld' in opt.force_step.split(r',') and opt.force)
            task.save()
            if opt.export_tag != "":
                task.export(f"{opt.export_tag}")
            if opt.eval in ['q10', 'cx', 'cx_fast']:
                if False and isClassifier:
                    print(f"INFO: skipping eva_predictor {facNm}, classifier ...")
                    continue

                rt['force'] = 'cx' in opt.force_step.split(r',') and opt.force

                smry = eval_predictor_with_with_transform(opt, rt, params, 'RAWFAC', is_binary_factor=isClassifier)

                continue

                if smry and 3000 in smry:
                    ir = smry[3000]['alphaQT_raw_IR']
                    if (ir<-1.5):
                        eval_predictor_with_with_transform(opt, rt, params, 'CHGSGN', is_binary_factor=isClassifier)
                    
                if not isClassifier: #FLD does not apply to binary classifier
                    smry = eval_predictor_with_with_transform(opt, rt, params, 'ABSFLD')
                    if smry and 3000 in smry:
                        ir = smry[3000]['alphaQT_raw_IR']
                        if (ir<-1.5):
                            eval_predictor_with_with_transform(opt, rt, params, 'NEGFLD')   

        break 

              

    

