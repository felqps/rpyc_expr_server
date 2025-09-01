#!/usr/bin/env python

import os,sys

import re
from fdf_colors import *
from fdf_helpers import buf2md5
from options_helper import get_options
from qpstimeit import timeit
import glob

def cmp_var(x):
    import string
    return f"{string.ascii_uppercase[len(x)%26]}{x}"

def isolate_tokens_old(expr, vars):
    for var in vars:
        expr = expr.replace(var,f"__{var}__") #isolate var token so sequential replacement works
    return expr   

def isolate_tokens(expr, var2md5):
    for var in var2md5.keys():
        expr = expr.replace(var,f"__{var2md5[var]}__") #isolate var token so sequential replacement works
    return expr   

def deisolate_tokens(expr, var2md5):
    for var in var2md5.keys():
        expr = expr.replace(f"__{var}__",f"{var2md5[var]}") 
    return expr   

def print_zipped(zipped):
    for k,v in var2inputs:
        print(f"zipped: {k} {v}")

def instrumenting_expr(expr, input_names):
    vars = new_var_list_in(expr)
    var2inputs = dict(zip(vars, input_names[:len(vars)]))
    #print("DEBUG_9453:", vars, var2inputs, file=sys.stderr)

    encode = {x: buf2md5(x)[:8] for x in vars}
    decode = {}
    
    for x,v in var2inputs.items():
        decode[buf2md5(x)[:8]] = v

    #print(f"DEBUG_3454: {expr}, zip= {var2inputs}, encode= {encode}, decode= {decode}", file=sys.stderr)

    expr = isolate_tokens(expr, encode)
    expr = deisolate_tokens(expr, decode)
    # for var,input_name in var2inputs:
    #     expr = expr.replace(f"__{var}__",input_name)
    # print(expr)
    return expr

def dimention_validation(expr):
    return True

def old2new_var(x):
    ##disable
    # return x
    ##enable
    if x.find("Rq")>=0:
        x = "RQ"
    elif x.find("WQ")>=0:
        x = "WQ"
    elif x.find("Alpha")>=0:
        x = "ALPHA"
    elif x.find("PREDSRC")>=0:
        x = "ALPHA"
    elif x.find("Fmtl")>=0:
        x = "FMTL"
    elif x.find("Qpss")>=0:
        x = "QPSS"
    elif x.find("_")>=0:
        x = x.replace("_","")
    x = x.replace("21d","_TWTYONE_DAY")
    x = x.replace("11d","_ELEVEN_DAY")
    x = x.replace("1d","_ONE_DAY")
    x = x.replace("2C","_TO_C")
    return x.upper()

def fldnm_fdf2fld(x):
    if x.find("RqWQ")>=0:
        #return x.replace('Rq','')
        return f"Pred_Rqraw_{x.replace('Rq','')}_1d_1"
    elif x in ['FmtlSbp']:
        return f"Pred_mysql_sbp_1d_1"
    elif x in ['FmtlEp']:
        return f"Pred_SRCH8f7ffc_FmtlEp_1d_1"  
    elif x in ['Sr2_1d', 'Qpssr2']:
        return f"Pred_mysql_sr2_1d_1"
    else:
        return x

def extract_argnm(e):
    return old2new_var(e.group(0).split("'")[1])

class ExpressionFldStyle:
    varRe = re.compile(r'[A-Z][A-Za-z0-9_]+')
    funcRe = re.compile(r'([A-Za-z_][A-Za-z_0-9]*)\(')

    newVarRe = re.compile(r'([A-Z][a-zA-Z_0-9]*)')
    newArgvRe = re.compile(r'([A-Z][a-zA-Z_0-9]*|[0-9]+)')
    newFuncRe = re.compile(r'([a-z][a-zA-Z_0-9]*)')

    def __init__(self, expr):
        self._expr = expr
        
    def func_list_in(self):
        return [x.strip() for x in sorted(list(set(self.funcRe.findall(self._expr))), key=cmp_var, reverse=True)]

    def var_list_in(self):
        self._expr = self._expr.replace("sr2_1d", 'Pred_mysql_sr2_1d_1')
        expr = self._expr
        funcsL = self.func_list_in()
        for x in funcsL:
            expr = expr.replace(x, "")
        
        return sorted(list(set([x for x in self.varRe.findall(self._expr) if x not in funcsL])), key=cmp_var, reverse=True)

    def new_argv_list_in(self):
        return [x for x in sorted(self.newArgvRe.findall(self._expr), key=cmp_var, reverse=True) if x.find("CN")<0]

    def new_var_list_in(self):
        return [x for x in sorted(self.newVarRe.findall(self._expr), key=cmp_var, reverse=True) if x.find("CN")<0]

    def new_func_list_in(self):
        return sorted(self.newFuncRe.findall(self._expr), key=cmp_var, reverse=True)
    
    def safe_replace(self, expr, ins, outs):
        encodes = [buf2md5(x)[:8] for x in ins]
        for i in range(len(ins)):
            expr = expr.replace(ins[i], encodes[i])
        for i in range(len(encodes)):
            expr = expr.replace(encodes[i], outs[i])
        return expr
    
    def format_fldnm(self, x, symset, formulas):
        if x.find('Rq')>=0 or x.find('Fmtl')>=0 or x.find('Qps')>=0 or x.find('sr2_1d')>=0:
            return f"fld_fgw('{fldnm_fdf2fld(x)}',CLOSE)"
            #return f"fld_fgw('{x}',CLOSE)"
        elif x.find('Alpha')>=0:
            return formulas[f"{symset}:{x}"].strip()
        else:
            return x
    
    def fdf_style(self, opt, symset, formulas, recursive=True):
        expr = str(self._expr)
        #func lower case
        expr = self.safe_replace(
            expr,
            self.func_list_in(),
            [x.lower() for x in self.func_list_in()]
        )   

        if recursive:
            #FOO_neg => neg(FOO)
            neg_vars = [x for x in self.var_list_in() if x.find("_neg")>=0]
            expr = self.safe_replace(
                expr,
                neg_vars,
                [f"neg({x.replace('_neg','')})" for x in neg_vars]
            )

            vars_1d = [x for x in self.var_list_in() if x.find("_1d")>=0]
            expr = self.safe_replace(
                expr,
                vars_1d,
                [f"{x.replace('_1d','')}" for x in vars_1d]
            )
    
        if recursive: #only do once
            #Wrap fld fields with fld_fgw()
            new_vars = [x.replace('_neg','') for x in self.var_list_in()]
            expr = self.safe_replace(
                expr,
                new_vars,
                [f"{self.format_fldnm(x, symset, formulas)}" for x in new_vars]
            )

        if recursive:
            #U=Factor(Univ) is disabled
            expr = expr.replace("U*","")

        if recursive:
            #F('H_1d')=>HIGH
            predefined_vars = [f"F('{x}_1d')" for x in "OHLCV"]
            new_predefined_vars = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            expr = self.safe_replace(
                expr,
                predefined_vars,
                new_predefined_vars
            )
            predefined_vars = [f"f('{x}_1d')" for x in "OHLCV"]
            new_predefined_vars = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            expr = self.safe_replace(
                expr,
                predefined_vars,
                new_predefined_vars
            )
            predefined_vars = [f"F('{x}')" for x in "OHLCV"]
            new_predefined_vars = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            expr = self.safe_replace(
                expr,
                predefined_vars,
                new_predefined_vars
            )
            predefined_vars = [f"f('{x}')" for x in "OHLCV"]
            new_predefined_vars = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
            expr = self.safe_replace(
                expr,
                predefined_vars,
                new_predefined_vars
            )

        expr = re.sub(r"factor\((.*)\)", "fld_fgw(\\1,CLOSE)", expr)

        #because variable replace, may need to run again
        if recursive:
            enew = ExpressionFldStyle(expr)
            expr = enew.fdf_style(opt, symset, formulas, recursive=False)
        else:
            return expr

        if opt.forecast_eval not in ["NA","0"]:
            #print("forecast_eval=", opt.forecast_eval)
            if (expr.find("calc_prc_flag")>=0 or expr.find("calc_ststs")>=0):
                pass
            else:
                expr = f"eval_by_corr({expr.strip()},Resp1_1)"
        return f"cs_1d_{opt.domain}:{symset}:{expr}".strip()
    
    def tidy_up_translated_expr(self, debug=0):
        expr=self._expr

        expr = expr.replace(' ','')

        if expr.find("factor")>=0:
            if debug:
                print(f"{BROWN}<: {expr}{NC}", file=sys.stderr)
            factorRe = re.compile(r'factor\(\S+?\)')
            #expr =  factorRe.sub(r'FAC', expr)
            expr =  factorRe.sub(extract_argnm, expr)
            if debug:
                print(f"{BLUE}>: {expr}{NC}", file=sys.stderr)

        if True and expr.find("f(")>=0 and expr.find("if")<0 and expr.find("ref")<0:
            if debug:
                print(f"{BROWN}<: {expr}{NC}", file=sys.stderr)
            fRe = re.compile(r'f\(\S+?\)')
            #expr = fRe.sub(r'FAC', expr)
            expr = fRe.sub(extract_argnm, expr)
            if debug:
                print(f"{BLUE}>: {expr}{NC}", file=sys.stderr)

        if expr.find("U*")>=0:
            #print("<", expr, file=sys.stderr)
            factorRe = re.compile(r'U*\(\S+\)')
            #factorRe = re.compile(r'U*\(\S+\)')
            expr = factorRe.findall(expr)[0]
            #print(">", expr, file=sys.stderr)

        expr = expr.replace("(if(", "(iif(")
        expr = expr.replace(",if(", ",iif(")
        expr = expr.replace("decay_linear1(", "decay_linear5(")
        expr = expr.replace("RQInCirculation", "RQ")
        expr = expr.replace("RQInCirculation_neg", "RQ")

        if expr.find("*U")>=0:
            expr = expr.replace("*U", '')

        if True and expr[0] == '(' and expr[-1] == ')': #remove outer-most parentithes
            expr = expr[1:-1]

        # if expr[0] == '-': 
        #     expr = expr[1:-1]

        has_arithmetic = False
        for c in "+=*/":
            if expr.find(c)>=0:
                has_arithmetic = True


        if len(ExpressionFldStyle(expr).new_func_list_in()) <= 0 and not has_arithmetic: #not using any function
            return None

        return expr

    def pretty_expr(self, debug=0):
        funcL = self.new_func_list_in()
        varL = self.new_var_list_in()

        # This replacement will not work as some strings are substring of other strings
        # for x in funcL:
        #     expr = expr.replace(x, f"{BLUE}{x}{NC}")
        # for x in varL:
        #     expr = expr.replace(x, f"{BROWN}{x}{NC}")

        encodeForward = {x: buf2md5(x)[:8] for x in funcL}
        encodeForward.update({x: buf2md5(x)[:8] for x in varL})
        encodeBackward = {buf2md5(x)[:8]:f"{BLUE}{x}{NC}" for x in funcL}
        encodeBackward.update({buf2md5(x)[:8]:f"{BROWN}{x}{NC}" for x in varL})
        expr = self._expr
        expr = isolate_tokens(expr, encodeForward)
        expr = deisolate_tokens(expr, encodeBackward)

        if debug:
            print("DEBUG_0009:", f"{RED}{expr}{NC}", funcL, file=sys.stderr)

        return expr
    
def get_all_production_exprs(opt):
    symset_formulas = []
    for ln in open("/Fairedge_dev/app_QpsData/bgen_raw.20.formula_search_worker.bash", 'r').readlines():
        if ln.find("symset")<0:
            continue
        symset = ln[ln.find("--symset"):-1].split()[1]
        facnm_idx = ln[ln.find("--facnm"):-1].split()[1]
        symset_formulas.append((symset, facnm_idx))
    return symset_formulas
                               

def get_all_research_formulas(opt, args):
    formulas = {}
    for fn in args:
        if opt.debug:
            print(f"INFO: processing {fn}", file=sys.stderr)
        symset = fn.split(r'/')[-2].split(r'.')[-1]
        for ln in open(fn, 'r').readlines():
            if ln.find("=")<0:
                continue
            if ln[0] == "#":
                continue
            ln = ln.strip()
            if ln.find(" = ")<0 or False:
                print(f"ln= {ln}", file=sys.stderr)
            (formula, expr) = (ln.split(r'#')[0]).split(" = ")
            formulas[f"{symset}:{formula}"] = expr
    return formulas   

@timeit
def get_all_research_exprs(opt,args):
    exprL = [v for k,v in get_all_research_formulas(opt,args).items()]
    print(f"exprL.len={len(exprL)}", file=sys.stderr)
    return exprL

def analyze_exprs(opt, exprL, formulas):
    varL = []
    funcL = []
    exprFdfL = []
    for x in exprL:
        if x.find(':')>=0:
            symset,expr = x.split(r':')
        else:
            symset = 'CS_ALL'
            expr = x
        e = ExpressionFldStyle(expr)
        funcL.extend(e.func_list_in())
        varL.extend(e.var_list_in())
        exprFdfL.append(e.fdf_style(opt, symset, formulas))

    return {"vars":varL, "funcs": funcL, "exprs": exprL, "exprs_fdf": exprFdfL}

def show_expr_ana(opt, expr_ana, len_only=False, return_exprs_fdf=False):
    funcn = "show_expr_ana()"
    if opt.verbose:
        for x in "vars,funcs,exprs,exprs_fdf".split(r','):
            if len_only:
                print(f"{funcn} {x:<10}: {len(expr_ana[x])}", file=sys.stderr)
            else:
                print(f"{funcn} {x:<10}: {expr_ana[x]}", file=sys.stderr)
        print("\n", file=sys.stderr)

    for x in expr_ana['exprs_fdf']:
        if return_exprs_fdf:
            return x
        else:
            if opt.verbose:
                print(f"{funcn}: python /Fairedge_dev/app_qpsrpyc/rpyc_expr_client.py --verbose 1 \"{x}\" --expr_md5 {buf2md5(x.split(r':')[-1])[:8]}", file=sys.stdout)

@timeit
def do_regtest(opt):
    for x in [
        "ts_argmax60(ts_argmax20(ts_argmax10(ma4(add(OPNINT,ts_argmax5(ts_max2(ts_max9(RETURN))))))))", 
        "false2nan(calc_CN300(CLOSE))",
        "ma(CLOSE001RETURN,10)",
        "ma(HIGH001,20)",
        "-1*rank((ts_argmax(signedpower(if((OPEN<0),stddev(OPEN,10),VOLUME),2.),9)))-0.5",
        "-STFLAGONEDAY", 
        "zz(PREDSRCH8F7FFCALPHAMA5ONEDAY1)"
        ]:
        e = ExpressionFldStyle(x)
        tidyx = e.tidy_up_translated_expr()
        expr_ana = analyze_exprs(opt, [x])
        expr_ana.update({"exprs_fdf": [tidyx]})
        show_expr_ana(opt, expr_ana)

@timeit
def remove_redundancies(ana):
    varsL = ana['vars']
    varsLN = list(set(varsL))
    print(len(varsL), len(varsLN))
    print(varsLN)

    funcsL = ana['funcs']
    funcsLN = list(set(funcsL))
    print(len(funcsL), len(funcsLN))
    print(funcsLN)

if __name__ == "__main__":
    funcn = "formula_helpers.main"
    opt, args = get_options(funcn)

    if len(args) == 0:
        args = [re.compile(r".*")]
    else:
        args = [re.compile(x) for x in args]

    if opt.regtest == "1":
        do_regtest(opt)
        exit(0)

    formulas = get_all_research_formulas(opt, glob.glob("/Fairedge_dev/proj_JVrc/Resp_C2C.*/*.99.txt"))
    for symset,formula in get_all_production_exprs(opt):
        match = 0
        for matchRe in args:
            if matchRe.findall(formula):
                match = 1
                break
        if match <= 0:
            continue

        formula_id = f"{symset}:{formula}"
        if formula_id not in formulas:
            formulas[formula_id] = formula_id.split(r':')[-1]
        if opt.verbose:
            print(f"formula: {symset:<8} {formula:<18} {formulas[formula_id]}", file=sys.stderr)
        show_expr_ana(opt, analyze_exprs(opt, [f"{symset}:{formulas[formula_id]}"], formulas))

    #ana = analyze_exprs(collect_all_current_exprs(opt, args))
    exit(0)



    #print(ana, file=sys.stderr)
    show_expr_ana(ana, len_only=True)
    remove_redundancies(ana)
    exit(0)
    funcL = sorted(list(set(funcL)), key=cmp_var, reverse=True)
    varL = sorted(list(set(varL)), key=cmp_var, reverse=True)

    var2md5 = {x: buf2md5(x)[:8] for x in varL}
    md52var = {buf2md5(x)[:8]:old2new_var(x) for x in varL}
    func2md5 = {x: buf2md5(x)[:8] for x in funcL}
    md52func = {buf2md5(x)[:8]:x.lower() for x in funcL}


    encode = {x: buf2md5(x)[:8] for x in varL}
    encode.update({x: buf2md5(x)[:8] for x in funcL})

    decode = {buf2md5(x)[:8]:old2new_var(x) for x in varL}
    decode.update({buf2md5(x)[:8]:x.lower() for x in funcL})

    print("funcs:", '\nfuncs: '.join(funcL))
    print("vars_reduced:", '\nvars_reduced: '.join(sorted(list(set(varLReduced)), key=cmp_var, reverse=True)))

    old2new_translation = {}
    exprTranslatedL = []
    for expr in exprL[:]:
        #print("<", expr)
        if 1:
            expr = isolate_tokens(expr, var2md5)
            expr = isolate_tokens(expr, func2md5)
        else:
            expr = isolate_tokens(expr, encode) 

        if opt.debug:
            print(f"DEBUG_0001: {CYAN}{expr}{NC}", file=sys.stderr)
        #print(">", expr)

        if 1:
            expr = deisolate_tokens(expr, md52func) #reverse-back-with-translation, as the loopup was changed
            expr = deisolate_tokens(expr, md52var)
        else:
            expr = deisolate_tokens(expr, decode)


        if opt.debug:
            print(f"DEBUG_0005: {CYAN}{expr}{NC}", file=sys.stderr)
        expr = tidy_up_translated_expr(expr, debug=opt.debug)
        if expr:
            exprTranslatedL.append(expr)
        #print("\n")

    exprTranslatedL = sorted(list(set(exprTranslatedL)), reverse=False)


    singleFuncExprs = {}
    for x in exprTranslatedL:
        funcL = new_func_list_in(x)
        varL = new_var_list_in(x)
        if len(funcL)==1 and len(varL)==1: 
            #contain only one func
            if funcL[0] in singleFuncExprs:
                continue
            else:
                singleFuncExprs[funcL[0]] = 1

        print("expr:", x)
        if opt.debug:
            print(f"DEBUG_0008: {pretty_expr(x, opt.debug)}", file=sys.stderr)


    # print("="*200)
    # print("vars(all): ", '\n'.join(sorted(list(set(varL)))))

