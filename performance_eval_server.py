#!/usr/bin/env python
import os,sys
from ast import parse
import sys

import os
from collections import defaultdict
import traceback
import re
import time
from itertools import product
import traceback
from QpsNotebookShared import *
from run_study import *
from shared_func import *
from copy import deepcopy

#from qdb_options import get_options_sgen
from common_options_helper import get_options
import QpsUtil
from formula_builder import *
from common_basic import *
from CTX import *
from common_yaml import yaml_dump
from cmdline_opt import *
from PostgreSqlTools import postgres_instance
from common_symsets import cnstk_indus
from FdfHelper import *
from qpstimeit import *
from ExprHelper import *

def flat_pkl_data(params):
    #unstack_keys = [x for x in params.keys() if x.find("pkl")>=0 and (x.find("factor_returns.")>=0 or x.find("factor_net_returns.")>=0)]
    unstack_keys = [x for x in params.keys() if x.find("pkl")>=0]
    for usk in unstack_keys:
        usk_prefix = usk.replace('_', '.').split(".")[0]
        pld = pickle.loads(params[usk])
        for k,v in pld.items():
            usk_new = f"{usk_prefix}.{k}"
            params[usk_new] = v
        del params[usk]
    return params

def flat_dict_data(params):
    ### second stage flatten all dictionary
    keys_to_remove = []
    updates = {}
    for k,v in params.items():
        if type(v) == type(dict()):
            for x,y in v.items():
                try:
                    x = x.replace(" ", "_")
                except:
                    pass
                k_new = f"{k}.{x}"
                updates[k_new] = y
            keys_to_remove.append(k)
    for k in keys_to_remove:
        del params[k]
    params.update(updates)

def prefix_all_keys(params, prefix="opt."):
    newparams = {}
    for k,v in params.items():
        newparams[f"{prefix}{k}"] = v
    return newparams

def flatten_params(params):
    flat_pkl_data(params)
    flat_dict_data(params)
    flat_dict_data(params)

def print_out_mixed_type_dict(params, cmdline_only=True):
    for k in params.keys(): #sorted(params.keys()):
        v = params[k]
        assert type(k) == type(str()), f"ERROR: invalid params key type(A): {k}:{type(k)}"


        typed_output = type(v)
        if typed_output in [type(""), type(True), type(0), type(1.0), type(None)]:
            typed_output = v
        elif str(typed_output).find("Timestamp")>=0:
            typed_output = v
        elif str(typed_output).find("bytes")>=0:
            if k.find("pkl")>=0:
                pld = pickle.loads(v)
                if type(pld) == type(dict()):
                    typed_output = f"pkl(dict:{pld.keys()})"
                else:
                    typed_output = f"bytes({type(pld)})"
            else:
                typed_output = f"bytes({len(v)})"
        elif str(typed_output).find("DataFrame")>=0:
            typed_output = f"DataFrame[{v.shape}]"
            print(BLUE, v.tail(5).iloc[:, :10], NC)
        elif str(typed_output).find("list")>=0:
             typed_output = f"list({len(v)})"
        elif str(typed_output).find("Series")>=0:
             typed_output = f"Series[{v.shape}]"
        elif str(typed_output).find("float")>=0:
             typed_output = float(v)

        if k.find(".pkl")>=0:
            typed_output = "pkl(...)"

        if cmdline_only:
            if k.find(".pkl")>=0:
                continue
            if k.find("alpha")>=0:
                continue

        print(f'\t{k:<60}: {typed_output}')
    return params

def get_factor_data(params, postgres): #factor that needs evaluation
    if "factor_data" in params:
        if params['factor_data'] is not None:
            return params['factor_data']

    (ex, freq, scn) = params['cfg'].split('_')
    symset = params['symset']

    factor_value_table =  params["factor_value_table"] if "factor_value_table" in params else "filter_fgprod_for_detailed_analysis"

    if scn in ['F']:
        (column_types, factor_values) = postgres.query_data(f"select factor_value from expr_eval_by_corr where id = 1752333253365") #F 
    if scn in ['G']:
        (column_types, factor_values) = postgres.query_data(f"select factor_value from expr_eval_by_corr where id = 1752333394935") #G 
    if scn in ['prod', 'W']:
        (column_types, factor_values) = postgres.query_data(f"select factor_value from expr_eval_by_corr where id = 1752333519325") #prod
    if scn in ['A']:
        (column_types, factor_values) = \
            postgres.query_data(f"select factor_value from {factor_value_table} where exprnm = '{params['exprnm']}' and symset = '{symset}' order by rec_tm desc limit 1") #A

    factor_data = factor_values[0]['factor_value']

    #print(type(factor_data))
    #params['factor_data'] = smart_load("/Fairedge_dev/app_qpsrpyc/test_predictor_dump.db")
    return factor_data

def get_detailed_analysis_factor_info(params, postgress):
    factor_value_table =  params["factor_value_table"] if "factor_value_table" in params else "filter_fgprod_for_detailed_analysis"

    (column_types, factor_values) = \
        postgres.query_data(f"select exprnm from {factor_value_table} where symset = '{params['symset']}'") #A
    return [x['exprnm'] for x in factor_values]

@timeit
def get_performance_report(params, alpha_perf, postgres=None):
    #params['factor_data'] = get_factor_data(opt, postgres)
    pfd = alpha_perf.GetPerformanceData()

    fctRets = alpha_perf.factor_return().GetFactorReturns()
    factorReturnsRaw = fctRets['factor_returns']
    factorReturnsNet = fctRets['factor_net_returns']
    cxAll = {}
    summaryAll = {}
    for an,ran in {'LS': 'LS', 'QT': 'QT', "QTma": "QT_ma", "Q3h": "QT_HS300", "Q5h": "QT_ZZ500"}.items():              
        d = {}
        origAlphaName = f"alpha_{ran}"
        if origAlphaName not in pfd['performance']:
            continue
        d['raw'] = pfd['performance'][origAlphaName][2]
        d['net'] = pfd['net_performance'][origAlphaName][2]
        for k,v in d.items():
            v['3000'] = v.pop('all')
            v.pop('is')
            v.pop('os')
            for yr,dta in v.items():
                yr = int(yr)
                if yr not in summaryAll:
                    summaryAll[yr] = {}
                s = summaryAll[yr]
                s[f'alpha{an}_{k}_AnnRet'] = dta['Annualized Return']
                s[f'alpha{an}_{k}_AnnVol'] = dta['Annualized Volatility']
                s[f'alpha{an}_{k}_IR'] = dta['Annualized IR']
                s[f'alpha{an}_{k}_MaxDD'] = dta['Max Drawdown']
                s[f'alpha{an}_{k}_RcyDays'] = dta['Max Recovery Periods']
                s[f'alpha{an}_{k}_Turnover'] = dta['Turnover']
                #print(an, yr, s)

        if origAlphaName in factorReturnsRaw:
            cxAll[f'alpha{an}_raw'] = factorReturnsRaw[origAlphaName]
            cxAll[f'alpha{an}_net'] = factorReturnsNet[origAlphaName]
            n = 10
            print(f"INFO: past {n} days pnl for alpha{an}:")
            print(pd.DataFrame.from_dict({k:cxAll[k] for k in [f'alpha{an}_raw', f'alpha{an}_net']}, orient='columns').tail(n))
                

    summaryAll[3000]['fcTurnover'] = float(pfd['factor_turnover']['1'])
    cxAll.update({3000: pd.DataFrame.from_dict(cxAll, orient='columns')})

    summaryDf = pd.DataFrame.from_dict(summaryAll, orient='index')
    summaryDf.sort_index(inplace=True)
    if params['verbose']:
        print(summaryDf.T)
        if params['verbose'] >= 2:
            print(summaryDf[[x for x in summaryDf.columns if x.find("alphaQ5h")>=0]].T)

    cxFp = f"{params['result_dir']}/cx.daily.pkl"
    smart_dump(cxAll, cxFp, debug=True, title=funcn)

    summaryFp = f"{params['result_dir']}/cx.summary.pkl"
    smart_dump(summaryAll, summaryFp, debug=True, title=funcn)

    for fn in glob.glob(f"{params['result_dir']}/*"):
        if fn.endswith(".cmd"):
            continue
        elif fn.endswith((".pkl", ".png", ".pdf")):
            params[os.path.basename(fn)] = Path(fn).read_bytes()


    db_table = "performance_eval_details"
    if 1:
        params.update(summaryAll[3000])
        #collect database fields
        params_to_postgres = {}
        for k,v in params.items():
            if k.find(".pkl")>=0 or\
                type(v) == type(pd.DataFrame()) or\
                k.find("universe")>=0:
                continue
            params_to_postgres[k] = v

        if params['verbose']:
            print(f"{DBG}")
            print_out_mixed_type_dict(params_to_postgres)
            print(f"INFO: number_of_columns= {len(params.keys())}")
            print(f"{NC}")

        if postgres is not None:
            postgres.save_dict(f"{db_table}", params_to_postgres)

    summary = {k: summaryAll[3000][k] for k in ['alphaQT_raw_AnnRet', 'alphaQT_raw_IR', 'alphaQT_net_AnnRet', 'alphaQT_net_IR']} #summary 
    return (params, summary)

def update_performance_eval_params_with_run_info(params_tmpl, run_info):
    params = deepcopy(params_tmpl)
    params['exprnm'] = run_info['exprnm']
    params['cfg'] = run_info['cfg']
    params['symset'] = run_info['symset']
    params['alpha_name'] = run_info['exprnm']
    params['alpha_id'] = run_info['exprnm']
    params['alpha_type'] = 'technical'
    params['alpha_sub_type'] = 'rank'
    params['evaluation_model'] = run_info["evaluation_model"]
    params['return_file_name'] = 'NA'
    params['resp_return_file_name'] = 'NA'
    params['verbose'] = run_info['verbose']
    # params['summary_file'] = 'summary'
    # params['result_file'] = 'performance'
    if 'factor_value_table' in run_info:
        params['factor_value_table'] = run_info['factor_value_table']
        print(f"INFO: factor_value_table= {params['factor_value_table']}")
    if 'is_quantile_combined_data' in run_info:
        params['is_quantile_combined_data'] = run_info['is_quantile_combined_data']

    result_dir_root = use_cache_if_exists("/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_results", "/NASQPS08.qpsdata.cache/research/performance_eval_server/performance_eval_results")
    params['result_dir'] = f"{result_dir_root}/{params['symset']}/{params['alpha_name']}/{params['cfg']}/{params['evaluation_model']}"
    os.system(f"mkdir -p {params['result_dir']}")

    if 'factor_data' in run_info:
        params['factor_data'] = run_info['factor_data']
    if 'return_data' in run_info and run_info['return_data'] is not None:
        params['return_data'] = run_info['return_data']
        if False:
            print(BLUE, "DEBUG: return_data", params['return_data'].tail(5).iloc[:, :10], NC)
        

    if params['evaluation_model'] in ['cx']:
        for k in params.keys():
            if k.find("calcQT_") >=0:
                params[k] = False
        params['calcLS'] = False
    else: #evaluation_model='cx_all'
        pass

    return params

def gen_performance_analysis(params_tmpl, run_info, postgres=None):
    params = update_performance_eval_params_with_run_info(params_tmpl, run_info)
    params['factor_data'] = get_factor_data(params, postgres)  #must be set before get_data()

    if False:
        print("INFO: params ============================================================")
        print_out_mixed_type_dict(params)
    alpha_perf = AlphaPerformance(params=params, auto_calc=False)
    alpha_perf.get_data()

    (params, summary) = get_performance_report(params, alpha_perf, postgres=postgres)

    if params['verbose'] > 0:
        print_out_mixed_type_dict(params)

    summary.update({'cfg': params['cfg'],
        'symset': params['symset'],
        'exprnm': params['exprnm']})
    
    chg = {}
    for k in summary.keys():
        v = summary[k]
        if isinstance(v, float):
            chg[k] = float(f"{v:>7.4f}")
    summary.update(chg)

    print(f"{CYAN}INFO: summary= {summary}{NC}", file=sys.stdout)
    return (params, summary)

#@deprecated
def split_and_save_params(params, postgres = None):
    #flat all records
    print(f"{DBG}")
    flatten_params(params)
    print_out_mixed_type_dict(params)
    print(f"{NC}")
    #columns over 1600, need to split into two

    split_for_postgress = {}
    postgress_sections = "cx,perf,qt,ls,run,univ,beta,rcydays,maxdd"
    for x in postgress_sections.split(','):
        split_for_postgress[x] = {}

    for k,v in params.items():
        assert type(k) == type(str()), f"ERROR: invalid params key type(B): {k}:{type(k)}"
        #print(k)

        if k.find("RcyDays")>=0:
            split_for_postgress['rcydays'][k] = v
        elif k.find("MaxDD")>=0:
            split_for_postgress['maxdd'][k] = v
        elif k.find("LS")>=0:
            split_for_postgress['ls'][k] = v
        elif k.find("cx.")>=0:
            split_for_postgress['cx'][k] = v
        elif k.find("_beta")>=0:
            split_for_postgress['beta'][k] = v
        elif k.find("_univ")>=0:
            split_for_postgress['univ'][k] = v
        elif k.find("QT")>=0:
            split_for_postgress['qt'][k] = v
        elif k.find("performance.")>=0:
            split_for_postgress['perf'][k] = v
        else:
            split_for_postgress['run'][k] = v

    summary = {k:len(v.keys()) for k,v in split_for_postgress.items()}
    print(f"INFO: key-count all= {len(params.keys())}, {summary}")
    #print(split_for_postgress['cx'].keys())
    #exit(0)
    # cx = {params[k] for k,v in params.items() if k.find("performance.")<0}
    # perf = {params[k] for k,v in params.items() if k.find("performance.")>=0}

    save_to_postgress = postgres is not None
    if save_to_postgress and params['cfg'].find("_A")>=0: #only store A
        for x in postgress_sections.split(','):
            postgres.save_dict(f"{db_table}_{x}", split_for_postgress[x])
            print(f"table {db_table}_{x}: ", '\n\t'.join(split_for_postgress[x].keys()))
    if False:
        #(column_types, row) = postgres.query_data(f"select alphaQT_net_AnnRet, alphaQT_net_IR from {db_table}")
        (column_types, row) = postgres.query_data(f"select alpha_name from {db_table}")
        print(f"SQL query returned {type(d)} {len(d)} rows")

def print_params_universe_related(params):
    funcn = "print_params_universe_related"
    for k,v in params.items():
        if k.find("universe")>=0:
            outstr = '\n'.join([str(x.shape) if isinstance(x, pd.DataFrame) else str(x) for x in v])
            print(f"INFO: {funcn} k={k}; v= {outstr}")

def gen_params_for_symset(opt, symset, scn, postgres):
    #canned_params_fn = f"/qpsdata/config/performance_eval_server_params/{opt.cfg}.{opt.symset}.pkl"
    params = pickle.loads(Path("/qpsdata/config/cross_section_fast_model.args/params.pkl").read_bytes())
    params['symset'] = symset
    params['cfg'] = f"cs_1d_{scn}"

    canned_params_fn = use_cache_if_exists(f"/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_server_params/{params['cfg']}.{symset}.pkl")
    print(f"INFO: canned params file {canned_params_fn}")
    if os.path.exists(canned_params_fn) and not opt.force:
        return

    params['display_result'] = False
    (ex, freq, scn) = opt.cfg.split('_')
    for k,v in params.items():
        if type(v) == type(str()):
            params[k] = v.replace("I65", symset).replace("G.pkl", f"{scn}.pkl")
    params["index_return_file_name"] = get_indices_file(opt)
    params["resp_return_file_name"] = get_resp_file(opt)
    params['return_data'] = smart_load(get_resp_file(opt))
    params['universe_file_name'] = [FdfHelper(x).scn(scn).symset(symset).fn() for x in params['universe_file_name']]
    #params['result_dir'] = f"/qpsdata/config/performance_eval_results/{params['alpha_name']}/cx_fast/" 
    params['result_dir'] = f"/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_result/{params['alpha_name']}/cx_fast" #test
    params['factor_data'] = get_factor_data(params, postgres)  #must be set before get_data()
    #params = prefix_all_keys(params, prefix="opt.")
    #print_out_mixed_type_dict(params)
    # print(params['universe'])
    # print(params['universe_file_name'])
    alpha_perf = AlphaPerformance(params=params, auto_calc=False)
    alpha_perf.get_data()

    if opt.verbose:
        print_params_universe_related(params)

    smart_dump(params, canned_params_fn)

def batch_calculate(opt, symset, exprnm_list, factor_value_table="filter_fgprod_for_detailed_analysis", postgres=None, is_quantile_combined_data=False):
    opt.symset = symset
    if type(exprnm_list) == type(list()):
        factor_data = {k:None for k in exprnm_list}
    else: #must be a dict
        factor_data = exprnm_list
        exprnm_list = list(factor_data.keys())

    #print(f"DEBUG_3211: {type(exprnm_list)}, {type(factor_data)}")

    #canned_params_fn = f"/qpsdata/config/performance_eval_server_params/{opt.cfg}.{opt.symset}.pkl"
    canned_params_fn = use_cache_if_exists(f"/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_server_params/{opt.cfg}.{opt.symset}.pkl")
    assert os.path.exists(canned_params_fn), f"ERROR: cannot find params file {canned_params_fn}"

    params_tmpl = smart_load(canned_params_fn)

    summaries = []

    exprnm_count = len(exprnm_list)
    print(f"INFO: exprnm_list= {exprnm_count}, {exprnm_list[:5] if exprnm_count>5 else exprnm_list}, ...")

    i = 0
    for exprnm in exprnm_list[:]:
        i += 1
        result_dir_root = use_cache_if_exists("/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_results", "/NASQPS08.qpsdata.cache/research/performance_eval_server/performance_eval_results")
        sentinel_file = f"{result_dir_root}/{opt.symset}/{exprnm}/{opt.cfg}/cx/cx.summary.pkl"
        if (sentinel_file in get_status_file_dict() or os.path.exists(sentinel_file)) and not opt.force and opt.do not in ["regtest"]:
            if opt.verbose:
                print(f"INFO: sentinel file exists, skipping ... {sentinel_file}")
            continue

        print(f"INFO: processing {symset} {exprnm}, {i} out of {exprnm_count}")

        is_quantile_combined_data = True if factor_value_table in ["combine_factors"] else is_quantile_combined_data
        if opt.verbose:
            print_params_universe_related(params_tmpl)

        summary = None
        try:
            (params, summary) = gen_performance_analysis(
                params_tmpl, 
                {
                    "cfg": opt.cfg, 
                    "symset": symset, 
                    "exprnm": exprnm, 
                    "verbose": opt.verbose, 
                    "evaluation_model": opt.evaluation_model, 
                    "factor_value_table": factor_value_table,
                    "is_quantile_combined_data": is_quantile_combined_data,
                    "factor_data": factor_data[exprnm][0],
                    "return_data": factor_data[exprnm][1]
                    },
                postgres=postgres
                )
        except Exception as e:
            print (e)
        
        if summary is not None:
            summaries.append(summary)


    for x in summaries:
        print("Summaries recap:", x, file=sys.stderr)
    return summaries

if __name__ == '__main__':
    funcn = 'formula_search.main'
    # (opt, args) = set_opt_defaults_formula_search(*get_options_sgen(list_cmds, customize_options=customize_formula_builder_options))
    (opt, args) = get_options()
    print_opt(opt)
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")

    if False:    
        opt = pickle.loads(Path("/qpsdata/config/eval_predictor_with_with_transform.args/opt.pkl").read_bytes())
        rt = pickle.loads(Path("/qpsdata/config/eval_predictor_with_with_transform.args/rt.pkl").read_bytes())
        sty_params = pickle.loads(Path("/qpsdata/config/eval_predictor_with_with_transform.args/sty_params.pkl").read_bytes())
        isClassifier = pickle.loads(Path("/qpsdata/config/eval_predictor_with_with_transform.args/isClassifier.pkl").read_bytes())

        #rt['scn'] = 'F'
        #rt['scn'] = 'G'
        rt['scn'] = opt.cfg.split("_")[-1]

        rt['symset'] = opt.symset
        opt.facnm = 'DNA_00afdc'
        print(opt)
        print(rt)
        print(sty_params["PRED"])
        sty_params["PRED"] = sty_params["PRED"].replace("DNA_666a95", opt.facnm)

        eval_predictor_with_with_transform(opt, rt, sty_params, 'RAWFAC', is_binary_factor=isClassifier)

    if opt.do in ["gen_params"]:
        scn = opt.cfg.split('_')[-1]
        if opt.symset in ["", "all"]:
            symset_list = cnstk_indus(plus_all=True)[:]
        else:
            symset_list = [opt.symset]
        for symset in symset_list:
            opt.symset = symset
            gen_params_for_symset(opt, symset, scn, postgres)
    elif opt.do in ["show_params"]:
        canned_params_fn = use_cache_if_exists(f"/NASQPS08.qpsdata/research/performance_eval_server/performance_eval_server_params/{opt.cfg}.{opt.symset}.pkl")
        assert os.path.exists(canned_params_fn), f"ERROR: cannot find params file {canned_params_fn}"
        params_tmpl = smart_load(canned_params_fn)
        print_out_mixed_type_dict(params_tmpl)
    elif opt.do in ["gen_all_prev_strategy_factors"]:
        expr_list = gen_all_prev_strategy_factors()
        exprnm_by_symset = ExprGenerator(expr_list).exprnm_by_symset()
        for symset in sorted(exprnm_by_symset.keys()):
            print(f"{symset}({len(exprnm_by_symset[symset])}): {exprnm_by_symset[symset]}")
            batch_calculate(opt, symset, exprnm_by_symset[symset], factor_value_table = "combine_factors", postgres=postgres)
    elif opt.do in ["eval_combined_factors"]:
        (column_types, rows) = postgres.query_data(f"select symset, exprnm from combine_factors")
        d = {}
        for row in rows:
            d[f"{row['symset']}.{row['exprnm']}"] = 1

        for k in d.keys():
            [symset, exprnm] = k.split('.')

            if symset in ['C20']:
            #if False or symset in ['CS_ALL']:
                batch_calculate(opt, symset, [exprnm], postgres=postgres, factor_value_table="combine_factors")


    else:
        if opt.exprnm not in ["", "NA", "all"]:
            exprnm_list = opt.exprnm.split(";")
        else:
            exprnm_list  = get_detailed_analysis_factor_info({"symset": opt.symset}, postgres)
        batch_calculate(opt, opt.symset, exprnm_list, postgres=postgres)
 
