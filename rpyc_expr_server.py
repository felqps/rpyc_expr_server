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
import socket

from options_helper import get_options,set_cmdline_opt
from FDF import *
from factor_evalulators import *
from fdf_logging import *
from df_helpers import *
from FdfExpr import *
from platform_helpers import *
from fdf_colors import *
from fdf_helpers import mkdir
from QpsSys import fileOlderThan,gethostname,gethostip
from QpsUtil import gettoday
from common_symsets import symsets_str_paper
from common_colors import *
from RpycExprService import RpycExprService
from common_logging import print_df,dump_df_to_csv
from ExprHelper import query_expr_for_exprnm
from performance_eval_server import batch_calculate
from factcomb import qtile_to_one
from PostgreSqlTools import postgres_instance
from QpsSys import running_remotely
import random

def rpyc_get_answer(opt, expr):
    myservice = RpycExprService(opt)
    myservice.jump_start(do=opt.regtest)

    print(f"{CYAN}INFO: {funcn} eval_expr {expr}{NC}")
    ans = myservice.exposed_get_answer({'expr': expr, 'force': 1})
    if ans is not None:
        ans = pickle.loads(ans)
        if 'payload' in ans:
            ans=ans['payload']
    return ans

def dtm2dtstr(dtm):
    return(str(dtm.date()).replace('-',''))

def add_qtiles(a, b):
    r = a.copy()
    for c in b.columns:
        if c in a.columns:
            r.loc[:,c] = r.loc[:,c] + b.loc[:,c]
    return r 

def calc_optimization_target(opt, db, db_fn, opt_k, base_symset, params_update, is_quantile_combined_data=True, force=False):
    if opt_k in db and not force:
        return db[opt_k]

    summaries = batch_calculate(opt, base_symset, params_update, is_quantile_combined_data)
    if len(summaries)>0:
        alphaQT_net_IR = summaries[0]['alphaQT_net_IR']
        print(opt_k, alphaQT_net_IR)
        db[opt_k] = alphaQT_net_IR
        pickle.dump(db, open(db_fn, "wb"))
        return alphaQT_net_IR

    return 0.0

if __name__ == "__main__":
    funcn = "rpyc_expr_server.main"
    opt, args = get_options(funcn)
    opt.symset2wkr = {}
    opt.hostip=gethostip()
    print_opt(opt)

    strat_cfgs = {}

    strat_cfgs['edge1001'] = {
        "I65:edge1001": 1.0, #The base null factor used to accumulate quantile scores
        "I65:a54f7b5d": 0.5,
        "I65:03e611f0": 0.5
    }

    strat_cfgs['edge1002'] = {
        "CS_ALL:edge1002": 1.0, #The base null factor used to accumulate quantile scores
        "CS_ALL:03e611f0": 0.5,
        "CS_ALL:26a7975b": 0.5
    }

    strat_qtile = {
        'edge1001': 0.050,
        'edge1002': 0.020
    }

    random_factor_selection_fn = "/Fairedge_dev/app_qpsrpyc/20250831_random_factor.selection.db"

    if opt.standalone in ['build_random_factor_selection']:
        qry = """
SELECT DISTINCT ON (symset, cfg, exprnm) * from (
SELECT cfg, symset, d.exprnm, "alphaQT_net_IR", e.expr
FROM performance_eval_details d, view_exprnm_expr_lookup e where d.exprnm = e.exprnm and  "evaluation_model" = 'cx' and "alphaQT_net_IR" > 0.75
ORDER BY "alphaQT_net_IR" DESC
)        
        """
        postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
        (column_types, rows) = postgres.query_data(qry)
        sample_size = 100
        random_sample = random.sample(rows, sample_size)
        start_k = f"cs_1d_A:CS_ALL:03e611f0"
        add_start_k = True
        for r in random_sample:
            k = f"{opt.cfg}:{r['symset']}:{r['exprnm']}"
            if k == start_k:
                add_start_k = False

        if add_start_k:
            for r in rows:
                k = f"{opt.cfg}:{r['symset']}:{r['exprnm']}"
                if k == start_k:
                    random_sample.append(r)
                    print(f"INFO: force_add {r}")

        for r in random_sample:
            print(r)
        print(f"INFO: dump {random_factor_selection_fn}")
        print(f"INFO: query returned {len(rows)} rows") 
        pickle.dump(random_sample, open(random_factor_selection_fn, "wb"))

    elif opt.standalone in ['display_random_factor_selection']:
        random_sample = pickle.load(open(random_factor_selection_fn, 'rb'))
        for x in random_sample:
            print(x['cfg'], x['symset'], x['exprnm'], x['alphaQT_net_IR'], x['expr'])
        print(f"INFO: totol rows {len(random_sample)}")

    elif opt.standalone in ['build_random_factor_data']:
        random_sample = pickle.load(open(random_factor_selection_fn, 'rb'))
        dta_fn = random_factor_selection_fn.replace('selection', 'data')
        if os.path.exists(dta_fn):
            dta = pickle.load(open(dta_fn, 'rb'))
        else:
            dta = {}
        cnt = 0
        total_cnt = len(random_sample)
        CodeTesting = running_remotely() and False
        existing_symsets = {r['symset']:1 for r in random_sample}

        for symset in existing_symsets.keys(): #process each symset together
            for r in random_sample:
                if symset != r['symset']: 
                    continue 
                if CodeTesting:
                    symset = 'C25' #testing remotely
                expr = f"{opt.cfg}:{symset}:{r['expr']}"
                k = f"{opt.cfg}:{symset}:{r['exprnm']}"
                cnt += 1
                if cnt <= total_cnt:
                    if k in dta.keys():
                        continue
                    
                    print(f"INFO: factor_data ({cnt}/{total_cnt}): {k}")
                    dta[k] = rpyc_get_answer(opt, expr)
                    print(BLUE, dta[k].tail(5).iloc[:, :10], NC)

        pickle.dump(dta, open(dta_fn, "wb"))
        print(f"INFO: created {dta_fn}")

    elif opt.standalone in ["build_random_factor_qtile"]:
        factor_data_fn = random_factor_selection_fn.replace('.selection', '.data')
        factor_data = pickle.load(open(factor_data_fn, 'rb'))
        factor_qtile = {}
        for k in factor_data.keys():
            factor_qtile[k] = qtile_to_one(factor_data[k], alphaQPct=0.10)
            print(f"INFO: factor_qtile k= {k}:")
            print(BLUE, factor_qtile[k].tail(5).iloc[:, :10], NC)
        dta_fn = random_factor_selection_fn.replace('.selection', '.qtile')
        pickle.dump(factor_qtile, open(dta_fn, "wb"))
        print(f"INFO: created {dta_fn}")

    elif opt.standalone in ["optimze_qtile_weights"]:
        start_k = f"cs_1d_A:CS_ALL:03e611f0"
        #start_k = f"cs_1d_A:CS_ALL:bf6f779e"
        #start_k = f"cs_1d_A:CS_ALL:dc2c9a6d"
        # start_k = "cs_1d_A:CS_ALL:8cc6e05a"
        # start_k = "cs_1d_A:I65:600db1ef"
        factor_qtile_fn = random_factor_selection_fn.replace('.selection', '.qtile')
        print(f"INFO: loading {factor_qtile_fn}")
        factor_qtile = pickle.load(open(factor_qtile_fn, 'rb'))
    
        # for k in factor_qtile.keys():
        #     factor_qtile[k].fillna(0, inplace=True)
        #     print(f"qtile k= {k}")

        assert start_k in factor_qtile, f"ERROR: cannot find start_k {start_k} in qtile data"
        factor_data = {}
        #base_symset = "CS_ALL"
        base_symset = start_k.split(':')[1]
        #factor_data["return"] = rpyc_get_answer(opt, f"{opt.cfg}:{base_symset}:RETURN")
        factor_data["return"] = rpyc_get_answer(opt, f"{opt.cfg}:{base_symset}:Resp1_1")
        
        factor_data["qtile"] = factor_qtile[start_k].copy()
        opt.force = 1 
    
        optimize_history = {}
        optimize_history_fn = "optimize_history.db"
        if os.path.exists(optimize_history_fn):
            optimize_history = pickle.load(open(optimize_history_fn, 'rb'))

        opt_k = f"{start_k}"
        cur_max = calc_optimization_target(opt, optimize_history, optimize_history_fn, opt_k, base_symset, {opt_k: [factor_data["qtile"], factor_data["return"]]}, is_quantile_combined_data=True, force=True)
        exit(0)

        for k in factor_qtile.keys():
            if k not in [start_k]:
                calc_optimization_target(opt, optimize_history, optimize_history_fn, k, base_symset, {k: [factor_qtile[k].copy(), factor_data["return"]]}, is_quantile_combined_data=True)

        for k in factor_qtile.keys():
        #for k in ["cs_1d_A:CS_ALL:bf6f779e"]:
            # if k.find("C25")>=0:
            #     continue
            # if k.find("I65")>=0:
            #     continue
            if k not in [start_k]:
                factor_data["qtile"] = add_qtiles(factor_qtile[start_k], factor_qtile[k])
                # print(f"DEBUG: shapes {factor_qtile[start_k].shape} + {factor_qtile[k].shape} = {factor_data['qtile'].shape}")
                # print(BLUE, factor_data["qtile"].tail(25).iloc[:, :10], NC)
                opt_k = f"{start_k},{k}"
                alphaQT_net_IR = calc_optimization_target(opt, optimize_history, optimize_history_fn, opt_k, base_symset, {opt_k: [factor_data["qtile"], factor_data["return"]]}, is_quantile_combined_data=True)

                if alphaQT_net_IR > cur_max:
                    print(f"{RED}INFO-----------------: cur_max improved by {alphaQT_net_IR - cur_max}{NC}")
                    cur_max = alphaQT_net_IR

        if False:
            for x in np.linspace(0.1,1,10):
                k = "cs_1d_A:CS_ALL:8cc6e05a"
                opt_k = f"{start_k},{k}*{x}"
                factor_data["qtile"] = add_qtiles(factor_qtile[start_k], factor_qtile[k]*x)
                alphaQT_net_IR = calc_optimization_target(opt, optimize_history, optimize_history_fn, opt_k, base_symset, {opt_k: [factor_data["qtile"], factor_data["return"]]}, is_quantile_combined_data=True)
                if alphaQT_net_IR > cur_max:
                    print(f"{RED}INFO-----------------: cur_max improved by {alphaQT_net_IR - cur_max}{NC}")
                    cur_max = alphaQT_net_IR

    elif opt.standalone in ['gen_tgt_pos']:
        factor_data = {opt.stratid: 0} #aggregate
        base_symset = 'CS_ALL'
        for k,v in strat_cfgs[opt.stratid].items():
            (symset, exprnm) = k.split(":")
            if exprnm in [opt.stratid]:
                expr = f"{opt.cfg}:{symset}:CLOSE"
                base_symset = symset
            else:
                expr = f"{opt.cfg}:{symset}:{query_expr_for_exprnm(exprnm)}"

            if k.find(opt.stratid)>=0:
                factor_data["dec_prc"] = rpyc_get_answer(opt, f"{opt.cfg}:{symset}:CLOSE")
                factor_data["return"] = rpyc_get_answer(opt, f"{opt.cfg}:{symset}:RETURN")
                factor_data["qtile"] = factor_data["dec_prc"] * 0.0
            else:
                factor_data["qtile"] += qtile_to_one(rpyc_get_answer(opt, expr), alphaQPct=strat_qtile[opt.stratid]) * v
            #factor_data["qtile"] += rpyc_get_answer(opt, expr) * v
       
        #print_df(factor_data["qtile"], title="opt.standalone")
        U = opt.dec_unit
        factor_data['tgt_sz'] = (factor_data["qtile"] / factor_data['dec_prc'] * U).round(decimals=-2)
        factor_data['tgt_amt'] =  factor_data['tgt_sz'] * factor_data['dec_prc']
        dump_df_to_csv(f"{opt.standalone}.qtile", factor_data["qtile"], fn = f"/tmp/debug_2056.xxx.csv")
        dump_df_to_csv(f"{opt.standalone}.dec_prc", factor_data['dec_prc'], fn = f"/tmp/debug_2056.xxx.csv")
        dump_df_to_csv(f"{opt.standalone}.tgt_sz", factor_data['tgt_sz'], fn = f"/tmp/debug_2056.xxx.csv")

        dec_dt = factor_data['tgt_sz'].index[-1]
        dec_dt_m1 = factor_data['tgt_sz'].index[-2] #dec_dt minus 1
        mkts = factor_data['tgt_sz'].columns

        postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")

        id_prefix = f"{opt.stratid}.{opt.dec_tag}.{str(dec_dt.date()).replace('-','')}"


        (column_types, rows) = postgres.query_data(f"select sym, tgt_sz, dec_prc from tgt_pos where stratid = '{opt.stratid}' and dec_tag = '{opt.dec_tag}' and dec_dt = '{dtm2dtstr(dec_dt_m1)}'")
        chk_dec_dt_m1 = {}
        for row in rows:
            chk_dec_dt_m1[row['sym']] = {
                "tgt_sz": row['tgt_sz'],
                "dec_prc": row['dec_prc'],
            }

        tgts = {}
        for dt in [dec_dt_m1, dec_dt]:
            tgts[dt] = []
            for mkt in mkts:
                tgt_sz = factor_data['tgt_sz'].loc[dec_dt,mkt]
                tgt = {}
                if tgt_sz != 0 and not np.isnan(tgt_sz) :
                    tgt['stratid'] = opt.stratid
                    tgt['dec_tag'] = opt.dec_tag
                    tgt['dec_dt'] = dtm2dtstr(dt)
                    tgt['sym'] = mkt
                    tgt['tgt_sz'] = int(tgt_sz)
                    tgt['dec_prc'] = float(factor_data['dec_prc'].loc[dt,mkt])
                    tgt['tgt_amt'] = float(factor_data['tgt_amt'].loc[dt,mkt])
                    tgt['id'] = f"{id_prefix}.{tgt['sym']}"
                    tgt['ord_gen_type'] = opt.dec_type
                    tgt['cfg'] = opt.cfg
                    tgt['dec_unit'] = opt.dec_unit
                    tgts[dt].append(tgt)


                    if dt == dec_dt_m1:
                        if (mkt in chk_dec_dt_m1) and (tgt['tgt_sz'] != chk_dec_dt_m1[mkt]['tgt_sz']):
                            tgtsubset = {k:tgt[k] for k in ['tgt_sz', 'dec_prc']}
                            print(f"PrevDecDiff: {tgtsubset} vs. {chk_dec_dt_m1[mkt]}")
            tgtsDf = pd.DataFrame(tgts[dt])
            tgtsDf['cnt'] = 1
            print(f"INFO: order summary({dtm2dtstr(dt)}):\n{tgtsDf['cnt,tgt_sz,tgt_amt'.split(',')].sum()}") 


        if not running_remotely():
            postgres.query_data(f"delete from tgt_pos where id like '%{id_prefix}%'", fetchall=False)
            for tgt in tgts[dec_dt]:
                print(tgt)
                postgres.save_dict("tgt_pos", tgt)

        if opt.cfg.find("_T")<0 or True:
            batch_calculate(opt, base_symset, {opt.stratid: [factor_data["qtile"], factor_data["return"]]}, is_quantile_combined_data=True)
            #batch_calculate(opt, base_symset, factor_data, is_quantile_combined_data=False)           
        exit(0) 

    elif opt.standalone in ['performance_eval_factors_in_strat']:
        for k in strat_cfgs[opt.stratid].keys():
            factor_data = {}
            (symset, exprnm) = k.split(":")
            expr = f"{opt.cfg}:{symset}:{query_expr_for_exprnm(exprnm)}"
            ans = rpyc_get_answer(opt, expr)
            print_df(ans, title='ans')
            factor_data[exprnm] = [ans,None]
            batch_calculate(opt, symset, factor_data)
                    
        exit(0)         

    elif opt.standalone in ['1', 'eval_expr']:
        print_opt(opt)
        workspaceFn = f"{opt.workspace_name}"
        if workspaceFn.find(".wk")<0:
            print("WARN: improper named workspace {workspaceFn}")

        myservice = RpycExprService(opt)
        myservice.jump_start(do=opt.regtest)

        for expr in args:
            print(f"{CYAN}INFO: {funcn} eval_expr {expr}{NC}")
            ans = myservice.exposed_get_answer({'expr': expr, 'force': 1})
            if ans is not None:
                ans = pickle.loads(ans)
                if 'payload' in ans:
                    ans=ans['payload']
                    if type(ans) == type(pd.DataFrame()):
                        print_df(ans, title='ans')
                
                smart_print(ans, title=funcn)

    elif opt.standalone in ['2', 'testing_alpha101_functions']: #testing alpha101 formula support
        myservice = RpycExprService(opt)
        myservice.jump_start(do=opt.regtest)
        counter = 0
        start = 263
        for expr in open_readlines("/Fairedge_dev/app_qpsrpyc/alpha101_expr_list.txt"):
            counter += 1
            if counter <= start:
                continue
            print(f"{CYAN}INFO: {funcn} counter= {counter}, eval_expr= {expr}{NC}")
            ans = myservice.exposed_get_answer({'expr': expr, 'force': 1})

            if ans is not None:
                ans = pickle.loads(ans)
                if 'payload' in ans:
                    ans=ans['payload']
                smart_print(ans, title=funcn)

    elif opt.dryrun:  #generate start up commands
        import subprocess
        existing_processes = [x for x in subprocess.run(f"ps -aux | grep server_id", capture_output=True, text=True, shell=True).stdout.split(r'\n') if x.find("--server_id")>=0]

        pids_to_kill = {}
        cmds_to_start = []
        for ln in open("/Fairedge_dev/app_qpsrpyc/rpyc_expr_server.cfg", 'r').readlines():
            ln = ln.strip()
            if not ln:
                continue
            if ln[0] == '#':
                continue
            (svr_id,ip,port,workspace,models,expr_re) = ln.split(r',')
            if ip != opt.hostip:
                continue
            for proc in existing_processes:
                if proc.find(ip)>=0:
                    pids_to_kill[proc.split()[1]] = 1
            cmds_to_start.append(f"python /Fairedge_dev/app_qpsrpyc/rpyc_expr_server.py --workspace_tmpl {workspace} --server_addr {ip} --server_port {port} --models {models} --server_id {svr_id} --expr_regex '{expr_re}' &")

        for pid in pids_to_kill:
            print(f"kill -9 {pid}")
        print("sleep 5")
        for cmd in cmds_to_start:
            print(cmd)

    else:
        #opt.server_port=18860
        myservice = RpycExprService(opt)
        myservice.jump_start(do=False)

        port = opt.server_port
        if opt.server_port == "":
            rpyc_cfg_fn = f"/Fairedge_dev/app_qpsrpyc/rpyc_expr_server.{opt.do}_{gethostname()}.cfg"
            assert os.path.exists(rpyc_cfg_fn), f"{rpyc_cfg_fn} does not exists"
            for ln in open(rpyc_cfg_fn, 'r').readlines():
                ln = ln.strip()
                if ln == "" or ln[0] == '#':
                    continue
                (svr_id,ip,port,workspace,models,expr_re) = ln.split(r',')

        server = rpyc.ThreadPoolServer(myservice, 
            port=int(port), 
            protocol_config={"allow_public_attrs": True},
            nbThreads = 5)
        server.start()

            




