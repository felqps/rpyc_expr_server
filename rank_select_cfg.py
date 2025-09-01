
#from doctest import OutputChecker
import sys
from webbrowser import get

import os
import time
import datetime
import re
import glob
import traceback

from common_basic import *
import QpsUtil
from qdb_options import *
from CTX import ctx_debug,ctx_verbose
from bgen_helpers import get_strat_ids
from common_symsets import *
from shared_cmn import print_df

def hdg300(r):
    if type(r) == type(str()):
        hdgstr = r
    else:
        hdgstr = r['index'].split(r'.')[-1]
    hdgstr = hdgstr.replace('u','')
    hdg300 = (int(hdgstr[:3])-300)/100
    return -hdg300 if hdg300!=0.0 else hdg300 

def hdg500(r):
    if type(r) == type(str()):
        hdgstr = r
    else:
        hdgstr = r['index'].split(r'.')[-1]
    hdgstr = hdgstr.replace('u','')
    hdg500 = (int(hdgstr[-3:])-500)/100

    #print("-"*60, hdgstr, hdg500)
    return -hdg500 if hdg500!=0.0 else hdg500 

def select_factrank_groups_matching(step, n_cut, symsetIn=None, debug=False):
    fact_groups = []
    factrank_db_fn = "/Fairedge_dev/app_factrank/factrank/factrank.summary.csv"
    funcn = f"=================================================================select_factrank_groups_matching(step={step}, n_cut={n_cut}, symsetIn={symsetIn}, db_fn= {factrank_db_fn})"
    factrank_db = pd.read_csv(factrank_db_fn, sep=',')

    print(f"INFO: {funcn}")

    factrank_db = factrank_db[factrank_db['index'] == step]
    factrank_db = factrank_db[factrank_db['dts_cfg'] == 'W']
    factrank_db = factrank_db[factrank_db['n_cut'] == int(n_cut)]

    if ctx_verbose(1) or debug:
        print_df(factrank_db, cols=100, title=f"{funcn} factrank_db")

    if symsetIn is not None:
        factrank_db = factrank_db[factrank_db['symset'] == symsetIn]
    
    if ctx_verbose(1) or debug:
        print_df(factrank_db, cols=20, title=funcn)

    factrank_db = factrank_db[['symset', 'filter']]
    if ctx_verbose(1) or debug:
        print_df(factrank_db, cols=20, title=f"{funcn} symset,filter")

    if factrank_db.shape[0]<=0:
        print(f"{ERR}ERROR: can not find factrank{NC} for {funcn}")
        return fact_groups
    for idx, (symset,filter) in factrank_db.iterrows():
        #symset,filter = symset_filter
        #print(f"{symset} === {filter}")
        fact_grp_fn = f"/qpsdata/egen_study/factrank/{symset}/factor_net_returns/{filter}/{step}.factor_net_returns.F.csv"
        print(f"INFO: {funcn} fact_grp_fn {fact_grp_fn}")
        fact_groups.append((symset, fact_grp_fn))

    if ctx_verbose(1):
        print(f"INFO: fact_groups=", '\n'.join([f"{x}" for x in fact_groups]))
    return fact_groups

def filter_rank_select_cfgs_W(opt, select_cfgs, stratid=None):
    filtered_cfgs = []
    for cfg in select_cfgs:
        # if (cfg['alphaQPct'] not in [10]) and (cfg['n_cut'] not in [8]):
        #     continue
        filtered_cfgs.append(cfg)
    return filtered_cfgs

__shown_once=False
def filter_rank_select_cfgs_T(opt, select_cfgs, stratid="STR_strat001"):
    funcn = "filter_rank_select_cfgs_T"
    filtered_cfgs = []
    strat_cfg_fn = f"/Fairedge_dev/app_factrank/strat_cfg/{stratid}.txt"

    if not hasattr(opt, 'strat_cfg_fn'):
        opt.strat_cfg_fn = {}
    opt.strat_cfg_fn[stratid] = strat_cfg_fn 
    
    for fn in glob.glob(strat_cfg_fn):
        global __shown_once
        if ctx_verbose(0) and not __shown_once:
            __shown_once = True
            print(f"{IMP}IMPORTANT: {funcn} reading strat_cfg {fn}{NC}", file=sys.stderr)
        stratname = fn.split(r'/')[-1].replace('.txt','')
        stratname = stratname.split(r'_')[0]
        for ln in QpsUtil.open_readlines(fn):
            (ssn, sel_name, uamt, hdg) = ln.split(".")
            for cfg in select_cfgs:
                #print(funcn, "ln=", ln, ", cfg=", cfg)
                if (cfg['sel_name'] == sel_name) and (cfg['ssn'] == ssn):
                    cfg['uamt'] = uamt.replace('u','')
                    cfg['stratname'] = stratname
                    cfg['hdgcode'] = hdg
                    cfg['hdg300'] = hdg300(hdg)
                    cfg['hdg500'] = hdg500(hdg)

                    filtered_cfgs.append(cfg)
    #print(filtered_cfgs)
    return filtered_cfgs

def generate_base_select_cfgs(ssn=None):
    select_cfgs = []
    for alphaQPct in [5,10,20]:
        for n_cut in [8,14]:
            select_cfg = {}
            select_cfg['step'] = "HierTr"
            select_cfg['n_cut'] = n_cut
            select_cfg['alphaQPct'] = alphaQPct
            select_cfg_str = f"{select_cfg}"
            if ctx_verbose(1):
                print("INFO: select_cfg_str=", select_cfg_str, file=sys.stderr)
            select_cfg_name = f"factcomb_{QpsUtil.buf2md5(select_cfg_str)[-6:]}"
            select_cfg['sel_name'] = select_cfg_name

            #This must be after select_cfg_name
            select_cfg['uamt'] = None
            select_cfg['ssn'] = ssn
            select_cfg['stratname'] = None

            select_cfgs.append(select_cfg)
    return select_cfgs

RANK_SELECT_CFGS = {}
def get_rank_select_cfgs_for_symset(opt, scn, ssn, stratid):
    funcn = f"get_rank_select_cfgs({scn},{ssn},{stratid})"
    #print(funcn)
    global RANK_SELECT_CFGS
    if ssn in RANK_SELECT_CFGS:
        return RANK_SELECT_CFGS[funcn]
    if ssn not in get_symsets(scn, "CS", all_indus = False): #symsets_paper():
        return []
        
    if scn not in ['T', 'U', 'A']:
        select_cfgs = filter_rank_select_cfgs_W(opt, generate_base_select_cfgs(ssn), stratid=stratid)
    else:
        select_cfgs = filter_rank_select_cfgs_T(opt, generate_base_select_cfgs(ssn), stratid=stratid)

    if ctx_debug(1):
        print(f"{DBG}DEBUG_2334{NC}: {funcn} scn= {scn}, ssn= {ssn} ... \n\t", '\n\t'.join([f"===rank_select_cfg: {x}" for x in select_cfgs]))

    RANK_SELECT_CFGS[funcn] = select_cfgs
    return select_cfgs

def get_rank_select_factors(symsetIn, select_cfg):
    funcn = f"get_rank_select_factors(symsetIn={symsetIn}, select_cfg={select_cfg})"
    if ctx_debug(5):
        print(f"{DBG}DEBUG_4353:{NC}", funcn)
    step = select_cfg['step']
    n_cut = select_cfg['n_cut']
    alphaQPct=select_cfg['alphaQPct']/100.0
    fldnmLst = []
    for symset, fn in select_factrank_groups_matching(step, n_cut, symsetIn=symsetIn):
        if ctx_debug(1):
            print(f"DEBUG_2341: {funcn} symset= {symset}, fn= {fn}")
            if not symset in [symsetIn]:
                continue
        for ln in QpsUtil.open_readlines(fn):
            fldnm = ln.split(",")[0]
            if fldnm == "":
                continue
            fldnmLst.append(fldnm)
    if ctx_debug(1):
        print(f"{DBG}DEBUG_1126{NC}: {funcn}\nfldnmLst= ", "\n\t".join(fldnmLst))
    return(fldnmLst)

# @deprecated
def get_rank_select_cfgs(opt, scn, symset, stratid):
    funcn = f"get_rank_select_cfgs(scn={scn})"
    rank_sel_cfgs = []
    # if symset in ['CS', 'NA']:
    #     symsets = symsets_paper()
    # else:
    #     symsets = [symset]

    for ssn in get_symsets(scn, symset if not hasattr(opt, 'indu_filter') else opt.indu_filter):
        rank_sel_cfgs.extend(get_rank_select_cfgs_for_symset(opt, scn, ssn, stratid))
    
    for x in rank_sel_cfgs:
        if False:
            if x['uamt'] is None:
                x['uamt'] = '1000'
                x['hdg300'] = 0
                x['hdg500'] = 0
                x['sel_name']='factcomb_0086c5'
                print(f"DEBUG_4355: {funcn} rank_sel_cfg= {x}")
        print(f"INFO: {funcn} scn= {scn}, ssn={symset}:", x)

    return rank_sel_cfgs

if __name__=='__main__':
    funcn = 'rank_select_cfg.main'
    (opt, args) = get_options_sgen(list_cmds=None, customize_options=None)
    #print(opt, file=sys.stderr)
    ctx_set_opt(opt)

    for scn in 'FGWTU':
        print(f'REGTEST: scn= {scn}, stratids= {get_strat_ids(scn)}')


    for symset in ['CS_ALL']: #'I65'
        opt.symset = symset
        for scn in 'WTU'[:]:
            for stratid in get_strat_ids(scn):
                print("-"*200)
                #stratid is None for research
                for select_cfg in get_rank_select_cfgs_for_symset(opt, scn, symset, stratid):
                    print(f"INFO: get_rank_select_cfgs_for_symset scn= {scn}, symset= {symset}, stratid= {stratid}, rank_sel_cfgs =>", select_cfg)                
                # else:
                #     for cfg in generate_base_select_cfgs(symset):
                #         print(f"INFO: generate_base_select_cfgs scn= {scn}, symset= {symset}:", cfg)
                print("-"*200)
                for cfg in get_rank_select_cfgs(opt, scn, 'CS', stratid=stratid):
                    print(f"INFO: get_rank_select_cfgs scn= {scn}, symset={opt.symset}, stratid={opt.stratid}, cfg= {cfg}")


    print("-"*200, "groups_matching")
    grps = select_factrank_groups_matching("HierTr", "8")
    print('\n'.join([f"REGTEST factrank grps matching (step=HierTr, n_cut=8): {x}" for x in grps]))

    

    for select_cfg in get_rank_select_cfgs_for_symset(opt, 'T', 'C20', 'STR_strat001'):
        if select_cfg['sel_name'] == "factcomb_0086c5" and opt.dryrun == 0:
            print("-"*200)
            fldnms = get_rank_select_factors('C20', select_cfg)
            print('\n'.join([f"REGTEST get_rank_select_factors: {x}" for x in fldnms]))
        
