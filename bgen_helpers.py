#!/home/shuser/anaconda3/bin/python
import sys

from multiprocessing import pool
import os
import QpsUtil
import re
from common_basic import *
import glob
from collections import defaultdict, OrderedDict
from FCAdvancedClient import FCAdvancedClient
from qdb_options import *
import glob
from common_symsets import *
from optparse import OptionParser,OptionConflictError

DIR_AQR = "/Fairedge_dev/app_QpsData_RQ"

def fast_hosts():
    return QpsUtil.open_readlines(f"/Fairedge_dev/app_QpsData/machines_fast.txt")[0]
    #return "che92"
    #return "MSI02"

def norm_scn(scn): #Normalize various scn names, which are used to control strat productions
    if scn in ['U']:
        return 'T'
    else:
        return scn

def get_strat_ids(scn):
    if scn in ['T', 'W', 'F', 'G', 'A']:
        return(["STR_strat001", "STR_strat002"])
    elif scn in ['U']:
        return(["STA_097d36fa", "STA_36578ee0", "STR_strat001"])
    else:
        return [None]
        # cfgs = glob.glob(f"/Fairedge_dev/app_factrank/strat_cfg/*_*.txt")
        # return([os.path.basename(x).replace('.txt','') for x in cfgs])


def todo_steps(opt):
    #print(f"DEBUG_1234: todo_steps= {opt.do}", file=sys.stderr)
    todo = {}
    QpsUtil.exec_file(f'/Fairedge_dev/app_QpsData/todo_steps.txt', {"todo": todo})

    do = opt.do
    if opt.do in ['gen_pred']:
        do = f"gen_{opt.scn}"


    # todo['flds'] = ['fgen', 'egen_flds', 'patch_flds', 'inc_flds', 'combine_db', 'formula_gen_univ', 'gen_univ_flds', 'formula_W_selected', 'formula_T_selected']
    # todo['rq'] = ['ricequant', *todo['flds']]
    # todo['all'] = todo['rq']
    # todo['ana'] = ['egen_indic', 'indic_ana', 'indic_influxdb']
    # todo['everything'] = "ricequant,fgen,egen_flds,patch_flds,inc_flds,egen_indic,egen_select,formula_builder,formula_search,formula_search_selected".split(r',')

    # #XXX factcomb take too much memory
    # todo['gen_W'] = "combine_db_W,formula_W_selected".split(r',')
    # todo['gen_fac_W'] = "factcomb,port_simu_fac".split(r',')
    # todo['gen_T'] = "roll_db_forward_1d,formula_T_selected,factcomb,port_simu_fac,strat_builder".split(r',')
    # todo['gen_U'] = todo['gen_T']  

    if do not in todo:
        todo[opt.do] = [do]
    else:
        todo[opt.do] = todo[do]
    return todo

def args_defaults(opt):
    argsDefault = defaultdict(lambda: ['daily'])
    argsDefault['ricequant'] = ['daily']
    argsDefault['fgen'] = ['Pred_']
    argsDefault['egen_flds'] = glob.glob('/Fairedge_dev/app_egen/egen_flds/*.fldlst')
    return argsDefault

__loadbalancing_data = None
def loadbalancing_data():
    global __loadbalancing_data
    if __loadbalancing_data is None:
        __loadbalancing_data = {}
        if os.path.exists(f"{rootdata()}/config/ssn2fc_host.pkl"):
            __loadbalancing_data['ssn2host'] = QpsUtil.smart_load(f"{rootdata()}/config/ssn2fc_host.pkl")
        else:
            __loadbalancing_data['ssn2host'] = {}
        #__loadbalancing_data['allhost'] = ['che02', 'che00', 'che91', 'che93', 'che95', 'che02', 'che00', 'che91', 'che93', 'che95', 'che02', 'che00', 'che91', 'che93', 'che95', 'che08']
        #__loadbalancing_data['allhost'] = ['che02', 'che00', 'che91', 'che93', 'che02', 'che00', 'che91', 'che93', 'che02', 'che00', 'che91', 'che93']
        __loadbalancing_data['allhost'] = ['che00', 'che91', 'che93', 'che00', 'che91', 'che93', 'che00', 'che91', 'che93']
        __loadbalancing_data['jobcnt'] = 0
        __loadbalancing_data['ssn2host']["CS_ALL"] = 'che93'
        __loadbalancing_data['ssn2host']['CF_ALL'] = 'che93'
    return __loadbalancing_data

def loadbalancing_to(opt, ssn, rscript=False): #symset name
    funcn = "loadbalancing_to"
    if int(opt.dryrun) > 0:
        return "any"

    if rscript:
        return "che93"

    if ssn in ['CS_ALL']:
        #return "any"
        assigned_host = loadbalancing_data()['allhost'][loadbalancing_data()['jobcnt']%(len(loadbalancing_data()['allhost']))]
        loadbalancing_data()['jobcnt'] = loadbalancing_data()['jobcnt'] + 1
    else:
        # print(f"=={ssn}--")
        # print(loadbalancing_data()['ssn2host'].keys())
        if ssn in loadbalancing_data()['ssn2host']:
            assigned_host = loadbalancing_data()['ssn2host'][ssn]
        else:
            assigned_host = "any"

    #print(f"DEBUG: {funcn}, {ssn}, {assigned_host}")
    return assigned_host


def getScnCfgs(scnNm):
    scnCfgs = {}
    scnCfgs['W'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'prod1w',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    scnCfgs['T'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'T',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    scnCfgs['U'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'T',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    scnCfgs['A'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'T',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    scnCfgs['P'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'prod',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    scnCfgs['R'] = {
        'session': 'SN_CS_DAY',
        'dts_cfg': 'rschhist',
        'asofdate': 'download',
        'bar': ['1d', '5m']
        #'bar': ['1d', '5m', '1m']
    }

    for x in ['E','F','G']:
        scnCfgs[x] = scnCfgs['R'].copy() #must make new copy here
        scnCfgs[x]['dts_cfg'] = x

    assert scnNm in scnCfgs, (f"ERROR: unknown --scn value: {scnNm}")
    return(scnCfgs[scnNm])

def expandBashTmp(opt, ssn, bashTmpl, cmd, runfromdir="/Fairedge_dev/app_QpsData", quiet=False, depTag="NA.0", host=None, heavy_score=20, timeout=3600, use_status=True):
    lns = [ln.strip() for ln in open(bashTmpl, 'r').readlines()]
    md5str = f"{cmd};{''.join(lns)};{opt.dryrun};{runfromdir};{ssn}"
    md5 = QpsUtil.buf2md5(md5str)[-8:]

    bashFn = f"{rootdata()}/temp/{os.path.basename(bashTmpl)}".replace(".tmpl", f"_{depTag}_{md5}.bash")

    statusFn = bashFn.replace(".bash", ".status")
    if cmd.find("Rscript")<0 and use_status:
        cmd = f"{cmd} -S {statusFn}"

    #print("*"*100, use_status, cmd)

    if not os.path.exists(bashFn) or True:
        lns = [ln.replace('SYMSET', ssn) for ln in lns]
        lns = [ln.replace('CMD', cmd) for ln in lns]
        lns = [ln.replace('RUNFROMDIR', runfromdir) for ln in lns]
        lns = [ln.replace('DRYRUN', str(opt.dryrun)) for ln in lns]
        lns = [ln.replace('STATUS_FILE', statusFn) for ln in lns]

        open(bashFn, 'w').write('\n'.join(lns))

    if host is None:
        return(f'{depTag} = bash {bashFn: <45} = {cmd}')
    else:
        return(f'{depTag} = bash {bashFn: <45} = {cmd} = hostname {host} = heavy_score {heavy_score} = timeout {timeout}') 

def getFlds(opt, fldSel=None, fldsFn=None, mklvlTh=10):
    if fldsFn is None:
        fldsFn = fn_fgen_flds_cfg('CS')
        
    sectype = fldsFn.split(r'.')[-2]
    flds = []

    if int(mklvlTh) <= 10:
        mklvls = list(range(int(mklvlTh)))
        mklvls.append(10)
    else:
        mklvls = [int(mklvlTh)]

    for ln in open(fldsFn, 'r').readlines():
        ln = ln.strip()
        if ln.find(';lazy')>=0 and opt.lazy<=0:
            continue
        fmt=QpsUtil.getLineParam(ln, desc='fmt', default='db')
        (bar, fldNm) = ln.split(r';')[0].split(r'/')

        mklvl = QpsUtil.getLineParam(ln, desc='lvl')

        if int(mklvl) not in mklvls:
            continue

        if fldSel is not None:
            for fldSelArg in fldSel.split(r','):
                if fldNm.find(fldSelArg)>=0:
                    flds.append([bar, fldNm, mklvl, fmt])
                    break
        else:
            flds.append([bar, fldNm, mklvl, fmt])

    return flds

def getFldDirs(opt, fldSel=None, fldsFn=f"{rootdata()}/config/fgen_flds.CS.cfg", mklvlTh=10):
    flddirs = []
    usr_dir = get_lastest_qpsuser_dir(opt.scn)
    for (bar, fldNm, mklvl, fmt) in getFlds(opt, fldSel, fldsFn, mklvlTh):
        flddir = f"{usr_dir}/{fldNm}"
        if os.path.exists(flddir):
            flddirs.append(flddir)
    return flddirs

def resp2idx():
    r2i = {
        'C2A': 1,
        'C2A1': 2,
        'O2A': 3,
        'O2A1': 4,
        'C2C': 5,
        'C2C1': 6,
        'O2C': 7,
        'O2C1': 8,
        'C2O': 9,
        'C2O1': 10,
        'C2C2': 11,
        'C2C3': 12,
        'C2C4': 13,
        'O2O': 14
        }
    return r2i

def respfn2idx(tmplFn):
    funcn = "egen_xy.respfn2idx"
    if False:
        print(f"DEBUG: {funcn}", tmplFn)
    r2i = resp2idx()
    for k in r2i.keys():
        if tmplFn.find(f"_{k}_")>=0:
            return(r2i[k])
    return 0

def getRespFldnms():
    return 'C2C C2O O2C O2C1'.split()
    #return list(resp2idx().keys())

def getRespFlds(resp_tag="all"):
    if resp_tag == "all":
        resps = [f"Resp_{x}_1d" for x in getRespFldnms()]
    else:
        resps = [f"Resp_{x}_1d" for x in resp2idx().keys() if re.match(f"^{resp_tag}$", x)]
    return(sorted(resps))

def getPredFlds(globstr, debug=False, prefixes=getPredFldPrefix()):
    facnms = []
    prefixes = prefixes.split(r',')
    for fn in qps_glob(globstr):
        #print(f"DEBUG_3455: {fn}")
        if fn.find('test')>=0:
            continue

        for ln in QpsUtil.open_readlines(fn, verbose=debug):
            if ln.find('=')<0:
                continue
            facnm = ln.split(r'=')[0]
            for x in prefixes:
                if facnm.find(x)>=0:
                    facnms.append(facnm.strip())
    return(facnms)

def func_df_get_conditional_cell(df, valCol, condCol, condVal):
    #print(df[df[condCol] == condVal].head())
    try:
        return df[df[condCol] == condVal][valCol][0]
    except:
        return None

def get_fldlst_selection_files(opt):
    arr = list(itertools.chain.from_iterable(
            #for prod1w, we generate data for all historical factor selections so we can track them
            #get_factor_selections(getRespFlds(opt.resp_tag), latest_only=(scn.dts_cfg!="prod1w")).values()
            get_factor_selections(getRespFlds(opt.resp_tag), latest_only=True).values()
    ))
    if len(arr) == 0:
        arr.append('NA.fldlst')  #If not found anything, fake one so following program do egen_select
        if opt.debug:
            print(f"DEBUG: get_factor_selection_file {arr}")

    if opt.debug:
        print(f"INFO: egen_select fldlst files:\n\t", '\n\t'.join(sorted(argsDefault['egen_select'])), file=sys.stderr)

    return arr

def simplify_if_echo(opt, cmds, disable=False):
    if opt.bash == "echo" and not disable:
        #simplify output for echo mode
        newCmds = []
        for x in cmds:
            segs = x.split("=")
            #segs[1] = " - "
            segs[2] = segs[2][:segs[2].find("-S")]
            newCmds.append("=".join(segs))
            #print("*"*30, "=".join(segs))
        return newCmds
    return cmds


def __simplify_if_echo(opt, cmds):
    if opt.bash == "echo":
        #simplify output for echo mode
        newCmds = []
        for x in cmds:
            # segs = x.split("=")
            # segs[1] = " - "
            # segs[2] = segs[2][:segs[2].find("-S")]
            # newCmds.append("=".join(segs))
            #print(f"DEBUG_2342: {x}")
            segs = x.split(r'=')
            if len(segs)==3:
                segs.extend([" NA "," NA ", " NA "])
            elif len(segs)==4:
                segs.extend([" NA "," NA "])
            (tag, dummy, cmd, hostname, heavy_score, timeout) = segs
            newCmds.append("=".join([tag, hostname, heavy_score, timeout+' ', cmd]))
        return newCmds
    return cmds

@timeit
def run_cmds_using_pool(cmds, pool_size=6):
    print(f"INFO: pool size {pool_size}", file=sys.stderr)
    po = Pool(pool_size)
    for cmdLn in cmds:
        cmdBash = cmdLn.split(r'=')[1].strip()
        po.apply_async(os.system, (cmdBash,))
    po.close()
    po.join()

def customize_bgen_options(parser): 
    #print("DEBUG_1111: customize_bgen_options", "X"*100)
    parser.add_option("--cmdgrp",
                    dest = "cmdgrp",
                    type = "str",
                    help = "--cmdgrp (default: %default)",
                    metavar = "--cmdgrp",
                    default = "A00")

    try:
        parser.add_option("--show",
                        dest = "show",
                        type = "str",
                        help = "--show (default: %default)",
                        metavar = "--show",
                        default = 'none')
    except OptionConflictError:
        pass

    parser.add_option("--force_step",
                    dest = "force_step",
                    type = "str",
                    help = "--force_step (default: %default)",
                    metavar = "--force_step",
                    default = 'fld,cx')

    # parser.add_option("--eval",
    #                 dest = "eval",
    #                 type = "str",
    #                 help = "--eval (default: %default)",
    #                 metavar = "--eval",
    #                 default = 'cx_fast')

    # parser.add_option("--export_tag",
    #                 dest = "export_tag",
    #                 type = "str",
    #                 help = "--export_tag (default: %default)",
    #                 metavar = "--export_tag",
    #                 default = 'NA')

    # parser.add_option("--results_dir",
    #                 dest = "results_dir",
    #                 type = "str",
    #                 help = "--results_dir (default: %default)",
    #                 metavar = "--results_dir",
    #                 default = 'NA')                

    # parser.add_option("--detailed",
    #                 dest = "detailed",
    #                 type = "int",
    #                 help = "--detailed (default: %default)",
    #                 metavar = "--detailed",
    #                 default = -99)

    # parser.add_option("--par_set_per_run",
    #                 dest = "par_set_per_run",
    #                 type = "int",
    #                 help = "--par_set_per_run (default: %default)",
    #                 metavar = "--par_set_per_run",
    #                 default = 3)

    # parser.add_option("--argpar_use_default",
    #                 dest = "argpar_use_default",
    #                 type = "int",
    #                 help = "--argpar_use_default (default: %default)",
    #                 metavar = "--argpar_use_default",
    #                 default = 0)

    # parser.add_option("--facnm",
    #                 dest = "facnm",
    #                 type = "str",
    #                 help = "--facnm (default: %default)",
    #                 metavar = "--facnm",
    #                 default = "NA")

    # parser.add_option("--mode",
    #                 dest = "mode",
    #                 type = "str",
    #                 help = "--mode (default: %default)",
    #                 metavar = "--mode",
    #                 default = "cmp")

    # parser.add_option("--parmd5",
    #                 dest = "parmd5",
    #                 type = "str",
    #                 help = "--parmd5 (default: %default)",
    #                 metavar = "--parmd5",
    #                 default = "NA")

    parser.add_option("--pluslines",
                    dest = "pluslines",
                    type = "int",
                    help = "--pluslines (default: %default)",
                    metavar = "--pluslines",
                    default = 50)

    parser.add_option("--minuslines",
                    dest = "minuslines",
                    type = "int",
                    help = "--minuslines (default: %default)",
                    metavar = "--minuslines",
                    default = 10)

    parser.add_option("--worker_host",
                    dest = "worker_host",
                    type = "str",
                    help = "--worker_host (default: %default)",
                    metavar = "--worker_host",
                    default = "NA")


@timeit
def run_cmds_using_worker(cmds, pool_size=20, list_cmds=None):
    (optFc, argsFc) = get_FCAdvancedClient_options(list_cmds, customize_options=customize_bgen_options)
    #optFc.pool_size = 12
    optFc.pool_size = pool_size
    fc_client = FCAdvancedClient(optFc)
    fc_client.load_jobs(cmds)
    fc_client.start()

@timeit
def run_cmds_using_shell(cmds):
    for cmdLn in cmds:
        if cmdLn.find("=")>=0:
            cmdBash = cmdLn.split(r'=')[1].strip()
        else:
            cmdBash = cmdLn
        if ctx_verbose(1):
            print(f"INFO: run_cmds_using_shell= {cmdBash}")
        os.system(cmdBash)

# @deprecated
def run(opt, cmds):
    try:
        if opt.bash in ['worker']:
            run_cmds_using_worker(cmds)

        elif opt.bash in ['shell']:
            run_cmds_using_shell(cmds)

        elif opt.bash in ['pool']:
            run_cmds_using_pool(cmds)

        elif opt.bash in ['print_shell']:
            for cmdLn in cmds:
                cmdBash = cmdLn.split(r'=')[1].strip()
                print(cmdBash) 

        elif opt.bash in ['print_python']:
            for cmdLn in cmds:
                cmdBash = cmdLn.split(r'=')[2].strip().replace('$PY', 'python')
                print(cmdBash) 
        
        elif opt.bash in ['echo', '']:
            pass

        else:
            assert False,  f"ERROR: invalide --bash {opt.bash}"
    except KeyboardInterrupt:
        exit(1)     

def cmd_geninstru(cmds, opt, cfg, ssn, matched_grep, depTag):
    bashTmpl = f'/Fairedge_dev/app_QpsData/FC.tmpl'
    #exe = "${cmd_gen_vps_instru}" #{DIR_AQR}/gen_vps_instru.py
    opt_basic = f"-F {opt.force} -s {cfg['session']} -c {cfg['dts_cfg']} -a {opt.asofdate} --bar 1d"
    cmd = f"$PY -u $cmd_gen_vps_instru {opt_basic} --instru_force 0 --symset {ssn} --do gen_stk_instru,gen_stk_univ --comment daily"
    if not matched_grep(cmd):
        return                   
    cmds.append(f"%s = hostname che93"%(expandBashTmp(opt, ssn, bashTmpl, cmd, runfromdir=f"{DIR_AQR}", depTag=depTag, quiet=opt.quiet)))  #only run this on che93 with Ricequant license  

def cmd_split_mysql(cmds, opt, cfg, ssn, matched_grep, depTag):
    #generate all mysql flds in one step
    bashTmpl = f'/Fairedge_dev/app_QpsData/FC.tmpl'
    opt_basic = f"-F {opt.force} -s {cfg['session']} -c {cfg['dts_cfg']} -a {opt.asofdate} --bar 1d"
    cmd = f"$PY -u $cmd_fgen_split_mysql {opt_basic} --symset {ssn}"
    if not matched_grep(cmd):
        return                        
    cmds.append(f"%s = hostname {loadbalancing_to(opt, ssn)}"%(expandBashTmp(opt, ssn, bashTmpl, cmd, runfromdir="/Fairedge_dev/app_fgen", depTag=depTag, quiet=opt.quiet)))
                    
def cmd_fgen_ss(cmds, opt, cfg, scn, ssn, matched_grep, depTagPrefix=f"A00.05"):
    for (bar, fldNm, mklvl, fmt) in getFlds(opt, "Pred_", fldsFn=fn_fgen_flds_cfg('CS'), mklvlTh=10):
        # if fldNm.find("5m") >= 0 or fldNm.find("1m")>=0:
        #     mklvl = int(mklvl)
        #     mklvl += 1

        if bar not in cfg['bar']:
            return
        # if cfg['dts_cfg'] == 'rschhist' and bar == '1m':
        #     continue #takes too long right now
        bashTmpl = f'/Fairedge_dev/app_QpsData/FC.tmpl'
        comment = "NA"
        if int(mklvl)<=1 and bar == "1d":
            comment = "daily"
        opt_basic = f"-F {opt.force} -s {cfg['session']} -c {cfg['dts_cfg']} -a {opt.asofdate} --bar {bar} --comment {comment}"
        dir_basic = f"{cfg['session']}/{cfg['dts_cfg']}_{scn.begDt}_{scn.endDt}/{bar}"
        depTag = f"{depTagPrefix}.%02d"%(int(mklvl))
        cmd = f"$PY -u $cmd_fgen_ss --symset {ssn} {opt_basic} --fp {dd('usr')}/CS/{dir_basic}/{fldNm}/{ssn}.{fmt}"
        if not matched_grep(cmd):
            continue                            
        cmds.append(f"%s = hostname {loadbalancing_to(opt, ssn)}"%(expandBashTmp(opt, ssn, bashTmpl, cmd, runfromdir="/Fairedge_dev/app_fgen", depTag=depTag, quiet=opt.quiet)))

def build_flds(opt, egenDirs, tmplFn, cfg):
    funcn = "egen_xy.build_flds"
    basename = os.path.basename(tmplFn)
    #print(basename, QpsUtil.buf2md5(basename)[-6:])
    major = '00'
    minor = QpsUtil.buf2md5(basename)[-6:]

    #Super's selection data file has special directory naming rules
    #If a cfg key is a response name, then this is a factor selection

    if tmplFn.find('Resp_')>=0:
        major = "%02d"%(respfn2idx(tmplFn))
        # selDta = re.findall(r'\d\d\d\d\d\d\d\d',tmplFn)[-1]
        # minor = f"{QpsUtil.buf2md5(tmplFn)[-2:]}{selDta[-4:]}"
        if True and tmplFn.find("baseline_") >= 0:
            minor = "A."
        else:
            minor = f"{QpsUtil.buf2md5(tmplFn)[-2:]}"
        minor =f"{minor}{QpsUtil.getDatesCfg('dcsndate', asofdate=opt.asofdate, cfg_name=opt.dts_cfg, verbose=False)[-4:]}"
        cfg = 'sel'

    dtaDir = f"{rootdata()}/egen/{cfg}/{major}/{minor}"
    fldlstFn = f'{dtaDir}/{basename}'
    lineTag=cfg

    egenDirs[dtaDir] = (f'{lineTag: <6} {fldlstFn: <100} {basename}')
    if opt.debug:
        print('DEBUG', dtaDir)
    QpsUtil.mkdir([dtaDir])
    os.system(f"cp -p {tmplFn} {dtaDir}")

def cmd_split_raw(cmds, opt, cfg, ssn, matched_grep, depTag):
    bashTmpl = f'/Fairedge_dev/app_QpsData/FC.tmpl'
    opt_basic = f"-F {opt.force} -s {cfg['session']} -c {cfg['dts_cfg']} -a {opt.asofdate} --bar 1d"
    cmd = f"$PY -u $cmd_fgen_split_raw {opt_basic} --symset {ssn}"
    if not matched_grep(cmd):
        return     
    cmds.append(f"%s = hostname {loadbalancing_to(opt, ssn)}"%(expandBashTmp(opt, ssn, bashTmpl, cmd, runfromdir="/Fairedge_dev/app_fgen", depTag=depTag, quiet=opt.quiet)))

def funcgen_matched_grep(opt, always_include_symsets=[]):
    def matched_grep(cmd):
        if cmd.find("rq_factors")>=0 or cmd.find("egen_select")>=0:
            #print(f"DEBUG_2343: matched_grep cmd= {cmd}", file=sys.stderr)
            return False
        for ssn in always_include_symsets:
            if cmd.find(f"--symset {ssn}")>=0:
                return True

        # orPatterns = [["ref"], opt.grep.split(r',')]
        orPatterns = [opt.grep.split(r',')]
        
        rc = False
        for andPatterns in orPatterns:
            #print(f"DEBUG_2349: matched_grep grep= {opt.grep},  orPattern= {orPatterns}, andPattern= {andPatterns}, cmd= {cmd}", file=sys.stderr)
            andRc = True
            for pattern in andPatterns:                
                pattern = re.compile(pattern)
                if not pattern.search(' '.join(cmd.split()[:])): #bypass script name
                    #print(f"DEBUG_2342: matched_grep pattern= {pattern}, cmd   = {cmd}", file=sys.stderr)
                    andRc = False
            if andRc == True:
                rc = True            
        return rc
    return matched_grep

def get_selected_factor_rows(opt):
    from copy import deepcopy
    import numpy as np
    from formula_search_query_results import get_search_results_df,filter_result_by_symset_scn,filter_result_by_column_value
    selOpt = deepcopy(opt)
    selOpt.scn = 'G'
    df = get_search_results_df(selOpt, f"Resp_C2C.{opt.symset}")
    dfSel = filter_result_by_symset_scn(selOpt, df)
    plusDf = filter_result_by_column_value(opt, dfSel, 'Q5h_net_Return', (0.01, np.inf))
    # print(dfSel)
    # plusDf = get_plusdf(selOpt, dfSel)[['parMd5', 'symset', 'UNIV', 'PRED']]
    print_df(plusDf, title=f"plus candadates {plusDf.shape}")
    return plusDf.iterrows()

def copy_check_predictor_files(factorDtaFns, pred, symset, tgtdir):
    factorDtaFnByScn = {}
    for fn in factorDtaFns:
        for scn in ["F", "G", "prod1w"]:
            if fn.find(f"/{scn}_")>0:
                factorDtaFnByScn[scn] = (fn, f"{tgtdir}/{pred}.{symset}.{scn.replace('prod1w', 'W')}.db")

    for (src,tgt) in (factorDtaFnByScn).values():
        print(f"cp -p {src} {tgt}")
        os.system(f"cp -p {src} {tgt}")

def append_cmds(cmds, opt, cfg, cmd, matched_grep, depTag, stage_no=None, symset="CS_ALL", host=None, heavy_score=20, timeout=3600, use_status=True):
    # Do not filter here now as it affects the jobs.txt file. Filter when print out
    # if not matched_grep(cmd):
    #     return

    if host is None:
        host = loadbalancing_to(opt, symset)
    
    if stage_no is not None:
        depTag=f"{depTag}.{'%02d'%stage_no}"

    cmds.append(expandBashTmp(opt, symset, f'/Fairedge_dev/app_QpsData/FC.tmpl', cmd, runfromdir="/Fairedge_dev/app_QpsData", 
        depTag=depTag, host=host, quiet=opt.quiet, heavy_score=heavy_score, timeout=timeout, use_status=use_status))

def cs_cmp_jobs(x):
    x_symset = re.findall(r"symset\s+(\S+)\s", x)
    if len(x_symset)>0:
        x_symset = x_symset[0]
    else:
        x_symset = 'NA'

    x_bar = re.findall(r"bar\s+(\S+)\s", x)
    if len(x_bar)>0:
        x_bar = x_bar[0]
    else:
        x_bar = '1d'

    x_facnm = re.findall(r"facnm\s+(\S+)\s", x)
    if len(x_facnm)>0:
        x_facnm = x_facnm[0]
    else:
        x_facnm = 'NA'

    x_fldnm = re.findall(r"Pred_(\S+)\s", x)
    if len(x_fldnm)>0:
        x_fldnm = x_fldnm[0]
    else:
        x_fldnm = 'NA'

    # x_factcomb = re.findall(r"\/(\S+?)\/factcomb\_??????/factcomb.db", x)
    # if len(x_factcomb)>0:
    #     if x_symset == 'NA':
    #         x_symset = x_factcomb[0]
    if x_symset == 'NA':
        if x.find(f"{rootdata()}/egen_study/factcomb")>=0:
            x_elem = x[x.find(f"{rootdata()}/egen_study/factcomb"):].split("/")
            if len(x_elem)>=6:
                x_symset = x_elem[5]

    job_code = x.split(r'=')[0].strip()
    cmd = x.split(r'=')[2]

    #print(x_symset, get_symset_weight(x_symset), file=sys.stderr)
    #print(x, file=sys.stderr)
    #key = ','.join((x.split(r'=')[0].strip(), "symset%03d"%(get_symset_weight(x_symset)), x_symset, x_bar, x_facnm, x_fldnm, cmd))
    key = ','.join((job_code, "symset%03d"%(get_symset_weight(x_symset)), x_symset, x_bar, x_facnm, x_fldnm, cmd))
    #key = ','.join((job_code, "symset%03d"%(get_symset_weight(x_symset)), x_symset, x_bar, x_facnm, x_fldnm))
    #print("DEBUG_7231: cs_cmp_jobs key=", key, file=sys.stderr)
    return key

def jobs_output_fn(opt):
    dirout = f"/Fairedge_dev/app_regtests/bgen_cs"
    return f"{dirout}/{opt.symset}.{opt.scn}.{opt.do}.txt"
    
def write_out_jobs(opt, cmds):
    
    if os.path.exists(os.path.dirname(jobs_output_fn(opt))):      
        print('\n'.join([str_minmax(x, disable=True) for x in cmds]), file=open(jobs_output_fn(opt), 'w'))
        return jobs_output_fn(opt)
    else:
        return "/dev/null"

def print_cmds(opt, cmds):
    print('\n'.join([x for x in simplify_if_echo(opt, cmds)]))
    print(f"{BROWN}INFO: jobs count= {len(cmds)}, file= {jobs_output_fn(opt)}{NC}", file=sys.stderr)

def run_jobs(opt, cmds, jobsFn):
    print_cmds(opt, cmds)
    if opt.bash in ['echo']:
        pass

    elif opt.bash in ['cron']:
        cmd = f"bash /Fairedge_dev/machines/cron_driver.bash --host che93 python /Fairedge_dev/app_QpsData/bgen_run.py --bash {opt.bash} {jobsFn}"
        print(f"INFO: run_jobs= {cmd}")
        os.system(cmd)

    elif opt.bash in ['worker']:
        run_cmds_using_worker(cmds, pool_size=100)

    elif opt.bash in ['shell']:
        run_cmds_using_shell(cmds)

    elif opt.bash in ['pool']:
        run_cmds_using_pool(cmds, pool_size=opt.pool_size)

    elif opt.bash in ['print_shell']:
        for cmdLn in cmds:
            cmdBash = cmdLn.split(r'=')[1].strip()
            print(cmdBash) 

    elif opt.bash in ['print_python']:
        for cmdLn in cmds:
            cmdBash = cmdLn.split(r'=')[2].strip().replace('$PY', 'python')
            print(cmdBash) 

    else:
        assert False,  f"ERROR: invalide --bash {opt.bash}"


if __name__=='__main__':
    # print("INFO: T", get_strat_ids("T"))
    # print("INFO: W", get_strat_ids("W"))
    #print(symsets_paper())
    funcn = sys.argv[1]
    print(eval(funcn))



