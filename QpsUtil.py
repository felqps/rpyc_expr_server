#!/cygdrive/c/Anaconda3/python.exe

import sys,pickle
appDir="%s/Fairedge_dev/app_common"%("c:" if sys.platform=="win32" else "")

import os
import subprocess
import re
import glob
import sys, stat
import time
import pytz
import socket
import getpass
import datetime
import toNative
from pathlib import Path
import numpy as np
from QpsSys import *
from QpsVts import *
from QpsDate import *
from QpsDatetime import *
from QpsCondor import *
import pandas as pd
from UpdateCfg import *
from SmartFile import *
from common_basic import *
from common_paths import fn_fgen_flds_cfg

def BN():
    return "751"

def DIE():
    import traceback
    traceback.print_stack()
    assert False, 'DIE()'


def FeConfigDir():
    return toNative.getNativePath("c:/fe/config")

sgn = lambda x: 1 if x > 0 else -1 if x < 0 else 0

def high2low(a,b):
    if a > b:
        return -1
    elif a == b:
        return 0
    else:
        return 1

def sed_replace(pattern, fromFn, toFn, verbose = 0):
    patternNative=pattern
    if sys.platform!="win32":	#unixlike
        patternNative=r"%ss/^c:\//python \//ig;s/\.bn[0-9][0-9][0-9]\//\//g;s/\.exe/.py/g;s/c:\//\//ig;s/\/FE\//\/fe\//g;s/\/Quanpass\//\/quanpass\//g;"%(patternNative)
    cmd = "%s -e \"%s\" %s > %s"%(toNative.getCmdBin("sed"),patternNative, fromFn.replace('^', '^^'), toFn.replace('^', '^^'))
    #print >> sys.stderr,cmd
    if verbose:
        print(cmd, file=sys.stderr)
    run_cmd(cmd, expandHat=0)

def dir_chain(dir):
    dirElem = dir.split(r'/')
    dirChain = []
    for i in xrange(2,len(dirElem)):
        dirChain.append('/'.join(dirElem[0:i+1]))
    return dirChain

def msgPrompt(dryrun, other):
    if dryrun:
        return "DRYRUN"
    else:
        return other

def print_help(command, waitFinish=0, debug=0):
    out = []
    for ln in run_command(command, waitFinish=waitFinish):
        ln = ln.strip()
        skip=0
        for skipPhrase in ("ERR",
                        "IFO",
                        "INF",
                        "zonespec database",
                        "getCfgFilePath",
                        "evt_meta_types.txt"):
            if ln.find(skipPhrase)>=0:
                skip=1
                break
        if skip==0:
            print(ln, file=sys.stdout)



def m4gen(outFn, inFnList, dbg):
    with open(outFn, 'w') as output:
        cmd = ["c:/cygwin/bin/m4.exe" if sys.platform=="win32" else "m4"]
        #cmd.append("-P")#--prefix-builtins
        for fn in inFnList:
            if os.path.exists(fn):
                cmd.append(fn)
        #print >> sys.stderr, "gen %s, cmd %s"%(outFn, cmd)
        if(dbg):
            print("gen %s, cmd %s"%(outFn, cmd), file=sys.stderr)

        subprocess.call(cmd, stdout=output)

def cvs_add_dir(pathToAdd):
    cmd = "%s %s --cfg %s %s"%(toNative.getCmdBin("perl"),toNative.getNativePath("c:/fe/scripts/auto_cvs.pl"), toNative.getNativePath("c:/quanpass/config/sid_auto_cvs.txt"), pathToAdd)
    print(cmd, file=sys.stderr)
    os.system(cmd)


def uncommentJS(js):
    list = re.findall(r'(\"([^\\\"]*(\\.)?)*\")|(\'([^\\\']*(\\.)?)*\')|(\/{2,}.*?(\r|\n))|(\/\*(\n|.)*?\*\/)',js) #remove notes //:6 /**/:8
    for array in list:
        if(array[6]!=None):
            js = js.replace(array[6],'')
        if(array[8]!=None):
            js = js.replace(array[8],'')
    return js


def getAttr(options, attr):
    if not hasattr(options, attr):
        return False
    return getattr(options, attr)


def python_version():
    s = run_command("python.exe --version")
    output = [x for x in s]
    return ''.join(output).strip()

def idxdt2trddt(idxdt, tm, offset=0):
    (prevdate, nextdate) = getPrevNextBusinessDay(str(idxdt))
    tmHour = int(tm.split(":")[0])
    if tmHour > 17:
        return nextdate
    else:
        return idxdt

def trddt2idxdt(trddt, tm, offset=0):
    (prevdate, nextdate) = getPrevNextBusinessDay(str(trddt))
    tmHour = int(tm.split(":")[0])
    if tmHour > 17:
        return prevdate
    else:
        return trddt

def pk2sym(pk):
    return pk.split(":")[2]

def pk2bk(pk):
    return pk.split(":")[1]

def pk2bn(pk):
    return pk.split(":")[0]

def obj2txt(path, obj):
    fn = open(path, 'wb')
    fn.write(pickle.dumps(obj))
    return True

def txt2obj(path):
    fn = open(path, 'rb')
    bin = fn.read()
    return pickle.loads(bin)

def eval_file(fn, debug=False):
    if debug:
        print(f'INFO: eval_file({fn})', file=sys.stderr)
    return(eval(''.join(open(fn, 'r').readlines())))

def exec_file(fn, vars=None, debug=False):
    if debug:
        print(f'INFO: exec_file({fn})', file=sys.stderr)
    exec(''.join(open(fn, 'r').readlines()), vars)

#@timeit
def fldnmRq2Qps(rqFldNm):
    if rqFldNm.find("Pred")>=0:
        return rqFldNm
    rc = ''.join([x[0].upper()+x[1:] for x in rqFldNm.split(r'_')])
    if rc.find("Rq")!=0:
        rc='Rq'+rc
    rc = rc.replace('RqWorldQuantAlpha', 'WQ')
    return(rc)

### ddf = dict of DataFrame
def ddf_delete_empty_elements(ddf): #ddf=db file, dict format
    emptyCol = []
    for k in ddf.keys():
        if isinstance(ddf[k], type(None)): 
        #print(f'INFO: calc_direct delete empty column {k}')
            emptyCol.append(k)
    for k in emptyCol:
        del ddf[k]
    return(ddf)

def df_fix_duplicate_columns_new(df, debug=False):
    funcn = "df_fix_duplicate_columns_new"
    def g_v(s):
        v = np.nan
        for d in s:
            try:
                if not np.isnan(d):
                    v = d
                    break
            except Exception as e:
                #print(f"EXCEPTION: {funcn}, e= {e}")
                return d
                pass
        return v
    cols = {}
    for col in df.columns:
        if col not in cols:
            cols[col] = 0
        cols[col] = cols.get(col) + 1
    dup_cols = [k for k,v in cols.items() if v > 1]
    
    n_pre = pd.DataFrame()
    for col in dup_cols:
        if cols[col]>2:
            if debug:
                print("DEBUG_3328:", col, cols[col])

    for col in dup_cols:
        #print(f"DEBUG_0034: {funcn} col_type= {type(col)}", col)
        df_del = df.pop(col)
        df_del = df_del.T.apply(lambda x: g_v(x)).T
        # n_pre = pd.concat([n_pre,df_del], axis=1)
        n_pre.insert(loc=n_pre.shape[1], column=col, value=df_del, allow_duplicates=False)
    df = pd.concat([df,n_pre], axis=1)

    return df

# def df_fix_duplicate_columns_new(df):
#     cols = {}
#     for col in df.columns:
#         if col not in cols:
#             cols[col] = 0
#         cols[col] = cols.get(col) + 1
#     dup_cols = [k for k,v in cols.items() if v > 1]
#     for col in dup_cols:
#         df_del = df.pop(col)
#         df_dedup = df_del.loc[:, ~(df_del.isnull().any(axis=0).values)]

#         if len(df_dedup.columns) == 0:
#             df_dedup = df_del.groupby(level=0, axis=1).first()

#         df.insert(loc=df.shape[1], column=col, value=df_dedup, allow_duplicates=False)
#     return df

def df_fix_duplicate_columns(df):
    return df.T.groupby(level=0).first().T
    
def ddf_fix_duplicate_entries(ddf):
    #Fix entries with_dataframe (to covert to series),
    #or series with mutiple columns (to use the first column)
    import pandas as pd

    for k in ddf.keys():
        if type(ddf[k]) == type(pd.DataFrame()) and len(ddf[k].shape)>1:
            print(f"INFO: ddf_fix_duplicate_entries fix series with dup columns {k}, type= {type(ddf[k])}, shape= {ddf[k].shape}, len= {len(ddf[k].shape)}, columns= {ddf[k].columns}")
            for i in ddf[k].columns:
                #print('ddf_fix_entries', i, ddf[k][i].count())
                if ddf[k][i].count() > 0 or i == ddf[k].columns[-1]: #If all columns contains only NAs, then use the last column. The columns can not be made empty
                    ddf[k] = ddf[k][i]
                    break
                
        #print(k, ddf[k])
    return(ddf)

def ddf_fix_duplicate_index(ddf):
    for x in ddf.keys():
        #check for duplicate indexes
        dup = ddf[x].index[ddf[x].index.duplicated()]
        if dup.shape[0]>0:
            print('-'*20, 'duplicated entries', x, dup)
            ddf[x] = ddf[x].drop_duplicates()
    return(ddf)
    

def get_rq_factors(file):
    firstPass = [(l.split(r',')[0]) for l in open(file, 'r').readlines()[1:] if l.find(',')>=0 and l.find("#")<0]
    secondPass = []
    for x in firstPass: #handle ';' seperated flds
        secondPass.extend(x.split(r';'))
    return secondPass

def gen_ana_rec_id(r):
    if r.mdlNm == 'basic': #backward compatible
        return buf2md5(f"{r.indu}.{r.resetDt}.{r.begDt}.{r.endDt}.{r.dts_cfg}.{r.asofdate}.{r.session}.{r.Resp}.{r.Indic}")
    else:
        return buf2md5(f"{r.indu}.{r.resetDt}.{r.begDt}.{r.endDt}.{r.dts_cfg}.{r.asofdate}.{r.session}.{r.Resp}.{r.Indic}.{r.mdlNm}")

def ana2df(dta):
    dfs = []
    for (mdlNm,val) in dta.items():
        df = pd.DataFrame.from_dict(val, orient='index')
        df['mdlNm'] = mdlNm
        df.set_index(['mdlNm', df.index], inplace=True)
        df.index.names = ['mdlNm', 'indu']
        df.reset_index(inplace=True)
        dfs.append(df)
    
    df = pd.concat(dfs, axis=0)
    df['recId'] = df.apply(gen_ana_rec_id, axis=1) 
    return(df)

def dbd_delete_empty_elements(dbd, debug=False): #dbd=db file, dict format
    emptyCol = []
    for k in dbd.keys():
        if isinstance(dbd[k], type(None)): 
            if debug:
                print(f'INFO: calc_direct delete empty column {k}')
            emptyCol.append(k)
    for k in emptyCol:
        del dbd[k]
    return(dbd)

def getLineParam(ln, desc='lvl', default="0"):
    for seg in ln.split(r';'):
        if seg.find(desc)>=0:	
            return seg.split(r'=')[-1]
    
    return default

def need_update(tgt, deps, funcnUpt, dts_cfg, debug=False, chk_time=True, force=False):
    funcn = 'need_update'
    rc = False
    cfg = UpdateCfg(funcnUpt, dts_cfg)

        
    if (not os.path.exists(tgt)):
        rc = True
    elif (not os.path.isfile(tgt)):
        rc = True
    elif cfg['force'] <= 0 and not force :
        tgtTime = os.stat(tgt).st_mtime

        for dep in deps:
            if (dep == tgt) or (not os.path.exists(dep)) or (not os.path.isfile(dep)):
                continue

            # if dep.find(".trigger")>=0 and cfg['chk_trigger'] > 0:
            # 	if debug:
            # 		print(f"DEBUG: {tgt} triggerred {dep}", file=sys.stderr)
            # 	rc = True
            # CHRIS: Triggers are treated as other dependencies

            depTime = os.stat(dep).st_mtime
            
            if tgtTime < depTime: #The first dep is probably the same file as tgt, so not not use '<='
                if debug:
                    print(f"DEBUG: {tgt} older than {dep}", file=sys.stderr)
                if cfg['chk_time'] > 0 or chk_time:
                    if debug:
                        print(f"DEBUG: tgtTm= {tgtTime}, depTm= {depTime}, diff= {tgtTime - depTime}")
                    rc = True
    else:
        if debug:
            print(f"DEBUG: {tgt} forced", file=sys.stderr)
        rc = True

    # if rc: 
    # 	if debug:
    # 		print(f'INFO: updating {tgt}, funcnUpt={funcnUpt}', file=sys.stderr)
    # else:
    # 	if debug:
    # 		print(f'INFO: no-update-needed for {tgt}, dts_cfg= {dts_cfg}, funcnUpt= {funcnUpt}', file=sys.stderr)

    if debug and rc:
        print(str_minmax(f"DEBUG: {funcn}(rc={rc}) tgt= {tgt}, deps= {QpsUtil.format_arr(deps, maxShown=1)}"), file=sys.stderr)
        print(f"DEBUG: ... funcUpt= {funcnUpt}, dts_cfg={dts_cfg}, cfg= {cfg}, force= {force}, chk_time= {chk_time}", file=sys.stderr)

    if rc == True and tgt.find(f"{rootuser()}")>=0: #Can only remove qpsuser files, not qpsdata
        if os.path.exists(tgt):
            if debug:
                print(f"INFO: remove before regen, {funcnUpt} {tgt}", file=sys.stderr)
            #QpsUtil.rm([tgt], title=f"{funcn} {funcnUpt}")

    return rc

def getVarN2FldN(fn = None, reverse_lookup = False, debug=False):
    varN2fldN = {}
    if fn is None:
        fn = fn_fgen_flds_cfg('CS')

    for ln in open_readlines(fn):
        (bar, fldN) = ln.split(";")[0].split(r'/')
        varN = getLineParam(ln, desc='nm', default="NA")
        if not reverse_lookup:
            varN2fldN[varN] = fldN
        else:
            varN2fldN[fldN] = varN

    if debug:
        for k,v in list(varN2fldN.items())[:5]:
            print("DEBUG: getVarN2FldN", k, ":", v)
    return(varN2fldN)

def format_arr(arr, maxShown=3):
    arrLen = len(arr)
    if arrLen<=maxShown*3:
        return str(arr)
    else:
        return f"[{arr[0:maxShown]}...{arr[int(arrLen/2):int(arrLen/2)+maxShown]}...{arr[-maxShown:]}"

def show_align(df, align='left'):
    left_aligned_df = df.style.set_properties(**{'text-align': align})
    left_aligned_df = left_aligned_df.set_table_styles(
        [dict(selector='th', props=[('text-align', align)])]
    )
    return left_aligned_df

def show_dict(dta, orient='columns'):
    return show_align(pd.DataFrame.from_dict(dta, orient=orient), align='left')

ALL_CS_DB = None
def all_cs_db():
    global ALL_CS_DB
    if ALL_CS_DB == None:
        ALL_CS_DB = smart_load(f"{rootdata()}/config/all_CS.db", debug=True, title="all_cs_db")
    return ALL_CS_DB

def sym2rqsym(sym):
    for rqsym in [sym, f'{sym}.XSHE', f'{sym}.XSHG']:
        if rqsym in all_cs_db():
            return rqsym
    return sym


def sym2qdbsym(sym, use_last=True):
    symsets = []
    sym = sym2rqsym(sym)

    if sym not in all_cs_db():
        #return (["CS_UNKNOWN"], sym)
        return (["Unknown"], sym)
    else:
        keysSorted = sorted(all_cs_db()[sym]['industry_history'].items(), key = lambda kv: kv[1], reverse=True)
        #print(f'============== {sym}, {keysSorted}')
        if len(keysSorted) == 0:
            return (["Unknown"], sym)
        if use_last:
            #return ([keysSorted[0][0]], sym)
            return ([all_cs_db()[sym]['industry_code']], sym)
        else:
            return ([x[0] for x in keysSorted], sym)

    return (["CF_ALL"], sym)

def syms_with_multi_indu_listing(debug=False):
    syms = []
    for k in sorted(all_cs_db().keys()):
        if len(all_cs_db()[k]['industry_history'].keys())>1:
            syms.append(k)
            if debug:
                print(f"DEBUG: syms_with_multi_indu_listing {k} {all_cs_db()[k]['industry_history']}", file=sys.stderr)
    print(f"INFO: syms_with_multi_indu_listing found {len(syms)} syms")
    return(syms)

def sym2indu(sym):
    return sym2qdbsym(sym)[0][0]

def sym2sectype(sym):
    if sym.find('.XS')>=0:
        return "CS"
    elif re.match(r'^\d\d\d\d\d\d$', sym):
        return "CS"
    else:
        return "CF"

def indu2syms(induSel):
    syms = []
    for ln in open_readlines(fn_all_instru('CS')):
        if ln.find('sym')<0:
            (qdbSym, indu) = ln.split()[:2]
            if induSel == indu or induSel is None:
                syms.append(qdbSym)
    return syms

def is_stk_index(sym):
    return ''.join(re.findall(r'[A-Za-z]', sym.upper())) in ["IF","IC","IH","T","TF","TS"]

def create_dir_if_needed(fn):
    QpsUtil.mkdir([os.path.dirname(fn)])
    return fn

# def gen_mkt_trned_data(input="/mdrive/temp/che/zz500.txt,/mdrive/temp/che/hs300.txt", save=False):
# 	rt_data = {}
# 	for f in input.split(","):
# 		all_trd_day = pd.Series(index=getDayList("CN"))
# 		all_trd_day = all_trd_day[all_trd_day.index >= "20100101"]

# 		lines = sorted(open_readlines(f))
# 		for line in lines:
# 			eles = line.split()
# 			begdt = eles[0]
# 			all_trd_day[all_trd_day.index >= begdt] = eles[1]
# 		rt_data[f] = all_trd_day
# 		if save:
# 			all_trd_day.to_csv(f.replace("txt", "1.csv"))
# 			pickle.dump(all_trd_day, open(f.replace("txt", "1.pkl"), "wb"))
# 	return rt_data

def gen_mkt_trned_data(fn_fld, fn_trend="/mdrive/temp/che/zz500.txt"):
    fld = pickle.load(open(fn_fld, "rb"))
    assert type(fld) == pd.DataFrame or type(fld) == pd.Series, f"Data type error, must be DataFrame or Series"
    
    lines = sorted(open_readlines(fn_trend))
    lines = [(l.split()[0].strip(), l.split()[1].strip()) for l in lines]
    
    for dt, trend in lines:
        fld[fld.index >= dt] = trend
    # fld.to_csv("/temp/test.csv")
    return fld

def write_file_safe(outFn, data):
    tmp_path = Path(f'{outFn}.tmp')
    tmp_path.write_bytes(pickle.dumps(data))
    tmp_path.rename(outFn)

def get_stk_exch(sym): # sym must be stk,etf
    if (sym < "600000" and sym.startswith("5")) or sym >= "600000":
        exch = 1
    else:
        exch = 0
    return exch


def run_time(fn):
    @wraps(fn)
    def measure_time(*args, **kwargs):
        t1 = time.time()
        result = fn(*args, **kwargs)
        t2 = time.time()
        print(f"@timefn: {fn.__name__} took {t2 - t1: .5f} s")
        return result
    return measure_time

def sorted_glob(globStr, verbose=False):
    funcn = f"sort_glob({globStr})"
    if verbose:
        print("INFO:", funcn, file=sys.stderr)
    return sorted(glob.glob(globStr))

def glob_args(argsIn):
    argsOut = []
    for x in argsIn:
        argsOut.extend(QpsUtil.sorted_glob(x))
    return argsOut

def get_permid(sym):
    return f"{sym}.XSHG" if sym >= "600000" else f"{sym}.XSHE"

def format_bytes(size: int, precision: int = 2) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    # print(size, f"{size:.{precision}f} {units[idx]}")
    return f"{size:.{precision}f} {units[idx]}"

if __name__=='__main__':
    """ 		
        #print(getNetworkFileUsingCache("N:/FE/xyft/config/ContMktSym.txt"), file=sys.stdout)
        #copyNetworkFileUsingCache("N:/FE/xyft/config/ContMktSym.txt", "c:/temp")
        print(python_version(), file=sys.stdout)		
        
        print_error("foobar", 3, file=sys.stdout)
        print(getDefaultBn(), file=sys.stdout)
        print_error("foobar2", 4, file=sys.stdout)

        for testListname in ['NXOM', 'NXNM', 'NXP']:
            print("INFO: listname=%s, category=%s"%(testListname, listname2TacticCategory(testListname)), file=sys.stdout)

        getFundCategory2Listnames()

        print("INFO: getDayRangeList('cn', 20170911, 20170912) returns %s"%(','.join([str(x) for x in getDayRangeList("cn", 20170911, 20170912)])), file=sys.stdout)
        print("INFO: getDayRangeList('cn', 20170912, 20170912) returns %s"%(','.join([str(x) for x in getDayRangeList("cn", 20170912, 20170912)])), file=sys.stdout)
        print("INFO: getDayRangeList('cn', 20170912, 20170912, 0, 0) returns %s"%(','.join([str(x) for x in getDayRangeList("cn", 20170912, 20170912, -1, 2)])), file=sys.stdout)
        print("INFO: getDayRangeList('cn', 20150105, 20170912, 0, 0) returns %s"%(','.join([str(x) for x in getDayRangeList("cn", "20150105", "20170912", 0, 0)])), file=sys.stdout)

        print( "Test splitBnListname:  %s"%("=".join(splitBnListname("0NXDP_TC949cd9"))), file=sys.stdout)
        print( "Test splitBnListname:  %s"%("=".join(splitBnListname("10XDP_TC949cd9"))), file=sys.stdout)
        print( "Test splitBnListname:  %s"%("=".join(splitBnListname("XDP_TC949cd9"))), file=sys.stdout)

        dir_chain("h:/FE_dev.v6.DEV/src/cmn/hrt")

        print(getuser(), file=sys.stdout)
        print(getuser(), file=sys.stdout)

        print(is_junction_dir("h:/FE_dev.v6.DEV/src"), file=sys.stdout)
        print(is_junction_dir("h:/FE_dev.v6.DEV/src/cmn"), file=sys.stdout)
        print(is_junction_dir("h:/FE_dev.v6.DEV/src/cmn/hrt"), file=sys.stdout)
        print(gettoday(), file=sys.stdout)

        #print_help("c:/Python27/python.exe c:/Quanpass/programs/python/strategies/scnOpt.py -h")

        get_bn_cfg(verbose=True)

        print(get_bn_cmd(toNative.getPy("/cygdrive/c/quanpass/programs/python/strategies.bn203/doVtsScn.exe")), file=sys.stdout)
        print(get_bn_cmd(toNative.getNativePath("/cygdrive/c/fe/vts.900.rel/bin/vts%s"%(".exe" if sys.platform=="win32" else ""))), file=sys.stdout)

        mkdir([toNative.getNativePath("c:/temp/test_dir")])
        create_junction_dir(toNative.getNativePath("c:/temp/test_dir_junction"), toNative.getNativePath("c:/temp/test_dir"), verbose=1)

        #print "INFO: found_condor_err = %d"%(check_condor_error("c:/fe/simu/lcai/chna/study_cfrev_opntf_w2/AA02/date^20161230.e74a_c58c/20161011.subtask"))

        #print "\n".join(open_readlines("c:/FE/stratfunds/ZS0/fund_tactic_config_NGT.frz.full"))

        testFn = toNative.getNativePath("c:/fe/simu/cli/chna/study_pair_AAxx/AA00/macro_pair8_fsm_freeze.FZ_bn206.SP_L.FC_4e94.REF_cfb6bf8a.tar.gz")
        newFn = remapFreezeFn(testFn, toNative.getNativePath("c:/fe/stratalloc/cn_futr_alloc_candidates/%s~"%("TEST")))
        recoverFn = reverseFreezeFn(newFn, toNative.getNativePath("c:/fe/stratalloc/cn_futr_alloc_candidates/%s~"%("TEST")))
        if (testFn != recoverFn):
            print(recoverFn, file=sys.stdout)

        run_cmd("%s %s %s"%(toNative.getCmdBin("cp"),toNative.getNativePath("c:/temp/test.txt"), toNative.getNativePath("c:/temp/test2.txt")), quiet=1, debug=1)

        print("INFO: getLastLimtPriceDate %s"%(getLastLimtPriceDate()), file=sys.stdout)

        subtaskDir = toNative.getNativePath("c:/fe/simu/lcai/chna/study_nsrev_regtime_speedup2_seg2_delist/AA00/date^20180330.1d42_58b0/20180102.subtask")
        print("INFO: %s isNightSessionTactic %s"%(subtaskDir, isNightSessionTactic(subtaskDir)), file=sys.stdout)
    """
    try:
        print(gen_mkt_trned_data(f"{rootuser()}/che/data_rq.20100101_uptodate/CS/SN_CS_DAY/A_20100101_uptodate/1d/Pred_OHLCV_C_1d_1/E47.db"))
        print("INFO:  getexchdate('CN', today='20180221') = %s, prevNextBusinessDay=%s"%(getexchdate('CN', today='20180221'), " ".join(getPrevNextBusinessDay())), file=sys.stdout)
        print("INFO:  getexchdate('US', today='20180221') = %s, prevNextBusinessDay=%s"%(getexchdate('US', today='20180221'), " ".join(getPrevNextBusinessDay(today=getexchdate("US"), region="US", ))), file=sys.stdout)
        print("INFO:  getexchdate('US', today='20180221') = %s, prevNextBusinessDay=%s"%(getexchdate('US'), " ".join(getPrevNextBusinessDay(today=getexchdate("US"), region="US", ))), file=sys.stdout)
        print("INFO:  getexchdate('US', today='20180221') = %s, prevNextBusinessDay=%s"%(getexchdate('CN'), " ".join(getPrevNextBusinessDay(today=getexchdate("CN"), region="CN", ))), file=sys.stdout)

        print(idxdt2trddt('20190115', '00:01:00.000'), file=sys.stdout)
        print(idxdt2trddt('20190115', '09:00:00.000'), file=sys.stdout)
        print(idxdt2trddt('20190115', '15:00:00.000'), file=sys.stdout)
        print(idxdt2trddt('20190115', '21:00:00.000'), file=sys.stdout)

        print(trddt2idxdt('20190115', '00:01:00.000'), file=sys.stdout)
        print(trddt2idxdt('20190115', '9:00:00.000'), file=sys.stdout)
        print(trddt2idxdt('20190115', '15:00:00.000'), file=sys.stdout)
        print(trddt2idxdt('20190115', '21:00:00.000'), file=sys.stdout)
        print(trddt2idxdt(20190118, "16:00:00.000"), file=sys.stdout)
        print("Found dates in str: %s"%(",".join(find_dates_in_str("R:/cloudvts/simu/che/chna/study_trd_cross_day_example03/AA00/date^20190329.ef8d_8cd4/20190115.subtask/20190115.pos"))), file=sys.stdout)

        print("getexchdate('CN')  %s"%(getexchdate('CN')), file=sys.stdout)
        print("getexchdate('US')  %s"%(getexchdate('US')), file=sys.stdout)

        print("asofdate=%s, prevdate=%s, tradedate=%s, nextdate=%s"%(
            getDatesCfg("asofdate"), getDatesCfg("prevdate"), 
            getDatesCfg("tradedate"), getDatesCfg("nextdate")), file=sys.stdout)

#		fn = "C:/Quanpass/programs/python/regtests/EM_missing_iid.PNG"
#		print("file md5 %s %s %s"%(fn, file2md5(fn), getFileMd5(fn)), file=sys.stdout)

        print(user_dir("che"))
    
        print('\nrq_balancesheet_factors.csv: '.join(get_rq_factors(file='/Fairedge_dev/app_QpsData_RQ/rq_balancesheet_factors.csv')))
        print('\nrq_momentum_factors.csv: '.join(get_rq_factors(file='/Fairedge_dev/app_QpsData_RQ/rq_momentum_factors.csv')))

        getVarN2FldN()
        getVarN2FldN(reverse_lookup=True)

        s_min = str_minmax(f"{dd('usr')}/CS/SN_CS_DAY/prod1w_20210810_20211110/1d/Pred_OHLCV_O_1d_1/I65.db")
        s_max = str_minmax(s_min, max=True)
        print(f"str_minmax: {s_min} {s_max}")

        syms_with_multi_indu_listing(debug=True)

        for use_last in [True, False]:
            for sym in ['688256.XSHG', '300919.XSHE', '603477.XSHG', '002212.XSHE', '301168.XSHE', '600539.XSHG']:
                print(f'sym2qdbsym({sym}) {sym2qdbsym(sym, use_last)}, indu={sym2indu(sym)}')

    except Exception as e:
        print(e)



