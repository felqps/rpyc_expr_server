import sys
import numpy as np
import pandas as pd
import inspect
#from platform_helpers import *

from copy import deepcopy
from dateutil.parser import parse as dtparse

from shared_cmn import *
from InstruHelper import *
from Ops import *
from DebugCfg import *
from CTX import *

def merge_dict_mkt2df(pld, varName, daily=False, debug=False):
    vs = []
    #debug=True

    if pld is None:
        return(pld)

    for pmid,v in sorted(pld.items()):
        #if type(v) != type(None):
        if v is not None and not v.empty:
            if debug:
                qps_print(f'merge_dict_mkt2df: pmid={pmid}, var={varName}, v.shape={v.shape}')
                print_df(v, title='merge_dict_mkt2df')
            if 'mkt' not in v.columns.values.tolist():
                add_mkt_pmid_cols(pmid,v)
            vs.append(v)
        else:
            if False and debug:
                qps_print(f'merge_dict_mkt2df --> {pmid}, {v} <--')
    if len(vs)<=0:
        return(None)

    df = pd.concat(vs)
    df.reset_index(inplace=True)
    if 'mkt' not in df.columns.values.tolist():
        raise Exception('ERROR: merge_df_by_mkt need "pmid" column')

    if daily:
        df.set_index(['mkt', 'date'], inplace=True)
    else:
        df.set_index(['mkt', 'datetime'], inplace=True)

    if debug:
        print_df(df, rows=10, title='merge_dict_mkt2df')

    #check for duplicate indexes
    dup = df.index[df.index.duplicated()]
    if dup.shape[0]>0:
        qps_print('WARNING: merge_dict_mkt2df found duplicated entries', dup)

    #print('merge_dict_mkt2df', df.tail(), file=sys.stderr)
    return(df.loc[:, varName])

def apply_func_pmid_df(func, pld, debug=False): #v is a dataframe
    if pld is not None:
        for pmid,v in sorted(pld.items()):
            func(pmid, v, debug=debug)
            if pmid == 'NI2104' and debug:
                print_df(v, title=f'apply_func_pmid_df({pmid}) addr(v)={hex(id(v))} addr(pld)={hex(id(pld))}', file=sys.stderr)
    return(pld)

def shift_dtm(res, bar, sft=-1, debug=False):
    debug=False
    if debug:
        print_df(res, rows=10, title=f'------------------- before shift_dtm {bar}---------------')

    if bar.find('m')>=0:
        #print('before:', res.index, res)
        res.index = res.index + pd.DateOffset(minutes=sft * int(bar.replace('m','')))
        #print('after:', res.index, res)
    else: #assumming daily
        res.index = res.index + pd.DateOffset(days=sft * int(bar.replace('d','')))

    if debug:
        print_df(res, rows=10,  title=f'------------------ after shift_dtm {bar}-------------')    
    return res

def opsgen_return(qdfRoot=None, ssn=None, funcName='lnret', funcArgs='C2C', 
        fldType='Resp', bar='5m', period=1, debug=False, sessions=None):
    ops = Ops()
    ops.c2name = {'C': 'close', 'O': 'open', 'H': 'high', 'L': 'low'}
    ops.qdfRoot=qdfRoot
    ops.ssn = ssn
    ops.varName = f'{fldType}_{funcName}_{funcArgs}_{bar}_{period}'
    ops.period = period
    ops.bar = bar
    ops.debug = debug
    ops.fldType = fldType
    ops.funcName = funcName
    ops.funcArgs = funcArgs

    if DebugCfg()['opsgen_return']['trace']:
        qps_print('============ opsgen_return ============', ops)

    def func(k, v, ops=ops, debug=debug, sessions=sessions): #, varName=ops.varName, funcName=ops.funcName, funcArgs=ops.funcArgs, 
        #opsType=ops.fldType, c2name=ops.c2name, period=period, debug=debug):

        if isinstance(v, type(None)):
            return

        (frm, to) = ops.funcArgs.split(r'2')
        frm = ops.c2name[frm]
        to = ops.c2name[to]

        add_mkt_pmid_cols(k,v)

        tmpFr = f'tmpFr_{ops.varName}'
        tmpTo = f'tmpTo_{ops.varName}'
        period = ops.period

        #Resp shift tmpTo and Pred shift 'frm'. 
        #This is because Resp is timed to start of the period, and Pred is timed to end of period

        if ops.is_pred(): #Pred
            if ops.funcArgs == 'O2C':
                #1-period open already on the bar
                if period > 1:
                    v.loc[:, tmpFr] = v[frm].shift(period-1)
                else:
                    v.loc[:, tmpFr] = v[frm]
            elif ops.funcArgs == 'C2C':
                    v.loc[:, tmpFr] = v[frm].shift(period)
            elif ops.funcArgs == 'C2O':
                    v.loc[:, tmpFr] = v[frm].shift(period)
            elif ops.funcArgs == 'O2O':
                    v.loc[:, tmpFr] = v[frm].shift(period)

            v.loc[:, tmpTo] = v[to]

            if False:
                print(f'>>>>> opsgen_return {ops.funcArgs} {frm} {to}')
                print('>>>>>', v.tail(10))
        else: #Resp
            v.loc[:, tmpFr] = v[frm]
            if ops.funcArgs == 'O2C':
                # if sessions is not None:
                #     v = sessions.sft_by(v, bar=ops.bar, num=-1)
                if period > 1:
                    v.loc[:, tmpTo] = v[to].shift(1-period)
                else:
                    v.loc[:, tmpTo] = v[to]

            elif ops.funcArgs == 'C2C':
                v.loc[:, tmpTo] = v[to].shift(-period)
            elif ops.funcArgs == 'C2O':
                v.loc[:, tmpTo] = v[to].shift(-period)
         
        if funcName == 'lnret': 
            v.loc[:, ops.varName] = np.log(v[tmpTo]/v[tmpFr])
        elif funcName == 'simpleret':
            v.loc[:, ops.varName] = (v[tmpTo]/v[tmpFr]) - 1.0

        v.drop(columns=[tmpFr, tmpTo], inplace=True)

        # if DebugCfg()['opsgen_return']['trace']:
        #     qps_print(f'opsgen_return {k} {frm} -> {to} : {ops.varName}', always=True)

        # if pmid2mkt_rq(k) in DebugCfg()["opsgen_return"]['always'] or DebugCfg()["opsgen_return"]['default']:  #Always print out A for data checking
        #     print_df(v, always=True, title='opsgen_return')

        if ops.bar == '1d': #daily bar is off by 1 bar?
            #print('*'* 50, ops.bar, period)
            #print(v[ops.varName].tail())
            #print(v[ops.varName].shift(period).tail())
            
            #CHRIS
            pass
            #v[ops.varName] = v[ops.varName].shift(period)

        return v[ops.varName]

    ops.func = func
    return(ops)

def opsgen_average(qdfRoot=None, ssn=None, funcName='ATR', funcArgs='hlc', fldType='Pred', bar='5m', period=1, debug=False):
    ops = Ops()
    ops.qdfRoot = qdfRoot
    ops.ssn = ssn
    ops.varName = f'{fldType}_{funcName}_{funcArgs}_{bar}_{period}'
    ops.period = period
    ops.bar = bar
    ops.debug = debug
    ops.fldType = fldType

    def func(pmid, v, varName=ops.varName, debug=debug):
        if type(v) == type(None):
            return
        if v.empty:
            return

        add_mkt_pmid_cols(pmid,v)

        if ctx_debug(1): #This output is needed in chk_daily_fut_update.bash
            print_df(v, rows=5, title=f'opsgen_average {pmid}')
        
        if funcName == 'ATR': 
            v[varName] = talib.ATR(v['high'], v['low'], v['close'], timeperiod=period)        
        elif funcName == 'V': 
            v[varName] = talib.ATR(v['high'], v['low'], v['close'], timeperiod=period)/v['close']

        return v[varName]

    ops.func = func

    return(ops)


def opsgen_ohlcv(qdfRoot=None, ssn=None, scn=None, funcName='OHLCV', funcArgs='ohlcv', fldType='Pred', bar='5m', period=1, map2mkt=False, debug=False, show_warning=False):
    funcn = "opsgen_ohlcv"
    ops = Ops()
    ops.qdfRoot = qdfRoot
    ops.ssn = ssn
    ops.varName = f'{fldType}_{funcName}_{funcArgs}_{bar}_{period}'
    #ops.varName = f'{funcArgs}'
    ops.period = None
    ops.bar = bar
    ops.debug = debug
    ops.fldType = fldType
    ops.map2mkt=map2mkt
    ops.scn = scn

    def func(pmids, v, varName=ops.varName, scn=scn, map2mkt=map2mkt, debug=debug, show_warning=show_warning):
        if isinstance(v, type(None)):
            return

        # qps_print('@'*300, k)
        # if isinstance(k, list):
        #     k = k[0]
        # #add_mkt_pmid_cols(k,v)

        if type(pmids) != type([]):
            pmids = [pmids]

        if ctx_debug(5):
            qps_print(f'DEBUG_1213: opsgen_ohlcv pmids={pmids}, var={varName}, funcArgs={funcArgs}, map2mkt={map2mkt}')

        #We should not filter dates here so that calculated time-series can be as long as possible (i.e. 52 week return)
        # if scn:
        #     v = filter_by_dominant_dates(v, scn)

        res = {}
        missing_cnt = 0
        for pmid in pmids:
            if type(v) == type({}):
                if pmid not in v:
                    print(f'WARN_1235: {pmid} not in {v.keys()}')
                    continue
                else:
                    val = v[pmid] 
            else:
                val = v
        
            if type(val) == type(None):
                missing_cnt = missing_cnt + 1
                if ctx_debug(1):
                    qps_print(f'WARNING_1234: opsgen_ohlcv cannot find ohlcv data for {pmid}')
                continue

            if len(val.index.names)>=2:
                if ctx_verbose(1):
                    print(f"WARNING_1236: {funcn} var= {varName}, fixing multi_index names= {val.index.names}")
                val.reset_index(val.index.names[0], drop=True, inplace=True)

            newK = pmid
            if map2mkt:
                newK = pmid2mkt_rq(pmid)
                qps_print(f'WARNING_1237: {funcn} opsgen_ohlcv map2mkt {pmid} to {newK}')

            assert newK not in res, f'ERROR: map2mkt encounter duplicated mkts {pmid} => {newK}'


            (scnBegDtm, scnEndDtm) = (dtparse(f'{scn.begDt} 00:00:00') - pd.Timedelta(126, unit='day'),
                dtparse(f'{scn.endDt} 23:59:59') + pd.Timedelta(1, unit='day')) #Add one extra day to include current day for real trading
            
            #print(type(val), file=sys.stderr)
            if False:
                pass
            elif bar != '1d':
                if ctx_debug(5):
                    print(f'DEBUG: {funcn} {varName} {bar}', scn.dtaBegDtmWithLead, scn.dtaEndDtm, ', scn sesn beg/endTm:', scn.sesn.begTm, scn.sesn.endTm, file=sys.stderr)
                keep = val[scn.dtaBegDtmWithLead:scn.dtaEndDtm].between_time(scn.sesn.begTm, scn.sesn.endTm)
                #print('keep', keep.index, file=sys.stderr)
                #print('remove', val.index[~val.index.isin(keep.index)], file=sys.stderr)
                val.drop(index=val.index[~val.index.isin(keep.index)], inplace=True)
            else:
                if funcArgs != 'close' and funcArgs != 'c':
                    if debug:
                        print(f'DEBUG: {varName} {bar}', scn.dtaBegDtmWithLead.date(), scn.dtaEndDtm.date(), file=sys.stderr)
                    keep = val[(val.index >= scn.dtaBegDtmWithLead) & (val.index <= scn.dtaEndDtm)]
                    val.drop(index=val.index[~val.index.isin(keep.index)], inplace=True)
                    #val = val.loc[scnBegDtm.date():scnEndDtm.date()]
                    #print('===', val.tail(), file=sys.stderr)


            if funcArgs == 'ohlcv':
                #res[newK] = val[['open', 'high', 'low', 'close', 'volume']]
                cpy = deepcopy(val[['open', 'high', 'low', 'close', 'volume']])
                res[newK] = cpy #create a copy, instead a slice/view of original
                # if pmid == 'NI2104':
                #     print("NEW_DEBUG val", pmid, hex(id(val)), val.tail(5), file=sys.stderr)
                #     print("NEW_DEBUG cpy", pmid, hex(id(cpy)), cpy.tail(5), file=sys.stderr)

            elif funcArgs == 'oc':
                res[newK] = deepcopy(val[['open', 'close']]) 
            elif funcArgs == 'close':
                val[varName] = deepcopy(val['close'])
                res[newK] = val[varName] #A series for single column
            elif funcArgs in ['c', 'C', 'uC', 'vC']:
                val[varName] = deepcopy(val[['close']])
                res[newK] = val #A series for single column
                #print(f'res[{newK}]', res[newK].tail(), file=sys.stderr)
                #print('val', val.tail(), file=sys.stderr)
            elif funcArgs in ['o', 'O', 'uO', 'vO']:
                val[varName] = deepcopy(val[['open']])
                res[newK] = val #A series for single column
            elif funcArgs in ['h', 'H', 'uH', 'vH']:
                val[varName] = deepcopy(val[['high']])
                res[newK] = val #A series for single column
            elif funcArgs in ['l', 'L', 'uL', 'vL']:
                val[varName] = deepcopy(val[['low']])
                res[newK] = val #A series for single column
            elif funcArgs in ['v', 'V', 'uV', 'vV']:
                val[varName] = deepcopy(val[['volume']])
                res[newK] = val #A series for single column
            else:
                pass

        return res

    ops.func = func

    return(ops)

def remove_row_by_index_value(df, dtmStrList=['2020-12-04 13:49:00', '2020-12-04 13:54:00'], dtmIndex=True, debug=False):
    for dtmStr in dtmStrList:
        if dtmIndex:
            dtm = pd.to_datetime(dtmStr)
        else:
            dtm = dtmStr
        if debug:
            for i in df.index:
                qps_print(f'{i} {dtm}')
        if dtm in df.index:
            qps_print(f'remove_row_by_index_value found error {dtmStr} {dtm}')
            df = df.drop(index=dtm)
    return df


def _filterByDominantDates(ii, df, lastEndDtm, mkt, scn, debug=False):
    funcn = '_filterByDominantDate'
    #debug=True
    if not ii.hasDominantDates():
        if debug:
            qps_print(f'ERROR: filterByDominantDates cannot find dominant_dates for {ii.permid}', file=sys.stderr)
        return None

    if (ii.get_dedomDt() == dtparse(scn._endDt).date()):
        ii.de_dominant_date = dtparse(scn._tradeDt).date()

    #print('====', ii.permid, ii.de_dominant_date)


    knownLapse = {'2019-09-30': 7,
                  '2017-09-29': 7}

    begDtm = pd.Timestamp(ii.get_domDt()) + pd.Timedelta(-3, unit='h')
    endDtm = pd.Timestamp(ii.get_dedomDt()) + pd.Timedelta(17, unit='h')
    lapseDays = 0
    if mkt in lastEndDtm:
        lapseDays = (begDtm - lastEndDtm[mkt]).days
        if lapseDays > 5:
            dt = f'{lastEndDtm[mkt]}'.split(r' ')[0]
            if dt in knownLapse and knownLapse[dt] == lapseDays:
                pass
            elif lapseDays == 7:
                pass  #probably golden weeks
            else:
                qps_print(f'{"!"*100} filterByDominantDates large lapse days: {ii.permid} lapse {lapseDays} ... last seen: {lastEndDtm[mkt]}, cur beg: {begDtm}', file=sys.stderr)
    lastEndDtm[mkt] = endDtm

    #df = remove_row_by_index_value(df, dtmStrList=['2020-12-04 13:49:00', '2020-12-04 13:54:00'])

    if debug:
        qps_print(f'filterByDominantDates: {ii.permid} domDt={ii.get_domDt()}, deDomDt={ii.get_dedomDt()} :  begDtm={begDtm}, endDtm={endDtm}, shape={df.shape}, lapse={lapseDays}')

    if debug:
        print_df(df, rows=5, title=f'DEBUG: {funcn} before dominate filter {ii.permid} [{begDtm}, {endDtm}]')

    #df = df[begDtm:endDtm]
    df = df[begDtm:endDtm].between_time(scn.sesn.begTm, scn.sesn.endTm)

    if debug:
        print_df(df, rows=5, title=f'DEBUG: {funcn} after dominate filter {ii.permid} [{begDtm}, {endDtm}]')

    return(df)


def filter_by_dominant_dates(pld, scn, varName=None, force=True, debug=False):
    if pld is None:
        return(pld)
    iis=scn.ss.iis('active')
    lastEndDtm = defaultdict(None) 
    for pmid,v in sorted(pld.items()):
        mkt = pmid2mkt_rq(pmid)
        if iis:
            if pmid not in iis:
                del pld[pmid]
            else:
                #Note for stmpToks, dominant dates are set to be beg/end of scnario dates
                if debug:
                    qps_print(f'filter_by_dominant_dates: {iis[pmid]}')
                pld[pmid] = _filterByDominantDates(iis[pmid], v, lastEndDtm, mkt, scn, debug=debug)

    return(pld)

def add_perturb(df, rngBeg=0, rngEnd=100, scale=100000000):    
    #Row iteration is very slow
    #for index in df.index[:]:
        #print('DEBUG', df.loc[index,], len(df.loc[index,]), file=sys.stderr)
        #df.loc[index,] = df.loc[index,] + [x/1000000.0 for x in range(len(df.loc[index,]))]
        #print('DEBUG2', df.loc[index,], len(df.loc[index,]), file=sys.stderr)
    #exit(0)
    
    np.random.seed(686)
    perturb = pd.DataFrame(data = np.random.randint(0,100, size=df.shape)/100000000, index=df.index, columns=df.columns)
    return df.add(perturb)

def rank_by_row(df, demean=True, debug=False):
    #add small pertubation to avoid equal rank. Note not random to repeat experiments
    df = add_perturb(df)

    df = df.rank(axis=1)
    if debug:
        qps_print(df.shape)
        print_df(df, title='rank_by_row')
    avgRank = (df.max(axis=1) + 1)/2.0 #start seq from 1, not 0
    if debug:
        print_df(avgRank, title='AvgRank')

    if demean:
        df = df.sub(avgRank, axis=0)

    if debug and demean:
        print_df(df, rows=10, title='rank_by_row(demean)')
    return(df)

def rank_to_equal_wgt(df, top=3, lsy='Y'):
    numCol = df.shape[1]
    threshold = numCol/2.0 - top
    #print(f'--------------------------- rank_to_equal_wgt threshold {threshold} {df.tail(20)}')
    df[(df>-threshold) & (df < threshold)] = 0
    df[df>=threshold] = 1.0
    df[df<=-threshold] = -1.0
    #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@", lsy)
    if lsy == 'L':
        df[df<0.0] = 0.0
    elif lsy == 'S':
        df[df>0.0] = 0.0

    return(df)

def list_qdf(varName, mktList, bar='5m', secType='futures'):
    fns = []
    for mkt in mktList:
        f = QCF()+f'{secType}'+f'{varName}' + f'{mkt}.db'
        fns.append(f)
    return(fns)


def fgen_select_col(colName, period, daily=False, debug=False):
    def func(pmids, pld, colName=colName, period=period, daily=daily, debug=debug):
        if True or debug:
            qps_print('-'*10, f'select_col({colName}) {period} daily={daily}')
        df = merge_dict_mkt2df(pld, colName, daily, debug=debug)
        return(df)

    return func

#Map func to pmid, and reduce pmid to mkt
def fgen_map_reduce(mkt, func, scn, varName, daily=False, debug=False):
    def func(pmids, pld, mkt=mkt, func=func, scn=scn, varName=varName, daily=daily, debug=debug):
        if ctx_debug(5):
            qps_print('-'*10, f'map_reduce(pmids={pmids}, pld={type(pld)}, ssn={scn.ss.name}, mkt={mkt}, '
                f'varName={varName}, daily={daily}, debug={debug})')

        pld = apply_func_pmid_df(func, pld, debug=debug) #map
        if not daily:
            #print(f'DEBUG: pre filter: addr(pld) = {hex(id(pld))}', file=sys.stderr)
            if isFutPmid(mkt):
                pld = filter_by_dominant_dates(pld, scn, varName, debug=debug)
            #print(f'DEBUG: post filter: addr(pld) = {hex(id(pld))}', file=sys.stderr)
        df = merge_dict_mkt2df(pld, varName, daily=daily, debug=False) #reduce
        return df

    return func

if __name__ == '__main__':

    pd.set_option('display.max_rows', None)

    from ExchSessions import *
    from copy import deepcopy

    bar = '5m'
    pmid='P2101'
    ssn = 'CF_TEST01'
    qdfRoot=QCF() + ssn + 'active'
    ts = ExchSessions(name='SN_CF_DNShort')

    src = QDFile(fp = dd('raw'), svrMode=True) + 'futures' + f'ohlcv_{bar}_pre' + f'{pmid}.db'

    test_dates = [('2020-11-11', '2020-11-17'), ('2020-09-25', '2020-10-13')]

    for (testBegDt, testEndDt) in test_dates:
        datesSubDir = f'test_dates_{testBegDt}_{testEndDt}'.replace('-','')
        
        testOutputRoot = f'/Fairedge_dev/app_QpsData/tests/logs/{datesSubDir}'
        mkdir([testOutputRoot])

        pld_raw = src.load()[pmid]
        pld_raw = ts._between_dates(pld_raw, testBegDt, testEndDt)
        f = open_ref(f'{testOutputRoot}/return.P2101.raw.ref', 'w')
        qps_print(pld_raw, file=f)    

        for funcArgs in ['O2C', 'C2C', 'C2O']:
            for period in [1,2]:
                for fldType in ['Pred', 'Resp']:
                    # Old calculations, deprecated.
                    # pld = deepcopy(pld_raw)
                    # fld = _opsgen_return(qdfRoot=qdfRoot, ssn=ssn, funcName='lnret', 
                    #     funcArgs=funcArgs, fldType=fldType, bar=bar, period=period, debug=False)
                    # v = fld.func(pmid, pld)
                    # f = open_ref(f'{testOutputRoot}/return.{fldType}.{pmid}.{funcArgs}.p{period}.ref', 'w')
                    # qps_print(pld, file=f)   

                    pld = deepcopy(pld_raw)
                    fld = opsgen_return(qdfRoot=qdfRoot, ssn=ssn, funcName='lnret', 
                        funcArgs=funcArgs, fldType=fldType, bar=bar, period=period, debug=True, sessions=None)
                    qps_print(fld.func)
                    v = fld.func(pmid, pld)
                    f = open_ref(f'{testOutputRoot}/return.{fldType}.{pmid}.{funcArgs}.p{period}.new.ref', 'w')
                    qps_print(pld, file=f)    

    (QDF, DataCfg) = config_qdf('all')
    fLnret = (QCF() + ssn + 'SN_CF_DNShort' + 'train_20100101_20181231' + '5m' + 'Pred_lnret_C2O_5m_1' + f'{ssn}.db')
    qps_print(fLnret.fp())
    dta = fLnret.load()
    print_df(dta, title='lnret')
    rnk = rank_by_row(dta, demean=True, debug=False)
    wgt = rank_to_equal_wgt(rnk)
    print_df(rnk.between_time('21:00', '21:00'), rows=200, show_head=False, show_body=False, title='shared_func')
    #print_df(wgt.between_time('21:00', '21:00'), rows=200, show_head=False, show_body=False)

