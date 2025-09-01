import sys

import os
os.environ['NUMEXPR_MAX_THREADS'] = '16'
os.environ['NUMEXPR_NUM_THREADS'] = '8'

import pandas as pd
from datetime import date, timedelta
from dateutil.parser import parse as dtparse
from QpsDatetime import utc2tic
from functools import wraps
import time
from QpsUtil import mkdir
from common_colors import *
from common_symsets import *
#from CTX import *

verbose = 1
def set_verbose(v):
    global verbose
    verbose = v

def qps_print(*args, **kwargs):
    doPrint = True
    if 'always' in kwargs:
        if kwargs['always'] == True:
            doPrint = True
        del kwargs['always']
    if 'quiet' in kwargs:
        if kwargs['quiet'] == False:
            doPrint = True
        del kwargs['quiet'] 

    global verbose
    if doPrint or verbose:
        print(*args, **kwargs)

def quotes(title, marker='=', length=3):
    return f'{marker*length}{title}{marker*length}'

def dtStr(dt, offset=0):
    if (dt.find('fullhist') >= 0):
        return dt
    dtStr = ('%s' % (dtparse(dt)+timedelta(days=offset))).split()[0]
    return (dtStr)

def list2str(arr, rows=2):
    length = len(arr)
    if length<3*rows:
        return(f'{arr}')
    else:
        mid = int(length/2-1)
        return(f'{arr[:rows]} ... {arr[mid:mid+2]} ... {arr[-rows:]} [len {length}]')

def config_print_df():
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 150)
    pd.set_option('display.max_rows', None)
    pd.options.display.float_format = '{:10.6f}'.format

def stringfy(l):
    return([str(x) for x in l])

def print_df(df, rows=3, cols=10, title='NA', debug=False, show_head=True, show_body=False, show_tail=True, file=sys.stdout, **kwargs):
    if False:
        qps_print(f'print_df Title({title}), type({str(type(df))}), {hex(id(df))}', file=file, **kwargs)
    else:
        qps_print(f'print_df Title({BLUE}{title}{NC}), type({str(type(df))})', file=file, **kwargs)

    config_print_df()
 
    if debug:
        qps_print(inspect.stack()[0], **kwargs)
        qps_print(inspect.stack()[1], **kwargs)

    notEmpty = False

    if (type(df) == type(dict())) or (str(type(df)) == "<class 'collections.defaultdict'>"):
        #assert False, print(df)

        for (k,v) in df.items():
            if debug:
                print(f'====================, {type(v)}')
            if type(v) == type(dict()):
                qps_print("INFO: print_df <dict> ", k, ':', v)
                continue
            else:
                if v is not None:
                    if f"{type(v)}".find('datetime')>=0:
                        print(v)
                    else:
                        print_df(v, rows, title=f'{title} k={k}', debug=False, file=file, **kwargs)
                    notEmpty = True
        return notEmpty

    elif type(df) == type(None):
        pass

    elif type(df) == type(list()):
        length = len(df)
        if length < 2 * rows:
            qps_print(df, file=file, **kwargs)
        else:
            if show_head:
                qps_print('\n'.join(stringfy(df[:rows])), file=file, **kwargs)
                qps_print('......', file=file, **kwargs)
            if show_body:
                qps_print('\n'.join(stringfy(df[int((length-rows)/2):int((length+rows)/2)])), file=file, **kwargs)
                qps_print('......', file=file, **kwargs)
            if show_tail:
                qps_print('\n'.join(stringfy(df[-rows:])), file=file, **kwargs)

    elif str(type(df)) == "<class 'pandas.core.series.Series'>":
        length = df.size
        if show_head:
            qps_print(df.head(rows), file=file, **kwargs)
            qps_print('......', file=file, **kwargs)
        if show_body:
            qps_print(df.iloc[int((length-rows)/2):int((length+rows)/2)], file=file, **kwargs)
            qps_print('......', file=file, **kwargs)
        if show_tail:
            qps_print(df.tail(rows), file=file, **kwargs)

    elif str(type(df)) == "<class 'rqdatac.services.basic.Instrument'>" or \
        str(type(df)) == "<class 'pandas.core.indexes.datetimes.DatetimeIndex'>":
        qps_print(df, file=file, **kwargs)

    else:
        #qps_print(df.shape)
        length = df.shape[0]
        width = df.shape[1]
        if width > cols:
            if debug:
                qps_print(f'Truncating columns: {list2str(list(df.columns))}')
            df = df[df.columns[:cols]]
        
        if df.shape[0]>rows*3:
            if show_head:
                qps_print(df.head(rows), file=file, **kwargs)
                qps_print('......', file=file, **kwargs)
            if show_body:
                qps_print(df.iloc[int((length-rows)/2):int((length+rows)/2)], file=file, **kwargs)
                qps_print('......', file=file, **kwargs)
            if show_tail:
                qps_print(df.tail(rows), file=file, **kwargs)
        else:
            qps_print(df, file=file, **kwargs)

    return True

def get_chk_dtm(name=None):
    dtsList = []
    if name == 'rschhist':
        dtsList.append(('20150629', '20150713'))
        dtsList.append(('20180726', '20180802'))
    else:
        dtsList.append(('20210325', '20210402'))

    dtmList = []
    for begDt, endDt in dtsList:
        (begDtm, endDtm) = (dtparse(f'{begDt} 0:00'), dtparse(f'{endDt} 0:00'))
        dtmList.append((begDtm, endDtm, f"{begDt}_{endDt}"))

    return dtmList

def get_chk_pmids(name=None):
    futs = ['A', 'AP', 'CU', 'HC', 'RB']
    stks = ['600600.XSHG']
    return futs + stks

def print_df_with_chk(df, fn, chkFilePathFixList=[], dtsList=['default'], pmidLists=['default']):
    #print_df(df, rows=10000000, file=open(fn, 'w'))
    print_df(df, rows=10000000, file=open(fn, 'w'))
    chked = False

    for name in pmidLists:
        for pmid in get_chk_pmids(name):
            if fn.find(f'.{pmid}.') >= 0:
                chked = True
    if not chked:
        return

    chkRoot = "/qpsdata/rpts/chks"
    fp = f"{chkRoot}/{fn}"
    for (i,o) in chkFilePathFixList:
        #print(f"/{i}/", f"/{o}/")
        fp = fp.replace(f"/{i}/", f"/{o}/")

    if not os.path.exists(os.path.dirname(fp)):
        mkdir([os.path.dirname(fp)])

    for name in dtsList:
        for (begDtm, endDtm, tag) in get_chk_dtm(name):
            fp = fp.replace('.txt', f'.{tag}.txt')
            filteredIndex = (df.index>begDtm) & (df.index<=endDtm)
            cnt = len(filteredIndex[filteredIndex==True])
            if cnt>0:
                print(df[filteredIndex], file=open(fp, 'w'))
                print(f'INFO: print_chk {fp}, {cnt} lines')

def transposeTailRowAsCol(df, name, row=1):
    ndf = df.tail(row).transpose()
    ndf.columns = [name]
    return(ndf)

def filterBySymset(symset, pld=None, debug=False):
    if symset == None:
        return (pld)
    res = {k: pld[k] for k in symset if k in pld}
    return (res)


def getColVal(cols, pld=None):
    if pld:
        return pld[cols]
    else:
        return (pld)


def getMainCtr(symset, pld=None, debug=False):
    res = {k: pld[k] for k in symset}
    return (res)


def getContCtr(symset, pld=None):
    res = sorted([x + '888' for x in symset])
    return (res)

def isFutPmid(pmid):
    return not isStkPmid(pmid)

def isStkPmid(pmid):
    return (pmid.find('XSHE')>=0 or pmid.find('XSHG')>=0 or pmid.find('CS_ALL')>=0)

def fldnm2bar(fldNm):
    bar = '1d'
    if fldNm.find('5m')>=0:
        bar = '5m'
    elif fldNm.find('1m')>=0:
        bar = '1m'
    return(bar)

def fp2pmid(fp):
    return (fp.split('/')[-1]).split('.db')[0]

def fp2sectype(fp):
    elems = fp.split('/')
    if len(elems)<=4:
        return "UNKNOWN"

    sectype = elems[4]
    if sectype in cnstk_indus():
        return "stocks"
    else:
        return 'futures'

def pmid2mkt_rq(pmid):
    if pmid.find('.X')>=0:
        return(pmid)
    elif pmid.find('CF_')>=0:
        return(pmid)
    elif pmid.find('CS_')>=0:
        return(pmid)
    elif len(pmid)<4:
        return pmid
    else:
        mkt = pmid[:-4]
        if pmid.find('888')>=0:
            mkt = pmid[:-3]
        return(mkt)

def getCtrData(symset, pld=None, debug=False):
    res = {}
    if debug and pld:
        qps_print('getCtrData pld', pld.keys())
    if pld == None:
        return (res)

    for k in symset:
        if debug:
            qps_print(f'getCtrData for {k}')
        if k in pld.keys():
            res[k] = pld[k]
            if type(pld[k]) != type(None):
                if debug:
                    qps_print(f'Found ctrData data for {k} {res[k].shape} ')
            else:
                if debug:
                    qps_print(f'Found ctrData data for {k} <None> ')

    qps_print(res)
    return (res)


def makeTs(row):
    # qps_print(row)
    return utc2tic(row['datetime'].tz_localize('Asia/Shanghai'))


def chkTs(row):
    return tic2utc(row['src_utc_ts'])

def unstack(symset, pld):
    return pld.unstack(level=0)

def generator_print(gen, printrows=10, details=False, debug=False):
    for ii in gen:
        if printrows:
            if details:
                qps_print(f'generator_print_detail Found {ii.__dict__}')
            else:
                if debug:
                    qps_print(f'generator_print Found {ii}')
            printrows -= 1

def sft2str(sft):
    if sft<=0:
        return f'fwd{-sft}'
    else:
        return f'lag{sft}'

def add_mkt_pmid_cols(pmid, v, debug=False):
    # v.loc[:, 'mkt'] = pmid2mkt_rq(pmid)
    # v.loc[:, 'pmid'] = pmid
    v['mkt'] = pmid2mkt_rq(pmid)
    v['pmid'] = pmid   
    if debug:
        qps_print(f'+++++++++++++++++++++++ {pmid}, {v.tail(10)} +++++++++++++++')

def open_ref(fn, mode):
    qps_print(f'open_ref {fn}', file=sys.stderr)
    return open(fn, mode)

def cnExchanges():
    exch2name = {
        "SSE":'上证所',
        "SZSE": '深交所',
		"CFFEX": '中金所',
		"DCE": '大商所',
		"CZCE":	'郑商所',
		"SHFE": '上期所'
        }
    return exch2name

def cnExch2Name(exch):
    return cnExchanges()[exch]

def cnStkSegment():
    stkSeg2name = {
        '002': '中小板',
        '300': '创业板',
        '600': '上交所',
        '200': 'B股',
        '000': '深交所',
        '688': '科创板'
    }
    return stkSeg2name

def exch2code(nm):
    if nm == 'XSHE':
        return '2'
    elif nm == 'XSHG':
        return '1'
    else:
        assert False, f'Error: exch2code unknown arg {nm}'

def combine_FU(fns, axis=1, unstack=False, debug=False): #Unstacked dataframe
    vs = []
    for f in fns:
        if debug:
            qps_print(f'combine_FU {f}')
        v = f.load()
        vs.append(v)
    df = pd.concat(vs, axis=axis)
    if unstack:
        df = df.unstack(level=1)
    return(df)

def combine_FS(fns, axis=1, unstack=True, debug=False, show_warning=False): #Stacked dataframe
    funcn = "combine_FS"
    if debug:
        print(f"DEBUG_8712: {funcn}", '\n'.join([f.fp() for f in fns]))
    vs = []
    empty_cnt = 0
    for f in fns:
        v = f.load()
        if type(v) == type(None):
            empty_cnt = empty_cnt + 1
            if debug:
                qps_print(f'{CYAN}WARNING: combine_FS empty file {f.fp()}{NC}')
            continue
        if unstack:
            if not v.index.is_unique:
                print(f"{CYAN}WARNING: combine_FS index uniq failed, drop duplicates: {f.fp()}{NC}", file=sys.stderr)
                #print(v[v.index.duplicated(keep='last')])
                v = v[~v.index.duplicated(keep='last')]
            v = v.unstack(level=0)
        vs.append(v)

    if empty_cnt > 0:
        print(f"{CYAN}WARNING: {funcn} found {empty_cnt} empty files out of {len(fns)}{NC}")

    if len(vs)>0:
        df = pd.concat(vs, axis=axis)
    else:
        df = pd.DataFrame()

    return(df)

def combine_DS(fns, debug=False): #dict of series
    if 1:
        qps_print('@'* 30, 'combine_DFS', fns)
    df = pd.DataFrame()
    for f in fns:
        pld = f.load()
        for k,v in pld.items():
            if debug:
                print_df(df)
            df[k] = v
    return (df)

def df_filter_by_dates(pld, begDt, endDt, data_type='db', bar='1d'):
    if 0:
        if data_type != 'db':
            return pld 

        (begDtm, endDtm) = (dtparse(f'{begDt} 00:00:00'), dtparse(f'{endDt} 23:59:59') + pd.Timedelta(1, unit='day')) #Add one extra day to include current day for real trading
        if type(pld) == type(pd.DataFrame()):
            print('xxxxxxxxxxxxxxxxxxxxxxxxxx filter Dataframe', file=sys.stderr)
            if bar != '1d':
                return pld.loc[(pld.index >= begDtm) & (pld.index <= endDtm)].between_time('9:00', '23:00')
            else:
                return pld.loc[(pld.index >= begDtm) & (pld.index <= endDtm)]
        elif type(pld) == type(pd.Series()):
            return pld
            print('xxxxxxxxxxxxxxxxxxxxxxxxxx filter Series', file=sys.stderr)
            if bar != '1d':
                print(type(pld.index), file=sys.stderr)
                print(pld.head(), file=sys.stderr)
                return pld.loc['NI'].between_time('9:00', '23:00')
                print(pld, file=sys.stderr)
                pld = pld.reset_index(level='mkt')
                print(pld.head(), file=sys.stderr)
                print(type(pld.index))
                #if str(type(pld.index)) == "<class 'pandas.core.indexes.multi.MultiIndex'>":
                if str(type(pld.index)) == "<class 'pandas.core.indexes.datetimes.DatetimeIndex'>":
                    print('xxxxxxxxxxxxxxxxxxxxxxxxxx filter Series do it', file=sys.stderr)
                    pld = pld.between_time('9:00', '23:00')   
                print(pld.head(), file=sys.stderr)
    
    return pld    

def set_hhmm(pld):
    def func(row):
        #qps_print(type(row), '=', row.name.strftime("%H%M"))
        return (row.name.strftime("%H%M"))
    #pld['hhmm'] = pld.apply(func, axis=1)
    pld.loc[:,'hhmm'] = pld.apply(func, axis=1)
    return(pld)

def flip_hhmm_row_sign(pld, hhmm):
    def func(row, hhmm=hhmm):
        row_hhmm = (row.name.strftime("%H%M"))
        if row_hhmm == hhmm:
            #print('before', row)
            for i, item in enumerate(row):
                row[i] = -row[i]
            #print('after', row)
        return(row)
    #pld['hhmm'] = pld.apply(func, axis=1)
    pld = pld.apply(func, axis=1)
    return(pld)


def set_yyyymmdd(pld):
    def func(row):
        #qps_print(type(row), '=', row.name.strftime("%H%M"))
        return (row.name.strftime("%Y%m%d"))
    pld.loc[:,'yyyymmdd'] = pld.apply(func, axis=1)
    #pld['yyyymmdd'] = pld.apply(func, axis=1)
    return(pld)

def set_dayofweek(pld):
    def func(row):
        #qps_print(type(row), '=', row.name.strftime("%H%M"))
        return (row.name.dayofweek+1)
    pld['d_o_w'] = pld.apply(func, axis=1)
    return(pld)

def set_index_hhmm_yyyymmdd(pld):
    pld = set_hhmm(pld)
    pld = set_yyyymmdd(pld)
    pld.reset_index(inplace=True)
    pld.drop(['datetime'], axis=1, inplace=True)
    pld.set_index(['hhmm', 'yyyymmdd'], inplace=True)
    return pld

def print_dict_fmt(fmt, d):
    for k,v in d.items():
        print(fmt%(k,v))
