import sys

import re,math,os
import pickle
from copy import deepcopy
import rqdatac as rc
import pandas as pd
from matplotlib import pyplot
import glob
import QpsSys
from QpsDate import getDatesCfg, getPrevBusinessDay
from filelock import Timeout, FileLock
import logging
from shared_cmn import *
from dateutil.parser import parse as dtparse
from QpsUtil import smart_load, syms_with_multi_indu_listing, cvt2businessdate
from QDFile import QDFile
import traceback
from common_basic import *
from CTX import *

#RQ_INIT = 0
def has_quota(opt = None, debug=False):
    funcn = "cache_or_run.has_quota"
    if opt is not None:
        assert(opt.pre2date != "") or f"has_quota must specify pre2date"
        assert(opt.tradedate != "") or f"has_quota must specify tradedate"
    import rq_init as rq_init
    if not rc.initialized():
        rc.init(rq_init.rq_username, rq_init.rq_password)
        if ctx_debug(1):
            print(f"INFO: {funcn} rq_user= {rq_init.rq_username} rq_pwd= {rq_init.rq_password}", file=sys.stderr)
        #print(f"INFO: rq_pwd {rq_init.rq_password}")
    # global RQ_INIT
    # if RQ_INIT == 0:
    #     import rq_init
    #     rc.init(rq_init.rq_username, rq_init.rq_password)
    #     RQ_INIT = 1

    pmids = ['000628.XSHE','600939.XSHG']
    #facLst = ['PCNT', 'VOL3', 'WorldQuant_alpha001', 'WorldQuant_alpha002']
    facLst = ['pe_ratio', 'pe_ratio_1', 'pe_ratio_2', 'ps_ratio']
    #df = rc.get_factor(pmids, facLst, opt.pre2date, opt.tradedate)
    df = rc.get_factor(pmids, facLst, '20210831', '20210903')
    if ctx_debug(1):
        df.sort_index(level=0, ascending=True, inplace=True)
        print(funcn, df)
        print(funcn, df.shape)
    assert (df.shape == (8, 4)), "ERROR: RiceQuant quota exceeded"
    return True

def has_check_date(df, pmid, chkDtIn, title="", debug=False, verbose=False):
    # debug = True
    ok = False
    try:
        if chkDtIn is None:
            return False
        #assert (chkDt is not None), f"ERROR: Must specify chkDt"

        chkDt = dtparse(cvt2businessdate(chkDtIn)).date()
        funcn = f"INFO: cache_or_run.has_check_date(title={title}, chkDt= {chkDt}, pmid= {pmid})"
        if ctx_debug(1):
            print(funcn, "type=", type(df), file=sys.stderr)

        if title.find("tick")>=0 or \
            title.find("factor_expusure")>=0 or \
            title.find("ex_factor.db")>=0 or \
            title.find("dividend_info.db")>=0 or \
            title.find("factor_rq_balancesheett_factors.db")>=0 or \
            title.find("private_placement") >= 0 or \
            title.find("block_trade") >= 0 or \
            title.find("ex_factor") >= 0 or \
            title.find("get_allotment") >= 0 or \
            title.find("split.db")>=0:
            #These are sparse data
            ok = True
        elif df is None:
            return ok
        elif type(df) == type(pd.DataFrame()):
            if ctx_debug(1):
                print(f"INFO: df.indexType {type(df.index)}", file=sys.stderr)
            # print(df.tail())
            if str(type(df.index)).find("multi")>=0:
                if len(df.index)>0:
                    if title.find("factor_expusure")>=0:
                        ok = True
                    elif title.find("is_s")>=0:
                        if f"{chkDt}" in df.index.get_level_values(0):
                            ok = True
                    elif chkDt in df.index.get_level_values(1).date:
                        validCnt = (df.iloc[df.index.get_level_values(1).date == chkDt].fillna(0).sum(axis=1))[0]
                        if ctx_debug(1):
                            print(f"INFO: validCnt_multi= {validCnt}")
                        if validCnt != 0:
                            ok = True
                    else:
                        if chkDt<dtparse("20220102").date():
                            ok = True
            elif str(type(df.index)).find("DatetimeIndex")>=0:
                if len(df.index)>0:
                    # lastDt = str(df.index.date[-1]).replace('-','')
                    # if lastDt != chkDt:

                    # if chkDt in df.index.date:
                    #     ok = True

                    # if title.find("factor_expusure")>=0 or 
                    #     title.find("ex_factor.db")>=0 or 
                    #     title.find("dividend_info.db")>=0 or
                    #     title.find("factor_rq_balancesheett_factors.db">=0 or
                    #     title.find("split.db")>=0
                    #     :
                    #     #These are sparse data
                    #     ok = True
                    
                    if title.find("is_s")>=0 or title.find("exchange_date")>=0 or title.find("ex_factor")>=0:
                        if f"{chkDt}" in df.index.get_level_values(0):
                            ok = True
                    else:
                        if f"{chkDt}" not in df.index.get_level_values(0):
                            validCnt = 0
                        elif (title.find("ohlcv")>=0):
                            #print(df.iloc[-1, :-1])
                            #print(df.loc[:, ~df.columns.isin(['dominant_id', 'trading_date'])])
                            validCnt = df.loc[:, ~df.columns.isin(['dominant_id', 'trading_date'])].iloc[-1].fillna(0).sum() #skip string type columns
                        else:
                            validCnt = df.iloc[-1].fillna(0).sum()

                        if ctx_debug(1):
                            print(f"INFO: validCnt_dtm= {validCnt}")

                        if validCnt != 0.0:
                            ok = True
            else: #not datetime index data
                ok = True
        else:
            ok = True #not dateindx date

        if not ok:
            if ctx_debug(1):
                if df is not None:
                    print(df.tail(), file=sys.stderr)
            if ctx_verbose(1):
                print(f"INFO: {funcn} pmid= {pmid}, chkDtIn= {chkDtIn}, chkDt= {chkDt} missing, do downloading ...", file=sys.stderr)
    
    except Exception as e:
            qps_print(f"ERROR:\nERROR:\nERROR: {funcn} {title}, {e}", file=sys.stderr)
            traceback.print_stack()

    return ok    

def print_info(res, pmid, fn, debug_dts=[]):
    funcn = 'cache_or_run.print_info'
    if pmid in res:
        qps_print(f'{funcn} pmid= {pmid}, fn= {fn}')
        if res[pmid] is not None:
            if len(debug_dts)>0:
                for dt in debug_dts:
                    qps_print(f'{dt} {res[pmid][dt]}')
            elif type(res[pmid]) == type(pd.DataFrame()):
                if len(res[pmid])>0:
                    qps_print(res[pmid].head(5))
                    qps_print(res[pmid].tail(5))
                else:
                    qps_print('%s zero length in %s'%(pmid, fn))
            elif type(res[pmid]) == type([]):
                if len(res[pmid])> 10:
                    qps_print(res[pmid][:5])
                    qps_print('........')
                    qps_print(res[pmid][-5:])
                else:
                    qps_print(res[pmid])
            else:
                qps_print(res[pmid])
    else:
        qps_print('%s not found in %s'%(pmid, fn))

def cache_or_run(qdf, pmids, func, endDt, debug=False, debug_ids=None, load_if_exists=True, chk_all_cfgs=False, force = False, chk_delisted = False, begDt=None):
    #use chk_all_cfgs for tick data as we download data for several days, and do not need to keep download large tick data
    funcn = 'cache_or_run'
    res = {}

    #debug=True

    if ctx_verbose(1):
        print(f"DEBUG_4388: {funcn}({qdf.fp()}), chk_all_cfgs= {chk_all_cfgs}, load_if_exists= {load_if_exists}")
    endDtIn = endDt
    endDt = cvt2businessdate(endDtIn)
    #print(f"DEBUG: cache_or_run {endDtIn} {endDt}")

    if debug_ids == None:
        debug_ids=pmids[0:1]

    (rc, fp) = qdf.exists(chk_all_cfgs)
    if rc:
        if ctx_debug(1):
            print(f'DEBUG_2232: qdf {rc} {fp}', file=sys.stderr)
        if load_if_exists:
            res = smart_load(fp, title=funcn)
            if ctx_debug(1):
                print(f"INFO: {funcn} load {fp}, type= {type(res)}")
        else:
            return(res, 0)    
    chged = 0
    delisted = []
    df = None
    for pmid in pmids:
        #print("DEBUG", funcn, pmid)
        doDownload = force
        if type(res) == type(dict()):
            if pmid not in res:
                doDownload = True
            else:
                df = res[pmid]
        else:
            df = res
            #print("XXX", df.tail())

        # if df is None:
        #     doDownload = True
        # else:
        if ctx_debug(1):
            if pmid in debug_ids:
                print("DEBUG_4561", funcn, qdf.fp(), df)

        if not has_check_date(df, pmid, endDt, title=f'{funcn} pre download check {fp}'):
            doDownload = True

        if doDownload:
            try:
                df = func(pmid)
                if df is not None and debug:
                    qps_print(f'========================================================= {funcn} Getting {fp}, pmid= {pmid},  {df.tail()}')

                #CHRIS: we store the data without checking because we check if we run download again. 
                #Also, some downloads may not be critcal and better have something than nothing
                #print(df)
                # if has_check_date(df, pmid, endDt, title='post download'):
                #     res[pmid] = df
                # else:
                #     res[pmid] = None 
                if fp.find("_1m") >= 0 or fp.find("_5m")>=0:
                    if len(df.index.names)>=2:
                        if ctx_debug(1):
                            print(f"DEBUG_4598: &&&&&&&&&&&&&&&&&& {funcn} {fp}, fixing multi_index names= {df.index.names}")
                        df.reset_index(df.index.names[0], drop=True, inplace=True)

                res[pmid] = df  
                chged += 1
            except ValueError as e:
                if ctx_debug(1):
                    qps_print(f"Exception: {funcn} No data (ValueError) for pmid={pmid}, qdf= {qdf.fp()}, endDt={endDt}, exception {e}", file=sys.stderr)
                res[pmid] = None
                chged += 1
            except Exception as e:
                if ctx_debug(1):
                    qps_print(f"Exception: {funcn} No data (Exception) for pmid={pmid}, qdf= {qdf.fp()}, endDt={endDt}, exception {e}", file=sys.stderr)
                res[pmid] = None
                chged += 1

        if chk_delisted:
            delistDt = df.de_listed_date.replace('-','')
            if  delistDt.find("00000000")<0 and (begDt is not None) and delistDt < begDt:
                delisted.append(pmid)
                #print(f"INFO removing delisted {pmid}, begDt= {begDt},  instru= {df}")

    if chk_delisted:
        for el in delisted:
            pmids.remove(el)

    if chged:
        if ctx_verbose(1): # or chged>1: #chged tends to be 1 mostly for 5m/1m/tick data where data split by markets
            print(f'INFO: {funcn} writing %s, chged %d'%(qdf.fp(), chged))
            if ctx_verbose(1):
                cnt = 0
                for pmid in res.keys():
                    if cnt <= 2:
                        print("DEBUG_6239:", pmid, res[pmid])
                    cnt = cnt + 1

        qdf.dump(res, debug=debug)
        #XXX
        #smart_dump(res, qdf.fp(), debug=True)
        # print(f"DEBUG_3221: done writing {qdf.fp()}")
        # exit(0)
    return(res, chged)

def cache_or_run_new(qdf, pmids, func, begDt, endDt, frequency, adjust_type, debug=False, debug_ids=None, load_if_exists=True, chk_all_cfgs=False, force=False):
    funcn = 'cache_or_run_new'
    assert has_quota(), "ERROR: cache_or_run_new"
    res = {}

    symsWithMultiIndus = syms_with_multi_indu_listing()

    if ctx_debug(1):
        print(f"DEBUG_1127: {funcn}({qdf.fp()}), chk_all_cfgs= {chk_all_cfgs}, load_if_exists= {load_if_exists}")

    (rc, fp) = qdf.exists(chk_all_cfgs)
    if rc:
        if ctx_debug(1):
            print(f'DEBUG_7231: qdf {rc} {fp}', file=sys.stderr)
        if load_if_exists:
            res = smart_load(fp, title=funcn)
            if ctx_debug(1):
                print(f"INFO: {funcn} load {fp}, type= {type(res)}")
        else:
            return(res, 0)    
    chged = 0
    for pmid in pmids:
        #qps_print("DEBUG", pmid, res)
        doDownload = force
        if pmid not in res:
            doDownload = True
        else:
            df = res[pmid]
            if df is None:
                print(f"INFO: {funcn} missing {pmid} in {qdf.fp()}, do downloading ...")
                doDownload = True
            else:
                if not has_check_date(df, pmid, endDt, title=f'{funcn} pre download {fp}'):
                    doDownload = True

        if doDownload:
            if ctx_debug(1):
                qps_print(f'{funcn} Getting {pmid}')
            try:
                if pmid.find('888')>=0 or \
                    pmid in symsWithMultiIndus or \
                    pmid in ["600939.XSHG"]:
                    begDtTmp = '2010-01-01'
                else:
                    begDtTmp = begDt

                if begDtTmp != begDt:
                    if ctx_debug(1):
                        print(f"INFO: {funcn} {pmid: <8} begDt changed from {begDt} to {begDtTmp} ...", file=sys.stderr)
                        print(f"rc.get_price('{pmid}', start_date='{begDtTmp}', end_date='{endDt}', frequency='1d', adjust_type='pre')", file=sys.stderr)


                df = func(pmid, start_date=begDtTmp, end_date=endDt, frequency='1d', adjust_type='pre')
                #CHRIS: we store the data without checking because we check if we run download again. 
                #Also, some downloads may not be critcal and better have something than nothing
                if ctx_debug(1):
                    print(f"DEBUG: {func} {df.head(5)} {df.tail(5)}", file=sys.stderr)
                # if has_check_date(df, pmid, endDt, title='post download'):
                #     res[pmid] = df
                # else:
                #     res[pmid] = None 
                res[pmid] = df  
                chged += 1
            except ValueError as e:
                if ctx_debug(1):
                    qps_print(f"Exception: {funcn} No data (ValueError) for {pmid} qdf={qdf.fp()}, {e}", file=sys.stderr)
                res[pmid] = None
                chged += 1
            except Exception:
                if ctx_debug(1):
                    qps_print(f"Exception: {funcn} No data (Exception) for {pmid} qdf={qdf.fp()}")
                res[pmid] = None
                chged += 1
    if chged:
        if ctx_debug(1):
            print(f'INFO: {funcn} writing %s, chged %d'%(qdf.fp(), chged))
        qdf.dump(res, debug=debug)
    
    if debug_ids == None:
        debug_ids=pmids[0:1]

    if ctx_debug(1):
        #debug_ids=pmids
        for pmid in debug_ids:
            print_info(res, pmid, qdf.fp())
        
    return(res, chged)

def cache_or_run_dates(qdf, pmids, dates, func, endDt, debug=False, debug_ids=[], debug_dts=[], load_if_exists=False, chk_all_cfgs=False):
    funcn = "cache_or_run_dates"
    #assert has_quota(), "ERROR: cache_or_run_dates"
    res = {}
    (rc, fp) = qdf.exists(chk_all_cfgs)
    if rc:
        if load_if_exists:
            res = smart_load(fp, title=funcn)
        else:
            return(res, 0)

    chged = False
    for pmid in pmids:
        #qps_print("DEBUG", pmid, res)
        if pmid not in res:
            res[pmid] = {} #dict of dates
            if ctx_debug(1):
                qps_print(f'INFO: {funcn} {pmid}')

        for dt in dates:
            if dt not in res[pmid]:
                try:
                    df = func(pmid,dt)
                    if has_check_date(df, pmid, endDt, title=f'{funcn} pre download check {fp}'):
                        res[pmid][dt] = df
                        chged = True
                    else:
                        res[pmid][dt] = None
                except ValueError:
                    if ctx_debug(1):
                        qps_print(f"Exception: cache_or_run_dates No data for {pmid} {dt} qdf={qdf.fp()}", file=sys.stderr)
    if chged:
        qps_print('-------------------------------> writing %s'%(qdf.fp()))
        qdf.dump(res, debug=debug)

    if ctx_debug(1):
        debug_ids=pmids
    for pmid in debug_ids:
        print_info(res, pmid, qdf.fp(), debug_dts=debug_dts)
    
    return(res, chged)

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser(description="SmartFile")

    parser.add_option("--regtests",
					  dest="regtests",
					  type="int",
					  help="regtests (default: %default)",
					  metavar="regtests",
					  default=1)

    parser.add_option("--delete",
					  dest="delete",
					  type="int",
					  help="delete (default: %default)",
					  metavar="delete",
					  default=0)

    parser.add_option("--debug",
					  dest="debug",
					  type="int",
					  help="debug (default: %default)",
					  metavar="debug",
					  default=0)

    parser.add_option("--asofdate",
                      dest="asofdate",
                      help="asofdate (default: %default)",
                      metavar="asofdate",
                      default="download")

    opt, args = parser.parse_args()

    for fn in [
        f"{rootdata()}/data_rq.20210810_uptodate/C33/ex_factor.db", 
    ]:
        for chkDt in ['2022-01-26', '2022-01-27']:
            df = smart_load(fn)['601686.XSHG']
            print(f"INFO: fn= {fn}, chkDt= {chkDt}, dfType= {type(df)}, ok=  {has_check_date(df, '601686.XSHG', chkDt, title=fn)}")

    for fn in [
        f"{rootdata()}/data_rq.20210810_uptodate/futures/ohlcv_5m_pre/ZC888.db"
    ]:
        for chkDt in ['2022-01-26', '2022-01-28']:
            df = smart_load(fn)['ZC888']
            print(f"INFO: fn= {fn}, chkDt= {chkDt}, dfType= {type(df)}, ok=  {has_check_date(df, 'ZC888.db', chkDt, title=fn)}")

