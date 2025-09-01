import os,sys
#from pathlib import Path
import pandas as pd 
import numpy as np 
import pickle
from dateutil.parser import parse as dtparse
from matplotlib import pyplot as plt
from fdf_helpers import print_df
from mysql_helpers import *
from fdf_logging import *
from QpsPlotly import *
from platform_helpers import *
from FDF import *
from df_helpers import zero2nan
from common_smart_load import buf2md5

TRD_RECORDS_AGG = None
def get_trd_recs(bookid):
    funcn = f"get_trd_recs({bookid})"
    if is_windows():
        return None
    global TRD_RECORDS_AGG

    if TRD_RECORDS_AGG is None:
        trdRecs = get_trade_records(bookid=bookid)['sym date time sz pri'.split()]
        print("="*300)

        print_df(trdRecs, title=f"{funcn} shape= {trdRecs.shape}")

        trdRecs['amt'] = trdRecs['sz'] * trdRecs['pri']
        TRD_RECORDS_AGG = trdRecs.groupby(['sym', 'date']).sum()
        TRD_RECORDS_AGG['pri'] = TRD_RECORDS_AGG['amt']/TRD_RECORDS_AGG['sz']
        TRD_RECORDS_AGG.reset_index(inplace=True)
        TRD_RECORDS_AGG['date'] = pd.to_datetime(TRD_RECORDS_AGG['date']) + pd.Timedelta('15:00:00')
        print_df(TRD_RECORDS_AGG, title=f"{funcn}")
    return TRD_RECORDS_AGG

def get_pos_recs(bookid):
    if is_windows():
        return None
    posRecs = get_position_records(bookid=bookid)['sym date time sz pri'.split()]
    return posRecs

def get_trd_fld(bookid, refprc, fldnm='sz', debug=False):
    if is_windows():
        return None
    if debug:
        print(f"DEBUG_3454: get_trd_fld('{bookid}')", file=sys.stderr)
    fdfRef = (refprc * 0.0).fillna(0)
    if 0:
        print_df(fdfRef)
    trdRecAgg = get_trd_recs(bookid)
    if 0:
        trdFld = trdRecAgg[['date', 'sym', fldnm]]
        trdFld.set_index(['date', 'sym'], inplace=True)
        trdFld = trdFld[fldnm].unstack('sym')
        trdFld.rename(columns=sym_to_exch_map(trdFld.columns, fdfRef.columns), inplace=True)
        trdFld = (fdfRef + trdFld).fillna(0.0)
        trdFld = trdFld.loc[fdfRef.index]
    else:
        trdFld = dbquery2fdf(trdRecAgg, refprc, fldnm, timedelta='') #timedelta='15:00:00'
        #trdFld = dbquery2fdf(trdRecAgg, refprc, fldnm, timedelta='15:00:00')

    if debug or True:
        print_df(trdFld, title=f"shape= {trdFld.shape}")
        #exit(0)
    return trdFld

def get_trd_qty(bookid, refprc):
    if is_windows():
        return pd.DataFrame()
    return get_trd_fld(bookid, refprc, fldnm='sz')


def get_trd_prc(bookid, refprc):
    if is_windows():
        return pd.DataFrame()
    return get_trd_fld(bookid, refprc, fldnm='pri')

def get_trd_amt(bookid, refprc):
    if is_windows():
        return pd.DataFrame()
    return get_trd_fld(bookid, refprc, fldnm='amt')

def get_pos_fld(bookid, refprc, fldnm='sz', debug=False):
    if is_windows():
        return None
    posFld = dbquery2fdf(get_pos_recs(bookid=bookid), refprc, fldnm)
    return posFld   

def get_open_qty(bookid, refprc):
    if is_windows():
        return None
    posSz = get_pos_fld(bookid=bookid, refprc=refprc, fldnm='sz')
    print_df(posSz, title=f"bookid={bookid}, shape={posSz.shape}")
    return posSz

#SM0263         数博增强500私募基金
#S29156        数博合鑫私募证券投资基金
#SR3479        华量数博内鑫增强私募基金
FUNDNAME_MAP = {'SM0263': 'zq500', 'S29156': 'sbhx', 'SR3479': 'sbnx'}
FUNDNAME_MAP = {'SM0263': 'zq500', 'S29156': 'sbhx', 'SR3479': 'overall'} #sbnx will be added to sbhx

def dbquery2fdf_without_refprc(df, fldnm):
    df = df[df['fund_id'].isin(['SM0263', 'S29156', 'SR3479'])]
    df.set_index(['date','fund_id'], inplace=True)
    df = df[fldnm].unstack(level=1)
    df.fillna(method='ffill', inplace=True)
    df.rename(columns=FUNDNAME_MAP, inplace=True)
    #print_df(df, rows=10, show_head=False, title=fldnm)
    return df

def calc_fund_stat_for_overall(df): #sbnx will be added to sbhx
    df = df.copy()
    df['sbhx'] = df['sbhx'] + df['overall']
    df['overall'] = df['sbhx'] + df['zq500']
    return df

def get_fund_stats(fundid='all', begdt='20230101'):
    if fundid in ["all"]:
        fundid = 'sbhx,zq500,overall'
    fund_stats = {}
    fund_stats['asset'] = dbquery2fdf_without_refprc(get_fund_netvalue_records(begdt), fldnm="net_assets")
    fund_stats['prc'] = dbquery2fdf_without_refprc(get_fund_netvalue_records(begdt), fldnm="netvalue")
    fund_stats['ret'] = fund_stats['prc']/fund_stats['prc'].shift(1)-1.0

    fund_stats['pnl'] = fund_stats['asset'] * fund_stats['ret']
    fund_stats['pnl'] = calc_fund_stat_for_overall(fund_stats['pnl'])
    fund_stats['pnlcum'] = fund_stats['pnl'].cumsum()

    fund_stats['asset_merge'] = calc_fund_stat_for_overall(fund_stats['asset'])
    fund_stats['ret_merge'] = fund_stats['pnl'] / fund_stats['asset_merge']
    fund_stats['retcum_merge'] = fund_stats['ret_merge'].cumsum()
    
    #fund_stats['prc'] = fund_stats['prc']['sbhx,zq500'.split(r',')] #overall is not correct
    fund_stats['prc']['overall'] = fund_stats['prc']['overall'] * np.nan

    for k,v in fund_stats.items():
        sel = v[fundid.split(r',')]
        print_df(sel, rows=10, show_head=False, title=k)
    return fund_stats

def plot_line(d, title='NA'):
    if type(d) == type(dict()):
        for k,v in d.items():
            px_line(v, title=f"{k}", label=k, figsize=(30, 20))
    else:
        px_line(d, title=title, label=title, figsize=(30, 20))
    return d

def get_trd_pnl(bookid, refprc, begdt_calc = '20221201', begdt_draw = '20240101', debug=True):
    funcn = f"get_trd_pnl({bookid})"
    begdt_calc = dtparse(f"{begdt_calc} 00:00:00")
    begdt_draw = dtparse(f"{begdt_draw} 00:00:00")
    trdQty = get_trd_qty(bookid, refprc).fillna(0)
    trdPrc = get_trd_prc(bookid, refprc).fillna(0)
    trdAmt = get_trd_amt(bookid, refprc).fillna(0)

    buyAmt = trdAmt[trdAmt>0].fillna(0)
    sellAmt = trdAmt[trdAmt<0].fillna(0)
    grossTrdAmt = (buyAmt - sellAmt).fillna(0)
    netTrdAmt = (buyAmt + sellAmt).fillna(0)
    trdCost = buyAmt * 0.00015 - sellAmt * 0.00065

    posQty = trdQty.cumsum()
    posAmt = posQty * refprc.fillna(0.0)

    cashflow = -trdAmt
    grossPnl = (cashflow + posAmt - posAmt.shift()).fillna(0)
    grossPnlCum = grossPnl.cumsum()

    netPnl = grossPnl - trdCost
    netPnlCum = netPnl.cumsum()
    
    daily_aggs = {}
    daily_aggs["TradeCount"] = trdQty.apply(lambda row: sum(row!=0), axis=1)
    daily_aggs["SharesTraded"] = trdQty.apply(lambda row: sum(row.abs()), axis=1)
    daily_aggs["BuyAmount"] = buyAmt.apply(lambda row: sum(row), axis=1)
    daily_aggs["SellAmount"] = sellAmt.apply(lambda row: sum(row), axis=1)
    daily_aggs["GrossTradeAmount"] = grossTrdAmt.fillna(0).apply(lambda row: sum(row), axis=1)
    daily_aggs["NetTradeAmount"] = netTrdAmt.fillna(0).apply(lambda row: sum(row), axis=1)
    daily_aggs["PositionCount"] = posQty.apply(lambda row: sum(row!=0), axis=1)
    daily_aggs["GrossPositionAmount"] = posAmt.apply(lambda row: sum(row.abs()), axis=1)
    daily_aggs["GrossPnl"] = grossPnl.apply(lambda row: sum(row), axis=1)
    daily_aggs["GrossPnlCum"] = grossPnlCum.apply(lambda row: sum(row), axis=1)
    daily_aggs["NetPnl"] = netPnl.apply(lambda row: sum(row), axis=1)
    daily_aggs["NetPnlCum"] = netPnlCum.apply(lambda row: sum(row), axis=1)

    from options_helper import cmdline_opt
    pnldir = f"{cmdline_opt().workspace_dir}/pnl_by_sym/{bookid}"
    os.system(f"mkdir -p {pnldir}")
    info(f"workspace= {cmdline_opt().workspace_name}, pnldir= {pnldir}")
    for k,v in daily_aggs.items():
        print_df(v, rows=5, show_head=False, title=k)
        v[v.index>begdt_draw].plot(title=k)
        fn = Path(f"{pnldir}/{k}.png")
        Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)
        plt.savefig(fn)
        plt.clf()

    daily_gross_pnl_by_sym = grossPnl.apply(lambda row: sum(row), axis=0)
    print_df((daily_gross_pnl_by_sym.sort_values()), rows = 10)
    print("INFO: total pnl gross= ", daily_gross_pnl_by_sym.sort_values().sum(), ", net=", netPnl.sum().sum())

    index_memberships = {}
    for k in ['all', '000300', '000905', '000016', '000852']:
        index_memberships[k] = index_membership(k, refprc)

    index_memberships['nonidx'] = get_nonindex_memberships(index_memberships)

    for k in cnstk_indus(cmdline_opt().symset):
        index_memberships[k] = industry_membership(k, refprc)


    for k,v in index_memberships.items():
        print(f"INFO: membership {k:<6} {'%4d'%(v.fillna(0.0).apply(lambda row: sum(row), axis=1).mean())}")


    for k,v in index_memberships.items():
        print(f"INFO: net pnl from {k:<8} = {'%12.2f'%((netPnl*v).sum().sum())}")

    if debug and not is_windows():
        for k,df in dict(zip(
            "trdQty,trdPrc,trdAmt,posAmt,refprc,grossPnl,grossPnlCum,netPnl,netPnlCum".split(r','), 
            [trdQty, trdPrc, trdAmt, posAmt, refprc, grossPnl, grossPnlCum, netPnl, netPnlCum])).items():
            #print_df(df, rows=5, show_head=False, title=f"{k}, {v.shape}")
            df = df[df.index > begdt_calc]
            df.index = df.index.date
            df.to_csv(f"{pnldir}/daily_{k}.csv")
            print(f"INFO: output {pnldir}/daily_{k}.csv")

    if debug and not is_windows():
        for chkSym in ["000006.XSHE", "000008.XSHE", "002812.XSHE"]:
            if chkSym in refprc.columns:
                df = pd.DataFrame.from_dict({"refprc": refprc[chkSym], "posQty": posQty[chkSym], "posAmt": posAmt[chkSym], 
                    "trdQty": trdQty[chkSym], "trdPrc": trdPrc[chkSym], "cashflow": cashflow[chkSym], "grossPnl": grossPnl[chkSym], 
                    "grossPnlCum": grossPnlCum[chkSym], "trdCost": trdCost[chkSym]}, orient="columns")
                df = df[df.index > begdt_calc]
                df.index = df.index.date
                df.to_csv(f"{pnldir}/{chkSym}.csv")

    print(f"INFO: {daily_aggs}")
    return daily_aggs

def cnstk_indus(symset='CS_ALL'):
    indus = "A01 A02 A03 A04 A05 B06 B07 B08 B09 B10 B11 C13 C14 C15 C17 C18 C19 C20 C21 C22 C23 C24 C25 C26 C27 C28 C29 C30 C31 C32 C33 C34 C35 C36 C37 C38 C39 C40 C41 C42"
    indus = f"{indus} D44 D45 D46 E47 E48 E49 E50 F51 F52 G53 G54 G55 G56 G58 G59 G60 H61 H62 I63 I64 I65 J66 J67 J68 J69 K70 L71 L72 M73 M74 M75 N77 N78 O80 P82 Q83 R85 R86 R87 R88 S90"
    indus = indus.split()
    if symset in indus:
        return [symset]
    else:
        return indus

def get_nonindex_memberships(index_memberships):
    all = index_memberships['all'] * 0
    for k,v in index_memberships.items():
        if k not in ["all"]:
            all = all + v
    all[all==0] = np.nan
    all = all * 0 
    all = all.fillna(1)
    return all

def print_membership_stats(funcn, df, debug=False):
    count_by_day = df.apply(lambda row: sum(row.abs()), axis=1)
    if debug:
        print_df(count_by_day, title=f"{funcn} count")
        print_df(df, title=f"{funcn}")

    info(f"{funcn} average_count_per_day= {int(count_by_day.mean())}, hist_len= {df.shape[0]}, mkt_counts= {df.shape[1]}, shape= {df.shape}")
    print_df(df, title=f"{funcn}")

INDEX_MEMBERSHIP_LOOKUP = {}
def index_membership(indexnm, refprc, debug=False):
    funcn = f"index_membership('{indexnm}')"
    global INDEX_MEMBERSHIP_LOOKUP  

    if indexnm in ['000905']:
        indexnm = 'cn500'
    elif indexnm in ['000300']:
        indexnm = 'cn300'
    else:
        indexnm = 'cn500'

    cache_key = f"{buf2md5(refprc.to_string())[-8:]}_{indexnm}"

    if cache_key in INDEX_MEMBERSHIP_LOOKUP.keys():
        return INDEX_MEMBERSHIP_LOOKUP[cache_key]

    index_membership = refprc.copy().fillna(0) * 0
    if indexnm == "all":
        index_membership = index_membership + 1.0
        INDEX_MEMBERSHIP_LOOKUP[indexnm] = index_membership
        return index_membership

    membership_info = {}
    membership_fn = f"/qpsdata/config/univ_{indexnm}/membership_hist.csv"
    print(f"{RED}INFO: reading {membership_fn}, cache_key= {cache_key}{NC}")
    for ln in open(membership_fn, 'r').readlines():
        ln = ln.strip()
        (sym,sym_set_name,permid,alive_date,reset_dt,dead_date) = ln.split(r',')
        if sym not in membership_info:
            membership_info[sym] = []
        membership_info[sym].append([alive_date,dead_date])
    #print(f"membership_info={membership_info}")
    for col in index_membership.columns:
        sym = col.split(r'.')[0]
        if sym in membership_info:
            #print(f"sym= {sym}, mem= {membership_info[sym]}")
            for idx in index_membership.index:
                dt = str(idx.date()).replace('-','')
                flag=0
                for (b,e) in membership_info[sym]:
                    if (b <= dt and dt<=e):
                        
                        flag = 1
                        break
                if flag>0:
                    #print(f"{RED}col= {col}; idx= {idx}; flag= {flag}{NC}")
                    index_membership.loc[idx, col] = flag

    index_membership = zero2nan(index_membership)
    index_membership.set_index(pd.to_datetime(index_membership.index.date) + pd.Timedelta('15:00:00'), inplace=True)
    #print_membership_stats(funcn, index_membership)

    INDEX_MEMBERSHIP_LOOKUP[cache_key] = index_membership
    return INDEX_MEMBERSHIP_LOOKUP[cache_key]

def index_membership_weights(indexnm, refprc, debug=False):
    funcn = f"index_membership('{indexnm}')"
    fdfref = refprc.fillna(0) * 0
    if indexnm == "all":
        fdfref = fdfref + 1.0
        return fdfref

    if indexnm in ['cn500']:
        indexnm = '000905'
    elif indexnm in ['cn300']:
        indexnm = '000300'

    fds = FDFCfg(cmdline_opt().cfg).ss(cmdline_opt().symset).fdf(funcn)
    info(f"{funcn}: fds= {fds}")
    if fds.fsize()>0:
        index_membership = fds.load()
    else:
        fn = f"/qpsdata/config/{indexnm}closeweight.csv"
        info(f"Reading {fn}")
        df = pd.read_csv(fn, encoding='GBK')[['成份券代码Constituent Code', '权重(%)weight']]
        df.columns = ['symbol', fdfref.index[0]]
        df.set_index('symbol', inplace=True)
        df = df.T
        df.rename(columns=sym_to_exch_map(df.columns, fdfref.columns), inplace=True)
        # df.reset_index(drop=True, inplace=True)
        df[df>0] = 1.0

        index_membership = (fdfref+df)
        index_membership = index_membership.apply(lambda x: index_membership.iloc[0], axis=1).fillna(0)
        fds.dump(index_membership)

        #index_membership = fdfref.add(df.iloc[0], axis='columns')


    print_membership_stats(funcn, index_membership)

    index_membership = zero2nan(index_membership)

    return index_membership

def industry_membership(indu, refprc, debug=False):
    funcn = f"industry_membership('{indu}')"
    fdfref = refprc.fillna(0) * 0
    if indu in ['all']:
        fdfref += 1
        return fdfref
    #print(fdfref)
    df = pickle.loads(Path(f"/qpsdata/data_rq.G/{indu}/univ.db").read_bytes())
    # print(fdfref.tail(10))
    # exit(0)
    for x in df[indu]:
        cols = [y for y in fdfref.columns if y in x]
        if len(cols)>0:
            fdfref.loc[:,cols] = 1.0

    print_membership_stats(funcn, fdfref)
    return fdfref


