#!/usr/bin/env python

import sys
import os

from pathlib import Path
import pickle
import datetime
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as ticker
from common import *
from QpsUtil import *
from QpsDatetime import *
from collections import defaultdict
from QpsPycharts import *

#plt.style.use('seaborn-white')
figsizeHalf=(18,6)
figsize=(18,13)
figsize2=(18,26)

def list_cmds(opt):
    pass

def mult_size(sz, fac):
    #print(sz[0]*fac, sz[1])
    return (sz[0], sz[1]*fac)

class Univ():
    def __init__(self, instruFn):
        self.instruFn = instruFn
        self.univ = pickle.loads(Path(instruFn).read_bytes())

    def __str__(self):
        return self.instruFn

class Ctx():
    def __init__(self, dts_cfg, syms, beg_dt=None, end_dt=None, asofdate='uptodate', bar='1d', session='', symset=''):
        funcn = 'Ctx.init'
        self.info = {}
        (opt, args) = get_options_jobgraph(list_cmds, args=['--asofdate', asofdate, '--force', '0'])
        sectype = QpsUtil.sym2sectype(syms.split(r',')[0])
        self.var2fld = getVarN2FldN(fn = f"/qpsdata/config/fgen_flds.{sectype}.cfg")
        self.fld2var = getVarN2FldN(fn = f"/qpsdata/config/fgen_flds.{sectype}.cfg", reverse_lookup=True)
        self.mkts = {}
        self.syms = syms
        self.bar = bar
        self.sectype = sectype
        self.dtaDict = None
        self.fldNms = []
        self.fldFns = []

        scn = None
        if sectype in ['CS']:
            for sym in syms.split(r','):
                symsets, qdbsym = QpsUtil.sym2qdbsym(sym)
                for symset in symsets:
                    # if symset.find("UNKNOWN")>=0:
                    #     print(f"DEBUG: Ctx.init() {sym} {symset}")
                    #     continue
                    info = {}
                    pmid = f"{qdbsym}.{symset}"
                    if session == '':
                        session = 'SN_CS_DAY'

                    scn = Scn(opt, symset, dts_cfg, session, asofdate=asofdate)
                    instru_fn = f"{dd('raw')}/{symset}/{session}/{scn.dts_cfg_expanded}/vps_instru.{symset}.db"
                    #print(f"DEBUG: {funcn} {instru_fn} qdbsym= {qdbsym}")
                    #univ = pickle.load(open(instru_fn, 'rb'))
                    u = Univ(instru_fn)
                    # print(u.univ)
                    # exit(0)

                    #Load data from file first, then specialize
                    info.update(u.univ[u.univ['permid'] == qdbsym].iloc[0].to_dict())
                    info['permid'] = pmid
                    info['symset'] = symset
                    info['instru_fn'] = instru_fn
                    info['sym'] = qdbsym
                    info['cnbc'] = '<a href="{}">{}</a>'.format('http://www.cnbc.com', 'http://www.cnbc.com')
                    info['session'] = session
                    info['scn'] = scn
                    info['fldRoot'] = f"{dd('usr')}/{sectype}/{session}/{scn.dts_cfg_expanded}"
                    info['sectype'] = sectype
                    info['univ'] = u
                    self.mkts[pmid] = info

        
        if sectype in ['CF']:
            for ctrRoot in syms.split(r','):
                if symset == '':
                    symset = 'CF_ALL'
                if session == '':
                    if symset.find('NS_')>=0:
                        session = 'SN_CF_DNShort'
                    else:
                        session = 'SN_CF_DAY'

                scn = Scn(opt, symset, dts_cfg, session)

                instru_fn = f"{dd('raw')}/{symset}/{session}/{scn.dts_cfg_expanded}/vps_instru.{symset}.active.db"
                u = Univ(instru_fn)
                frontMths = 3
                for found in list(u.univ[u.univ['metaId'] == ctrRoot].transpose().to_dict().values())[:frontMths]:
                    info = {}           
                    info.update(found)
                    info['sym'] = ctrRoot
                    info['symset'] = symset
                    info['cnbc'] = '<a href="{}">{}</a>'.format('http://www.cnbc.com', 'http://www.cnbc.com')
                    info['session'] = session
                    info['scn'] = scn
                    info['fldRoot'] = f"{dd('usr')}/{sectype}/{session}/{scn.dts_cfg_expanded}"
                    info['univ'] = u
                    self.mkts[info['instruId']] = info

        self.info['begDt'] = dtparse(scn.begDt)
        self.info['endDt'] = dtparse(scn.endDt)
        self.info['begDtm'] = dtparse(f"{scn.begDt} 00:00:00")
        self.info['endDtm'] = dtparse(f"{scn.endDt} 23:59:59")

    def mkt_arr(self):
        return (list(self.mkts.values()))

    def univ_arr(self):
        return [m['univ'].univ for m in self.mkt_arr()]

    def display(self):
        print(pd.DataFrame.from_dict(self.mkts))
       
    def fldnm2fn(self, fldNm, debug=False):
        fnLst = []
        for pmid,info in self.mkts.items():
            if self.sectype == 'CS':
                fnLst.append([info['permid'], info['sym'], info['symset'], f"{info['fldRoot']}/{fldnm2bar(self.var2fld[fldNm])}/{self.var2fld[fldNm]}/{info['symset']}.db"])
            else:
                fnLst.append([info['permid'], info['sym'], info['symset'], f"{dd('raw')}/futures/ohlcv_1d_pre/{info['permid']}.db"])
        if self.sectype == 'CF':
            fnLst.append([f"{info['sym']}888", info['sym'], info['symset'], f"{dd('raw')}/futures/ohlcv_1d_pre/{info['sym']}888.db"])
        if debug:
            print(fnLst)
        return(fnLst)

    def get_series(self, pld, sym, fldNm, debug=False):
        if sym in pld.columns:
            # if debug:
            #     print(sym, pld.columns)
            #     print(pld[sym])
            return pld[sym]
        elif fldNm.find('_1d') >= 0:
            pld = pld.loc[(pld.index >= self.info['begDtm']) & (pld.index <= self.info['endDtm'])]
            fldLookup = {
                "O_1d": "open",
                "H_1d": "high",
                "L_1d": "low",
                "C_1d": "close",
                "V_1d": "volume",
            }
            #pld.set_index(pld.index + pd.Timedelta('15:00:00'), inplace=True)
            if fldNm in fldLookup:
                return (pld[fldLookup[fldNm]])
            else:
                return None
        else:
            return None

    def load_flds(self, fldNms=["O_1d", "H_1d", "L_1d", "C_1d", "V_1d"], plot_symset='ByMkt', debug=False):
        funcn = "QpsNotebook.load_flds"
        self.dtaDict = defaultdict(None)
        hasValidDta = False
        self.fldFns = []
        msgs = []
        for fldNm in fldNms:
            for permid,sym,symset,fldFn in self.fldnm2fn(fldNm):
                self.fldFns.append((fldNm, permid, sym, QpsUtil.str_minmax(fldFn)))

                if debug:
                    print(f'INFO: {funcn} fldFn', fldFn)

                # if not os.path.exists(fldFn):
                #     msgs.append(f"WARNING: {funcn} can not find {fldFn}")
                #     continue
                pld = QpsUtil.smart_load(fldFn)
                #pld = pickle.load(open(fldFn, 'rb'))

                if fldNm in ['C_1d']:
                    pld.set_index(pld.index + pd.Timedelta('15:00:00'), inplace=True)
                

                if plot_symset == 'ByUniv':
                    for col in list(pld):
                        self.dtaDict[f'{col}.{fldNm}'] = pld[col]
                else:
                    self.dtaDict[f'{permid}.{fldNm}'] = self.get_series(pld, sym, fldNm, debug=debug)
                    if self.dtaDict[f'{permid}.{fldNm}'] is not None:
                        hasValidDta = True
                    
        if not hasValidDta:
            self.dtaDict = None
        return msgs

    def set_data_dict(self, d):
        self.dtaDict = d

    def data_matplot(self, fldNm=None):
        if fldNm is None:
            return self.dtaDict
        return {fldNm: self.dtaDict[fldNm]}
        
    def plot_flds(self, begDtm="", endDtm="", by_fld = False, plot_days=0, cumsum=True, debug=False, 
            figsize=figsizeHalf, grid=True, linewidth=1.5, hide_spaces=True, plot_symset='ByMkt', chart_type="", cols = 3, marker=""): 
        funcn = 'QpsNotebook.plot_flds'
        begDtm = dtparse(begDtm) if begDtm else self.info['begDtm']
        endDtm = dtparse(endDtm) if endDtm else self.info['endDtm']
        if self.dtaDict is not None:
            if by_fld:
                rows = 1
                if not hide_spaces:
                    rows = math.ceil(len(self.dtaDict)/cols)
                    fig, axs = plt.subplots(rows, cols)
                    import itertools
                    axs = list(itertools.chain(*axs))

                i= 0
                for fldNm in self.dtaDict.keys():
                    if hide_spaces:
                        ax = None
                    else:
                        ax = axs[i]
                    p = self.plot_matplot(fldNm, plot_symset, begDtm, endDtm, plot_days, cumsum, debug, mult_size(figsize, rows), grid, linewidth, hide_spaces, chart_type, ax=ax, marker=marker)

                    i = i + 1                
            else:
                p = self.plot_matplot(None, plot_symset, begDtm, endDtm, plot_days, cumsum, debug, figsize, grid, linewidth, hide_spaces, chart_type, marker=marker)

            if debug:
                print(pd.DataFrame.from_dict(self.dtaDict))
            #return(fldFns, dtaDict, p)

            plt.show()
            plt.close()
        #     return(pd.DataFrame(self.fldFns))
        #     #return(self.fldFns, self.dtaDict, None)
        # else:
        #     return([], {}, None)
        
    def fldFnsDf(self):
        return(pd.DataFrame(self.fldFns))
    
    def fldDta(self):
        return(pd.DataFrame.from_dict(self.dtaDict))

    def plot_matplot(self, fldNm, title, begDtm, endDtm, plot_days, cumsum, debug, figsize, grid, linewidth, hide_spaces, chart_type, ax = None, marker=""):
        df = get_data_from_between_dt(pd.DataFrame.from_dict(self.data_matplot(fldNm), orient='columns'), begDtm, endDtm, plot_days)
        if len(df) < 1:
            print("Df is empty, no data to plot.")
            return

        if cumsum:
            df = df.cumsum()
        if debug:
            print(df.tail())
        
        # if chart_type == "line":
        if False:
            p = plot_line(df, title, figsize, grid, linewidth)
        else:
            if hide_spaces == True:
                p = plot_intrday(df, title, figsizeHalf, grid, linewidth, ax=None, marker=marker)
            else:
                #p = df.plot(figsize=figsize, title=title, grid=grid, linewidth=linewidth)
                p = df.plot(figsize=figsize, title=title, grid=grid, linewidth=linewidth, ax=ax, marker=marker)

            # plt.show()
            # plt.close()
        return p

    def plot_pyechart(self, begDtm="", endDtm="", plot_days=0, cumsum=False, debug=False, figsize=figsizeHalf, linewidth=1.5, chart_type="line"):
        d = self.dtaDict
        if len(d) < 1:
            print("No such data to plot.")
            return

        if chart_type == "kline":
            if len(self.dtaDict) < 5:
                print("WEEOR: please load open,high,low,close,volume.")
                return

            if cumsum:
                print("Warning: Kline cannot set cumsum.")

            columns = ["close", "high", "low", "open", "volume"]
            data = sorted(self.dtaDict.items(), key=lambda x:x[0])
            d = {columns[i]:v[1] for i,v in enumerate(data)}

        title = list(self.dtaDict.keys())[0][:-4]
        df = pd.DataFrame(d)
        begDtm = dtparse(begDtm) if begDtm else self.info['begDtm']
        endDtm = dtparse(endDtm) if endDtm else self.info['endDtm']
        df = get_data_from_between_dt(pd.DataFrame(d), begDtm, endDtm, plot_days)

        if cumsum:
            if debug:
                print(df.cumsum().tail())
            df = df.cumsum()
        else:
            if debug:
                print(df.tail())

        chart = None
        if chart_type == "kline":
            chart = plot_kline(df, title=title, figsize=figsize, linewidth=linewidth)
        elif chart_type == "line":
            chart = plot_line(df, title=title, figsize=figsize, linewidth=linewidth)
        return chart


def plot_all_fldNms(dtaDict, title, begDtm, endDtm, plot_days, cumsum, debug, figsize, grid, linewidth, hide_spaces, chart_type):
    df = get_data_from_between_dt(pd.DataFrame.from_dict(dtaDict, orient='columns'), begDtm, endDtm, plot_days)
    if len(df) < 1:
        print("Df is empty, no data to plot.")
        return

    if cumsum:
        if debug:
            print(df.cumsum().tail())
        df = df.cumsum()
    else:
        if debug:
            print(df.tail())
    
    if chart_type == "line":
        p = plot_line(df, title, figsize, grid, linewidth)
    elif chart_type == "kline":
        p = plot_kline(df, title, figsize, grid, linewidth)
    else:
        if hide_spaces:
            p = plot_intrday(df, title, figsize, grid, linewidth)
        else:
            p = df.plot(figsize=figsize, title=title, grid=grid, linewidth=linewidth)
        plt.show()
        plt.close()
    return p

def get_data_from_between_dt(df, beg_dt, end_dt, plot_days):
    plot_days = -1*plot_days
    if plot_days < 0:
        beg_dt = end_dt + datetime.timedelta(days=plot_days)
    elif plot_days > 0:
        end_dt = beg_dt + datetime.timedelta(days=plot_days)
    # print(beg_dt, end_dt)
    if beg_dt and end_dt:
        df = df[beg_dt : end_dt]
    elif beg_dt and not end_dt:
        df = df[beg_dt :]
    elif not beg_dt and end_dt:
        df = df[: end_dt]
    return df

def plot_intrday(df, title, figsize, grid, linewidth, ax=None, marker=""):
    n = len(df)
    ind = np.arange(n)
    def format_date(x, pos=None):
        thisind = np.clip(int(x+0.5), 0, n-1)
        return pd.to_datetime(str(df.index.values[thisind])).strftime("%Y%m%d %H%M%S")

    fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(111, title=title)
    ax.plot(ind, df, linewidth = linewidth, marker=marker)
    ax.grid(grid)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    plt.legend(list(df))
    # plt.gcf().autofmt_xdate()

def extend_fldnm_adj(fldNms, adjs=['', 'u', 'v']):
    allFldNms = []
    for fldNm in fldNms:
        for adj in adjs:
            fldNmAdj = f"{adj}{fldNm}"
            allFldNms.append(fldNmAdj)
    return allFldNms

if __name__ == '__main__':

    if True:
        for dts_cfg in ['prod1w']:
        #for dts_cfg in ['prod1w']:
            for syms in ['603288,002124', 'ZC,A']:
                ctx = Ctx(dts_cfg, syms, asofdate="uptodate")
                ctx.display()
                ctx.fldnm2fn("C_1d", debug=True)

    if False:
        for ctx in [Ctx('download', '000628', asofdate="download"), Ctx('prod1w', '000416', asofdate="download"), Ctx('rschhist', '603288', asofdate="download")]:
            print(ctx.info)
    

