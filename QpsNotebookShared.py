
from tkinter import N
import matplotlib as mpl
mpl.rcParams['figure.max_open_warning'] = 0
            
import sys

import os
from EgenNew import *
from FldNamespace import *
from EgenStudy import *
from shared_cmn import *
import timeit
import QpsUtil
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from common_basic import *
from EgenStudy import get_classifier_flds,analyze_qSummary
import itertools
from daily_updates import *
from defaultlist import defaultlist

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 30)
##pd.set_option('display.max_colwidth', -1) # or 199
pd.options.display.float_format = '{:.4f}'.format

#plt.style.use('seaborn-white')
figsizeHalf=(18,6)
figsize=(18,13)
figsize2=(18,26)

def load_fld_show_data(params):
    funcn = 'load_fld_show_data'
    data = []
    desc = {}
    if params.find(',')>=0:
        (dts_cfg, fldNm, symset, *pmids) = params.split(r',')
    else:
        #File path: {rootuser()}/che/data_rq.20100101_uptodate/CS/SN_CS_DAY/prod_20200901_20211115/1d/Pred_Rq6b54a7_srv_1d_1/CS_ALL.egen
        # gen_dta = f"gen_data.{QpsUtil.buf2md5(params)[-6:]}.pkl"
        # if os.path.exists(gen_dta):
        #     gen_dta = smart_load(gen_dta)
        # else:
        #     gen_dta = None

        if params.find(".egen")>=0:
            desc = smart_load(params.replace(".egen", ".desc"))

        elist = params.split(r'/')
        symset = elist[-1].split(r'.')[0]
        fldNm = elist[-2]
        dts_cfg = dts_cfg_shortform(elist[-4].split(r'_')[0])
        pmids=['']

    #print(funcn, dts_cfg, fldNm, symset, pmids)
    for (dts_cfg, fldNm, symset, pmids) in list(itertools.product(dts_cfg, fldNm.split(), symset.split(), pmids)):
        #print(funcn, dts_cfg, fldNm, symset, pmids)
        bar = '1d'
        (opt, args) = get_options_jobgraph(list_cmds = lambda x: "", 
            args=['--asofdate', 'download', 
                '--force', '0', 
                '--bar', '1d',
                '--debug', "0"])
        scn = Scn(opt, symset, dts_cfg, 'SN_CS_DAY', asofdate='download')
        #print(scn)

        #df = smart_load(fldFn)
        df = load_fld_new(opt, scn, fldNm, debug=False, use_local=False)
        #print_df(df)
        d = {"dts_cfg": dts_cfg, "fldNm": fldNm, "symset": symset, "pmids": pmids, "df": df}
        d.update(desc)
        data.append(d)
    return data

def make_list(dfs):
    if not isinstance(dfs, list):
        dfs = [dfs]
    return dfs

def plot_fld_overview(dfs, label=None, value_limits=None):
    labelWithDefault = defaultlist(lambda: 'NA')
    if label != None:
        for i in range(len(label)):
            labelWithDefault[i] = label[i]

    fig, axs = plt.subplots(2, 2)
    axs = list(itertools.chain(*axs))

    #dfCmb = pd.DataFrame(dict(zip(cmpNms, [bunch[nm].stack() for k in cmpNms])))
    labelIdx = -1
    for df in make_list(dfs):
        labelIdx += 1
        if value_limits is not None:
            # df[df>value_limits[1]] = np.nan
            # df[df<value_limits[0]] = np.nan
            df = df[df<value_limits[1]]
            df = df[df>value_limits[0]]

        if df.count().sum()==0:
            print(f"WARNING: no valid data found for 'labelWithDefault[labelIdx]'", file=sys.stderr)
            continue

        (df.iloc[1:,:].count(axis=1).plot(figsize=figsize, title="valid value count", ax=axs[0], legend=True, label=labelWithDefault[labelIdx]))
        (df.iloc[1:,:].mean(axis=1).plot(figsize=figsize, title="daily average", ax=axs[1], legend=True, label=labelWithDefault[labelIdx]))

        #for large data set with lots of columns, only used data from first 100 columns
        subset = len(df.columns) if len(df.columns)<100 else 100  
        (df.iloc[1:,:subset].stack().plot.kde(figsize=figsize, title="kde", ax=axs[2], legend=True, label=labelWithDefault[labelIdx]))

        plot_fld_quantile(df, ax=axs[3], label=labelWithDefault[labelIdx])


def plot_fld_quantile(df, ax=None, label='NA'):
    df.quantile(0.05, axis=1).plot(figsize=figsize, ax=ax, label=f"{label} 0.05", legend=True)
    df.quantile(0.10, axis=1).plot(figsize=figsize, ax=ax, label=f"{label} 0.10", legend=True)
    df.quantile(0.50, axis=1).plot(figsize=figsize, ax=ax, label=f"{label} 0.50", legend=True) 
    df.quantile(0.90, axis=1).plot(figsize=figsize, ax=ax, label=f"{label} 0.90", legend=True)
    df.quantile(0.95, axis=1).plot(figsize=figsize, ax=ax, label=f"{label} 0.95", legend=True)

def plot_fld_by_mkt(dfs, mkts, label=None):
    fig, axs = plt.subplots(3, 2)
    axs = list(itertools.chain(*axs))

    labelIdx=0

    for df in make_list(dfs):
        if label is not None:
            lbl = label[labelIdx]
            labelIdx += 1

        cnt = 0
        for mkt in mkts:
            df[mkt].plot(figsize=figsize, ax=axs[cnt%len(axs)], legend=True, label=f"{mkt}-{lbl}")
            cnt += 1

def show_fld_overview(data):    
    for d in data:
        desc = {k:v for k,v in d.items() if k!='df'}
        display(pd.DataFrame.from_dict(desc, orient='index').T)
        plot_fld_overview(d['df'], label=d['dts_cfg'])


def show_fld_stocks(data):
    fig, axs = plt.subplots(2, 2)
    axs = list(itertools.chain(*axs))

    for d in data:
        df = d['df']
        pmids = d['pmids']
        dts_cfg = d['dts_cfg']
        if len(pmids)<=0:
            stkCols = [x for x in df.columns if df[x].count()>20][:min(len(df.columns),4)]
        else:
            stkCols = [x for x in df.columns if x in pmids.split()]
        if len(stkCols)<=0:
            print(f"DEBUG: some stocks are missing in ({dts_cfg}, {fldNm}, {symset})")
            continue
        else:
            display(stkCols)
        
        (df[stkCols].plot(figsize=figsize, title="example data", ax=axs[0], legend=True, label=dts_cfg))
        (df[stkCols].cumsum().plot(figsize=figsize, title="example data(cum)", ax=axs[1], legend=True))
        (df[stkCols].plot.kde(figsize=figsize, title="kde", ax=axs[2], legend=True, label=dts_cfg))

def show_fld_values(data):
    for d in data:
        df = d['df']
        show_lines = 5
        print("Head")
        display(df.iloc[:,:6].head(show_lines))
        print("Tail")
        display(df.iloc[:,:6].tail(show_lines))
        # print("Valid")
        # display((~df.isna()).iloc[:,:6].head(show_lines))

def show_fld_gen(data):
    for d in data:
        if 'dtaDir' not in d or d['dtaDir'] is None:
            continue
        for fn in glob.glob(f"{d['dtaDir']}/sample_mkt.*.pkl"):
            df = smart_load(fn)
            #df['Mkt'] = fn.split(r'.')[1]
            display(df.tail())

def fld_to_fld_gen_task_dir(d):
    e = d.split(r'/')
    r=['/qpsdata/egen_study/*']
    r.append(e[8].split(r'_')[-3])
    r.append(e[8].split(r'_')[-4][-6:])
    return glob.glob('/'.join(r))[0]

def cx_to_fld_gen_task_dir(d):
    e = d.split(r'/')
    r = e[:4]
    r.append(e[4].split(r'_')[-2])
    r.append(e[4].split(r'_')[-3][-6:])
    return '/'.join(r)

NOTEBOOK_DATA_PATH=""
def set_notebook_data_path(path):
    global NOTEBOOK_DATA_PATH
    NOTEBOOK_DATA_PATH = path

def get_notebook_data_path():
    global NOTEBOOK_DATA_PATH
    return NOTEBOOK_DATA_PATH

def show_images(fp, patterns=None):
    from IPython.display import Image
    from IPython.display import display
    imgs = []
    for fn in glob.glob(f"{fp}/*.png"):
        show = False
        if patterns is not None:
            for p in patterns.split(","):
                if fn.find(p)>0:
                    show = True
        else:
            show = True
        if show:
            imgs.append(Image(filename=fn))
    for img in imgs:
        display(img)   

def load_cx_data(path, debug=True):
    funcn = "load_cx_data"
    set_notebook_data_path(path)
    print(f"DEBUG_4561: {path}", file=sys.stderr)
    return None
    dta = {}
    dta['summary'] = smart_load(f"{path}/cx.summary.pkl")
    dta['daily'] = smart_load(f"{path}/cx.daily.pkl")
    desc = {}
    desc['path'] = path
    desc['symset'], desc['scn'] = path.split(r'/')[6].split(r'.')

    for paramsFn in [f"{path}/../../params.pkl", f"{cx_to_fld_gen_task_dir(path)}/params.pkl"]:
        if os.path.exists(paramsFn):
            desc.update(smart_load(paramsFn, debug=debug, title=funcn))
        else:
            print(f"WARNING: {funcn} can not find {paramsFn}", file=sys.stderr)
    dta['desc'] = desc
    return dta

def show_cx_overview(dta, detailed=False):
    if dta is None:
        return
    pd.options.display.float_format = '{:.4f}'.format
    if not detailed:
        show_cx_alpha_fullhist(dta)
        display(pd.DataFrame.from_dict(dta['desc'], orient='index').T)

    df = pd.DataFrame.from_dict(dta['summary'], orient='index').T
    df = df[sorted(df.columns, reverse=True)]
    #display(df[df.index.str.find("alphaQT_raw_")>=0])
    #display(df[df.index.str.find("alphaQT_net_")>=0])
    if not detailed:
        alphaNms = ['QT', 'LS', ]
    else: 
        alphaNms = ['Q3h', 'Q5h', 'QTma']
    for x in alphaNms:
        sec = [df[df.index.str.find(f"alpha{x}_raw_")>=0], df[df.index.str.find(f"alpha{x}_net_")>=0]]
        #display(sec)
        if len(sec)<=0:
            continue

        sryDf = pd.concat(sec, axis=0)
        if sryDf.empty:
            display(sryDf)
            continue
        sryDf.reset_index(inplace=True)
        sryDf[['Name','r/n','Stats']]=sryDf['index'].str.split(r'_',expand=True)
        sryDf.drop(['index'], axis=1, inplace=True)
        sryDf.set_index(['Name', 'r/n', 'Stats'], inplace=True)
        display(sryDf)
    
    # if detailed:
    #     display(df[df.index.str.find('ic')>=0])
    #     #display(df)
    #     display(df[df.index.str.find('turnover')>=0])

    #display(dta['daily']['All'].head())

def get_alpha_names():
    aNms = []
    for at in ['QT', 'LS', 'Q3h', 'Q5h', 'QTma']: #'QL', 'LC',
        aNms.extend([f'alpha{at}_raw', f'alpha{at}_net'])
    return(aNms)

def show_cx_alpha_fullhist(dta):
    fig, axs = plt.subplots(1, 2)
    #axs = list(itertools.chain(*axs))
    gidx = 0

    for rn in ['raw', 'net']:
        alpha = {}
        for an in [x for x in get_alpha_names() if x.find(rn)>=0]:
            if ('daily' not in dta) or (3000 not in dta['daily']) or (an not in dta['daily'][3000]):
                continue
            alpha[an] = dta['daily'][3000][an]
        if not alpha:
            continue
        df = pd.DataFrame.from_dict(alpha, orient='columns')
        df = df[sorted(df.columns, reverse=True)]
        df.cumsum().plot(figsize=figsizeHalf, title=f"alpha {rn}", ax=axs[gidx], legend=True, label=True)
        axs[gidx].axhline(0, color='gray')
        gidx += 1
    plt.show()

def show_cx_alpha(dta):
    if dta is None:
        return

    fig, axs = plt.subplots(2, 2)
    axs = list(itertools.chain(*axs))
    gidx = 0
    for an in [x for x in get_alpha_names() if (x.find("QT")>=0 or x.find("QL")>=0) and x.find("QTma")<0]:
        alpha = {}
        for year in dta['daily'].keys():
            if dta['daily'][year] is None:
                continue
            if an in dta['daily'][year]:
                alpha[str(year)] = dta['daily'][year][an]

        if not alpha: #.empty:
            continue

        df = pd.DataFrame.from_dict(alpha, orient='columns')
        df = df[sorted(df.columns, reverse=True)]
        dummy = df.cumsum().plot(figsize=figsize, title=an, ax=axs[gidx], legend=True, label=True)
        axs[gidx].axhline(0, color='gray')
        gidx += 1
    plt.show()


def show_cx_alpha_continue(dta):
    fig, axs = plt.subplots(2, 2)
    axs = list(itertools.chain(*axs))
    gidx = 0
    for an in ['alpha3_ss']:
        alpha = {}
        for year in dta['daily'].keys():
            if dta['daily'][year] is None:
                continue
            alpha[year] = dta['daily'][year][an]

        pd.DataFrame.from_dict(alpha, orient='columns').cumsum().plot(figsize=figsize, title=an, ax=axs[gidx], legend=True, label=True)
        axs[gidx].axhline(0, color='gray')
        gidx += 1

def show_cx_ic(dta):
    if dta is None:
        return 
        
    fig, axs = plt.subplots(2, 2)
    axs = list(itertools.chain(*axs))
    gidx = 0
    for an in ['ic', 'ic_decay_01', 'ic_decay_02', 'ic_decay_05']:
        alpha = {}
        for year in dta['daily'].keys():
            if year == 'All':
                continue
            if dta['daily'][year] is None:
                continue
            if an in dta['daily'][year]:
                alpha[year] = dta['daily'][year][an]
        if alpha:
            pd.DataFrame.from_dict(alpha, orient='columns').plot(figsize=figsize, title=an, ax=axs[gidx], legend=True, label=True)
        gidx += 1


if __name__ == '__main__':
    (opt, args) = get_options_jobgraph(list_cmds = lambda x: "")
    #print_df(load_fld("P,Pred_Rq6b54a7_srv_1d_1,CS_ALL,")[0][-1])
    #print('*'*180)
    #print_df(load_fld(f"{rootuser()}/che/data_rq.20100101_uptodate/CS/SN_CS_DAY/prod_20200901_20211115/1d/Pred_Rq6b54a7_srv_1d_1/CS_ALL.egen")[0][-1])
    print('*' * 180)
    print_df(smart_load(f"{rootuser()}/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_20220324/1d/Pred_Rq6b54a7_srv_1d_1/C15.egen"))



