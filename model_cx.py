#!/home/shuser/anaconda3/bin/python

import sys

import pandas as pd


from common_basic import *
from common import *
from SmartFile import *
from sklearn.linear_model import LinearRegression
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from funcs_fld import *
from funcs_series import *
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean

NUMBER_DAYS_PER_YEAR = 243 #vs252
EXCEL_DATA_DUMP = False

def calc_cx_at_dtm(opt, symset, xy1d, respNm, predNm, xy1dHist, detailed, title="no-title", debug=False):
    funcn = "calc_cx_ad_dtm"
    #debug = True
    if debug:
        print(f'+++++++++++++++++++++++++++++++++++++++++{funcn}++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    cx = {}
    xy1d = xy1d.replace(np.inf, np.nan)
    xy1d = xy1d.replace(-np.inf, np.nan)

    if debug:
        print_df(xy1d, title=f'{predNm} before')
        print(f"DEBUG: {funcn} mean {xy1d.mean()}")

    pred = xy1d[predNm]
    pred = pred.unstack('Mkt')
    if debug:
        print_df(pred, rows=20, title='pred df before')
    pred = Z(pred) #demean the forecast
    if debug:
        print_df(pred, rows=20, title='pred df after')
    #print(pred.stack().reset_index().sort_values(['Mkt', "Dtm"]).set_index(['Mkt', "Dtm"]))
    xy1d[predNm] = stack(pred)

    if debug:
        print_df(xy1d, title=f'{predNm} after')
        print(xy1d.mean())

    xy1d.dropna(axis=0, how='any', inplace=True)
    if xy1d.empty:
        if debug:
            print(f"WARNING: no data for title= {title}, skipping...")
        return cx

    ic = xy1d.corr(method='pearson')
    rankic = xy1d.corr(method='spearman')
    # print(f"ic={ic}")
    # print(f"rankic={rankic}")
    cx['ic'] = ic.iloc[0,1]
    cx['rankic'] = rankic.iloc[0,1]

    import statsmodels.api as sm

    #print_df(xy1d[[respNm, predNm]])
    regr = sm.OLS(xy1d[respNm], xy1d[predNm]).fit()
    cx["alpha1"] = regr.params[predNm]

    if 0:
        ic_p = xy1d[respNm].dot(xy1d[predNm])/len(xy1d[respNm])/xy1d[respNm].std()/xy1d[predNm].std()
        cx['ic_p'] = ic_p
    # print(f"ic_p={ic_p}")

    alpha2 = xy1d[predNm].dot(xy1d[predNm])/xy1d[predNm].abs().sum()*cx["alpha1"]
    cx['alpha2'] = alpha2
    if 0:
        alpha2_p = xy1d[predNm].dot(xy1d[respNm])/xy1d[predNm].abs().sum()
        cx['alpha2_p'] = alpha2_p

    #symsetRets = xy1d.groupby(['Dtm'])[respNm].mean()
    #print_df(symsetRets, title='symsetRets')
    symsetRets = None #do not calc symset returns for now
    bmRets = getBmRets(opt, symset, symsetRets)
    for bm in bmRets.columns: #align with xy1d
        xy1d[bm] = xy1d.index.map(lambda idx: mktdtmIdx2IndexReturn(bmRets, idx, bm)) #bmRets[bm]

    if debug:
        print("xy1d", xy1d.tail(10))

    fPlus = (np.maximum(xy1d[predNm], 0))
    #for bm in ['rf', '3h', '5h', 'ss']:
    for bm in ['rf', '3h', '5h']:
        alpha3 = fPlus.dot(xy1d[respNm] - xy1d[bm])/fPlus.sum()/2.0
        cx[f'alpha3_{bm}'] = alpha3
    
    for days in [1,2,5,10,20]:
        if len(xy1dHist)<days:
            continue
            
        xy1dLag = xy1dHist[-days]
        pairDf = pd.DataFrame.from_dict({'x':xy1d[predNm], 'y': xy1dLag[predNm]}, orient='columns')
        pairDf['y'] = pairDf['y'].shift(1)
        pairDf.dropna(axis=0, how='any', inplace=True)
        #print_df(pairDf, title='pairDf')
        cx[f'alpha_turnover_%02d'%(days)] = (pairDf['x'] - pairDf['y']).abs().sum()/pairDf['y'].abs().sum()
        cx[f'ic_decay_%02d'%(days)] = pairDf.corr(method='spearman').iloc[0,1]
        #cx[f'ic_decay_%02d'%(days)] = pair[[predNm, predDecayNm]].corr(method='spearman').iloc[0,1]
        cx[f'mean_ic_decay_%02d'%(days)] = cx[f'ic_decay_%02d'%(days)]/days
    
    xy1dHist.append(xy1d)
    #print(f"xy1dHist {len(xy1dHist)}")

    if debug:
        print("cx", cx)
    return cx

@timeit
def calc_cx_period(task, modelNm, symset, outputTag, respNm, predNm, df, year, cxAll, detailed=False, debug=False):
    funcn = "calc_cx_period"

    s = {}
    if year != 3000:
        xy = df[df.index.get_level_values(1).year == year][[respNm, predNm]].copy()
        xy.dropna(axis=0, how='any', inplace=True)
        s['Mkts'] = len(xy.index.get_level_values(0).unique())
        dtms = sorted(xy.index.get_level_values(1).unique())
        if debug or True:
            print(f"DEBUG: ------------------- {funcn} calculating year= {year}, shape= {xy.shape}, dtms= {QpsUtil.format_arr(dtms, maxShown=1)}")
        if xy.empty:
            return None

        xy1dHist = []
        cxByDay = {}
        for dtm in dtms:
            if True and debug:
                print(f"INFO: calc {dtm}")
            xy1d = xy[xy.index.get_level_values(1) == dtm].copy()#[[respNm, predNm]]
            # print(xy1d.tail())
            cxByDay[dtm] = calc_cx_at_dtm(task.opt(), symset, xy1d, respNm, predNm, xy1dHist, detailed, title=f"{funcn}.{year}.{dtm}", debug=False)

        cxByDayDf = pd.DataFrame.from_dict(cxByDay, orient='index')
        cxByDayDf = cxByDayDf[sorted(cxByDayDf.columns)]

    else:
        #cxByDayDf = pd.concat(list(cxAll.values()).reverse()) #need to revese as recent years first, and concat need to be in time-order
        #cxAllRev = dict(sorted(list(cxAll.items())))

        print("-"*200)
        print(sorted(cxAll.keys()))
        cxByDayDf = pd.concat([cxAll[x] for x in sorted(cxAll.keys()) if x != 3000])

    if debug:
        print_df(cxByDayDf, show_head=True, show_body=True)

    if debug:
        print(cxByDayDf.tail())

    cxAll[year] = cxByDayDf

    s.update(calc_mean_values(cxByDayDf))
    (s['Days'], dummy) = cxByDayDf.shape
    
    s['AnnRet'] = s['alpha2'] * NUMBER_DAYS_PER_YEAR
    s['IR'] = math.sqrt(NUMBER_DAYS_PER_YEAR) * s['alpha2'] / cxByDayDf['alpha2'].std()
    if 0:
        s['MaxDD_p'] = max_drawdown(cxByDayDf['alpha2'])
    (s['MaxDD'], s['RcyDay']) = max_drawdown2(cxByDayDf['alpha2'], title=year)[:2]
    #(s['MaxDD-'], s['RcyDay-']) = max_drawdown2(cxByDayDf['alpha2'], mult = -1.0, title=year)[:2]
    s['NetAnnRet'] = (s['alpha2'] - 0.0002 * s['alpha_turnover_01']) * NUMBER_DAYS_PER_YEAR
    s['NetIR'] = math.sqrt(NUMBER_DAYS_PER_YEAR) * (s['alpha2'] - 0.002 * s['alpha_turnover_01']) / (
            cxByDayDf['alpha2'] - 0.0002 * cxByDayDf['alpha_turnover_01']).std()

    if True or debug:
        print(f"INFO: {funcn}, year={year}, {s['IR']}")

    return {year: s}

# def update_summary_all(summaryAll, year, s):
#     summaryAll[year] = s

@timeit
def cross_section_model(task, modelNm, symset, outputTag, respNm, predNm, debug=False, force=False, write_pred_file=False):
    funcn = 'cross_section_model'
    doneFile = BaseSmartFile()
    try:
        fp = f"{task.dta_dir(cache=False)}/{modelNm}/{done_version()}.pkl"
        use_if_exists(doneFile, fp, force=force)
    except Exception as e:
        print(f"INFO: skipping {funcn} {e}, skipping ...")
        if task.opt().show:
            task.show()
        return

    df = task.df()[[respNm, predNm]]#.copy()
    print(f"INFO: funcn df.shape= {df.shape}")
    if write_pred_file:
        QpsUtil.smart_dump(df[predNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db", debug = True)
    
    #df.dropna(axis=0, how='any', inplace=True)

    # cxAll = {'All': None}
    # for year in (df.index.get_level_values(1).year.unique()):
    #     cxAll[str(year)] = None

    # we sort latest years first so we can inspect recent years' performance first, and decide if we want to calculate more
    allYears = sorted(df.index.get_level_values(1).year.unique(), reverse=True)
    #allYears.append(3000)
    cxAll = {}
    summaryAll = {}

    USE_THREAD = 5
    if USE_THREAD:
        while len(allYears)>0:
            if len(allYears) > USE_THREAD:
                calcYears = allYears[:USE_THREAD]
                allYears = allYears[USE_THREAD:]
            else:
                calcYears = allYears
                allYears = []

            print(f"INFO: calculating for years {calcYears}")

            with ThreadPoolExecutor(max_workers=USE_THREAD) as po:
                for future in as_completed([po.submit(lambda cxp:calc_cx_period(*cxp), (task, modelNm, symset, outputTag, respNm, predNm, df, year, cxAll)) for year in calcYears]):
                    rs = future.result()
                    if rs is not None:
                        summaryAll.update(rs)

            #Update for all years every batch, and then decide if continue
            summaryAll.update(calc_cx_period(task, modelNm, symset, outputTag, respNm, predNm, df, 3000, cxAll))
            summaryAll[3000]['Mkts'] = int(mean([summaryAll[x]['Mkts'] for x in sorted(cxAll.keys()) if x != 3000]))
            if abs(summaryAll[3000]['IR']) < 1.0:
                print(f"INFO: IR less than threshold, skipping rest calc ...")
                break
            
    # else:  #muli-process excounter problem with pickling lambda funtions
    #     po = Pool(5)
    #     results = []
    #     for year in allYears:
    #         results.append(po.apply_async(calc_cx_period, (task, modelNm, symset, outputTag, respNm, predNm, df, year, cxAll,)))
    #     po.close()
    #     po.join()
    #     for rs in results:
    #         summaryAll.update(rs.get())

    

    summaryDf = pd.DataFrame.from_dict(summaryAll, orient='index')
    summaryDf.sort_index(inplace=True)
    print(summaryDf.T)

    cxFp =  f"{task.dta_dir(cache=False)}/{modelNm}/cx.daily.pkl"
    QpsUtil.smart_dump(cxAll, cxFp, debug=True)
    summaryFp =  f"{task.dta_dir(cache=False)}/{modelNm}/cx.summary.pkl"
    QpsUtil.smart_dump(summaryAll, summaryFp, debug=True)

    doneFile.dump(done_version())



