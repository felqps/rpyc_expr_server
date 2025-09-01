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
from main_che import *

NUMBER_DAYS_PER_YEAR = 243 #vs252
EXCEL_DATA_DUMP = False

xxx2001 = None
def trd_amt(a, b, dtm=None):
    global xxx2001
    ab = pd.DataFrame.from_dict({'a':a, 'b':b})
    ab.fillna(0.0, inplace=True) 
    rc = (ab['a'] - ab['b']).abs().sum()
    if dtm is not None and dtm.year == 2021:
        if xxx2001 is None:
            xxx2001 = open(f"/Fairedge_dev/app_QpsData/regtests/xxx2001.csv", 'w')
        print(f"%s,%.6f"%(dtm,rc), file=xxx2001)

    return rc

@timeit_real
def calc_cx_fast_at_dtm(opt, symset, xy1d, respNm, predNm, xy1dHist, dtm, detailed, alphaQPct=0.2, 
    roundTripFees=0.002, title="no-title", debug=False, debug_print_cnt=-1, future_margin=0.20):
    funcn = "calc_cx_fast_at_dtm"
    debug = debug or debug_print_cnt >= 0
    # debug = True
    if debug:
        print(f'+++++++++++++++++++++++++++++++++++++++++{funcn}++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    cx = {}
    xy1d = xy1d.replace(np.inf, np.nan)
    xy1d = xy1d.replace(-np.inf, np.nan)
    xy1d.dropna(axis=0, how='any', inplace=True)
    xy1d = xy1d.droplevel(level=1)
    predNmMa = f"{predNm}.ma"
    predNmZ = f"{predNm}.Z"
    xy1d[predNmZ] = Z(xy1d[[predNm]], axis=0)
    predNmRank = f"{predNm}.Rank"
    #predNmRankMa = f"{predNm}.RankMa"
    xy1d[predNmRank] = xy1d[[predNmZ]].rank(axis=0, pct=True)
    #xy1d[predNmRankMa] = xy1d[[predNmMa]].rank(axis=0, pct=True)

    predNmAlphaQT = f"{predNm}.AlphaQT"
    predNmAlphaQTma = f"{predNm}.AlphaQTma"
    xy1d[predNmAlphaQT] = 0.0
    if False: #orig
        xy1d[predNmAlphaQT][xy1d[predNmRank]>=(1-alphaQPct)] = 1.0 #rank ascending, high rank assigned to 1
        xy1d[predNmAlphaQT][xy1d[predNmRank]<=alphaQPct] = -1.0    
    else: #Rui
        xy1d[predNmAlphaQT][xy1d[predNm] > xy1d[predNm].quantile(1-alphaQPct)] = 1.0 #rank ascending, high rank assigned to 1
        xy1d[predNmAlphaQT][xy1d[predNm] < xy1d[predNm].quantile(alphaQPct)] = -1.0
    #print(f"DEBUG: {funcn} {dtm} {xy1d[predNm].count()} {xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]>0].sum()} {xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]<0].sum()}")
    xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]>0] /= xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]>0].sum() * 2.0
    xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]<0] /= -xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]<0].sum() * 2.0


    xy1d[predNmAlphaQTma] = 0.0
    if True:
        #print_df(xy1d)
        xy1d[predNmAlphaQTma][xy1d[predNmMa] > xy1d[predNmMa].quantile(1-alphaQPct)] = 1.0 #rank ascending, high rank assigned to 1
        xy1d[predNmAlphaQTma][xy1d[predNmMa] < xy1d[predNmMa].quantile(alphaQPct)] = -1.0

    xy1d[predNmAlphaQTma][xy1d[predNmAlphaQTma]>0] /= xy1d[predNmAlphaQTma][xy1d[predNmAlphaQTma]>0].sum() * 2.0
    xy1d[predNmAlphaQTma][xy1d[predNmAlphaQTma]<0] /= -xy1d[predNmAlphaQTma][xy1d[predNmAlphaQTma]<0].sum() * 2.0

    predNmAlphaQL = f"{predNm}.AlphaQL"
    xy1d[predNmAlphaQL] = 0.0
    xy1d[predNmAlphaQL][xy1d[predNmAlphaQT]>0] = xy1d[predNmAlphaQT][xy1d[predNmAlphaQT]>0]

    predNmAlphaLC = f"{predNm}.AlphaLC" #long cash
    xy1d[predNmAlphaLC] = 0.0
    xy1d[predNmAlphaLC][xy1d[predNmZ]>0] = xy1d[predNmZ][xy1d[predNmZ]>0]

    predNmAlphaLS = f"{predNm}.AlphaLS"
    xy1d[predNmAlphaLS] = M(xy1d[[predNm]], axis=0)
    xy1d[predNmAlphaLS] /= xy1d[predNmAlphaLS].abs().sum()

    if detailed:
        # symsetRets = xy1d.groupby(['Dtm'])[respNm].mean()
        # print_df(symsetRets, title='symsetRets')
        symsetRets = None  # do not calc symset returns for now
        bmRets = getBmRets(opt, symset, symsetRets)
        for bm in bmRets.columns:  # align with xy1d
            #xy1d[bm] = xy1d.index.map(lambda idx: mktdtmIdx2IndexReturn(bmRets, idx, bm))  # bmRets[bm]
            xy1d[bm] = mktdtmIdx2IndexReturn(bmRets, (0,dtm), bm)

    cx["alphaQT_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaQT])
    #cx["alphaQT_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaQT]) / (~xy1d[respNm].isna() * xy1d[predNmAlphaQT]).abs().sum()
    cx["alphaQL_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaQL])
    cx["alphaLC_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaLC])/xy1d[predNmAlphaLC].abs().sum()
    cx["alphaLS_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaLS])
    
    if detailed:        
        cx["alphaQ3h_raw"] = (xy1d[respNm]-xy1d['3h']).dot(xy1d[predNmAlphaQL]*(1 - future_margin)) / xy1d[predNmAlphaQL].abs().sum()
        cx["alphaQ5h_raw"] = (xy1d[respNm] - xy1d['5h']).dot(xy1d[predNmAlphaQL]*(1 - future_margin)) / xy1d[predNmAlphaQL].abs().sum()
        #cx["alphaLM_raw"] = xy1d[respNm].dot(xy1d[predNmZ])/xy1d[predNmZ].dot(xy1d[predNmZ])
        cx["alphaQTma_raw"] = xy1d[respNm].dot(xy1d[predNmAlphaQTma])
        
        #print(xy1d[predNmRank].max(), xy1d[predNmRank].min())


    if xy1d.empty:
        if debug:
            print(f"WARNING: no data for title= {title}, skipping...")
        return cx

    ic = xy1d[[respNm,predNm]].corr(method='pearson')
    rankic = xy1d[[respNm,predNm]].corr(method='spearman')
    # print(f"ic={ic}")
    # print(f"rankic={rankic}")
    cx['ic'] = ic.iloc[0, 1]
    cx['rankic'] = rankic.iloc[0, 1]

    if detailed:
        daysL = [1, 2, 5, 10, 20]
    else:
        daysL = [1]

    for days in daysL:
        if len(xy1dHist) < days:
            if len(xy1dHist) <= 0:
                cx["alphaQT_net"] = cx["alphaQT_raw"] 
                cx["alphaQTma_net"] = cx["alphaQTma_raw"] 
                cx["alphaQL_net"] = cx["alphaQL_raw"]
                cx["alphaLS_net"] = cx["alphaLS_raw"]
                cx["alphaLC_net"] = cx["alphaLC_raw"]
                if detailed:
                    cx["alphaQ3h_net"] = cx["alphaQ3h_raw"]
                    cx["alphaQ5h_net"] = cx["alphaQ5h_raw"]
            continue
        
        xy1dLag = xy1dHist[-days][1]
        if days == 1:            
            #print(dtm, xy1d[predNmAlphaQT_trd].abs().sum(), xy1d[predNmAlphaQT].abs().sum(), xy1dLag[predNmAlphaQT].abs().sum(),)
            if len(xy1dHist)>0:
                cx["alphaQT_net"] = cx["alphaQT_raw"] - 0.5 * roundTripFees * trd_amt(xy1d[predNmAlphaQT], xy1dLag[predNmAlphaQT])
                cx["alphaQTma_net"] = cx["alphaQTma_raw"] - 0.5 * roundTripFees * trd_amt(xy1d[predNmAlphaQTma], xy1dLag[predNmAlphaQTma])
                cx["alphaQL_net"] = cx["alphaQL_raw"] - 0.5 * roundTripFees * trd_amt(xy1d[predNmAlphaQL], xy1dLag[predNmAlphaQL])/xy1d[predNmAlphaQL].abs().sum()
                cx["alphaLS_net"] = cx["alphaLS_raw"] - 0.5 * roundTripFees * trd_amt(xy1d[predNmAlphaLS], xy1dLag[predNmAlphaLS], dtm=None)
                cx["alphaLC_net"] = cx["alphaLC_raw"] - 0.5 * roundTripFees * trd_amt(xy1d[predNmAlphaLC], xy1dLag[predNmAlphaLC])/xy1d[predNmAlphaLC].abs().sum()
                if detailed:
                    pos = xy1d[predNmAlphaQL]*(1-future_margin)/xy1d[predNmAlphaQL].abs().sum()
                    posLag = xy1dLag[predNmAlphaQL]*(1-future_margin)/xy1dLag[predNmAlphaQL].abs().sum()
                    cx["alphaQ3h_net"] = cx["alphaQ3h_raw"] - 0.5 * roundTripFees * trd_amt(pos, posLag)
                    cx["alphaQ5h_net"] = cx["alphaQ5h_raw"] - 0.5 * roundTripFees * trd_amt(pos, posLag)

        pairDf = pd.DataFrame.from_dict({'x': xy1d[predNmZ], 'y': xy1dLag[predNmZ]}, orient='columns')
        #pairDf['y'] = pairDf['y'].shift(1)
        pairDf.dropna(axis=0, how='any', inplace=True)
        #print_df(pairDf, title='pairDf')
        cx[f'alpha_turnover_%02d' % (days)] = (pairDf['x'] - pairDf['y']).abs().sum() / pairDf['y'].abs().sum()
        cx[f'ic_decay_%02d' % (days)] = pairDf.corr(method='spearman').iloc[0, 1]
        # cx[f'ic_decay_%02d'%(days)] = pair[[predNm, predDecayNm]].corr(method='spearman').iloc[0,1]--
        cx[f'mean_ic_decay_%02d' % (days)] = cx[f'ic_decay_%02d' % (days)] / days

    if debug:
        print_df(xy1d.T, title=f'xy1d {dtm}')
        #print(f"DEBUG: {funcn} mean {xy1d.mean()}")

    xy1dHist.append((dtm, xy1d))
    # print(f"xy1dHist {len(xy1dHist)}")

    if debug:
        print("cx", cx)
    return cx


@timeit
def calc_cx_fast_period(task, modelNm, symset, outputTag, respNm, predNm, df, year, cxAll, detailed, ma_days, debug=False):
    funcn = "calc_cx_fast_period"

    s = {}
    if year != 3000:
        xy = df[df.index.get_level_values(1).year == year].copy()
        
        maDf = xy[predNm].unstack(level=0).rolling(ma_days).mean()
        #print_df(maDf.head(50), title="xxx2007")

        maDf.fillna(0.0, inplace=True)

        xy.dropna(axis=0, how='any', inplace=True)
        xy[f"{predNm}.ma"] = stack(maDf)

        s['Mkts'] = len(xy.index.get_level_values(0).unique())
        dtms = sorted(xy.index.get_level_values(1).unique())
        if debug or True:
            print(f"DEBUG: ------------------- {funcn} calculating year= {year}, shape= {xy.shape}, dtms= {QpsUtil.format_arr(dtms, maxShown=1)}")
            print_df(xy)
        if xy.empty:
            return None

        xy1dHist = []
        cxByDay = {}
        debug_print_cnt = 0
        for dtm in dtms:
            if True and debug:
                print(f"INFO: calc {dtm}")
            xy1d = xy[xy.index.get_level_values(1) == dtm].copy()  # [[respNm, predNm]]
            # print(xy1d.tail())
            debug_print_cnt -= 1
            cxByDay[dtm] = calc_cx_fast_at_dtm(task.opt(), symset, xy1d, respNm, predNm, xy1dHist, dtm, detailed, 
                                          title=f"{funcn}.{year}.{dtm}", debug=False, debug_print_cnt=debug_print_cnt)


        cxByDayDf = pd.DataFrame.from_dict(cxByDay, orient='index')
        cxByDayDf = cxByDayDf[sorted(cxByDayDf.columns)]

        #Data for Rui
        if EXCEL_DATA_DUMP:
            fp = f"{task.dta_dir(cache=False)}/{modelNm}/cx.daily.{year}.csv"
            smart_dump(cxByDayDf, fp, fmt='csv', debug=True)
            addDtm = []
            for (dtm, y) in xy1dHist:
                y = y.reset_index()
                y['Dtm'] = dtm
                addDtm.append(y)
            xy1dHistDf = pd.concat(addDtm, axis=0)
            xy1dHistDf.set_index(['Dtm', 'Mkt'], inplace=True)
            xy1dHistDf.reset_index(inplace=True)
            print('='*300)
            print_df(xy1dHistDf)
            fp = f"{task.dta_dir(cache=False)}/{modelNm}/xy.daily.{year}.csv"
            smart_dump(xy1dHistDf, fp, fmt='csv', debug=True)
    else:
        # cxByDayDf = pd.concat(list(cxAll.values()).reverse()) #need to revese as recent years first, and concat need to be in time-order
        # cxAllRev = dict(sorted(list(cxAll.items())))

        print("-" * 200)
        print(sorted(cxAll.keys()))
        cxByDayDf = pd.concat([cxAll[x] for x in sorted(cxAll.keys()) if x != 3000])

    if debug:
        print_df(cxByDayDf, show_head=True, show_body=True)

    if debug:
        print(cxByDayDf.tail())

    cxAll[year] = cxByDayDf

    s.update(calc_mean_values(cxByDayDf))
    (s['Days'], dummy) = cxByDayDf.shape

    aNms = ['alphaQT_raw', 'alphaQT_net', 'alphaQL_raw', 'alphaQL_net', 'alphaLS_raw', 'alphaLS_net', 'alphaQTma_raw', 'alphaQTma_net']

    if detailed:
        aNms.extend(['alphaLC_raw', 'alphaLC_net', 'alphaQ3h_raw', 'alphaQ3h_net', 'alphaQ5h_raw', 'alphaQ5h_net'])

    if year == 2021:
        smart_dump(cxByDayDf['alphaLS_net'], f"/Fairedge_dev/app_QpsData/regtests/xxx2002.csv")

    for aNm in aNms:
        s[f'{aNm}_AnnRet'] = s[aNm] * NUMBER_DAYS_PER_YEAR
        s[f'{aNm}_AnnVol'] = math.sqrt(NUMBER_DAYS_PER_YEAR) * cxByDayDf[aNm].std()
        s[f'{aNm}_IR'] = math.sqrt(NUMBER_DAYS_PER_YEAR) * s[aNm] / cxByDayDf[aNm].std()
        if False:
            s[f'{aNm}_MaxDD_p'] = max_drawdown(cxByDayDf[aNm])
        #(s[f'{aNm}_MaxDD'], s[f'{aNm}_RcyDay']) = max_drawdown2(cxByDayDf[aNm], title=year)[:2]
        s[f'{aNm}_MaxDD'] = max_drawdown2(cxByDayDf[aNm], title=year)[0]
        s[f'{aNm}_RcyDay'] = CalcRecDays(cxByDayDf[aNm])
        if False:
            # (s['MaxDD-'], s['RcyDay-']) = max_drawdown2(cxByDayDf[aNm], mult = -1.0, title=year)[:2]
            s['NetAnnRet'] = (s[aNm] - 0.0002 * s['alpha_turnover_01']) * NUMBER_DAYS_PER_YEAR
            s['NetIR'] = math.sqrt(NUMBER_DAYS_PER_YEAR) * (s[aNm] - 0.002 * s['alpha_turnover_01']) / (
                    cxByDayDf[aNm] - 0.0002 * cxByDayDf['alpha_turnover_01']).std()

        if debug:
            print(f"INFO: {funcn}, year={year}, {s[f'{aNm}_IR']}")

    return {year: s}


@func_called
def cross_section_fast_model(task, modelNm, symset, outputTag, respNm, predNm, debug=False, force=False, write_pred_file=False, detailed=False, ma_days=10, is_binary_factor=False):
    funcn = 'cross_section_fast_model'
    doneFile = BaseSmartFile()
    try:
        fp = f"{task.dta_dir(cache=False)}/{modelNm}/{done_version()}.{detailed}.pkl"
        use_if_exists(doneFile, fp, force=force)
    except Exception as e:
        print(f"INFO: skipping {funcn} {e}, skipping ...")
        if task.opt().show:
            task.show()
        return

    print(f'Start {funcn} ...', file=sys.stderr)
    params = get_perf_params(task._opt.scn, 
                             symset, 
                             predNm, 
                             simple_report=False, 
                             return_data=task.nsByVar()[respNm].copy(), 
                             factor_data=task.nsByVar()[predNm].copy(), 
                             is_binary_factor=is_binary_factor)
    params['result_dir'] = f"{task.dta_dir(cache=False)}/{modelNm}/"
    params['pdf_file'] = 'cx.pdf'
    QpsUtil.mkdir([params['result_dir']])

    smart_dump(params, f"cross_section_fast_model.args/params.pkl", debug=True, title=funcn, verbose=1)

    fm = AlphaPerformance(params=params)
    pfd = fm.GetPerformanceData()

    if ctx_debug(1):
        print(f"DEBUG_1223: {funcn} factor_turnover", pfd['factor_turnover'])

    fctRets = fm.factor_return().GetFactorReturns()
    if ctx_verbose(1):
        print(funcn, list(fctRets.keys()))
        print(funcn, fctRets['factor_returns'].keys())

    factorReturnsRaw = fctRets['factor_returns']
    factorReturnsNet = fctRets['factor_net_returns']

    cxAll = {}
    
    summaryAll = {}
    #an = alpha-name; ran = run-against-name
    for an,ran in {'LS': 'LS', 'QT': 'QT', "QTma": "QT_ma", "Q3h": "QT_HS300", "Q5h": "QT_ZZ500"}.items():              
        d = {}
        origAn = f"alpha_{ran}"
        if origAn not in pfd['performance']:
            continue
        d['raw'] = pfd['performance'][origAn][2]
        d['net'] = pfd['net_performance'][origAn][2]
        for k,v in d.items():
            v['3000'] = v.pop('all')
            v.pop('is')
            v.pop('os')
            for yr,dta in v.items():
                yr = int(yr)
                if yr not in summaryAll:
                    summaryAll[yr] = {}
                s = summaryAll[yr]
                s[f'alpha{an}_{k}_AnnRet'] = dta['Annualized Return']
                s[f'alpha{an}_{k}_AnnVol'] = dta['Annualized Volatility']
                s[f'alpha{an}_{k}_IR'] = dta['Annualized IR']
                s[f'alpha{an}_{k}_MaxDD'] = dta['Max Drawdown']
                s[f'alpha{an}_{k}_RcyDays'] = dta['Max Recovery Periods']
                s[f'alpha{an}_{k}_Turnover'] = dta['Turnover']
                #print(an, yr, s)

                if origAn in factorReturnsRaw:
                    cxAll[f'alpha{an}_raw'] = factorReturnsRaw[origAn]
                    cxAll[f'alpha{an}_net'] = factorReturnsNet[origAn]
                    

    summaryAll[3000]['fcTurnover'] = pfd['factor_turnover']['1']
    #print("DEBUG_1224", cxAll)
    cxAll.update({3000: pd.DataFrame.from_dict(cxAll, orient='columns')})

    if False: #orignal implementation
        detailed = True
        cxAll = {}
        summaryAll = {}
        df = task.df()[[respNm, predNm]]#.copy()
        print(f"INFO: {funcn} df.shape= {df.shape}")
        #print_df(df)

        if write_pred_file:
            QpsUtil.smart_dump(df[predNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db", debug = True)

        # we sort latest years first so we can inspect recent years' performance first, and decide if we want to calculate more
        allYears = sorted(df.index.get_level_values(1).year.unique(), reverse=True)
        #allYears.append(3000)

        USE_THREAD = 1 #1
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
                    for future in as_completed([po.submit(lambda cxp:calc_cx_fast_period(*cxp), (task, modelNm, symset, outputTag, respNm, predNm, df, year, cxAll, detailed, ma_days, )) for year in calcYears]):
                        rs = future.result()
                        if rs is not None:
                            summaryAll.update(rs)

                #Update for all years every batch, and then decide if continue
                summaryAll.update(calc_cx_fast_period(task, modelNm, symset, outputTag, respNm, predNm, df, 3000, cxAll, detailed, ma_days))
                summaryAll[3000]['Mkts'] = int(mean([summaryAll[x]['Mkts'] for x in sorted(cxAll.keys()) if x != 3000]))
                if abs(summaryAll[3000]['alphaQT_raw_IR']) < 1.0 and False:
                    print(f"INFO: IR less than threshold, skipping rest calc ...")
                    break

    save_cx_reports(task, modelNm, cxAll, summaryAll)

    if True:
        task.save(what='setup')

    smart_dump(done_version(), fp, title=funcn)

    return summaryAll
    
def save_cx_reports(task, modelNm, cxAll, summaryAll):
    funcn = 'save_cx_reports'
    summaryDf = pd.DataFrame.from_dict(summaryAll, orient='index')
    summaryDf.sort_index(inplace=True)
    if ctx_verbose(1):
        print(summaryDf.T)
    elif ctx_verbose(0):
        print(summaryDf[[x for x in summaryDf.columns if x.find("alphaQ5h")>=0]].T)
    cxFp = f"{task.dta_dir(cache=False)}/{modelNm}/cx.daily.pkl"
    smart_dump(cxAll, cxFp, debug=True, title=funcn)
    summaryFp = f"{task.dta_dir(cache=False)}/{modelNm}/cx.summary.pkl"
    smart_dump(summaryAll, summaryFp, debug=True, title=funcn)

    if EXCEL_DATA_DUMP:
        smart_dump(cxAll, cxFp.replace('pkl', 'txt'), fmt='txt', debug=True, title=funcn)
        smart_dump(summaryAll, summaryFp.replace('pkl', 'txt'), fmt='txt', debug=True, title=funcn)

