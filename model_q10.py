#!/home/shuser/anaconda3/bin/python

import sys

import matplotlib.pyplot as plt
from common_basic import *
from common import *
from SmartFile import *
from sklearn.linear_model import LinearRegression
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import statsmodels.api as sm
from funcs_fld import *
from funcs_series import *

def analyze_qSummary(qSummaryFp):
    fcName = os.path.dirname(qSummaryFp)
    isClassifier = False
    for classifierFld in get_classifier_flds():
        if fcName.find(classifierFld)>=0:
            isClassifier = True
    if isClassifier:
        return

    print(qSummaryFp)        
    qS = smart_load(qSummaryFp)
    rf = qS['rf.ExRet'] * 10000 #convert to bps
    #     regr = linear_model.LinearRegression()
    #     regr.fit(np.array(rf.index).reshape(-1, 1), rf.values)
    #     rfPred = regr.predict(np.array(rf.index).reshape(-1, 1))
    #     print("Coefficient of determination: %.2f" % r2_score(rf.values, rfPred))
 
    x = np.array(rf.index)
    y = (np.array(rf.values))
    X = sm.add_constant(x)
    regr = sm.OLS(y, X).fit()
    yPred = regr.predict(X) 
    #print(regr.summary())
    A = np.identity(len(regr.params))
    A = A[1:,:]
    fstat = regr.f_test(A)
    #print(regr.rsquared, regr.pvalues, fstat.fvalue[0][0], fstat.pvalue)
    plt.bar(x, y, color='blue') #bps 
    plt.plot(x, yPred, color='red', linewidth=3)
    plt.savefig(f"{qSummaryFp.replace('pkl', 'png')}")
    plt.show()
    # fcStats[fcName] = {'rsquared': regr.rsquared, 'fstat': fstat.fvalue[0][0], 'pvalue': fstat.pvalue}
    # smart_dump(fcStats, f"{qSummaryFp.replace('pkl', 'stats')}")

@deprecated
def quantile_model(task, modelNm, symset, outputTag, respNm, predNm, debug=False, force=False):
    funcn = 'quantile_model'
    doneFile = BaseSmartFile()
    try:
        fp = f"{task.dta_dir(cache=False)}/{modelNm}/{done_version()}.pkl"
        use_if_exists(doneFile, fp, force=force)
    except Exception as e:
        print (f"INFO: skipping {funcn} {e}, skipping ...")
        return

    df = task.df()

    predFacNm = f"{predNm}.{modelNm}"

    if debug:
        print_df(df, title=f'{modelNm} {predNm} before')
    pred = df[predNm]
    #pred = pred.drop_duplicates()
    pred = pred.unstack('Mkt')
    QpsUtil.smart_dump(pred, f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db", debug=True)
    # print(f"DEBUG: pred duplicated {pred.columns.duplicated()}")
    # pred = pred.loc[:, ~pred.columns.duplicated()]  
    pred.dropna(axis=0, how='all', inplace=True)
    # predBegDtm = pred.index[0]
    # predEndDtm = pred.index[-1]
    predShape = pred.shape

    qsize = int(modelNm.replace('q',''))
    if qsize == 5:
        qmean = 2
    elif qsize == 9:
        qmean = 4
    else:
        assert False, f"ERROR: unsupported qsize {qsize}"

    predQ = pred.apply(lambda x: pd.qcut(x, q=qsize, labels=False, duplicates='drop'), axis = 1)
    predQ = predQ - qmean
    predQ = predQ.stack().reset_index().sort_values(['Mkt', "Dtm"])
    predQ = predQ.set_index(['Mkt', 'Dtm'])
    df[predFacNm] = predQ
    #df = df.iloc[df.index.get_level_values('Dtm')>=predBegDtm]
    #print_df(df, title=f'{modelNm} {predNm} after', file=sys.stderr)
    #QpsUtil.smart_dump(df[predNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db")
    QpsUtil.smart_dump(df[predFacNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predFacNm}.db")

    for col in df.columns:
        for func in ['raw', 'cum']:
            colDta = df[col]
            #colDta = df[col].drop_duplicates()
            #print(f"DEBUG: duplicated colDta {colDta.index.duplicated()}")
            if func == 'cum':
                #pass
                colDta.unstack(level=0).fillna(0).cumsum().plot(figsize=(12,10), legend=False, title=f"{col} {func}")
            else:
                #pass
                colDta.unstack(level=0).fillna(0).plot(figsize=(12,10), legend=False, title=f"{col} {func}")
            
            #plt.legend(loc='upper left')
            #plt.legend(loc='lower center', ncol=5, bbox_to_anchor=(0.0, 0.0))
            plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{col}.{func}.png"))


    rdf = df.groupby([predFacNm, 'Dtm'])[respNm].mean()    
    rdf = rdf.unstack(level=0) #Making bin as columns
    #print(rdf.tail(2000))
    rdf.cumsum().plot(figsize=(12,10), legend=False)
    #plt.legend(loc='upper left')
    plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qret.png"))

    task._qSummary = pd.DataFrame()

    print(f"INFO: rdf mean {rdf.mean()}")

    for bmNm in bmRets().columns[:]:
    #for bmNm in ['rf', '3h']:
        calc_benchmark_perf(task._qSummary, rdf, bmNm, bmRets()[bmNm])

    task._qSummary['Symset'] = symset
    task._qSummary['BegDt'] = task.respBegDtm().date()
    task._qSummary['EndDt'] = task.respEndDtm().date()
    task._qSummary['Days'] = predShape[0]
    task._qSummary['Mkts'] = predShape[1]

    qSummaryFp =  f"{task.dta_dir(cache=False)}/{modelNm}/qSummary.pkl"
    QpsUtil.smart_dump(task._qSummary, qSummaryFp)
    
    # print("==============")

    # analyze_qSummary(qSummaryFp)

    # print("++++++++++++++")

    pdf = df.groupby([predFacNm])[task.predNm()]
    calc_pred_cx_traits(task._qSummary, pdf)


    avg = pd.DataFrame(rdf.mean(), columns=[respNm])
    #print(type(avg))
    avg.plot.bar(figsize=(12,10))
    plt.legend(loc='upper left')
    plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qmean.png"))

    qrng = range(-qmean, qmean+1)
    pairs = list(product(qrng, qrng))
    calc_quantile_pairs(task, rdf, pairs, respNm, predNm, modelNm, title="pairwise", force=force)
    pairs4 = [(-4,4), (-3,3), (-2,2), (-1,1)]
    calc_quantile_pairs(task, rdf, pairs4, respNm, predNm, modelNm, title="pair4", force=force)
    doneFile.dump(done_version())

    return rdf

@timeit
def decile_model(task, modelNm, symset, outputTag, respNm, predNm, debug=False, force=False):
    funcn = 'decile_model'
    doneFile = BaseSmartFile()
    try:
        fp = f"{task.dta_dir(cache=False)}/{modelNm}/{done_version()}.pkl"
        use_if_exists(doneFile, fp, force=force)
    except Exception as e:
        print (f"INFO: skipping {funcn} {e}, skipping ...")
        return

    df = task.df()

    predFacNm = f"{predNm}.{modelNm}"

    if debug:
        print_df(df, title=f'{modelNm} {predNm} before')

    pred = df[predNm]
    #pred = pred.drop_duplicates()
    pred = pred.unstack('Mkt')
    QpsUtil.smart_dump(pred, f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db")
    # print(f"DEBUG: pred duplicated {pred.columns.duplicated()}")
    # pred = pred.loc[:, ~pred.columns.duplicated()]  
    pred.dropna(axis=0, how='all', inplace=True)
    # predBegDtm = pred.index[0]
    # predEndDtm = pred.index[-1]
    predShape = pred.shape

    qsize = 10
    qmean = 4.5

    predQ = pred.apply(lambda x: pd.qcut(x, q=qsize, labels=False, duplicates='drop'), axis = 1)
    predQ = predQ.stack().reset_index().sort_values(['Mkt', "Dtm"])
    predQ = predQ.set_index(['Mkt', 'Dtm'])
    df[predFacNm] = predQ
    #df = df.iloc[df.index.get_level_values('Dtm')>=predBegDtm]
    #print_df(df, title=f'{modelNm} {predNm} after', file=sys.stderr)
    #QpsUtil.smart_dump(df[predNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predNm}.raw.db")
    QpsUtil.smart_dump(df[predFacNm], f"{task.dta_dir(cache=False)}/{modelNm}/{predFacNm}.db")

    for col in df.columns:
        for func in ['raw', 'cum']:
            colDta = df[col] 
            if col == predFacNm:
                colDta  = colDta - qmean
            #colDta = df[col].drop_duplicates()
            #print(f"DEBUG: duplicated colDta {colDta.index.duplicated()}")
            if func == 'cum':
                #pass
                colDta.unstack(level=0).fillna(0).cumsum().plot(figsize=(12,10), legend=False, title=f"{col} {func}")
            else:
                #pass
                colDta.unstack(level=0).fillna(0).plot(figsize=(12,10), legend=False, title=f"{col} {func}")
            
            #plt.legend(loc='upper left')
            #plt.legend(loc='lower center', ncol=5, bbox_to_anchor=(0.0, 0.0))
            plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{col}.{func}.png"))


    rdf = df.groupby([predFacNm, 'Dtm'])[respNm].mean()    
    rdf = rdf.unstack(level=0) #Making bin as columns
    #print(rdf.tail(2000))
    rdf.cumsum().plot(figsize=(12,10), legend=True)
    #plt.legend(loc='upper left')
    plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qret.png"))

    task._qSummary = pd.DataFrame()

    print(f"INFO: rdf mean {rdf.mean()}")

    bmRets = getBmRets(task.opt(), symset, df.groupby(['Dtm'])[respNm].mean())
    for bmNm in bmRets.columns[:]:
    #for bmNm in ['rf', '3h']:
        calc_benchmark_perf(task._qSummary, rdf, bmNm, bmRets[bmNm])

    task._qSummary['Symset'] = symset
    task._qSummary['BegDt'] = task.respBegDtm().date()
    task._qSummary['EndDt'] = task.respEndDtm().date()
    task._qSummary['Days'] = predShape[0]
    task._qSummary['Mkts'] = predShape[1]

    qSummaryFp =  f"{task.dta_dir(cache=False)}/{modelNm}/qSummary.pkl"
    QpsUtil.smart_dump(task._qSummary, qSummaryFp)
    
    # print("==============")

    # analyze_qSummary(qSummaryFp)

    # print("++++++++++++++")

    pdf = df.groupby([predFacNm])[predNm]
    calc_pred_cx_traits(task._qSummary, pdf)


    avg = pd.DataFrame(rdf.mean(), columns=[respNm])
    #print(type(avg))
    avg.plot.bar(figsize=(12,10))
    plt.legend(loc='upper left')
    plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qmean.png"))

    # qrng = range(-qmean, qmean+1)
    # pairs = list(product(qrng, qrng))
    # calc_quantile_pairs(task, rdf, pairs, respNm, predNm, modelNm, title="pairwise", force=force)
    pairs4 = [(0,9), (1,8), (2,7), (3,6), (4,5)]
    calc_quantile_pairs(task, rdf, pairs4, respNm, predNm, modelNm, title="pair4", force=force)
    doneFile.dump(done_version())

    return rdf


def calc_pred_cx_traits(qSummary, pdf):
    from scipy.stats import kurtosis, skew
    #print(f"DEBUG: pdf", pdf.tail())
    qSummary["Pred.mean"] = pdf.mean()
    qSummary["Pred.std"] = pdf.std()
    qSummary["Pred.max"] = pdf.max()
    qSummary["Pred.min"] = pdf.min()
    qSummary["Pred.obs"] = pdf.count()
    #print(f"DEBUG: uniq {type(pdf.unique())}, {pdf.value_counts()}")
    qSummary["Pred.obsuniq"] = pdf.apply(lambda x: x.unique().size)
    
    #print(np.array(pdf))
    #qSummary["Pred.skew"] = pdf.skew(axis = 0, skipna = True)
    qSummary["Pred.skew"] = pdf.apply(lambda x: skew(x))
    qSummary["Pred.kurtosis"] = pdf.apply(lambda x: kurtosis(x))
    return qSummary

def calc_qsummary(qSummary, rdf, pairs, respNm, predNm, debug=False):
    print("INFO: quantile summary")
    qSummary["RetMean"] = rdf.mean() #*10000.0
    qSummary['RetCum'] = rdf.sum() #*100.0
    qSummary['RetStd'] = rdf.std() #* 16.0 * 100.0
    qSummary['IR'] = rdf.mean()/rdf.std()*16.0
    if debug:
        print(task._qSummary)
    return qSummary

def calc_benchmark_perf(qSummary, rdf, bmNm, bmRet):
    funcn = 'calc_qsummary_benchmark'
    # bmRetMean = bmRet.mean() #* 10000.0
    # qSummary[f'{bmNm}.Ret'] = bmRetMean 
    # print(f"DEBUG: {funcn} {bmNm} {rdf.shape} {rdf.mean()} {rdf.sum()}")
    # qSummary[f'{bmNm}.ExRet'] = rdf.mean() - bmRetMean
    # qSummary[f'{bmNm}.ExRetCum'] = rdf.sum() - bmRet.sum()

    for yN in rdf.columns[:]:
        mdl = lm(rdf[yN], bmRet, title=f"{bmNm}:{yN} {rdf[yN].mean()}")
        if mdl is None:
            continue
        for k in mdl:
            qSummary.loc[yN, k] = mdl[k]

@timeit
def calc_quantile_pairs(task, rdf, pairs, respNm, predNm, modelNm, title, force=False):
    funcn = "calc_quantile_pairs"

    dtaFp = f"{task.dta_dir(cache=False)}/{modelNm}/{title}.pkl"
    # if os.path.exists(dtaFp) and not force:
    #     return
    
    qPairs = {'rets': {}, 'mean': {}, 'std': {}, 'IR': {}}
    rdf = rdf.replace(np.inf, 0.0)
    rdf = rdf.replace(-np.inf, 0.0)
    for bins in ([x for x in pairs if x[0]<x[1]]):
        [b1, b2] = bins
        b1 = int(b1)
        b2 = int(b2)
        if b1 not in rdf.columns or b2 not in rdf.columns:
            print(f"DEBUG: rdf does not has {b1}, {rdf.columns}", file=sys.stderr)
            continue
        bdiff = rdf[b1] - rdf[b2]
        
        bdiff.dropna(inplace=True)
        # print("Before", bdiff[np.isinf(bdiff)])
        # bdiff = bdiff.replace(np.inf, 0.0)
        # bdiff = bdiff.replace(-np.inf, 0.0)
        # print("After", bdiff[np.isinf(bdiff)])
        # print(bdiff.head())

        # if bdiff.mean()<0:
        #     bdiff = bdiff * (-1.0)
            
        #bin = f"{b1}_{b2}"
        bin = f"s{b1}"
        #print(f"INFO: bin= {bin}, mean= {bdiff.mean()}, std= {bdiff.std()}", file=sys.stderr)
        qPairs['rets'][bin] = bdiff
        qPairs['mean'][bin] = bdiff.mean()
        qPairs['std'][bin] = bdiff.std()
        qPairs['IR'][bin] = qPairs['mean'][bin]/qPairs['std'][bin] * 16.0

    if qPairs['rets']: #not empty      
        diffDF = pd.DataFrame.from_dict(qPairs['rets'], orient='columns')
        diffDF.sort_index(inplace=True)
        diffDF.cumsum().plot(figsize=(12,10), legend=True)
        plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qret_{title}.png"))

        irDf = pd.DataFrame.from_dict(qPairs['IR'], orient='index', columns=['IR'])
        print(irDf)
        #irDf.sort_values(['IR'], ascending=True, inplace=True)
        irDf.plot.bar(figsize=(12,10))
        plt.legend(loc='upper left')
        plt.savefig(QpsUtil.create_dir_if_needed(f"{task.dta_dir(cache=False)}/{modelNm}/{respNm}.qret_{title}_IR.png"))

    QpsUtil.smart_dump(qPairs, dtaFp)

