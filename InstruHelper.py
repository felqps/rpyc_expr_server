#!/usr/bin/env python

import os,sys
import pickle

from optparse import OptionParser
import pandas as pd
from dateutil.parser import parse as dtparse
from QDData import *
#from VpsObjMaker import *
from VpsInstru import *
import rqdatac as rc
import glob
from QDF import *
from shared_cmn import *
from rq_init import *
#from shared_func import *  #can not be imported here as shared_func import this file.
from QDFile import *
from cache_or_run import *
from common_basic import *
from CTX import *


dominant_dates = None

def get_dominant_dates(debug=False):
    funcn = "get_dominant_dates"
    debug=False

    global dominant_dates
    if dominant_dates != None:
        return dominant_dates

    dominant_dates = defaultdict(dict)

    domnQdf = QDFile(fp=dd('raw')) + 'futures' + 'dominant_contract.db' #QSF()+'futures'+'dominant_contract.db'
    assert os.path.exists(domnQdf.fp()), f"ERROR: get_dominant_dates cannot find {domnQdf.fp()}"
    if debug:
        print(f"DEBUG: {funcn} {domnQdf.fp()}")
    #for domnFn in glob.glob('/qpsdata/data_rq.2010*_uptodate/futures/dominant_contract.db'):
    for domnFn in [domnQdf.fp()]:
        qps_print(f"INFO: get_dominant_dates reading {domnFn}")
        dominant_ctr = pickle.loads(Path(domnFn).read_bytes())
        for mkt in dominant_ctr.keys(): #a dict of pandas series
            for dt, value in dominant_ctr[mkt].items(): #value is a pandas series
                if type(value)!=type(None):
                    for dtm,ctr in value.iteritems():
                        mdts = dominant_dates[ctr]
                        if 'dominant_date' not in mdts or mdts['dominant_date']>dt:
                            mdts['dominant_date']=dt
                        if 'de_dominant_date' not in mdts or mdts['de_dominant_date']<dt:
                            mdts['de_dominant_date']=dt

    cont_ctrs = set([ctr[:-4] + '888' for ctr in dominant_dates.keys()])
  
    for cont_ctr in cont_ctrs:
        dominant_dates[cont_ctr] = {'dominant_date': dtparse('1990-01-01').date(), 'de_dominant_date': dtparse('2030-01-01').date()}
        #qps_print(cont_ctr)

    for ctr in [x for x in sorted(dominant_dates.keys()) if x.find('888')<0]:
        if debug:
            print('get_dominant_dates', '%-6s'%(ctr), '%s'%(dominant_dates[ctr]['dominant_date']), '%s'%(dominant_dates[ctr]['de_dominant_date']))

    #qps_print(dominant_dates)

    return(dominant_dates)

DOMINATE_DATES = None
def dominant_dates_cache(qdfRoot):
    funcn = "dominant_dates"
    global DOMINATE_DATES
    if DOMINATE_DATES is None:
        domnFn = QDFile(fp=dd('raw')) + 'futures' + 'dominant_contract.db' #QSF()+'futures'+'dominant_contract.db'
        dominant_dates_QDF = qdfRoot + 'dominant_dates.df'

        print("++++++++++++++++++cache file:", dominant_dates_QDF.fp())

        need_update = QpsUtil.need_update(dominant_dates_QDF.fp(), [domnFn.fp()], funcn, 'prod1w') 
        #if True and dominant_dates_QDF.exists():
        #if os.path.exists(dominant_dates_QDF.fp()):
        if not need_update:
            #dominant_dates = pickle.loads(dominant_dates_cache_path.read_bytes())
            print(f'INFO: {funcn} reading cached {dominant_dates_QDF.fp()}, deps: {domnFn.fp()}')
            DOMINATE_DATES = dominant_dates_QDF.load()
        else:
            DOMINATE_DATES = get_dominant_dates()
            print(f'INFO: {funcn} creating {dominant_dates_QDF.fp()}, deps: {domnFn.fp()}')
            dominant_dates_QDF.dump(DOMINATE_DATES)
            #dominant_dates_cache_path.write_bytes(pickle.dumps(dominant_dates))

    return DOMINATE_DATES

def get_existing_indu():
    dirs = glob.glob('%s/???'%(dd('raw')))
    return [d.split(r'/')[-1] for d in dirs if d.find('FOO')<0]

def get_active_pmids(futsRoots, qdfRoot, begDt, endDt, debug=False):
    funcn = "get_active_pmids"
    qdf = QDFile(fp=dd('raw')).label('futures').label('all_instruments.db')  #QSF().label('futures').label('all_instruments.db'),
    print(f"DEBUG_7765: {funcn} {qdf.fp()}")
    # XXX
    (dp, chged) = cache_or_run(qdf, 
        ['Future'],
        func = lambda type: rc.all_instruments(type=type, market='cn'),
        endDt=endDt,
        debug=debug
    )
    print_df(dp, title=f"{funcn}:dp")

    futs = dp['Future']
    if debug or True:
        print_df(futs[futs.underlying_symbol=='BC'], title='get_active_pmids futs pre-filter')

    # if asofdate != 'fullhist':
    #     futs = futs[(futs['de_listed_date']>=dtStr(asofdate))]
    #     futs = futs[(futs['listed_date']<=dtStr(asofdate))]


    futs = futs[(futs['de_listed_date']>=dtStr(begDt))]
    futs = futs[(futs['listed_date']<=dtStr(endDt))]

    print_df(futs[futs.underlying_symbol=='BC'], title='get_active_pmids futs post-filter')

    qps_print('get_active_pmids', futsRoots, begDt, endDt, len(futs))

    vs = []
    for mkt in futsRoots:
        vs.append(futs[futs['underlying_symbol'] == mkt].copy())
    filtered = pd.concat(vs)
    #qps_print(filtered)
    pmids = sorted(list(filtered['order_book_id']))
    pmids = [x for x in pmids if x.find('888')<0] #remove cont contracts
    qps_print('get_active_contracts', list2str(pmids))
    return(pmids)

def get_main_pmids(futsRoots, qdfRoot, begDt, endDt):
    funcn = "get_main_pmids"
    inputFn = QDFile(fp=dd('raw')).label('futures').label('dominant_future.db')  #QSF().label('futures').label('dominant_future.db') 
    print(f"DEBUG: get_main_pmids input fp= {inputFn.fp()}, futsRoots={futsRoots}")
    pdf = pickle.loads(Path(inputFn.fp()).read_bytes())
    #qps_print(pdf)
    pmids = []
    for mkt in futsRoots:
        try:
            if (type(pdf[mkt])!=type(None)):
                # if asofdate == 'fullhist':
                #     pmids.extend(list(set(pdf[mkt])))
                # else:
                #     pmid = pdf[mkt][dtStr(asofdate)]
                #     pmids.append(pmid)
                for dt,pmid in pdf[mkt].items():
                    #qps_print(f'DEBUG: {dt} {pmid},  {begDt} {endDt}')
                    if dt >= dtparse(begDt) and dt<=dtparse(endDt) + timedelta(days=3):
                        if pmid not in pmids:
                            pmids.append(pmid)
            else:
                qps_print(f'ERROR: no main ctr for mkt={mkt}')
        except KeyError:
            qps_print(f'ERROR: no dominant_info for {mkt}')
    qps_print(f'get_main_pmids found: {begDt}_{endDt} {pmids}')
    return(pmids)

def get_cont_pmids(futsRoots, qdfRoot, begDt, endDt):
    pmids = sorted([(x+'888' if x.find('888')<0 else x) for x in futsRoots])
    return(pmids)

def get_active_futures(symset, qdfRoot, begDt, endDt, force=False):
    iiGen = generator_vps_instru(symset, qdfRoot, begDt, endDt, secType='futures', secFilter='active', force=force)
    iis = {}
    for ii in iiGen:
        iis[ii.permid] = ii
    return(iis)

def get_main_futures(symset, qdfRoot, begDt, endDt, force=False):
    iiGen = generator_vps_instru(symset, qdfRoot, begDt, endDt, secType='futures', secFilter='main', force=force)
    iis = {}
    for ii in iiGen:
        iis[ii.permid] = ii
    return(iis)

def create_vps_instru_futures(symset, qdfRoot, begDt, endDt, tgtFn, secFilter):
    funcn = "create_vps_instru_futures"
    df = pd.DataFrame()
    futsRoots = getSymset(symset)
    qps_print(symset, futsRoots)

    if secFilter=='main':
        pmids = get_main_pmids(futsRoots, qdfRoot, begDt, endDt)
    elif secFilter=='active':
        pmids = get_active_pmids(futsRoots, qdfRoot, begDt, endDt)
    elif secFilter=='cont':
        pmids = get_cont_pmids(futsRoots, qdfRoot, begDt, endDt)
    else:
        raise Exception(f"ERROR: invalid secFilter {secFilter}")

    #outFn = qdfRoot + symset + 'symset.db'


    inputFn = QDFile(fp=dd('raw')).label('futures').label('instruments.db') #QSF().label('futures').label('instruments.db') 
    qps_print(f'DEBUG: create_vps_instru_futures reading {inputFn.fp()}', file=sys.stdout)
    # dd = pickle.loads(Path(inputFn.fp()).read_bytes())
    # dta = QDData()
    # dta.step(filterBySymset, pmids, inFn=inputFn, saveas='symset')
    # dta.run(outFn=outFn, cache_mode='no')
    # df = instru_rq2qd(dta.pld['symset'])

    #pld = inputFn.load()
    pld = smart_load(inputFn.fp(), debug=True)
    pld = filterBySymset(pmids, pld)
    #qps_print("DEBBUG: create_vps_instru_futures", pld)
    df = instru_rq2qd(pld, qdfRoot)
    print(f"INFO: {create_vps_instru_futures} qdfRoot= {qdfRoot}, tgtFn= {tgtFn}")
    print_df(df, title=funcn)
    tgtFn.dump(df)
    #pickle.dump(df, open(tgtFn.fp(), 'wb'))
    return(df)

def create_vps_instru_stocks(symset, qdfRoot, begDt, endDt, tgtFn):
    funcn = "create_vps_instru_stocks"
    inputFn = QDFile(fp=dd('raw')).label(symset).label('instruments.db')   #QSF().label(symset).label('instruments.db') 
    qps_print(f'create_vps_instru_stocks reading {inputFn.fp()}')
    #outFn = qdfRoot + symset + 'symset.db'

    pld = smart_load(inputFn.fp(), title=funcn)
    if pld is None:
        return pld

    if not rc.initialized():
        rc.init(rq_username, rq_password)
    df = instru_rq2qd(pld, qdfRoot, begDt=begDt, endDt=endDt) #Only pass in beg/endDt for stocks
    tgtFn.dump(df)
    #pickle.dump(df, open(tgtFn.fp(), 'wb'))
    return(df)

def instru_rq2qd(pld, qdfRoot, begDt=None, endDt=None, debug=False):
    funcn = "InstruHelper.instru_rq2qd()"
    pmids = sorted(pld.keys())
    #debug = True

    iis = []
    df = pd.DataFrame()
    for pmid in pmids:
        instru = pld[pmid]
        iStr = "%s"%(instru)
        if debug:
            qps_print("<---")
            qps_print(iStr)

        #iStr = iStr.replace(")", "}").replace("=", "':").replace(", ", ", '").replace("Instrument(","global ii\nii={'")
        iStr = iStr.replace(")", "}").replace("=", "':").replace(", ", ", '").replace("Instrument(","ii={'")

        if debug:
            qps_print('-->')
            qps_print(iStr)

        loc = locals()
        #loc['ii'] = ii
        
        try:
            nan = np.nan
            exec(iStr)
        except Exception as e:
            traceback.print_exc()
            print(f"DEBUG_4335: {iStr}")
        
        ii = loc['ii']
        #print('^^^^^^^^^^^^', ii)

        if ii['type'] != "CS":
            if ii['order_book_id'] in dominant_dates_cache(qdfRoot):
                bkid = ii['order_book_id']
                if bkid == "ZN2204":
                    foo = 1
                dmdt = dominant_dates_cache(qdfRoot)[bkid]
                ii.update(dmdt)
            elif ii['order_book_id'].find('21')<0 and ii['order_book_id'].find('22')<0: #2021/2022 are not here yet
                if debug:
                    qps_print('ERROR: can not find dominant dates for %s'%(ii['order_book_id']))

            if 'underlying_order_book_id' not in ii:
                ii['underlying_order_book_id'] = 'null'
        else: #For CS, fake dominant dates
            assert begDt != None, f'ERROR: missing begDt for stocks'
            ii.update({'dominant_date': begDt, 'de_dominant_date': endDt})

        iis.append(ii)  
        if debug:
            qps_print(ii)

    df = df.from_dict(iis)

    df = df.rename(columns = {'order_book_id':'permid',
        'type': 'secType',
        'industry_code': 'category',
        'symbol': 'lclName',
        'contract_multiplier': 'factor',
        'underlying_symbol': 'metaId'   
        })

    if 'permid' in df.columns:
        df['instruId'] = df['permid']
        df['sym'] = df['permid'].apply(lambda x: x.split(r'.')[0])
        #df['metaId'] = df['permid']
        #df['src_utc_ts_chk'] = df.apply(chkTs, axis=1)
    return(df)

def generator_vps_instru(symset, qdfRoot, begDt, endDt, secType='stocks', secFilter='main', force=False, instru_force=False, debug=False):
    funcn = "generator_vps_instru"
    debug=True
    #because gen instru require RQ license, most people should only use already generated files
    tgtFn = None
    if secType in ['stocks', 'CS']:
        tgtFn = qdfRoot + f'vps_instru.{symset}.db'
    else:
        tgtFn = qdfRoot + f'vps_instru.{symset}.{secFilter}.db'
    
    #Symset information is stored on qpsdata (server side), and directory structure is different from client side
    #print("%%%%%%%%%%%%%%", tgtFn, symset)
    tgtFn = tgtFn.switchToQSF(symset)
    
    if instru_force and ctx_verbose(1):
        print(f"INFO: {funcn} {symset} {begDt}_{endDt} {secType} {secFilter} instru_force={instru_force}: {tgtFn}")

    ##need a better solution here for force mode, called multiple times
    # if os.path.exists(tgtFn.fp().replace('SN_CF_DAY', 'SN_CF_DNShort')):
    #     #assert os.path.exists(tgtFn.fp().replace('SN_CF_DAY', 'SN_CF_DNShort')), f'ERROR: cannot find {tgtFn.fp()}'
    #     df = pickle.load(open(tgtFn.fp().replace('SN_CF_DAY', 'SN_CF_DNShort'), 'rb'))

    #print(f"DEBUG: {funcn} using vps_instru file {tgtFn.fp()}, secFilter= {secFilter}")

    if os.path.exists(tgtFn.fp()):
        #assert os.path.exists(tgtFn.fp().replace('SN_CF_DAY', 'SN_CF_DNShort')), f'ERROR: cannot find {tgtFn.fp()}'
        df = smart_load(tgtFn.fp(), title=funcn, debug=True)
    else:
        if instru_force == False:
            errMsg = f"ERROR: {funcn} expected file missing {tgtFn.fp()}"
            print(errMsg, file=sys.stderr)
            assert False, errMsg
            return

        if secType in ['stocks', 'CS']:
            df = create_vps_instru_stocks(symset, qdfRoot, begDt, endDt, tgtFn)
        else:
            df = create_vps_instru_futures(symset, qdfRoot, begDt, endDt, tgtFn, secFilter)
        #qps_print('DEBUG: ', df)

        if df is None:
            return

        qps_print(f"INFO: {funcn} writing {tgtFn.fp()} {df.shape}", file=sys.stderr)
        # pickle.dump(df, open(tgtFn.fp(), 'wb'))
        # tgtFn.dump(df)
        # for usr in ['axiong', 'jli']:
        #     newQdf = tgtFn.switchUser("che", usr)
        #     qps_print(f'{"="* 50} create_vps_instru writing {newQdf.fp()} {df.shape}')
        #     newQdf.dump(df)

    cnt = 0
    iis = []
    while cnt < len(df):
        ii = VpsInstru()
        ii.__dict__.update(dict(df.iloc[cnt].items()))
        #qps_print('raw_rq_instr_info:  ', dict(df.iloc[cnt].items()))
        cnt = cnt + 1
        iis.append(ii)
        yield (ii)
    
    if ctx_verbose(1):
        qps_print(f'generator_vps_instru summary: symset={symset}, secType={secType}, secFilter={secFilter}, begDt={begDt}, endDt={endDt} rows={cnt}: {tgtFn}', file=sys.stdout)
    #qps_print('\t', '\n\t'.join(['%s'%ii.__dict__ for ii in iis]))
    #qps_print('\t'+'\n\t'.join(['%s'%ii for ii in iis]))


def get_iis_for_mkt(mkt, qdfRoot, begDt, endDt, secType, secFilter, force=False, instru_force=False):
    funcn = "get_iis_for_mkt"
    iiGen = generator_vps_instru(mkt, qdfRoot, begDt, endDt, secType=secType, secFilter=secFilter, force=force, instru_force=instru_force)
    iis = {}
    for ii in iiGen:
        iis[ii.permid] = ii
    
    #pmids = sorted(list(iis.keys()))
    return iis

if __name__ == '__main__':
    funcn = "InstruHelper.main()"
    parser = OptionParser(description="Symset()")

    parser.add_option("--cfg",
                      dest="cfg",
                      help="cfg (default: %default)",
                      metavar="cfg",
                      default="all")

    parser.add_option("--symset",
                      dest="symset",
                      help="symset (default: %default)",
                      metavar="symset",
                      default='CF_TEST01')    
            
    parser.add_option("--force",
					  dest="force",
					  type="int",
					  help="force (default: %default)",
					  metavar="force",
					  default=0)

    parser.add_option("--regtests",
					  dest="regtests",
					  type="int",
					  help="regtests (default: %default)",
					  metavar="regtests",
					  default=0)

    parser.add_option("--dryrun",
					  dest="dryrun",
					  type="int",
					  help="dryrun (default: %default)",
					  metavar="dryrun",
					  default=0)

    parser.add_option("--asofdate",
                      dest="asofdate",
                      help="asofdate (default: %default)",
                      metavar="asofdate",
                      default="download")
                      
    opt, args = parser.parse_args()

    if opt.regtests:
        print(f"INFO: regtests {funcn}")

