from pathlib import Path
import os,sys,pickle
import pandas as pd
import glob
from fdf_helpers import *
from options_helper import *

def add_date_dash(dt):
    return f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"

def create_cs_symsets(begdt='20210810', enddt='20300101'):
    begdt = add_date_dash(begdt)
    enddt = add_date_dash(enddt)
    symsets = {}
    instrudb = pickle.loads(Path("/qpsdata/data_rq.20210810_uptodate/whole_market/all_instruments.db").read_bytes())['CS']
    instrudb.loc[instrudb['de_listed_date']=='0000-00-00', 'de_listed_date'] = '2030-01-01' 
    instrudb = instrudb[instrudb['de_listed_date']>begdt]
    instrudb = instrudb[instrudb['status']!='Unknown']
    # instrudb = instrudb[['order_book_id', 'type', 'de_listed_date']]
    # exit(0)
    # print(instrudb.shape)
    symsets['CS_ALL'] = list(instrudb['order_book_id'])

    for fn in glob.glob(f"/qpsdata/config/symset_constituent/*.xls"):
        try:
            ssn = "CS_" + os.path.basename(fn).split(".")[0]
            df = pd.read_excel(fn, dtype={"sym":str})
            symsets[ssn] = [f"{sym}.XSHG" if sym >= "600000" else f"{sym}.XSHE" for sym in df["sym"]]
        except Exception as e:
            logger().exception(e)
            logger().error(f"Read {fn} error, no such column: 'sym'")

    for dbFn in glob.glob("/qpsdata/data_rq.20210810_uptodate/???/univ.db"):
        ssn = dbFn.split(r'/')[-2]
        symsets[ssn] = list(pickle.loads(Path(dbFn).read_bytes())[ssn])

    return symsets

def create_crypto_symsets():
    symsets = {}
    fn = "/qpsdata/data_rq.20210810_uptodate/crypto_binance/instruments.db"
    d = pickle.load(open(fn, "rb"))
    syms = []
    for k,v in d.items():
        if v["status"] == "TRADING":
            syms.append(k)
    symsets["crypto_binance"] = syms
    return symsets

def create_us_etf_symsets():
    symsets = {}
    fn = "/qpsdata/data_rq.20210810_uptodate/US_ETF/instruments.db"
    d = pickle.load(open(fn, "rb"))
    d = d[d["active"] == True]
    symsets["US_ETF"] = list(set(d["permid"]))
    return symsets

def create_cv_symsets(begdt='20210810', enddt='20300101'):
    funcn = f"create_cv_symsets({begdt}, {enddt})"
    begdt = add_date_dash(begdt)
    enddt = add_date_dash(enddt)
    symsets = {}
    instrudb = pickle.loads(Path("/qpsdata/data_rq.20210810_uptodate/convertible/all_instruments.db").read_bytes())['Convertible']
    print_df(instrudb, cols=100, title=funcn, loglevel='debug')
    # exit(0)

    instrudb.loc[instrudb['de_listed_date']=='0000-00-00', 'de_listed_date'] = '2030-01-01' 
    instrudb = instrudb[instrudb['de_listed_date']>begdt]
    instrudb = instrudb[instrudb['status']!='Unknown']
    # instrudb = instrudb[['order_book_id', 'type', 'de_listed_date']]
    # exit(0)
    # print(instrudb.shape)
    symsets['CV_ALL'] = list(instrudb['order_book_id'])
    return symsets

    
def create_cf_symsets_cont(symsets_root):
    symsets = {}
    #create cont contracts
    for (ssn, mkts) in symsets_root.items():
        symsets[ssn] = sorted(mkts)
        if ssn.find("CF_")>=0:
            symsets[f"{ssn}_cont"] = sorted([f"{x}168" for x in mkts])
    return symsets

def create_CF_symsets_root():
    ssn2mkts = {}
    exec(Path("/Fairedge_dev/app_fdf/cfgs/ssn2mkts.txt").read_text(), locals())

    #remove inactive
    for ssn in ssn2mkts.keys():
        if ssn != 'CF_INACTIVE':
            ssn2mkts[ssn] = [x for x in ssn2mkts[ssn] if x not in ssn2mkts['CF_INACTIVE']]
    dbg(ssn2mkts['CF_ALL'])
    dbg(ssn2mkts['CF_INACTIVE'])
    return ssn2mkts

SSN2MKTS = None
def ssn2mkts(begdt='20210810', enddt='20300101', force=False):
    funcn = f"ssn2mkts(begdt={begdt}, enddt={enddt}, force={force})"
    global SSN2MKTS
    if SSN2MKTS is None:
        SSN2MKTS = {}
        dbFn = f"/qpsdata/config/symset_helpers_{begdt}_{enddt}.db"
        dbg(f"{funcn} dbFn={dbFn}")
        if os.path.exists(dbFn) and not force:
            SSN2MKTS = pickle.loads(Path(dbFn).read_bytes())
        else:
            cf_symset_root = create_CF_symsets_root()
            cf_symset_cont = create_cf_symsets_cont(cf_symset_root)
            SSN2MKTS.update(cf_symset_root)
            SSN2MKTS.update(cf_symset_cont)
            SSN2MKTS.update(create_cs_symsets())
            SSN2MKTS.update(create_cv_symsets())
            SSN2MKTS.update(create_crypto_symsets())
            SSN2MKTS.update(create_us_etf_symsets())
            pickle.dump(SSN2MKTS, open(dbFn, 'wb'))
            info(f"{funcn} dump dbFn={dbFn}")

    return SSN2MKTS

def symsets_like(pattern):
    ssns = [x for x in ssn2mkts().keys() if x.find(pattern)>=0]
    if len(ssns)<=0:
        print(f"ERROR: cannot find symsets like {pattern}", file=sys.stderr)
    return ssns

def cf_symset_names(match='cont'):
    #return [x for x in ssn2mkts().keys() if x.find("CF_")>=0]
    return [x for x in ssn2mkts().keys() if x.find("CF_")>=0 and x.find(match)>=0]

def cs_symset_all():
    return [x for x in ssn2mkts().keys() if (x.find("CF_")<0 and x.find("CV_")<0)]

def cs_symset_names():
    return [x for x in cs_symset_all() if x not in ['CS_FUNDS', 'CS_INDICES']]

def cv_symset_names():
    return ['CV_ALL']

def is_symset(x):
    #return (x in cs_symset_names()) or (x in cf_symset_names()) or x in (['CS_FUNDS', 'CS_INDICES'])
    rc = (x in ssn2mkts())
    #print(f"DEBUG_8753: is_symset({x})={rc}")
    return rc

def is_index(x):
    return (x in ssn2mkts()['CS_INDICES'])

def is_fund(x):
    return (x in ssn2mkts()['CS_FUNDS'])

def is_cs(x):
    return x.split(".")[0].isdigit() or len(x) == 3 or x.find("CS_")>=0

def is_cf(x):
    return (not is_cs(x))

def is_cs_symset(x):
    return x in cs_symset_names() or len(x) == 3 or x.find("CS_")>=0

def is_cf_symset(x):
    return x in cf_symset_names()

def sym2ex(sym):
    if is_cs(sym):
        return 'cs'
    else:
        return 'cf'

def ssn2mkts_subset(lenmax=10000):
    subset = {}
    for k,v in ssn2mkts(force=True).items():
        length = lenmax if len(v)>=lenmax else len(v)
        subset[k] = v[:length]
    return subset

SYM2INFODB = None
def sym2rqindu_db(dbFn=f"/qpsdata/config/sym2indu.db", force=False):
    funcn = f"sym2rqindu_db(force={force})"
    global SYM2INFODB
    if SYM2INFODB is None:
        dbg(f"{funcn} dbFn= {dbFn}")
        if os.path.exists(dbFn) and not force:
            SYM2INFODB = pickle.loads(Path(dbFn).read_bytes())
        else:
            induDfList = []
            for scn in ['F', 'G', '20210810_uptodate']:
                for fn in glob.glob(f"/qpsdata/data_rq.{scn}/???/industry_codes.db"):
                #for fn in glob.glob(f"/qpsdata/data_rq.20210810_uptodate/*/industry_codes.db"):
                #for fn in glob.glob(f"/qpsdata/data_rq.F/???/industry_codes.db"):
                #for fn in glob.glob(f"/qpsdata/data_rq.G/???/industry_codes.db"):
                    dbg(f"{funcn} loading indu code {fn}")
                    #df = pd.DataFrame.from_dict(pickle.loads(Path(fn).read_bytes()), orient='index')
                    df = pd.concat(pickle.loads(Path(fn).read_bytes()).values())
                    df['scn'] = scn if scn not in ['20210810_uptodate'] else 'W'
                    #print(df)
                    induDfList.append(df)

            db = pd.concat(induDfList)
            db.reset_index(inplace=True)
            db = db.sort_values(by=['order_book_id', 'scn'])
            print_df(db, rows=20, cols=20, title="rqInduInfo", loglevel='debug')
            pickle.dump(db, open(dbFn, 'wb'))
            info(f"{funcn} dump {dbFn}")
            SYM2INFODB = db
    return SYM2INFODB

def rqindu_listing(db, level='first'):
    rqInduInfo = db[[f"{level}_industry_name", f"{level}_industry_code"]]
    # rqInduInfo = rqInduInfo[['first_industry_name','first_industry_code']]
    rqInduInfo = rqInduInfo.drop_duplicates()
    print_df(rqInduInfo, rows=10000, cols=20, title="rqInduInfo", loglevel='debug')
    info(f'{funcn} level={level}, rqInduInfo.shape= {rqInduInfo.shape}')

if __name__ == "__main__":
    funcn = "symset_helpers.main"
    opt, args = get_options(funcn)
    tot = 0
    for k,v in ssn2mkts(force=True).items():
        dbg(f"{k:<32}: len= {len(v)}")
        tot += len(v)
    info(f"{'ALL_MKTS':<32}: len= {tot}")

    lenmax=2
    for k,v in ssn2mkts_subset(lenmax=lenmax).items():
        dbg(f"membership subset(lenmax={lenmax}) {k:<32}: {v}")

    for lvl in ['first', 'second', 'third']:
        rqindu_listing(sym2rqindu_db(force=opt.force), level=lvl)

    if False:
        print_df(pd.DataFrame.from_dict({k:len(v) for k,v in ssn2mkts(force=True).items()}, orient='index', columns=['count']), rows=200, title="SSN2MKTS")

    for k,v in ssn2mkts().items():
        print(f"{funcn} membership count for {k:<32}: {len(v)}")
