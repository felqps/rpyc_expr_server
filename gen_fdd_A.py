import os,sys
import pickle
import pandas as pd
import numpy as np

from options_helper import get_options,set_cmdline_opt
            #print(f"pld key={k}, type= {type(pld[k])}, dtypes= {pld[k].dtypes}")
from df_helpers import df_merge_non_uniq_columns
from concat_factors import concat_factors
from PostgreSqlTools import postgres_instance
from FDF import FDFCfg
from common_colors import *
from get_sizeof import total_size

def deprecated_markets(mkts):
    return [x for x in mkts if x.find("WH")>=0]

def merge_fdf_for_cfgs(opt):
    cfgs = []
    if opt.cfg.split(r'_')[-1] in ['A']:
        for x in ["F", "G", "prod"]:
            cfgs.append(opt.cfg.replace("A", x))
        #print(f"DEBUG_345s: cfgs= {cfgs}")
    else:
        cfgs = [opt.cfg]

    news = []
    for cfg in cfgs:
        cfg = cfg.split(r'=')[-1] #CHE handle var assign
        #print(f"DEBUG_dsfs: {cfg}")
        #fdd = FDFCfg(cfg).ss(opt.symset).fdf()
        fdd_expr = "FdfDict('@ohlcv_cs')" if opt.symset.find("CF")<0 else "FdfDict('@ohlcv_cf')"
        fdd = FDFCfg(cfg).ss(opt.symset).fdf(fdd_expr)
        fdd_file = fdd.fpath() #fdf_fpath(cfg, opt.symset, "FdfDict('@ohlcv_cs')" if cfg.find("cs")>=0 else "FdfDict('@ohlcv_cf')")
        funcn = f"FEWorkerBase.pld(fdd_file= {fdd_file})"
        print(f"{BLUE}INFO: {funcn}{NC}")

        # fdd = Path(fdd_file)
        assert fdd.exists(), f"ERROR: fdd= {fdd} missing, try run fdd_maker?"
        pld = fdd.load() #pickle.loads(fdd.read_bytes())
        if opt.debug:
            info(f"{funcn} fdd_file= {fdd_file}, keys= {pld.keys()}")
        for k in pld.keys():
            #print(f"pld key={k}, type= {type(pld[k])}, dtypes= {pld[k].dtypes}")
            try:
                pld[k] = pld[k].astype('float')
            except Exception as e:
                pass
        
        new = {}
        for k,v in pld.items():
            if v is None:
                continue
            l = df_merge_non_uniq_columns(v, copy=True)
            if len(deprecated_markets(v.columns))>0:
                l = l.drop(deprecated_markets(v.columns), axis=1)
            new[k] = l
            if opt.debug or opt.verbose:
                info(f"INFO: pld key={k:<8}, shape= {l.shape}")
        news.append(new)

    #use tradable returns
    #self.ctx()['RETURN'] = new['RETURN_T_RQ']

    if len(news)<=1:
        pld = news[0]
        
    else:
        pld = {}
        for k in news[0].keys():
            pld[k] = concat_factors(k, [x[k] for x in news])
        
    return pld

if __name__ == "__main__":
    funcn = "gen_fdd_A.main"
    opt, args = get_options(funcn)

    
    if opt.cfg.split(r'_')[-1] in ['A']:
        pld = merge_fdf_for_cfgs(opt)
        dta = {
            'cfgnm': opt.cfg,
            'symIn': opt.symset,
            'symOut': opt.symset,
            'rootdir': '',
            'begdt': '20100101',
            'enddt': 'uptodate',
            'fldnm': '',
            'ftype': 'fdd',
            'file_size': f"{int(total_size(pld))} MB",
            'no_of_mkts': len(pld['CLOSE'].columns),
            'fpath': pld
        }
        postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
        postgres.save_dict("fdf_info", dta)
    else:
        print(f"ERROR: gen_fdd_A cfg= {opt.cfg}, must be cs_1d_A")

   
