#!/usr/bin/env python

import sys

import os
from optparse import OptionParser
from pathlib import Path
import pandas as pd
import pickle
from copy import deepcopy
#import QpsUtil
from fdf_basic import *
#from shared_cmn import print_df
from fdf_helpers import *
from symset_helpers import *
from options_helper import *
from fdf_colors import *
from macro_helper import *
import datetime
from FDFFile import *
from FDFCfg import *



def fdf_fpath(cfg, symset, fldExpr, debug=False):
    fdd = FDFCfg(cfg).ss(symset).fdf(f"{fldExpr}")
    if debug:
        print(f"DEBUG_5456: fdf_fpath({fdd}, {symset}, {fldExpr})= {fdd.fpath()}")
    return fdd.fpath()

def fdf_fpath_to_id(fp):
    (id_major, id_minor) = fp.split(r'/')[-2:]
    id_minor = id_minor.split(r'.')[0]
    id = f"{id_major}{id_minor}"

    if len(id) == 32:
        return id
    else:
        return ""

def fdf_id_to_ord(id, qpsnas_cfg='46789'): #ordinal
    if not id:
        return -1
    nm_ord = 0
    nm_ord += ord(id[0])
    if qpsnas_cfg == '467':
        return nm_ord%3
    else:
        return nm_ord%5

def fdf_ord_to_server(ord, qpsnas_cfg='46789'):
    return f"NASQPS0{qpsnas_cfg[ord]}.qpsdata"

def fdf_fpath_remap(fp, qpsnas_cfg='46789', backup_tag=None):
    id = fdf_fpath_to_id(fp)
    id_major = id[:2]
    id_minor = id[2:]
    ord = fdf_id_to_ord(id,qpsnas_cfg=qpsnas_cfg)
    if ord < 0:
        return fp
    nasqps_svr = fdf_ord_to_server(ord)
    ele = fp.split(r'/')
    ele[1] = nasqps_svr
    if backup_tag:
        ele[-1] = '.'.join([id_minor, ele[-1].strip().split(r'.')[-1], backup_tag]) 
    
    return '/'.join(ele)

if __name__ == '__main__':
    funcn = "FDF.main"
    opt, args = get_options(funcn)
    if opt.output_file:
        opt.output_fp = open(opt.output_file, 'w')
    else:
        opt.output_fp = None

    # for period in ['1d', '5m'][:]:
    #     for cfg in [FDFCfg(f"cf_{period}_res", opt=opt)]:
    #         for ssn in ['CF_EQTY_cont', 'CF_TEST01_cont', 'RU168', 'IF168', 'SI168']: #'CF_EQTY_cont',
    #             sscfg = cfg.ss(ssn)
    #             for fldnm in [f"Factor('{x}')" for x in ['open', 'high', 'low', 'close', 'volume']]:
    #                 sscfg.eval(f"print({fldnm}.dropna(axis=0, how='all').tail(5))")
    #                 eval("print(Factor('close', shift=1)*Factor('high', shift=1))", ctx)
                
    # for period in ['1d'][:]:
    #     for cfg in [FDFCfg(f"cs_{period}_prod", opt=opt)]:
    #         for ssn in ['CS_INDICES']: 
    #             sscfg = cfg.ss(ssn)
    #             for fldnm in [f"Factor('{x}')" for x in ['ret']]:
    #                 sscfg.eval(f"print({fldnm}.dropna(axis=0, how='all').tail(5))")


    for fn in [
        "/NASQPS04.qpsdata/fdf/cf_T/1d/25/2aaecb9c739697434b9c02fcb4261e.fdf",
        "/NASQPS09.qpsdata/fdf/cf_T/1d/11/d5beed2ad8058e307c4066e7b60af8.fdf",
        '/NASQPS08.qpsdata/fdf/cf_T/1d/b9/bef611f86fb4f4a2d1613357b9d34f.fdf',
        "/NASQPS09.qpsdata/fdf/test.txt"
    ]:
        print(fn,
            "\n",
            '\tdefault=', fdf_fpath_remap(fn),
            "\n",
            '\t46789=', fdf_fpath_remap(fn, qpsnas_cfg='46789'),
            "\n",
            '\t467=', fdf_fpath_remap(fn, qpsnas_cfg='467'),
            "\n",
            '\t467(backup)=', fdf_fpath_remap(fn, qpsnas_cfg='467', backup_tag='backup'),
            "\n",
            '\t46789(recover)=', fdf_fpath_remap(fdf_fpath_remap(fn, qpsnas_cfg='467', backup_tag='backup').replace('.backup',''), qpsnas_cfg='46789'),
            "\n",
            '\t467(recover)=', fdf_fpath_remap(fdf_fpath_remap(fn, qpsnas_cfg='467', backup_tag='backup').replace('.backup',''), qpsnas_cfg='467'),
            )
