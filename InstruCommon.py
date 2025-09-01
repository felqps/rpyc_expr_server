#!/usr/bin/env python

import sys
import os
import pickle

from optparse import OptionParser
import pandas as pd
import glob
from dateutil.parser import parse as dtparse
from VpsObjMaker import *
from common_basic import *
import QpsUtil

def generator_vps_instru_common(symset='CF_ALL', secFilter='active', debug=False):
    funcn = "generator_vps_instru_common"
    tgtFn = ""
    if symset2sectype(symset) in ['stocks', 'CS']:
        tgtFn = sorted(glob.glob(f"{dd('raw')}/{symset}/SN_CS_DAY/prod1w_????????_????????/*.db"))[-1]
    else:
        tgtFn = sorted(glob.glob(f"{dd('raw')}/CF_ALL/SN_CF_DAY/prod1w_????????_????????/*.{secFilter}.db"))[-1]

    print(f"INFO: {funcn} symset={symset}, secFilter={secFilter}, {tgtFn}", file=sys.stderr)

    assert tgtFn!= "" and os.path.exists(tgtFn), f"ERROR: {funcn} can not find vps_instru file"
    df = smart_load(tgtFn, debug=True)


    cnt = 0
    iis = []
    while cnt < len(df):
        ii = VpsInstru()
        ii.__dict__.update(dict(df.iloc[cnt].items()))
        #qps_print('raw_rq_instr_info:  ', dict(df.iloc[cnt].items()))
        cnt = cnt + 1
        iis.append(ii)
        yield (ii)
    
    if debug:
        qps_print(f'generator_vps_instru summary: symset={symset}, secType={secType}, secFilter={secFilter}, begDt={begDt}, endDt={endDt} rows={cnt}: {tgtFn}', file=sys.stdout)
    #qps_print('\t', '\n\t'.join(['%s'%ii.__dict__ for ii in iis]))
    #qps_print('\t'+'\n\t'.join(['%s'%ii for ii in iis]))

def get_all_instruments():
    # pu = PytdxUtil()
    # for k,v in pu.get_quotes(['688107']).items():
    #     print(k,v.toString(9))
    dates = QpsUtil.get_all_dates()
    fn = f"/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_20210810_{dates['dcsndate']}/1d/Pred_OHLCV_C_1d_1/CS_ALL.db"
    data = {}
    if os.path.exists(fn):
        data = pickle.load(open(fn, "rb")).iloc[-1]

    instrus = list(generator_vps_instru_common("CS_ALL"))
    instrus.extend(list(generator_vps_instru_common("CF_ALL")))
    for instru in instrus:
        instru.preclo_pri = 0
        if instru.permid in data:
            instru.preclo_pri = data[instru.permid]

    instrus = {instru.sym:instru for instru in instrus}
    return instrus

if __name__ == '__main__':
    if 0:
        [print(instru.__dict__) for instru in get_all_instruments()]
        exit()

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
    opt, args = parser.parse_args()

    for symset in ['CF_ALL', 'C15']:
        secFilters = ['active']
        if symset.find('CF')>=0:
            secFilters.append('main')
        for secFilter in secFilters:
            for ii in generator_vps_instru_common(symset, secFilter=secFilter):
                print(ii)
