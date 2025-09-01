#!/home/shuser/anaconda3/bin/python

import sys

from optparse import OptionParser
from alpha_performance import AlphaPerformance
import time
import sys
from default_params import set_default_params
from shared_cmn import print_df
from common_basic import *
from common_paths import *
from Scn import Scn
from qdb_options import get_options_jobgraph

def customize_options_che(parser):
    parser.add_option("--test",
                      dest="test",
                      type="string",
                      help="test (default: %default)",
                      metavar="test",
                      default="all")

    parser.add_option("--factor_search",
                      dest="factor_search",
                      type=int,
                      help="factor_search (default: %default)",
                      metavar="factor_search",
                      default=0)

def get_perf_params(dts_cfg, symset, predNm, simple_report=False, return_data=None, factor_data=None, data_ver='rkan5',
                    is_binary_factor=False, factor_search=False):
    params = set_default_params(simple_report)
    data_tag = f'{symset}{dts_cfg}'
    import platform
    if platform.system().find("Windows") >= 0:
        db_root = 'c:/chris_wrk/'
    else:
        db_root = '/qpsdata/config'

    if 0:
        params.update({'alpha_name': predNm,
                    'data_dir': f'{db_root}/{data_ver}/',
                    'factor_dir': f'{db_root}/{data_ver}/',
                    'result_dir': f'{db_root}/{data_ver}/',
                    'factor_file': '', #f'Pred_Alpha_Alpha001_TEST489532_{data_tag}.pkl'
                    'return_file_name': '', #f'Resp_LogReturn_Daily_{data_tag}.pkl',
                    'index_return_file_name': f'Resp_LogIndexReturn_Daily_Indices{dts_cfg}.db',
                    'beta_HS300_file_name': f'Pred_Beta_Daily_CN300_{data_tag}.pkl',
                    'beta_ZZ500_file_name': f'Pred_Beta_Daily_CN500_{data_tag}.pkl',
                    'sector_file_name': f'Pred_Industry_Daily_Citic_{data_tag}.pkl',
                    'ipo_age_file_name': f'Pred_IpoAge_Daily_{data_tag}.pkl',
                    'st_flag_file_name': f'Pred_StFlag_Daily_{data_tag}.pkl',
                    'universe': ['All', 'Top1800', 'Top1200', 'ZZ800', 'ZZ500', 'HS300'],
                    'universe_file_name': ['', *[f'Pred_Universe_{x}_{data_tag}.pkl' for x in
                                                    ['Top1800', 'Top1200', 'CN800', 'CN500', 'CN300']]],

                    'display_result': True,
                    'display_graph': False,
                    'exclude_st': False,
                    'min_ipo_age': 0,
                    'is_binary_factor': is_binary_factor, 
                    'quiet': 0
                    })
    else:
        params.update({'alpha_name': predNm,
                    'data_dir': f'{db_root}/{data_ver}/',
                    'factor_dir': f'{db_root}/{data_ver}/',
                    'result_dir': f'{db_root}/{data_ver}/',
                    'factor_file': '', #f'Pred_Alpha_Alpha001_TEST489532_{data_tag}.pkl'
                    'return_file_name': '', #f'Resp_LogReturn_Daily_{data_tag}.pkl',
                    'index_return_file_name': ctx_fldnm2fp("Resp_lnret_C2C_1d_1", "Indices"),
                    'beta_HS300_file_name': f'Pred_Beta_Daily_CN300_{data_tag}.pkl',
                    'beta_ZZ500_file_name': f'Pred_Beta_Daily_CN500_{data_tag}.pkl',
                    'sector_file_name': f'Pred_Industry_Daily_Citic_{data_tag}.pkl',
                    'ipo_age_file_name': f'Pred_IpoAge_Daily_{data_tag}.pkl',
                    'st_flag_file_name': f'Pred_StFlag_Daily_{data_tag}.pkl',
                    'universe': ['All', 'Top1800', 'Top1200', 'ZZ800', 'ZZ500', 'HS300'],
                    'universe_file_name': ['', *[ctx_fldnm2fp(f'Pred_univ_{x}_1d_1') for x in
                                                    ['TOP1800', 'TOP1200', 'CN800', 'CN500', 'CN300']]],
                    'display_result': True,
                    'display_graph': False,
                    'exclude_st': False,
                    'min_ipo_age': 0,
                    'is_binary_factor': is_binary_factor, 
                    'quiet': 0
                    })

    if return_data is not None:
        params['return_data'] = return_data
    else:
        params['return_file_name'] = f'Resp_LogReturn_Daily_{data_tag}.pkl'

    if factor_data is not None:
        params['factor_data'] = factor_data
    else:
        params['factor_file'] = f'Pred_Alpha_Alpha001_TEST489532_{data_tag}.pkl'
        #print(f"WARNING: no factor data specified, using test {params['factor_file']}")

    return params


if __name__ == '__main__':
    (opt, args) = get_options_jobgraph(list_cmds = lambda x: "", customize_options=customize_options_che)

    #opt.test = "all"
    testSteps = [opt.test]
    if opt.test == "all":
        testSteps = "detail,simple,st_age,factor_search".split(r',')

    dts_cfg = 'G'
    t0 = time.perf_counter()
    for symset in ['C39', 'CS_ALL'][:]:
        scn = Scn(opt, symset, dts_cfg, 'SN_CS_DAY', asofdate="download")
        ctx_set_scn(scn)

        for alphaNm in ['Alpha001', 'Alpha027', 'Alpha076']:
            for testStep in testSteps:
                opt.test = testStep

                simple_report = False
                factor_search = False
                if opt.test == 'detail':
                    simple_report = False
                elif opt.test == 'simple':
                    simple_report = True
                elif opt.test == 'factor_search':
                    factor_search = True
                    simple_report = True

                params = get_perf_params(dts_cfg, symset, f'{alphaNm}_{symset}', simple_report=simple_report, factor_search=factor_search)

                if opt.test == 'st_age':
                    params.update({'min_ipo_age': 120, 'exclude_st': True})

                t1 = time.perf_counter()
                with open(f"alpha_performance/algor_perf.{symset}.{alphaNm}.{testStep}.txt", 'w') as sys.stdout:
                    fm = AlphaPerformance(params=params)
                t2 = time.perf_counter()
                print(f'Finished {opt.test}, Run time：%12s %12s %7.3f'%(symset, alphaNm, t2 - t1), ' sec', file=sys.stderr)
    t99 = time.perf_counter()
    print(f'Finished  total, Run time：%7.3f' % (t99 - t0), ' sec', file=sys.stderr)


