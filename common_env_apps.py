#!/cygdrive/c/Anaconda3/python.exe

import sys
import os
import glob
from pathlib import Path

def srcs_apps(pysrc):
    # relver = 'lnx314'
    # devver = 'lnx315'
    # pysrc['bgen_cs'] = f'/Fairedge_dev/app_QpsData/bgen_cs.py:lnx309' 
    # pysrc['bgen_cs'] = f'/Fairedge_dev/app_QpsData/bgen_cs.py:lnx312' #fixed formula_builder
    # pysrc['bgen_cs'] = f'/Fairedge_dev/app_QpsData/bgen_cs.py:lnx315' #adjust minor for ['Pred_lnret_C2C_1d_1', 'Pred_univ_NonStSts_1d_1', *[f"Pred_OHLCV_{x}Flag_1d_1" for x in "OHLC"]]

    if False:
        # use cloud base version
        pysrc['bgen_cs_new'] = f'/Fairedge_dev/app_QpsData/bgen_cs_new.py:lnx319' #add -u
        pysrc['bgen_cs_new__ordergen'] = f'/Fairedge_dev/app_QpsData/bgen_cs_new.py:lnx320' #add -u
        pysrc['bgen_cf_new'] = f'/Fairedge_dev/app_QpsData/bgen_cf_new.py:lnx319' #add -u
    else:
        # use cloud dealer version
        pysrc['bgen_cs_new'] = f'/Fairedge_dev/app_QpsData/bgen_cs_new.py:lnx321' 
        pysrc['bgen_cs_new__ordergen'] = f'/Fairedge_dev/app_QpsData/bgen_cs_new.py:lnx321'
        pysrc['bgen_cf_new'] = f'/Fairedge_dev/app_QpsData/bgen_cf_new.py:lnx321' #add -u

    #pysrc['bgen_cf'] = f'/Fairedge_dev/app_QpsData/bgen_cf.py:lnx309' 
    

    pysrc['jobs2makefile'] = f'/Fairedge_dev/app_QpsData/jobs2makefile.py' #pyc version does not work

    #pysrc['formula_search'] = f'/Fairedge_dev/app_QpsData/formula_search.py:lnx314'
    #pysrc['formula_search'] = f'/Fairedge_dev/app_QpsData/formula_search.py:lnx315'
    #pysrc['formula_search'] = f'/Fairedge_dev/app_QpsData/formula_search.py:lnx317' #default run detailed for non-T
    #pysrc['formula_search'] = f'/Fairedge_dev/app_QpsData/formula_search.py:lnx318' #do not print __calculate_performance missing year '2021' exception
    #pysrc['formula_search'] = f'/Fairedge_dev/app_QpsData/formula_search.py:lnx319' #fix less size problem

    #pysrc['formula_search_worker'] = f'/Fairedge_dev/app_formula_search/formula_search_worker.py:lnx320'

    pysrc['formula_search_worker'] = f'/Fairedge_dev/app_formula_search/formula_search_worker.py:lnx322' #fixed missing dscndt row
    pysrc['formula_search_worker'] = f'/Fairedge_dev/app_formula_search/formula_search_worker.py:lnx326' #20240702 nats
    pysrc['formula_search_worker'] = f'/Fairedge_dev/app_formula_search/formula_search_worker.py:lnx325' #20240702 nats
    pysrc['formula_search_worker'] = f'/Fairedge_dev/app_formula_search/formula_search_worker.py:lnx327' #bug fix

    pysrc['formula_genetic'] = f'/Fairedge_dev/app_QpsData/formula_genetic.py:lnx319'

    pysrc['formula_builder'] = f'/Fairedge_dev/app_QpsData/formula_builder.py:lnx319'

    pysrc['fc_by_horizon'] = f'/Fairedge_dev/app_QpsData/fc_by_horizon.py:lnx319'
    pysrc['chk_fc_by_horizon'] = f'/Fairedge_dev/app_QpsData/chk_fc_by_horizon.py:lnx319'

    #pysrc['daily_updates'] = f'/Fairedge_dev/app_QpsData/daily_updates.py:lnx307'
    pysrc['daily_updates'] = f'/Fairedge_dev/app_QpsData/daily_updates.py:lnx320' #fixed /qpsdata/config/symsets/prod1w/???.pkl generation

    #pysrc['run_study'] = f'/Fairedge_dev/app_QpsData/run_study.py:lnx307'
    pysrc['run_study'] = f'/Fairedge_dev/app_QpsData/run_study.py:lnx319'
    pysrc['csall2grp'] = f'/Fairedge_dev/app_QpsData/csall2grp.py:lnx319'
    #pysrc['QDMergeUpdates'] = f'/Fairedge_dev/app_QpsData_RQ/QDMergeUpdates.py:lnx307'
    #pysrc['QDDownloaderRq'] = f'/Fairedge_dev/app_QpsData_RQ/QDDownloaderRq.py:lnx307' #XXX12/16/22
    #pysrc['QDMergeUpdates'] = f'/Fairedge_dev/app_QpsData_RQ/QDMergeUpdates.py:lnx314' #XXX bad
    # pysrc['QDMergeUpdates'] = f'/Fairedge_dev/app_QpsData_RQ/QDMergeUpdates.py:lnx319' #20241108
    pysrc['QDMergeUpdates'] = f'/Fairedge_dev/app_QpsData_RQ/QDMergeUpdates.py:lnx320'
    pysrc['QDDownloaderRq'] = f'/Fairedge_dev/app_QpsData_RQ/QDDownloaderRq.py:lnx319' #run on che02

    #pysrc['fgen_ss'] = f'/Fairedge_dev/app_fgen/fgen_ss.py:lnx307' #CHG 20221220
    pysrc['fgen_bash'] = f'/Fairedge_dev/app_fgen/fgen_bash.py:lnx319' #CHG 20221220
    pysrc['fgen_ss'] = f'/Fairedge_dev/app_fgen/fgen_ss.py:lnx319' #Force remove tgt file first
    # pysrc['fgen_bash'] = f'/Fairedge_dev/app_fgen/fgen_bash.py:lnx314'
    
    #pysrc['roll_db'] = f'/Fairedge_dev/app_QpsData/roll_db.py:lnx307'
    pysrc['roll_db'] = f'/Fairedge_dev/app_QpsData/roll_db.py:lnx319' #20230102, add real-time return patch
    pysrc['roll_db'] = f'/Fairedge_dev/app_QpsData/roll_db.py:lnx326' #20240702 nats

    #pysrc['combine_db'] = f'/Fairedge_dev/app_QpsData/combine_db.py:lnx308'
    #pysrc['combine_db'] = f'/Fairedge_dev/app_QpsData/combine_db.py:lnx314' #XXX for che02
    pysrc['combine_db'] = f'/Fairedge_dev/app_QpsData/combine_db.py:lnx319'

    #pysrc['gen_vps_instru'] = f'/Fairedge_dev/app_QpsData_RQ/gen_vps_instru.py:lnx319'
    # pysrc['gen_vps_instru'] = f'/Fairedge_dev/app_QpsData_RQ/gen_vps_instru.py:lnx323' #20240124 added some new futures markets
    pysrc['gen_vps_instru'] = f'/Fairedge_dev/app_QpsData_RQ/gen_vps_instru.py:lnx324' #20241122 use JQ

    pysrc['Scn'] = f'/Fairedge_dev/lib_QpsData/Scn.py:lnx319'
    pysrc['gen_bar_updates'] = f'/Fairedge_dev/app_QpsData_RQ/gen_bar_updates.py:lnx319'
    pysrc['qddisplay'] = f'/Fairedge_dev/app_QpsData/qddisplay.py:lnx319'
    pysrc['chk_flds_gen'] = f'/Fairedge_dev/app_QpsData/chk_flds_gen.py:lnx319'
    pysrc['patch_fld'] = f'/Fairedge_dev/app_QpsData/patch_fld.py:lnx319'
    pysrc['inc_fld'] = f'/Fairedge_dev/app_QpsData/inc_fld.py:lnx319'
    #pysrc['egen'] = f'/Fairedge_dev/app_egen/egen.py:lnx308'
    pysrc['egen'] = f'/Fairedge_dev/app_egen/egen.py:lnx319'
    #pysrc['daily_ohlcv'] = f'/Fairedge_dev/app_fgen/daily_ohlcv.py:lnx311'
    pysrc['daily_ohlcv'] = f'/Fairedge_dev/app_fgen/daily_ohlcv.py:lnx319'
    #pysrc['fgen_split_mysql'] = f'/Fairedge_dev/app_fgen/fgen_split_mysql.py:lnx308'
    pysrc['fgen_split_mysql'] = f'/Fairedge_dev/app_fgen/fgen_split_mysql.py:lnx319'

    pysrc['factrank'] = f'/Fairedge_dev/app_factrank/factrank.py:lnx319'
    pysrc['factcomb'] = f'/Fairedge_dev/app_factrank/factcomb.py:lnx319' #solve a new block issue
    pysrc['port_simu_fac'] = f'/Fairedge_dev/app_portsimu/port_simu_fac.py:lnx319'
    pysrc['plot_simu'] = f'/Fairedge_dev/app_portsimu/plot_simu.py:lnx319'
    pysrc['ana_simu'] = f'/Fairedge_dev/app_factrank/ana_simu.py:lnx319'
    # pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx307'  #split 50/20
    # pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx309'  #change split to 50/50
    # pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx310'  #fixed split unbalance issue
    # pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx311'  #add tgt_muliplier, reading prod open files
    # pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx314'  #fix 688
    #pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx320' #fix dcsn_prc issue
    #pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx322' #increase strat002 tgt amt
    pysrc['strat_builder'] = f'/Fairedge_dev/app_factrank/strat_builder.py:lnx323' #no longer zero out xxxx-xx

    pysrc['strat_opt'] = f'/Fairedge_dev/app_factrank/strat_opt.py:lnx319'

    pysrc['gen_single_factors'] = f'/Fairedge_dev/app_QpsData/gen_single_factors.py:lnx319'
    pysrc['formula_search_query_results'] = f'/Fairedge_dev/app_QpsData/formula_search_query_results.py:lnx319'

    pysrc['sqlquery_db'] = f'/Fairedge_dev/app_QpsData/sqlquery_db.py:lnx319' #add save to csv file

    #pysrc['fgen_split_raw'] = f'/Fairedge_dev/app_fgen/fgen_split_raw.py'
    pysrc['fgen_split_raw'] = f'/Fairedge_dev/app_fgen/fgen_split_raw.py:lnx319'
    pysrc['genMktOMSDates'] = f'/Fairedge_dev/app_QpsData/genMktOMSDates.py:lnx319'
    pysrc['formula_builder'] = f'/Fairedge_dev/app_QpsData/formula_builder.py:lnx319'
    pysrc['formula_genetic'] = f'/Fairedge_dev/app_QpsData/formula_genetic.py:lnx319'
    pysrc['calc_corr'] = f'/Fairedge_dev/app_QpsData/calc_corr.py:lnx319'

    pysrc['build_predictors_summary_db'] = f'/Fairedge_dev/app_QpsData/build_predictors_summary_db.py:lnx319'
    pysrc['pkl2csv'] = f'/Fairedge_dev/app_QpsData/pkl2csv.py:lnx319'
    pysrc['combine_df'] = f'/Fairedge_dev/app_QpsData/combine_df.py:lnx319'

    pysrc['txt2factcomb'] = f'/Fairedge_dev/app_factrank/txt2factcomb.py:lnx319'
    pysrc['get_current_rets'] = f'/Fairedge_dev/app_QpsData/get_current_rets.py:lnx319'
    pysrc['set_current_rets'] = f'/Fairedge_dev/app_QpsData/set_current_rets.py:lnx319'
    pysrc['flds_binary'] = f'/Fairedge_dev/app_fgen/flds_binary.py:lnx319'

    pysrc['performance_eval_server'] = f'/Fairedge_dev/app_formula_search/performance_eval_server.py:lnx330'

    for fn in glob.glob("/Fairedge_dev/app_*/env_apps.py"):
        if os.path.exists(fn):
            print(f"INFO: evaluating {fn}", file=sys.stderr)
            exec(Path(fn).read_text())



