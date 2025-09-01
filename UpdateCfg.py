#!/usr/bin/env python

import sys

import QpsUtil

from collections import defaultdict

def updateCfgDefault():
    rc =  {
        'prod1w': {'chk_time': 1, 'chk_trigger': 1, 'force': 0},
        'prod':  {'chk_time': 1, 'chk_trigger': 1, 'force': 0},     
        'rschhist':  {'chk_time': 1, 'chk_trigger': 1, 'force': 0}, #If not chk_time and force, then if target file exists, do not regenerate
    }
    for x in "EFG":
        rc[x] = rc['rschhist'].copy()
    return rc

updateCfgOverwrites = {
    'egen.gen_data_by_ssn': {'prod1w': {'force': 1}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
    'fgen_split_raw.db': {'prod1w': {'force': 1}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
    'fgen_ss.run': {'prod1w': {'force': 1}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
    'indic_ana.run_models': {'prod1w': {'force': 1}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
    'QDMergeUpdates.merge_files': {'prod1w': {'force': 1}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
    'calc_daily_ops_for_mkt': {'prod1w': {'force': 0}, 'prod': {'force': 0}, 'rschhist': {'force': 0}, 'E': {'force': 0}, 'F': {'force': 0}, 'G': {'force': 0}},
}   

def UpdateCfg(funcn, dts_cfg, debug=False):
    cfg = updateCfgDefault()[dts_cfg]
    if funcn in updateCfgOverwrites:
        cfg.update(updateCfgOverwrites[funcn][dts_cfg])
        if debug:
            print(f"DEBUG: UpdateCfg({funcn}, {dts_cfg}): {cfg}")
    return cfg


if __name__ == '__main__':
    for dts_cfg in ['prod', 'prod1w']:
        for fname in ['func1', 'func2', 'egen.gen_data_by_ssn', 'fgen_ss.run']:
            print(fname, dts_cfg, UpdateCfg(fname, dts_cfg))


