#!/usr/bin/env python

import sys
import os

import QpsUtil

def DebugCfg(fn="DebugCfg.datachk.txt", debug=False):
    if DebugCfg.cfg == None or (DebugCfg.cfg is not None and fn is not None):
        fn = QpsUtil.find_first_available([f"/Fairedge_dev/lib_QpsData/{fn}", f"/Fairedge_dev/app_QpsData_RQ/{fn}"])
        if debug:
            print(f'INFO: DebugCfg {fn}', file=sys.stderr)
        DebugCfg.cfg = QpsUtil.eval_file(fn)
    return DebugCfg.cfg
DebugCfg.cfg = None

def test(debug=DebugCfg()):
    print(debug)

if __name__ == '__main__':
    print(DebugCfg())
    test()

