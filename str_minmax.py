#!/usr/bin/env python

import sys

#import os
from optparse import OptionParser
#import re
#from QpsUtil import str_minmax

def str_minmax(s, max=False, use_only=None, disable=False):
    if disable:
        return s

    lookup = {
        "~A": "/Fairedge_dev/app_QpsData",
        "~B": "/Fairedge_dev/app_fgen",
        "~D": "/Fairedge_dev/app_QpsData_RQ",
        "~E": "/Fairedge_dev/app_egen",
        "~J": "/qpsdata/egen_study/predictors/corr_001",
        "~U": "data_rq.20100101_uptodate",
        "~W": "/qpsdata/raw",
        "~T": "/qpsdata/temp",
        "~F": "futures",
        "~Q": "data_rq",
        "~W": "prod1w",
        "~P": "prod",
        "~R": "rschhist",
        "~S": "/qpsdata/egen/sel",
        "~L": "/qpsdata/egen/flds",
        "~N": "SN_CS_DAY",
        "~P": "/home/shuser/anaconda3/bin",
        "~r": "/qpsdata/raw",
        "~c": "/qpsuser/che",
        "~p": "/home/che/anaconda3/bin",
        "~j": "/qpsdata/egen_study",
        "~x": "20100101_",
        "~y": "20210810_",
        "~z": "20200901_",
        #"~o": "/qpsdata/config/factor_file"
    }
    for (s_min, s_max) in lookup.items():
        if use_only is not None:
            if s_min not in use_only:
                continue
        if max:
            s = s.replace(s_min, s_max)
        else:
            s = s.replace(s_max, s_min)
    return s

if __name__ == '__main__':
    parser = OptionParser(description="str_minmax")

    parser.add_option("--debug",
					  dest="debug",
					  type="int",
					  help="debug (default: %default)",
					  metavar="debug",
					  default=0)

    opt, args = parser.parse_args()

    if len(args)>0:
        s = [str_minmax(x, True) for x in args]
        print(' '.join(s))
    else:
        for ln in sys.stdin.readlines():
            ln = ln.strip()
            print(str_minmax(ln, True))
            sys.stdout.flush()



