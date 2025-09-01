#!/usr/bin/env python

import sys
import os
import re
import pickle
import pandas as pd
from fdf_colors import *
from fdf_helpers import buf2md5
from options_helper import get_options
from collections import defaultdict

varRe = re.compile(r'[A-Z][A-Za-z0-9_]+')
funcRe = re.compile(r'([A-Za-z_]+)\(')
newVarRe = re.compile(r'([A-Z][a-zA-Z_0-9]*)')
newArgvRe = re.compile(r'([A-Z][a-zA-Z_0-9]*|[0-9]+)')
newFuncRe = re.compile(r'([a-z][a-zA-Z_0-9]*)')
runidRe = re.compile(r'[a-z0-9]{16}')
xyRe = re.compile(r"xy= (\S*?);(\S*?);")
#msi= [' 0.000099', ' 0.003807', ' 0.405590']
msiRe = re.compile(r"msi=\s+\[.*?([0-9\.\+\-]+?),.*?([0-9\.\+\-]+?),.*?([0-9\.\+\-]+?)\]")
#corr= [-0.018788,  0.366130, -0.799920]
corrRe = re.compile(r"corr=\s+\[.*?([0-9\.\+\-]+?),.*?([0-9\.\+\-]+?),.*?([0-9\.\+\-]+?)\]")
#workspace/test_case_001
workspaceRe = re.compile(r"(workspace\/\S+)")



def var_list_in(expr):
    return sorted(varRe.findall(expr), key=cmp_var, reverse=True)

def new_argv_list_in(expr):
    return [x for x in sorted(newArgvRe.findall(expr), key=cmp_var, reverse=True) if x.find("CN")<0]

def new_var_list_in(expr):
    return [x for x in sorted(newVarRe.findall(expr), key=cmp_var, reverse=True) if x.find("CN")<0]

def func_list_in(expr):
    return sorted(funcRe.findall(expr), key=cmp_var, reverse=True)

def new_func_list_in(expr):
    return sorted(newFuncRe.findall(expr), key=cmp_var, reverse=True)

def web_report_key_exclude():
    return ['expr']

def process_file(id2data, fn):
    funcn = "process_file"
    print(f"INFO: {funcn} {fn}")
    for ln in open(fn, 'r').readlines():
        runids = runidRe.findall(ln)
        if len(runids)<=0:
            print(f"ERROR: ill-formed", ln)
        else:
            runid = runids[0]
            e = id2data[runid]
            xys = xyRe.findall(ln)
            if xys:
                (e['respNm'], e['expr']) = xys[0]
            msi = msiRe.findall(ln)
            if msi:
                (e['retMean'], e['retStdev'], e['retIR']) = msi[0]
            corr = corrRe.findall(ln)
            if corr:
                (e['corrMean'], e['corrStdev'], e['corrIR']) = corr[0]
            workspace_name = workspaceRe.findall(ln)
            if workspace_name:
                e['workspace_name'] = workspace_name[0]

            e['adopted_by'] = 'na'
            if 0:
                print({k:v for k,v in e.items() if k not in web_report_key_exclude()})
if __name__ == "__main__":
    funcn = "data_by_runid.main"
    opt, args = get_options(funcn)
    id2data = defaultdict(dict)
    for fn in args:
        process_file(id2data, fn)

    webdata = {k:{k:v for k,v in id2data[k].items() if k not in web_report_key_exclude()} for k in id2data.keys()}

    df = pd.DataFrame.from_dict(webdata, orient='index')
    df.index.name = 'runid'
    pickle.dump(df, open("/qpsdata/rpts/web_fairedge/run_stats/stats_by_runid.db", 'wb'))
    df.to_csv("/qpsdata/rpts/web_fairedge/run_stats/stats_by_runid.csv")
    print(df)

