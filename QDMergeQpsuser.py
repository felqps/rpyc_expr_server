#!/home/shuser/anaconda3/bin/python

import sys

import pickle
import glob
import os
import shutil
import re
import time
from optparse import OptionParser

from pathlib import Path
import pandas as pd
from QpsDatetime import *
import QpsUtil
import logging
# from QDMergeUpdates import merge_dfs,write_file_safe
from qdb_options import get_options_common

# pd.set_option("display.max_rows", None)
# pd.set_option("display.max_columns", None)
# pd.set_option("display.width", 1000)


def build_merge_group(input_dirs, output_dir):
    merge_group = {}
    input_dirs = sorted(input_dirs, key = lambda x:re.findall(r"\d{8}", x)[0])
    print(f"Merge dirs {input_dirs} to {output_dir}")
    for d in input_dirs:
        fns = glob.glob(f"{d}/*/*/???.db")
        fns.extend(glob.glob(f"{d}/*/*/Unknown.db"))
        fns.extend(glob.glob(f"{d}/*/*/CS_ALL.db"))
        fns.extend(glob.glob(f"{d}/*/*/Indices.db"))
        for f in fns:
            # if "_Rqraw_" in f:
            #     continue
        # for f in glob.glob(f"{d}/1d/Pred_age_Prc_1d_1/E47.db"):
        # for f in glob.glob(f"{d}/1d/*OHLCV*/???.db"):
            f_output = output_dir +  f.replace(d, '')
            if f_output not in merge_group:
                merge_group[f_output] = []
            merge_group[f_output].append(f)
    return merge_group

def process_merge(opt, output_file, input_files):
    funcn = "process_merge"
    if not os.path.exists(os.path.dirname(output_file)):
        os.makedirs(os.path.dirname(output_file))
    merge_data = merge_files(opt, output_file, input_files)
    tmp_path = Path(f'{output_file}.tmp')
    tmp_path.write_bytes(pickle.dumps(merge_data))
    tmp_path.rename(output_file)
    print(f"INFO: {funcn} {input_files} to {output_file}")

def merge_files(opt, output_file, input_files):
    try:
        if len(input_files) == 1:
            shutil.copy(input_files[-1], output_file)

        df_left = pickle.load(open(input_files[0], "rb"))
        for f in input_files[1:]:
            df_right = pickle.load(open(f, "rb"))
            
            
            if df_right is None:
                if opt and opt.verbose:
                    print(f"WARNING: {f}: data is None")
                continue

            if df_left is None:
                if opt and opt.verbose:
                    print(f"WARNING: {input_files[0]}: data is None")
                df_left = df_right
                continue

            if df_right.shape[0]<=0:
                if opt and opt.debug:
                    print(f'DEBUG: funcn', df_right.shape)
                continue

            # if "ohlcv" in f.lower() and "ohlcv_v" not in f.lower(): 
            # revise_data(df_left, df_right)

            # df_right = df_right[df_right.index > df_left.index.values[-1]]
            # df_left = pd.concat([df_left, df_right])
            df_left = merger_data(df_left, df_right)

            if False and opt.verbose:
                print('INFO: merge_dfs:', df_left.shape, df_right.shape)

        return df_left
    except Exception as err:
        logging.exception(err)
        print(f"Error {input_files} to {output_file}")

def merger_data(df_left, df_right, revise=True):
    if revise:
        revise_data(df_left, df_right)

    df_right = df_right[df_right.index > df_left.index.values[-1]]
    df_left = pd.concat([df_left, df_right])
    return df_left


def revise_data(df_left, df_right):
    try:
        for col in df_left:
            if col not in df_right:
                continue

            if df_left.index[-1] not in df_right[col]:
                continue

            if abs(df_left[col][-1] - df_right[col][df_left.index[-1]]) > 0.000001: # > 1 tics
                # print(f"Revise sym={col}, tm={df_left.index[-1]}, l={df_left[col][-1]}, r={df_right[col][df_left.index[-1]]}")
                percentage = df_right[col][df_left.index[-1]]/df_left[col][-1]
                df_left[col] = df_left[col].map(lambda x: round(x*percentage, 6))
    except Exception as err:
        logging.exception(err)

def list_cmds(opt=None):
    cmds = [
        "python /Fairedge_dev/app_QpsData_RQ/QDMergeQpsuser.py --dryrun 1 qpsuser",
        "python /Fairedge_dev/app_QpsData_RQ/QDMergeQpsuser.py --dryrun 1 /qpsdata/egen_study/check_predictors_01",
        "python /Fairedge_dev/app_QpsData_RQ/QDMergeQpsuser.py --dryrun 1 /qpsdata/egen_study/check_predictors_02",
    ]
    print('\n'.join(cmds))

if __name__ == "__main__":
    (opt, args) = get_options_common(list_cmds)
    if opt.list_cmds:
        list_cmds()
        exit(0)

    flag = "qpsuser"
    if len(args) > 0:
        flag = args[0] if args[0] else flag
    if flag == "qpsuser":
        input_dirs = [
            "/qpsuser/che/data_rq.20200101_20220101/CS/SN_CS_DAY/G_20200101_20220101",
            "/qpsuser/che/data_rq.20100101_20210101/CS/SN_CS_DAY/F_20100101_20210101",
        ]
        # if len(args) == 0:
        #     input_dirs = args[0].strip().split(r',')

        input_dirs.append(sorted(glob.glob("/qpsuser/che/data_rq.20210810_uptodate/CS/SN_CS_DAY/prod1w_????????_????????"))[-1])

        # output_dir = "/qpsuser/che/data_rq.20100101_uptodate/CS/SN_CS_DAY/A_20100101_uptodate"
        output_dir = "/NASQPS06.qpsdata/data_rq.A/A_20100101_uptodate"
        # print(input_dirs, output_dir)
        merge_group = build_merge_group(input_dirs, output_dir)
        print(len(merge_group))
        for output_file, input_files in merge_group.items():
            if opt.dryrun:
                print(f"Merge {input_files} to {output_file}")
            else:
                if 1 and os.path.exists(output_file):
                    continue
                    # if time.time() - os.path.getmtime(output_file) < 24*3600:
                    #     continue

                process_merge(opt, output_file, input_files)
            # break

    # elif flag == "check_predictors_01":
    elif "check_predictors" in flag:
        if not os.path.exists(flag):
            print(f"No such {flag}")
            exit(1)
        # fns_dir = "/qpsdata/egen_study/check_predictors_01"
        fns_dir = flag
        fns = glob.glob(f"{fns_dir}/*.db")
        fns = [fn for fn in fns if fn[-4:-3] in ["F", "G", "W"]]
        # [print(f) for f in fns]
        merge_group = {}
        for fn in sorted(fns):
            if False and "Resp_lnret_C2C_1d_1" not in fn:
                continue

            output_file = fn[:-4] + "A" + fn[-3:]
            if output_file not in merge_group:
                merge_group[output_file] = []
            merge_group[output_file].append(fn)
        [print(output_file, input_files) for output_file, input_files in merge_group.items()]
        for output_file, input_files in merge_group.items():
            if opt.dryrun:
                print(f"Merge {input_files} to {output_file}")
            else:
                process_merge(opt, output_file, input_files)


