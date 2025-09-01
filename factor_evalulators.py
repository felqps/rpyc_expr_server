import os,sys
import pickle
import pandas as pd
import numpy as np
import random
import math
import time
import glob
import traceback
from fdf_colors import *
from options_helper import *
from matplotlib import pyplot as plt
from fdf_helpers import *
from fdf_logging import *
from df_helpers import *
import register_all_functions

class FactorEvaluatorCorr:
    def __init__(self):
        pass

    def calculate_corr(self, x, y):
        # x_bar = x.sub(x.mean(axis=1), axis=0)
        # y_bar = y.sub(y.mean(axis=1), axis=0)
        # corr = (x_bar * y_bar).mean(axis=1) / (np.std(x, axis=1) * np.std(y, axis=1))
        # corr_mean = corr.mean()
        # corr_std = corr.std()
        # if corr_std > 0:
        #     ir = math.sqrt(243) * corr_mean / corr_std
        # else:
        #     ir = 0
        # return [corr_mean, corr_std, ir]
        return register_all_functions.calculate_corr(x,y)

    def corr_mean_std_ir(self, mean, std, ir, expr, mean_threshold=0.0250, pass_threshold_only=False):
        ln = f"[{mean: 8.6f}, {std: 8.6f}, {ir: 8.6f}]"
        if not pass_threshold_only:
            mean_threshold = 0.0
        if abs(mean) > mean_threshold or not pass_threshold_only:
            COLOR = RED
            if mean < -mean_threshold:
                COLOR = GREEN
            print(f"corr= {COLOR}{ln}{NC}, formula= {expr}", file=sys.stderr)
        else:
            if pass_threshold_only and False:
                ln = None
        return ln

    def pretty_mean_std_ir(self, mean, std, ir, mean_threshold=0.0250, pass_threshold_only=False):
        ln = f"corr= [{mean: 8.6f}, {std: 8.6f}, {ir: 8.6f}]"
        if not pass_threshold_only:
            mean_threshold = 0.0
        if abs(mean) > mean_threshold or not pass_threshold_only:
            COLOR = RED
            if mean < -mean_threshold:
                COLOR = GREEN
            print(f"{COLOR}corr= {ln}{NC}", file=sys.stderr)
        else:
            if pass_threshold_only and False:
                ln = None
        return ln

    def do(self, runid, x, y, normal_run=True):
        #print(f"INFO: FactorEvaluatorCorr", file=sys.stderr)
        (mean, std, ir) = self.calculate_corr(x,y)
        # rc[yname] = (mean, std, ir)
        # xy = f"{yname};{expr};{md5}"
        ln = self.pretty_mean_std_ir(mean, std, ir, pass_threshold_only=normal_run)
        return ln

class FactorEvaluatorRank:
    def __init__(self, workspace_root, top=1):
        self._top = top
        self._workspace_root = workspace_root
        self._name = f"rank{self._top}"
        pass

    def name(self):
        return self._name

    def do(self, runid, x, y, normal_run=True, valid_rate=None, ir_threshold=0.5, debug=False, opt=None):
        funcn = f"FactorEvaluatorRank({self._top})"
        #dbg(f"INFO: FactorEvaluatorRank({self._top})")
        rank_low = x.fillna(1e10).rank(axis=1) #fix nan issues by fillna with a large number for rank_low and small number for rank_hgh
        rank_low = rank_low[rank_low<=self._top] #low rank
        
        rank_hgh = x.fillna(-1e10).rank(axis=1, ascending=False) #high rank
        rank_hgh = rank_hgh[rank_hgh<=self._top]

        rank_low[rank_low>0] = -1
        rank_hgh[rank_hgh>0] = 1

        rank_low = rank_low.fillna(0)
        rank_hgh = rank_hgh.fillna(0)
        rank_combined = (rank_low + rank_hgh)/self._top
        yRankWghted = y.fillna(0) * rank_combined
        yDaily = yRankWghted.apply(lambda row: sum(row), axis=1)

        if debug:
            for df in (x, rank_low, rank_hgh, rank_combined, y, yRankWghted):
                print(df.tail(800))        
            print(yDaily.tail(800))

        mean = yDaily.mean()
        std = yDaily.std()
        if not std:
            return None
            
        ir = mean/std*15.59

        ln = f"({mean*243: 8.6f}, {std*15.59: 8.6f}, {ir: 8.6f})"

        if abs(ir)>ir_threshold:
            symset = "NA"
            if opt is not None:
                symset = opt.symset
            print(f"{CYAN}{funcn}: {self.name()}= {ln}, valid_rate= {valid_rate: 4.2f}, ir_threshold= {ir_threshold: 8.6f}, runid= {runid}, symset= {symset}{NC}", file=sys.stderr)
            # pd.DataFrame(yDaily*np.sign(ir)).cumsum().plot()
            # plt.savefig(f"{self._workspace_root}/eval_pngs/{runid}_rank{self._top}_cumret.png")
            # plt.clf()
            # plt.close(plt.figure())
        return ln

def get_factor_evaluators(workspace_root, spec):
    #return [FactorEvaluatorCorr(), FactorEvaluatorRank(2), FactorEvaluatorRank(1)]
    return [FactorEvaluatorRank(workspace_root, 2), FactorEvaluatorRank(workspace_root, 1)]

if __name__ == "__main__":
    funcn = "factor_evaluators.main"
    opt, args = get_options(funcn)

