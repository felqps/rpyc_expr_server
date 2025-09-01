import sys
from webbrowser import get

import pandas as pd
from shared_cmn import *

def concat_factors(fldnm, dfs, debug=False):
    dfs_trim_heads = []
    prev_df_last_dtm = None
    for df in dfs:
        if df is None:
            continue
        if prev_df_last_dtm is not None:
            df = df.loc[df.index>prev_df_last_dtm]
        if df.shape[0]<=0:
            continue
        #print_df(df, title="foobar")
        prev_df_last_dtm = df.index[-1]
        if debug:
            print(f"DEBUG_2134: fldnm= {fldnm}, shape= {df.shape}, prev_df_last_dtm= {prev_df_last_dtm}")
        dfs_trim_heads.append(df)
    return(pd.concat(dfs_trim_heads, axis=0))
