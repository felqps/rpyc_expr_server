#!/usr/bin/env python

import sys,os,re
import hashlib
from  fdf_basic import *
import pandas as pd
import numpy as np
from collections import Counter
from pathlib import Path

macroRe = re.compile(r'@[A-Za-z0-9_]+')

FDF_MACROS = None
def fdf_expand_macros(v):
    funcn = f"fdf_expand_macros('{v}'')"
    global FDF_MACROS

    if type(v) != type(str()):
        return v

    #print("++", v)
    if v.find("@")<0:
        return v

    if FDF_MACROS is None:
        FDF_MACROS = {}
        exec(Path("/qpsdata/config/fdf_cfgs/macro_helper.txt").read_text(), globals())
    
    macros = macroRe.findall(v)
    #print(f"INFO: {funcn}", macros)
    for macro in macros:
        rep = macro
        if macro in FDF_MACROS:
            rep = FDF_MACROS[macro]
        v = v.replace(macro, rep)
    #     print(macro, v)
    # exit(0)
    return v

