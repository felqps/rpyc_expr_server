#!/usr/bin/env python
import sys

from ctypes import BigEndianStructure
import sys

import re
from FldNamespace import *
from EgenBase import EgenBase
import operator
from funcs_fld import *
from CTX import *
import numpy as np
from common_colors import RED

from register_cf_functions import *


def geppy_register_func(funcn, func, arity, pset=None, debug=False):
    if debug:
        print(f"DEBUG_3278: _register_func {funcn}")

    EgenBase.register_script_func(funcn, func)
    if pset is not None:
        pset.add_function(func, arity)

def build_function_registrating_statements():
    funcn = "build_function_registrating_statements"
    lns = []
    lns.append(f"geppy_register_func('add', add, 2, pset=pset)")
    lns.append(f"geppy_register_func('sub', sub, 2, pset=pset)")
    lns.append(f"geppy_register_func('mul', mul, 2, pset=pset)")
    lns.append(f"geppy_register_func('div', div, 2, pset=pset)")
    lns.append(f"geppy_register_func('pow2', pow2, 1, pset=pset)")
    lns.append(f"geppy_register_func('pow3', pow3, 1, pset=pset)")
    lns.append(f"geppy_register_func('z', z, 1, pset=pset)")
    lns.append(f"geppy_register_func('w', w, 1, pset=pset)")
    lns.append(f"geppy_register_func('zw', zw, 1, pset=pset)")
    lns.append(f"geppy_register_func('m', m, 1, pset=pset)")
    lns.append(f"geppy_register_func('inv', inv, 1, pset=pset)")
    lns.append(f"geppy_register_func('neg', neg, 1, pset=pset)")
    lns.append(f"geppy_register_func('sign', sign, 1, pset=pset)")
    lns.append(f"geppy_register_func('log', log, 1, pset=pset)")
    lns.append(f"geppy_register_func('sqrt', sqrt, 1, pset=pset)")
    lns.append(f"geppy_register_func('abs', abs, 1, pset=pset)")
    lns.append(f"geppy_register_func('scale', scale, 1, pset=pset)")
    lns.append(f"geppy_register_func('sigmoid', sigmoid, 1, pset=pset)")
    lns.append(f"geppy_register_func('iif', iif, 3, pset=pset)")

    lns.append(f"geppy_register_func('rank', rank, 1, pset=pset)")
    lns.append(f"geppy_register_func('rank_sub', rank_sub, 2, pset=pset)")
    lns.append(f"geppy_register_func('rank_div', rank_div, 2, pset=pset)")

    # for i in [1,2,3,]:
    #     print(f"{RED}ERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR{NC}")
    #     lns.append(f"geppy_register_func('signedpower{i}', lambda f: signedpower(f,e={i}), 1, pset=pset)")

    for i in ts_days():
        lns.append(f"geppy_register_func('ts_sft{i}', ts_sft{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_delta{i}', ts_delta{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_sum{i}', ts_sum{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_stddev{i}', ts_stddev{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_rank{i}', ts_rank{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_min{i}', ts_min{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_max{i}', ts_max{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_argmin{i}', ts_argmin{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_argmax{i}', ts_argmax{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_prod{i}', ts_prod{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_zscore{i}', ts_zscore{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ts_corr{i}', ts_corr{i}, 2, pset=pset)")
        lns.append(f"geppy_register_func('ts_cov{i}', ts_cov{i}, 2, pset=pset)")
        lns.append(f"geppy_register_func('decay_linear{i}', decay_linear{i}, 1, pset=pset)")
        lns.append(f"geppy_register_func('ma{i}', ma{i}, 1, pset=pset)")

    if False:
        print(f"DEBUG_2121: {funcn}", '\n'.join(lns))
    return lns

def geppy_register_script_functions(pset=None, func_select='all', funcn = f"register_script_functions"):
    func_patterns = []
    if func_select == 'macd':
        func_patterns = 'add,sub,mul,div,neg,ma'.split(r',')
    elif func_select == 'ma':
        func_patterns = 'add,sub,mul,div,neg,.*ma.*'.split(r',')
    else:
        func_patterns = [".*"]
        
    for func_pattern in func_patterns:
        for ln in sorted(build_function_registrating_statements()):
            func_name = re.findall(r"\'(.+?)\'", ln)[0]
            #print(f"DEBUG_9812: {func_name}")
            if re.match(rf"^{func_pattern}\d*$", func_name):
                if pset is not None:
                    if ctx_verbose(5) or False:
                        print(f"INFO: {funcn} '{func_pattern}' => {ln}", file=sys.stderr)
                exec(ln)

    EgenBase.register_script_func("calc_StSts", calc_StSts)
    EgenBase.register_script_func('calc_prc_flag', calc_prc_flag)
    EgenBase.register_script_func('reverse_bool', reverse_bool)
    EgenBase.register_script_func('false2nan', false2nan)
    EgenBase.register_script_func('calc_CN300', calc_CN300)
    EgenBase.register_script_func('calc_CN500', calc_CN500)
    EgenBase.register_script_func('calc_CN800', calc_CN800)
    EgenBase.register_script_func('mdvrank', mdvrank)
    EgenBase.register_script_func('toprank', toprank)
    EgenBase.register_script_func('lnret', lnret)
    EgenBase.register_script_func('calc_prc_tradable', calc_prc_tradable)


