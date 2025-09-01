#!/usr/bin/env python

import sys

import traceback
from optparse import OptionParser
import glob
import os
from CTX import *
#from common_basic import *
from common_smart_load import smart_load

class SymsetMM:
    def __init__(self, dts_cfg):
        print(f"SymsetMM {dts_cfg}")
        if dts_cfg == 'T':
            self._dts_cfg = 'W'
        else:
            self._dts_cfg = dts_cfg
        globstr = f"{rootdata()}/config/symsets/{self._dts_cfg}/*.pkl"
        self._pkl_fn_list = glob.glob(globstr)
        if ctx_debug(1) or True:
            print(f"{DBG}DEBUG_8672{NC}: SymsetMM", '\n'.join(self._pkl_fn_list))

        self._ssn2pkl = {}
        self._ssn2dta = {}
        for x in self._pkl_fn_list:
            ssn = os.path.basename(x).split(r'.')[0]
            self._ssn2pkl[ssn] = x
            self._ssn2dta[ssn] = None

        

    def all_groups(self):
        return sorted(self._ssn2pkl.keys())

    def all_indus(self):
        return [x for x in self.all_groups() if len(x)==3 or x in ['Unknown']]

    def ssn2pkl(self, ssn):
        return self._ssn2pkl[ssn]

    def ssn2dta(self, ssn):
        if self._ssn2dta[ssn] is None:
            self._ssn2dta[ssn] = smart_load(self.ssn2pkl(ssn), title="SymsetMM.ssn2dta")
        return self._ssn2dta[ssn]

    def ssn2indus(self, ssn):
        return sorted(self.ssn2dta(ssn).keys())

    def ssn2mkts(self, ssn):
        l = []
        for x in self.ssn2dta(ssn).values():
            l.extend(x)
        return sorted(l)    


if __name__ == '__main__':
    funcn = 'SymsetMM.main'
    from qdb_options import get_options_sgen
    (opt, args) = get_options_sgen()
    ctx_set_opt(opt)
    sm = SymsetMM(ctx_dts_cfg())
    print(sm.all_groups())
    print(sm.all_indus())
    print(sm.ssn2indus("I65"))
    print(sm.ssn2mkts("I65")[:20])

    print(sm.ssn2indus("citics_35"))
    print(sm.ssn2mkts("citics_35")[:20])


