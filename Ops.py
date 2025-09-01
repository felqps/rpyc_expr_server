#!/usr/bin/env python

import sys

from optparse import OptionParser
import numpy as np
#from platform_helpers import *
import pandas as pd
from QDData import *
import rqdatac as rc

class Ops:
    def __init__(self, qdfRoot=None, ssn=None, varName=None, scn=None, 
            func=None, funcName=None, funcArgs=None,
            period=None, fldType=None, bar=None, map2mkt=False, debug=False):
        self.qdfRoot = qdfRoot
        self.ssn = ssn
        self.varName = varName 
        self.funcName = funcName
        self.funcArgs = funcArgs
        self.func = func
        self.period = period
        self.fldType = fldType
        self.bar = bar
        self.debug = debug
        self.map2mkt = map2mkt
        self.scn = scn


    def __str__(self):
        return f'Ops(varName={self.varName}, ssn={self.ssn}, funcName={self.funcName}, \
funcArgs={self.funcArgs}, period={self.period}, \
fldType={self.fldType}, bar={self.bar}, debug={self.debug})'

    def is_resp(self):
        return self.varName.find('Resp')>=0

    def is_pred(self):
        return not self.is_resp()

    def qdf(self, db='all'):
        return (self.qdfRoot + self.bar + self.varName + f'{db}.db')

def ops2func_info(e):
    if type(e) == type(Ops()):
        (var, func, note, debug) = (e.varName, e.func, e.varName, e.debug)
    else:
        (var, func, note) = e
        debug = False  
    return(var, func, note, debug)

if __name__ == '__main__':
    qps_print(Ops())



