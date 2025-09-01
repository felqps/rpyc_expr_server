#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class ExtTgtRjt(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.ticker = ''
        self.pos_tgt = ''
        self.company_name = ''
        self.notes = ''
        self.src_utc_ts = 0
        self.snk_utc_ts = 0

    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            if self.ticker:
                str += "%s:%s;"%("ticker",self.ticker)
            if self.pos_tgt:
                str += "%s:%s;"%("pos_tgt",self.pos_tgt)
            if self.company_name:
                str += "%s:%s;"%("company_name",self.company_name)
            if self.notes:
                str += "%s:%s;"%("notes",self.notes)
            if self.src_utc_ts == self.src_utc_ts:
                str += "%s:%s;"%("src_utc_ts",tic2utc(self.src_utc_ts))
        if ll>=1:
            pass
        if ll>=2:
            if self.snk_utc_ts == self.snk_utc_ts:
                str += "%s:%s;"%("snk_utc_ts",tic2utc(self.snk_utc_ts))
        if ll>=3:
            pass
        if ll>=4:
            pass
        if ll>=5:
            pass
        if ll>=6:
            pass
        if ll>=7:
            pass
        if ll>=8:
            pass
        if ll>=9:
            pass
        return str

    def __expr__(self):
        return "ExtTgtRjt["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "ExtTgtRjt"
