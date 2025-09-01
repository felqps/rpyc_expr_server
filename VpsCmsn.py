#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class VpsCmsn(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.os0 = 0
        self.ol0 = 0
        self.cs0 = 0
        self.cl0 = 0
        self.cl1 = 0
        self.cs1 = 0
        self.date = ''
        self.notes = ''

    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            if self.oid:
                str += "%s:%s;"%("oid",self.oid)
            if self.sym:
                str += "%s:%s;"%("sym",self.sym)
            if self.date:
                str += "%s:%s;"%("date",self.date)
            if self.os0 == self.os0:
                str += "%s:%s;"%("os0",self.os0)
            if self.ol0 == self.ol0:
                str += "%s:%s;"%("ol0",self.ol0)
            if self.cs0 == self.cs0:
                str += "%s:%s;"%("cs0",self.cs0)
            if self.cl0 == self.cl0:
                str += "%s:%s;"%("cl0",self.cl0)
            if self.cl1 == self.cl1:
                str += "%s:%s;"%("cl1",self.cl1)
            if self.cs1 == self.cs1:
                str += "%s:%s;"%("cs1",self.cs1)
        if ll>=1:
            pass
        if ll>=2:
            pass
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
        return "VpsCmsn["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "VpsCmsn"
    
