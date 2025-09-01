
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class ExtFilRpt(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.client_ord_id = ''
        self.ext_ord_id = ''
        self.fil_sz = 0
        self.fil_pri = 0
        self.src_utc_ts = 0
        self.snk_utc_ts = 0
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
            if self.client_ord_id:
                str += "%s:%s;"%("client_ord_id",self.client_ord_id)
            if self.ext_ord_id:
                str += "%s:%s;"%("ext_ord_id",self.ext_ord_id)
            if self.fil_sz == self.fil_sz:
                str += "%s:%s;"%("fil_sz",self.fil_sz)
            if self.fil_pri == self.fil_pri:
                str += "%s:%s;"%("fil_pri",self.fil_pri)
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
        return "ExtFilRpt["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "ExtFilRpt"
