
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class VpsSndAck(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.client_ord_id = ''
        self.brkr_ord_id = ''
        self.fill_id = ''
        self.brkr = ''
        self.ord_side = ''
        self.ord_type = ''
        self.sym = ''
        self.pos_id = ''
        self.ord_sz = 0
        self.ord_pri = 0
        self.fil_amt = 0
        self.fil_sz = 0
        self.fil_pri = 0
        self.src_utc_dt = ''
        self.src_utc_ts = 0
        self.ord_notes = ''
    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            if self.oid:
                str += "%s:%s;"%("oid",self.oid)
            if self.client_ord_id:
                str += "%s:%s;"%("client_ord_id",self.client_ord_id)
            if self.brkr_ord_id:
                str += "%s:%s;"%("brkr_ord_id",self.brkr_ord_id)
            if self.fill_id:
                str += "%s:%s;"%("fill_id",self.fill_id)
            if self.brkr:
                str += "%s:%s;"%("brkr",self.brkr)
            if self.ord_side:
                str += "%s:%s;"%("ord_side",self.ord_side)
            if self.ord_type:
                str += "%s:%s;"%("ord_type",self.ord_type)
            if self.sym:
                str += "%s:%s;"%("sym",self.sym)
            if self.pos_id:
                str += "%s:%s;"%("pos_id",self.pos_id)
            if self.ord_sz == self.ord_sz:
                str += "%s:%s;"%("ord_sz",self.ord_sz)
            if self.ord_pri == self.ord_pri:
                str += "%s:%s;"%("ord_pri",self.ord_pri)
            if self.fil_amt == self.fil_amt:
                str += "%s:%s;"%("fil_amt",self.fil_amt)
            if self.fil_sz == self.fil_sz:
                str += "%s:%s;"%("fil_sz",self.fil_sz)
            if self.fil_pri == self.fil_pri:
                str += "%s:%s;"%("fil_pri",self.fil_pri)
            if self.src_utc_dt:
                str += "%s:%s;"%("src_utc_dt",self.src_utc_dt)
            if self.src_utc_ts == self.src_utc_ts:
                str += "%s:%s;"%("src_utc_ts",tic2utc(self.src_utc_ts))
            if self.ord_notes:
                str += "%s:%s;"%("ord_notes",self.ord_notes)
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
        return "VpsSndAck["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "VpsSndAck"
