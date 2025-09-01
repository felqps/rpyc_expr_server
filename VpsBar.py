
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj
#--code--

class VpsBar(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.note = ''
        self.trading_date = 0
        self.open = 0
        self.high = 0
        self.low = 0
        self.close = 0
        self.volume = 0
        self.ts = 0
        self.type = ''
        self.open_interest = 0
        self.total_turnover = 0
        self.up_lmt_pri = 0
        self.low_lmt_pri = 0

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
            if self.type:
                str += "%s:%s;"%("type",self.type)
            if self.note:
                str += "%s:%s;"%("note",self.note)
            if self.trading_date == self.trading_date:
                str += "%s:%s;"%("trading_date",self.trading_date)
            if self.open == self.open:
                str += "%s:%s;"%("open",self.open)
            if self.high == self.high:
                str += "%s:%s;"%("high",self.high)
            if self.low == self.low:
                str += "%s:%s;"%("low",self.low)
            if self.close == self.close:
                str += "%s:%s;"%("close",self.close)
            if self.up_lmt_pri == self.up_lmt_pri:
                str += "%s:%s;"%("up_lmt_pri",self.up_lmt_pri)
            if self.low_lmt_pri == self.low_lmt_pri:
                str += "%s:%s;"%("low_lmt_pri",self.low_lmt_pri)
            if self.volume == self.volume:
                str += "%s:%s;"%("volume",self.volume)
            if self.open_interest == self.open_interest:
                str += "%s:%s;"%("open_interest",self.open_interest)
            if self.total_turnover == self.total_turnover:
                str += "%s:%s;"%("total_turnover",self.total_turnover)
            if self.ts == self.ts:
                str += "%s:%s;"%("ts",tic2utc(self.ts))
        return str

    def __expr__(self):
        return "VpsBar["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

    def hhmm(self):
        import datetime
        return datetime.datetime.fromtimestamp(self.ts/1000.0).strftime("%H%M")

    def msgType(self):
        return "VpsBar"
