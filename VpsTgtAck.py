
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class VpsTgtAck(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.tactic = ''
        self.book = ''
        self.opn_sz = 0
        self.cur_sz = 0
        self.act_sz = 0
        self.tgt_sz = 0
        self.todo_sz = 0

        self.tgt_pri = 0
        self.prc_lmt_up = 0
        self.prc_lmt_down = 0
        self.pricer = 0
        self.mdl_id = ''

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
            if self.tactic:
                str += "%s:%s;"%("tactic",self.tactic)
            if self.book:
                str += "%s:%s;"%("book",self.book)
            if self.opn_sz == self.opn_sz:
                str += "%s:%s;"%("opn_sz",self.opn_sz)
            if self.cur_sz == self.cur_sz:
                str += "%s:%s;"%("cur_sz",self.cur_sz)
            if self.act_sz == self.act_sz:
                str += "%s:%s;"%("act_sz",self.act_sz)
            if self.tgt_sz == self.tgt_sz:
                str += "%s:%s;"%("tgt_sz",self.tgt_sz)
            if self.todo_sz == self.todo_sz:
                str += "%s:%s;"%("todo_sz",self.todo_sz)
            if self.prc_lmt_up == self.prc_lmt_up:
                str += "%s:%s;"%("prc_lmt_up",self.prc_lmt_up)
            if self.prc_lmt_down == self.prc_lmt_down:
                str += "%s:%s;"%("prc_lmt_down",self.prc_lmt_down)
            if self.pricer == self.pricer:
                str += "%s:%s;"%("pricer",self.pricer)
        return str

    def __expr__(self):
        return "VpsTgtAck["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "VpsTgtAck"


