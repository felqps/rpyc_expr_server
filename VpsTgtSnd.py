
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

#This class is not auto-generated

class VpsTgtSnd(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.tactic = '' #Execution tactic
        self.book = ''
        self.opn_sz = 0
        self.cur_sz = 0
        self.act_sz = 0
        self.tgt_sz = 0
        self.todo_sz = 0
        self.lmt_sz = 0

        self.tgt_pri = 0
        self.prc_lmt_up = 0
        self.prc_lmt_down = 0
        self.pricer = ''

        self.mdl_id = ''
        self.tgt_type = ''  #lmt,mkt
        self.send_ts = 0   #Like 1621560600000
        self.end_ts = 0   #Like 1621560600000
        self.is_lmt = 0   #is_lmt=1: create order by tgt_pri and don`t change pri, pos contemporary mult order, is_lmt=0:create order by tgt_pri and allow change pri, pos contemporary one order


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
            if self.book:
                str += "%s:%s;"%("book",self.book)
            if self.tactic:
                str += "%s:%s;"%("tactic",self.tactic)
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
            if self.lmt_sz == self.lmt_sz:
                str += "%s:%s;"%("lmt_sz",self.lmt_sz)
            if self.tgt_pri == self.tgt_pri:
                str += "%s:%s;"%("tgt_pri",self.tgt_pri)
            if self.prc_lmt_up == self.prc_lmt_up:
                str += "%s:%s;"%("prc_lmt_up",self.prc_lmt_up)
            if self.prc_lmt_down == self.prc_lmt_down:
                str += "%s:%s;"%("prc_lmt_down",self.prc_lmt_down)
            if self.pricer == self.pricer:
                str += "%s:%s;"%("pricer",self.pricer)
            if self.mdl_id == self.mdl_id:
                str += "%s:%s;"%("mdl_id",self.mdl_id)
            if self.tgt_type == self.tgt_type:
                str += "%s:%s;"%("tgt_type",self.tgt_type)
            if self.end_ts == self.end_ts:
                str += "%s:%s;"%("end_ts",self.end_ts)
            if self.send_ts == self.send_ts:
                str += "%s:%s;"%("send_ts",self.send_ts)
            if self.is_lmt == self.is_lmt:
                str += "%s:%s;"%("is_lmt",self.is_lmt)
        return str

    def __expr__(self):
        return "VpsTgtSnd["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

    def msgType(self):
        return "VpsTgtSnd"


