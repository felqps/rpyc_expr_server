
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class VpsBook(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.cycle_num = 0
        self.cash = 0
        self.gross = 0
        self.net = 0
        self.equity = 0
        self.l_opn_sz = 0
        self.l_inc_pnl = 0
        self.l_opn_amt = 0
        self.l_cur_sz = 0
        self.l_tot_pnl = 0
        self.l_cur_amt = 0
        self.s_opn_sz = 0
        self.s_inc_pnl = 0
        self.s_opn_amt = 0
        self.s_cur_sz = 0
        self.s_tot_pnl = 0
        self.s_cur_amt = 0
        self.tgt_long_shares = 0
        self.tgt_long_gross = 0
        self.tgt_shrt_shares = 0
        self.tgt_shrt_gross = 0
        self.local_ccy = 1
        self.base_ccy = 1
        self.inc_opn_pnl = 0
        self.trd_opn_pnl = 0
        self.tot_opn_pnl = 0
        self.un_fill_sz = 0
        self.l_orig_todo_sz = 0
        self.s_orig_todo_sz = 0
        self.l_fill_sz = 0
        self.s_fill_sz = 0
        self.l_tgt_amt = 0
        self.s_tgt_amt = 0
        self.book_name = ''
        self.domain = ''
        self.machine = ''
        self.timestamp = 37;
        self.recv_utc_dt = 38;
        self.recv_utc_tm = 39;
        self.utctimestamp = 40;
        self.l_trd_pnl = 0
        self.l_fil_amt = 0
        self.s_trd_pnl = 0
        self.s_fil_amt = 0
    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            pass
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
            if self.cycle_num == self.cycle_num:
                str += "%s:%s;"%("cycle_num",self.cycle_num)
            if self.cash == self.cash:
                str += "%s:%s;"%("cash",self.cash)
            if self.gross == self.gross:
                str += "%s:%s;"%("gross",self.gross)
            if self.net == self.net:
                str += "%s:%s;"%("net",self.net)
            if self.equity == self.equity:
                str += "%s:%s;"%("equity",self.equity)
            if self.l_opn_sz == self.l_opn_sz:
                str += "%s:%s;"%("l_opn_sz",self.l_opn_sz)
            if self.l_inc_pnl == self.l_inc_pnl:
                str += "%s:%s;"%("l_inc_pnl",self.l_inc_pnl)
            if self.l_opn_amt == self.l_opn_amt:
                str += "%s:%s;"%("l_opn_amt",self.l_opn_amt)
            if self.l_cur_sz == self.l_cur_sz:
                str += "%s:%s;"%("l_cur_sz",self.l_cur_sz)
            if self.l_tot_pnl == self.l_tot_pnl:
                str += "%s:%s;"%("l_tot_pnl",self.l_tot_pnl)
            if self.l_cur_amt == self.l_cur_amt:
                str += "%s:%s;"%("l_cur_amt",self.l_cur_amt)
            if self.s_opn_sz == self.s_opn_sz:
                str += "%s:%s;"%("s_opn_sz",self.s_opn_sz)
            if self.s_inc_pnl == self.s_inc_pnl:
                str += "%s:%s;"%("s_inc_pnl",self.s_inc_pnl)
            if self.s_opn_amt == self.s_opn_amt:
                str += "%s:%s;"%("s_opn_amt",self.s_opn_amt)
            if self.s_cur_sz == self.s_cur_sz:
                str += "%s:%s;"%("s_cur_sz",self.s_cur_sz)
            if self.s_tot_pnl == self.s_tot_pnl:
                str += "%s:%s;"%("s_tot_pnl",self.s_tot_pnl)
            if self.s_cur_amt == self.s_cur_amt:
                str += "%s:%s;"%("s_cur_amt",self.s_cur_amt)
            if self.tgt_long_shares == self.tgt_long_shares:
                str += "%s:%s;"%("tgt_long_shares",self.tgt_long_shares)
            if self.tgt_long_gross == self.tgt_long_gross:
                str += "%s:%s;"%("tgt_long_gross",self.tgt_long_gross)
            if self.tgt_shrt_shares == self.tgt_shrt_shares:
                str += "%s:%s;"%("tgt_shrt_shares",self.tgt_shrt_shares)
            if self.tgt_shrt_gross == self.tgt_shrt_gross:
                str += "%s:%s;"%("tgt_shrt_gross",self.tgt_shrt_gross)
            if self.local_ccy == self.local_ccy:
                str += "%s:%s;"%("local_ccy",self.local_ccy)
            if self.base_ccy == self.base_ccy:
                str += "%s:%s;"%("base_ccy",self.base_ccy)
            if self.inc_opn_pnl == self.inc_opn_pnl:
                str += "%s:%s;"%("inc_opn_pnl",self.inc_opn_pnl)
            if self.trd_opn_pnl == self.trd_opn_pnl:
                str += "%s:%s;"%("trd_opn_pnl",self.trd_opn_pnl)
            if self.tot_opn_pnl == self.tot_opn_pnl:
                str += "%s:%s;"%("tot_opn_pnl",self.tot_opn_pnl)
            if self.un_fill_sz == self.un_fill_sz:
                str += "%s:%s;"%("un_fill_sz",self.un_fill_sz)
            if self.l_orig_todo_sz == self.l_orig_todo_sz:
                str += "%s:%s;"%("l_orig_todo_sz",self.l_orig_todo_sz)
            if self.s_orig_todo_sz == self.s_orig_todo_sz:
                str += "%s:%s;"%("s_orig_todo_sz",self.s_orig_todo_sz)
            if self.l_fill_sz == self.l_fill_sz:
                str += "%s:%s;"%("l_fill_sz",self.l_fill_sz)
            if self.s_fill_sz == self.s_fill_sz:
                str += "%s:%s;"%("s_fill_sz",self.s_fill_sz)
            if self.l_tgt_amt == self.l_tgt_amt:
                str += "%s:%s;"%("l_tgt_amt",self.l_tgt_amt)
            if self.s_tgt_amt == self.s_tgt_amt:
                str += "%s:%s;"%("s_tgt_amt",self.s_tgt_amt)
            if self.book_name:
                str += "%s:%s;"%("book_name",self.book_name)
            if self.domain:
                str += "%s:%s;"%("domain",self.domain)
            if self.machine:
                str += "%s:%s;"%("machine",self.machine)
            if self.timestamp == self.timestamp:
                str += "%s:%s;"%("timestamp",self.timestamp)
            if self.recv_utc_dt == self.recv_utc_dt:
                str += "%s:%s;"%("recv_utc_dt",self.recv_utc_dt)
            if self.recv_utc_tm == self.recv_utc_tm:
                str += "%s:%s;"%("recv_utc_tm",self.recv_utc_tm)
            if self.utctimestamp == self.utctimestamp:
                str += "%s:%s;"%("utctimestamp",self.utctimestamp)
            if self.l_trd_pnl == self.l_trd_pnl:
                str += "%s:%s;"%("l_trd_pnl",self.l_trd_pnl)
            if self.l_fil_amt == self.l_fil_amt:
                str += "%s:%s;"%("l_fil_amt",self.l_fil_amt)
            if self.s_trd_pnl == self.s_trd_pnl:
                str += "%s:%s;"%("s_trd_pnl",self.s_trd_pnl)
            if self.s_fil_amt == self.s_fil_amt:
                str += "%s:%s;"%("s_fil_amt",self.s_fil_amt)
        return str

    def __expr__(self):
        return "VpsBook["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def name(self):
        return self.oid
    
    def setOid(self, name):
        self.oid = name
        return self

    def add(self, pos):
        if "pos_map" not in self.__dict__:
            self.pos_map = {}
            self.l_cnt = 0
            self.s_cnt = 0
            self.missing_qt_cnt = 0
        if pos.oid in self.pos_map.keys():
            print("Warning: existed posId: {}".format(pos.oid))
            return
        
        self.pos_map[pos.oid] = pos
        if pos.opn_sz > 0:
            self.l_opn_sz += pos.opn_sz
            self.l_cnt += 1
        else:
            self.s_opn_sz += pos.opn_sz
            self.s_cnt += 1
        self.missing_qt_cnt += 1
        self.updateElements(pos, mult=1.0)

    def testPrintPnl(self, pos=None):
        if pos != None:
            print("---- update pos with id: {}".format(pos.oid))
            print(self.genPrintMsg(pos))
        print(self.genPrintMsg(self.__dict__))
    
    def getMissingQtCnt(self):
        if self.missing_qt_cnt == 0:
            return 0
        missingCnt = 0
        for posId in self.pos_map.keys():
            if "lst_pri" not in self.pos_map[posId].__dict__:
                missingCnt += 1
        self.missing_qt_cnt = missingCnt
        return missingCnt

    def genPrintMsg(self, obj=None):
        if obj == None:
            obj = self.__dict__
        if "pos_map" not in self.__dict__:    ## no pos in current book
            return ""
        printStr = "%4s %12s, "%('oid', obj['oid'])
        genObj = {
            "IAmt": ["l_opn_amt", "s_opn_amt"],
            "CAmt": ["l_cur_amt", "s_cur_amt"],
            "IPnl": ["l_inc_pnl", "s_inc_pnl"],
            "TPnl": ["l_tot_pnl", "s_tot_pnl"],
            "IRet": ["l_opn_amt", "s_opn_amt", "l_inc_pnl", "s_inc_pnl"],
            "TRet": ["l_cur_amt", "s_cur_amt", "l_tot_pnl", "s_tot_pnl"],
            "Cnt": ["l_cnt", "s_cnt", "missing_qt_cnt"]
        }
        for key in genObj.keys():
            if key.find("Ret") > 0:
                [l_amt, s_amt, l_pnl, s_pnl] = genObj[key]
                l_data = (0 if obj[l_amt]==0 else obj[l_pnl]/abs(obj[l_amt])) * 100
                s_data = (0 if obj[s_amt]==0 else obj[s_pnl]/abs(obj[s_amt])) * 100
                printStr += "%6s(%%):(%6.2f, %6.2f, %6.2f)"%(key, l_data, s_data, l_data+s_data)
            elif key.find("Cnt") >=0:
                [l_ele, s_ele] = genObj[key][0: 2]
                l_data = obj[l_ele]
                s_data = obj[s_ele]
                missing_data = self.getMissingQtCnt()
                printStr += "%6s:(%5.0f, %5.0f, %5.0f)"%(key, l_data, s_data, missing_data)
            else:
                [l_ele, s_ele] = genObj[key]
                l_data = obj[l_ele]/1000
                s_data = obj[s_ele]/1000
                printStr += "%6s(k):(%7.2f, %7.2f, %7.2f)"%(key, l_data, s_data, l_data+s_data)
        return printStr
    
    def updateElements(self, pos, mult=1.0):
        l_s = "l" if pos.cur_sz>0 else "s"
        for ele in ["inc_pnl", "trd_pnl", "tot_pnl", "opn_amt", "fil_amt", "cur_amt"]:
            self.__dict__["{}_{}".format(l_s, ele)] += pos.__dict__[ele]*mult

    def msgType(self):
        return "VpsBook"
    
