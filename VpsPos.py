
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsOmsObj import VpsOmsObj

class VpsPos(VpsOmsObj):
    def __init__(self):
        super().__init__()
        self.oid = ''
        self.sym = ''
        self.note = ''
        self.opn_sz = 0
        self.cur_sz = 0
        self.act_sz = 0
        self.fil_amt = 0
        self.shrt_apvd = 0
        self.domain = ''
        self.tactic = 0
        self.pos_flags = 0
        self.sug_sz = 0
        self.tgt_sz = 0
        self.ord_sz = 0
        self.fil_sz = 0
        self.todo_sz = 0
        self.fil_pri = 0
        self.cur_pri = 0
        self.clo_pri = 0
        self.sug_pri = 0
        self.cur_amt = 0
        self.opn_amt = 0
        self.todo_amt = 0
        self.inc_pnl = 0
        self.trd_pnl = 0
        self.tot_pnl = 0
        self.inc_opnpnl = 0
        self.trd_opnpnl = 0
        self.tot_opnpnl = 0
        self.src_utc_dt = 0
        self.src_utc_tm = 0
        self.agg_sz = 0
        self.apvd_sz = 0
        self.src_utc_ts = 0
        self.snk_utc_ts = 0
        self.client_ord_id = ''
        self.brkr_ord_id = ''
        self.pos_id = ''
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
            if self.note:
                str += "%s:%s;"%("note",self.note)
            if self.opn_sz == self.opn_sz:
                str += "%s:%s;"%("opn_sz",self.opn_sz)
            if self.cur_sz == self.cur_sz:
                str += "%s:%s;"%("cur_sz",self.cur_sz)
            if self.act_sz == self.act_sz:
                str += "%s:%s;"%("act_sz",self.act_sz)
            if self.fil_amt == self.fil_amt:
                str += "%s:%s;"%("fil_amt",self.fil_amt)
            if self.shrt_apvd == self.shrt_apvd:
                str += "%s:%s;"%("shrt_apvd",self.shrt_apvd)
            if self.domain:
                str += "%s:%s;"%("domain",self.domain)
            if self.tactic == self.tactic:
                str += "%s:%s;"%("tactic",self.tactic)
            if self.pos_flags == self.pos_flags:
                str += "%s:%s;"%("pos_flags",self.pos_flags)
            if self.sug_sz == self.sug_sz:
                str += "%s:%s;"%("sug_sz",self.sug_sz)
            if self.tgt_sz == self.tgt_sz:
                str += "%s:%s;"%("tgt_sz",self.tgt_sz)
            if self.ord_sz == self.ord_sz:
                str += "%s:%s;"%("ord_sz",self.ord_sz)
            if self.fil_sz == self.fil_sz:
                str += "%s:%s;"%("fil_sz",self.fil_sz)
            if self.todo_sz == self.todo_sz:
                str += "%s:%s;"%("todo_sz",self.todo_sz)
            if self.fil_pri == self.fil_pri:
                str += "%s:%s;"%("fil_pri",self.fil_pri)
            if self.cur_pri == self.cur_pri:
                str += "%s:%s;"%("cur_pri",self.cur_pri)
            if self.clo_pri == self.clo_pri:
                str += "%s:%s;"%("clo_pri",self.clo_pri)
            if self.sug_pri == self.sug_pri:
                str += "%s:%s;"%("sug_pri",self.sug_pri)
            if self.cur_amt == self.cur_amt:
                str += "%s:%s;"%("cur_amt",self.cur_amt)
            if self.opn_amt == self.opn_amt:
                str += "%s:%s;"%("opn_amt",self.opn_amt)
            if self.todo_amt == self.todo_amt:
                str += "%s:%s;"%("todo_amt",self.todo_amt)
            if self.inc_pnl == self.inc_pnl:
                str += "%s:%s;"%("inc_pnl",self.inc_pnl)
            if self.trd_pnl == self.trd_pnl:
                str += "%s:%s;"%("trd_pnl",self.trd_pnl)
            if self.tot_pnl == self.tot_pnl:
                str += "%s:%s;"%("tot_pnl",self.tot_pnl)
            if self.inc_opnpnl == self.inc_opnpnl:
                str += "%s:%s;"%("inc_opnpnl",self.inc_opnpnl)
            if self.trd_opnpnl == self.trd_opnpnl:
                str += "%s:%s;"%("trd_opnpnl",self.trd_opnpnl)
            if self.tot_opnpnl == self.tot_opnpnl:
                str += "%s:%s;"%("tot_opnpnl",self.tot_opnpnl)
            if self.src_utc_dt == self.src_utc_dt:
                str += "%s:%s;"%("src_utc_dt",self.src_utc_dt)
            if self.src_utc_tm == self.src_utc_tm:
                str += "%s:%s;"%("src_utc_tm",self.src_utc_tm)
            if self.agg_sz == self.agg_sz:
                str += "%s:%s;"%("agg_sz",self.agg_sz)
            if self.apvd_sz == self.apvd_sz:
                str += "%s:%s;"%("apvd_sz",self.apvd_sz)
            if self.src_utc_ts == self.src_utc_ts:
                str += "%s:%s;"%("src_utc_ts",tic2utc(self.src_utc_ts))
            if self.client_ord_id:
                str += "%s:%s;"%("client_ord_id",self.client_ord_id)
            if self.brkr_ord_id:
                str += "%s:%s;"%("brkr_ord_id",self.brkr_ord_id)
            if self.pos_id:
                str += "%s:%s;"%("pos_id",self.pos_id)
            if self.mdl_id:
                str += "%s:%s;"%("mdl_id",self.mdl_id)
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
        return "VpsPos["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def msgType(self):
        return "VpsPos"

    def posId(self):
        return self.oid
        #return "%d:%s:%s"%(self.book, self.list_name, self.sym)
    
    def setPosId(self,v):
#        (book, list_name, sym) = v.split(":")
#        self.book = int(book)
#        self.list_name = list_name
#        self.sym = sym
        self.oid=v
        return self

    def add(self, book):
        if "book_list" not in self.__dict__:
            self.book_list = []
        self.book_list.append(book)

    def qtUpdate(self, qt):
        if ("opn_pri" not in self.__dict__ ) or (self.opn_pri == 0):
            self.opn_pri = qt.preclo_pri
        if ("lst_pri" not in self.__dict__) or (self.lastPrice() != qt.last_pri):
            self.lst_pri = qt.last_pri
            return True
        return False

    def lastPrice(self):
        try:
            return self.lst_pri
        except:
            try:
                return self.opn_pri
            except:
                return 0

    def getFactor(self):
        try:
            return self.instru.factor
        except Exception as e:
            print("Error: {}".format(e))
            return 1

    def get_ord_sz(self):
        return self.tgt_sz - self.cur_sz

    def updateElements(self):
        factor = self.getFactor()
        for book in self.book_list:
            book.updateElements(self, mult=-1.0)
        if self.lastPrice() == 0:
            self.inc_pnl = 0
            self.trd_pnl = 0
            self.opn_amt = 0
            self.fil_amt = 0
        else:
            self.inc_pnl = self.opn_sz * (self.lastPrice()-self.opn_pri) * factor
            self.trd_pnl = self.getTrdPnl()
            self.opn_amt = self.opn_sz * self.lastPrice() * factor
            self.fil_amt = self.fil_sz * self.lastPrice() * factor

        self.tot_pnl = self.inc_pnl+self.trd_pnl
        self.cur_amt = self.opn_amt + self.fil_amt
        # print(self.inc_pnl, self.trd_pnl, self.tot_pnl, self.opn_amt, self.fil_amt, self.cur_amt)
        for book in self.book_list:
            book.updateElements(self, mult=1.0)

    # def updateElements(self, qt):
    #     if self.qtUpdate(qt):
    #         # print(f"INFO: oid: {self.oid}, quoteUpdate lastPrice changed: {self.lastPrice()} => {last_pri}")
    #         factor = self.getFactor()
    #         for book in self.book_list:
    #             book.updateElements(self, mult=-1.0)
    #         self.inc_pnl = 0 if self.lastPrice() == 0 else self.opn_sz * (self.lastPrice()-self.opn_pri) * factor
    #         self.trd_pnl = self.getTrdPnl()
    #         self.tot_pnl = self.inc_pnl+self.trd_pnl

    #         self.opn_amt = self.opn_sz * self.lastPrice() * factor
    #         self.fil_amt = self.fil_sz * self.lastPrice() * factor
    #         self.cur_amt = self.opn_amt + self.fil_amt
    #         # print(self.inc_pnl, self.trd_pnl, self.tot_pnl, self.opn_amt, self.fil_amt, self.cur_amt)
    #         for book in self.book_list:
    #             book.updateElements(self, mult=1.0)
    
    def getTrdPnl(self):
        if "ords" not in self.__dict__:
            return 0
        trd_pnl = 0
        for ordId in self.ords.keys():
            ord = self.getOrd(ordId)
            trd_pnl += ord.getTrdPnl(self.lastPrice(), self.getFactor())
        return trd_pnl

    def hasOrd(self, client_ord_id):
        if "ords" in self.__dict__ and (client_ord_id in self.ords.keys()):
            return True
        return False
    
    def getOrd(self, client_ord_id):
        if self.hasOrd(client_ord_id):
            return self.ords[client_ord_id]
        return None

    def insertOrd(self, ord):
        if "ords" not in self.__dict__:
            self.ords = {}
        
        self.ords[ord.client_ord_id] = ord
        self.ord_sz += ord.ord_sz
    
    def insertFill(self, filrpt, ord=None):
        if ord == None:
            ord = self.getOrd(filrpt.client_ord_id)
        if ord != None:
            if not self.hasOrd(ord.client_ord_id):
                self.insertOrd(ord)
            if ord.msgType() in ["VpsOrd", "VpsOrdSnd"]:    ## TODO: other msgType orders, how to manage.
                ord.insertFill(filrpt)
            
            self.fil_sz += filrpt.fil_sz
            self.cur_sz += filrpt.fil_sz
    
    def insertFillFromFile(self, ord, filrpt):
        ord.insertFill(filrpt)
        self.insertOrd(ord)
        self.fil_sz += filrpt.fil_sz

    @property
    def pos_sz(self):
        if not hasattr(self, '_pos_sz'):
            self._pos_sz = {'OL1':0, 'OS1':0, 'OL0':0, 'OS0':0}
        return self._pos_sz
    
    @pos_sz.setter
    def pos_sz(self, v):
        if type({}) == type(v):
            self._pos_sz = v
        else:
            self._pos_sz = {'OL1':0, 'OS1':0, 'OL0':0, 'OS0':0}
            

if __name__ == '__main__':
    pos = VpsPos()
    if 1:
        pos.sym="ESMO"
        pos.oid="0_test_ESMO"
        pos.note="ml=2345;from=gradient"
        print("%s"%(pos))
        pos.pos_sz['OL1'] = 2
        print(pos.pos_sz)
        pos.pos_sz = {'OL1':4, 'OS1':3, 'OL0':2, 'OS0':1}
        print(pos.pos_sz)

