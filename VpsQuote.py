
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
from QpsDatetime import tic2utc
from VpsObj import VpsObj

class VpsQuote(VpsObj):
    def __init__(self):
        super().__init__()
        self.sym = ''
        self.src_utc_ts = NaN
        self.bid_pri = NaN
        self.bid_sz = NaN
        self.ask_pri = NaN
        self.ask_sz = NaN
        self.last_pri = NaN
        self.last_sz = NaN
        self.tottrd_sz = NaN
        self.last_bid_tm = NaN
        self.last_ask_tm = NaN
        self.last_trd_tm = NaN
        self.preclo_pri = NaN
        self.opn_pri = NaN
        self.high_pri = NaN
        self.low_pri = NaN
        self.bid_aggr_sz = NaN
        self.ask_aggr_sz = NaN
        self.bid_count = NaN
        self.ask_count = NaN
        self.last_trd_dt = NaN
        self.last_bid_dt = NaN
        self.last_ask_dt = NaN
        self.vwap = NaN
        self.vwap_vol = NaN
        self.quote_cond = NaN
        self.trade_cond = NaN
        self.sequence_number = NaN
        self.exch = NaN
        self.decision_tag = True
        self.bid_mmid = NaN
        self.ask_mmid = NaN
        self.trade_exch_id = NaN
        self.pre_clo_spl = NaN
        self.tottrd_amt = NaN
        self.advise = False
        self.bid_tick = 111
        self.ask_tick = 111
        self.status = NaN
        self.no_qts = NaN
        self.no_trds = NaN
        self.last_recv_tm = NaN
        self.sampling_timestamp = NaN
        self.recv_utc_dt = NaN
        self.recv_utc_tm = NaN
        self.last_bid_utc_ts = NaN
        self.last_ask_utc_ts = NaN
        self.last_trd_utc_ts = NaN
        self.open_interest = NaN
        self.snk_utc_ts = NaN
        self.up_lmt_pri = NaN
        self.low_lmt_pri = NaN
        self.bid_pri2 = NaN
        self.bid_pri3 = NaN
        self.bid_pri4 = NaN
        self.bid_pri5 = NaN
        self.bid_sz2 = NaN
        self.bid_sz3 = NaN
        self.bid_sz4 = NaN
        self.bid_sz5 = NaN
        self.ask_pri2 = NaN
        self.ask_pri3 = NaN
        self.ask_pri4 = NaN
        self.ask_pri5 = NaN
        self.ask_sz2 = NaN
        self.ask_sz3 = NaN
        self.ask_sz4 = NaN
        self.ask_sz5 = NaN
    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            if self.sym:
                str += "%s:%s;"%("sym",self.sym)
            if self.src_utc_ts == self.src_utc_ts:
                str += "%s:%s;"%("src_utc_ts",tic2utc(self.src_utc_ts))
            if self.bid_pri == self.bid_pri:
                str += "%s:%s;"%("bid_pri",self.bid_pri)
            if self.ask_pri == self.ask_pri:
                str += "%s:%s;"%("ask_pri",self.ask_pri)
            if self.last_pri == self.last_pri:
                str += "%s:%s;"%("last_pri",self.last_pri)
            if self.bid_pri2 == self.bid_pri2:
                str += "%s:%s;"%("bid_pri2",self.bid_pri2)
            if self.bid_pri3 == self.bid_pri3:
                str += "%s:%s;"%("bid_pri3",self.bid_pri3)
            if self.bid_pri4 == self.bid_pri4:
                str += "%s:%s;"%("bid_pri4",self.bid_pri4)
            if self.bid_pri5 == self.bid_pri5:
                str += "%s:%s;"%("bid_pri5",self.bid_pri5)
            if self.ask_pri2 == self.ask_pri2:
                str += "%s:%s;"%("ask_pri2",self.ask_pri2)
            if self.ask_pri3 == self.ask_pri3:
                str += "%s:%s;"%("ask_pri3",self.ask_pri3)
            if self.ask_pri4 == self.ask_pri4:
                str += "%s:%s;"%("ask_pri4",self.ask_pri4)
            if self.ask_pri5 == self.ask_pri5:
                str += "%s:%s;"%("ask_pri5",self.ask_pri5)
        if ll>=1:
            if self.bid_sz == self.bid_sz:
                str += "%s:%s;"%("bid_sz",self.bid_sz)
            if self.ask_sz == self.ask_sz:
                str += "%s:%s;"%("ask_sz",self.ask_sz)
            if self.last_sz == self.last_sz:
                str += "%s:%s;"%("last_sz",self.last_sz)
            if self.bid_sz2 == self.bid_sz2:
                str += "%s:%s;"%("bid_sz2",self.bid_sz2)
            if self.bid_sz3 == self.bid_sz3:
                str += "%s:%s;"%("bid_sz3",self.bid_sz3)
            if self.bid_sz4 == self.bid_sz4:
                str += "%s:%s;"%("bid_sz4",self.bid_sz4)
            if self.bid_sz5 == self.bid_sz5:
                str += "%s:%s;"%("bid_sz5",self.bid_sz5)
            if self.ask_sz2 == self.ask_sz2:
                str += "%s:%s;"%("ask_sz2",self.ask_sz2)
            if self.ask_sz3 == self.ask_sz3:
                str += "%s:%s;"%("ask_sz3",self.ask_sz3)
            if self.ask_sz4 == self.ask_sz4:
                str += "%s:%s;"%("ask_sz4",self.ask_sz4)
            if self.ask_sz5 == self.ask_sz5:
                str += "%s:%s;"%("ask_sz5",self.ask_sz5)
        if ll>=2:
            if self.preclo_pri == self.preclo_pri:
                str += "%s:%s;"%("preclo_pri",self.preclo_pri)
            if self.opn_pri == self.opn_pri:
                str += "%s:%s;"%("opn_pri",self.opn_pri)
            if self.high_pri == self.high_pri:
                str += "%s:%s;"%("high_pri",self.high_pri)
            if self.low_pri == self.low_pri:
                str += "%s:%s;"%("low_pri",self.low_pri)
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
            if self.tottrd_sz == self.tottrd_sz:
                str += "%s:%s;"%("tottrd_sz",self.tottrd_sz)
            if self.last_bid_tm == self.last_bid_tm:
                str += "%s:%s;"%("last_bid_tm",self.last_bid_tm)
            if self.last_ask_tm == self.last_ask_tm:
                str += "%s:%s;"%("last_ask_tm",self.last_ask_tm)
            if self.last_trd_tm == self.last_trd_tm:
                str += "%s:%s;"%("last_trd_tm",self.last_trd_tm)
            if self.bid_aggr_sz == self.bid_aggr_sz:
                str += "%s:%s;"%("bid_aggr_sz",self.bid_aggr_sz)
            if self.ask_aggr_sz == self.ask_aggr_sz:
                str += "%s:%s;"%("ask_aggr_sz",self.ask_aggr_sz)
            if self.bid_count == self.bid_count:
                str += "%s:%s;"%("bid_count",self.bid_count)
            if self.ask_count == self.ask_count:
                str += "%s:%s;"%("ask_count",self.ask_count)
            if self.last_trd_dt == self.last_trd_dt:
                str += "%s:%s;"%("last_trd_dt",self.last_trd_dt)
            if self.last_bid_dt == self.last_bid_dt:
                str += "%s:%s;"%("last_bid_dt",self.last_bid_dt)
            if self.last_ask_dt == self.last_ask_dt:
                str += "%s:%s;"%("last_ask_dt",self.last_ask_dt)
            if self.vwap == self.vwap:
                str += "%s:%s;"%("vwap",self.vwap)
            if self.vwap_vol == self.vwap_vol:
                str += "%s:%s;"%("vwap_vol",self.vwap_vol)
            if self.quote_cond == self.quote_cond:
                str += "%s:%s;"%("quote_cond",self.quote_cond)
            if self.trade_cond == self.trade_cond:
                str += "%s:%s;"%("trade_cond",self.trade_cond)
            if self.sequence_number == self.sequence_number:
                str += "%s:%s;"%("sequence_number",self.sequence_number)
            if self.exch == self.exch:
                str += "%s:%s;"%("exch",self.exch)
            if self.decision_tag == self.decision_tag:
                str += "%s:%s;"%("decision_tag",self.decision_tag)
            if self.bid_mmid == self.bid_mmid:
                str += "%s:%s;"%("bid_mmid",self.bid_mmid)
            if self.ask_mmid == self.ask_mmid:
                str += "%s:%s;"%("ask_mmid",self.ask_mmid)
            if self.trade_exch_id == self.trade_exch_id:
                str += "%s:%s;"%("trade_exch_id",self.trade_exch_id)
            if self.pre_clo_spl == self.pre_clo_spl:
                str += "%s:%s;"%("pre_clo_spl",self.pre_clo_spl)
            if self.tottrd_amt == self.tottrd_amt:
                str += "%s:%s;"%("tottrd_amt",self.tottrd_amt)
            if self.advise == self.advise:
                str += "%s:%s;"%("advise",self.advise)
            if self.bid_tick == self.bid_tick:
                str += "%s:%s;"%("bid_tick",self.bid_tick)
            if self.ask_tick == self.ask_tick:
                str += "%s:%s;"%("ask_tick",self.ask_tick)
            if self.status == self.status:
                str += "%s:%s;"%("status",self.status)
            if self.no_qts == self.no_qts:
                str += "%s:%s;"%("no_qts",self.no_qts)
            if self.no_trds == self.no_trds:
                str += "%s:%s;"%("no_trds",self.no_trds)
            if self.last_recv_tm == self.last_recv_tm:
                str += "%s:%s;"%("last_recv_tm",self.last_recv_tm)
            if self.sampling_timestamp == self.sampling_timestamp:
                str += "%s:%s;"%("sampling_timestamp",self.sampling_timestamp)
            if self.recv_utc_dt == self.recv_utc_dt:
                str += "%s:%s;"%("recv_utc_dt",self.recv_utc_dt)
            if self.recv_utc_tm == self.recv_utc_tm:
                str += "%s:%s;"%("recv_utc_tm",self.recv_utc_tm)
            if self.last_bid_utc_ts == self.last_bid_utc_ts:
                str += "%s:%s;"%("last_bid_utc_ts",tic2utc(self.last_bid_utc_ts))
            if self.last_ask_utc_ts == self.last_ask_utc_ts:
                str += "%s:%s;"%("last_ask_utc_ts",tic2utc(self.last_ask_utc_ts))
            if self.last_trd_utc_ts == self.last_trd_utc_ts:
                str += "%s:%s;"%("last_trd_utc_ts",tic2utc(self.last_trd_utc_ts))
            if self.open_interest == self.open_interest:
                str += "%s:%s;"%("open_interest",self.open_interest)
            if self.up_lmt_pri == self.up_lmt_pri:
                str += "%s:%s;"%("up_lmt_pri",self.up_lmt_pri)
            if self.low_lmt_pri == self.low_lmt_pri:
                str += "%s:%s;"%("low_lmt_pri",self.low_lmt_pri)
        return str

    def __expr__(self):
        return "VpsQuote["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    @property
    def delay_tics(self):
        from numpy import isnan
        if isnan(self.snk_utc_ts) or isnan(self.src_utc_ts):
            return 0
        return int(self.snk_utc_ts) - int(self.src_utc_ts)
    
    def fair_pri(self, ourBuySz=0, ourSellSz=0):
        if self.bid_pri <= 0 or self.bid_pri > self.ask_pri or self.ask_sz <= ourSellSz or self.bid_sz <= ourBuySz:
            return 0
        return (self.bid_pri*(self.ask_sz-ourSellSz) + self.ask_pri*(self.bid_sz-ourBuySz))/(self.ask_sz + self.bid_sz - ourBuySz - ourSellSz)


    def msgType(self):
        return "VpsQuote"

