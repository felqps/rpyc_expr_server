
#!/cygdrive/c/Anaconda3/python.exe

# -*- coding: utf-8 -*-
import sys
import pickle
from numpy import nan as NaN
import pandas as pd
from QpsDatetime import tic2utc
from VpsObj import VpsObj

class VpsInstru(VpsObj):
    def __init__(self):
        super().__init__()
        self.instruId = ''
        self.metaId = ''
        self.permid = ''
        self.blpSym = ''
        self.factor = 1
        self.category = ''
        self.lclName = ''
        self.beta = 1.0
        self.secType = ''
        self.sym = ''
        self.trading_hours = ''
        self.listed_date = ''
        self.de_listed_date = ''
        self.dominant_date = ''
        self.de_dominant_date = ''
        self.underlying_order_book_id = ''
        self.margin_rate = ''
        self.market_tplus = ''
        self.exchange = ''
        self.product = ''
        self.industry_name = ''
    def serializeToString(self):
        return pickle.dumps(self)
    # Use pickle.loads(data) directly to de-serialize

    def toString(self, ll=5):
        str=""
        if ll>=0:
            if self.metaId:
                str += "%s:%s;"%("metaId",self.metaId)
            if self.permid:
                str += "%s:%s;"%("permid",self.permid)
            if self.factor == self.factor:
                str += "%s:%s;"%("factor",self.factor)
            if self.category:
                str += "%s:%s;"%("category",self.category)
            if self.lclName:
                str += "%s:%s;"%("lclName",self.lclName)
            if self.beta == self.beta:
                str += "%s:%s;"%("beta",self.beta)
            if self.secType:
                str += "%s:%s;"%("secType",self.secType)
            if self.listed_date:
                str += "%s:%s;"%("listed_date",self.listed_date)
            if self.de_listed_date:
                str += "%s:%s;"%("de_listed_date",self.de_listed_date)
            if self.dominant_date:
                str += "%s:%s;"%("dominant_date",self.dominant_date)
            if self.de_dominant_date:
                str += "%s:%s;"%("de_dominant_date",self.de_dominant_date)
            if self.margin_rate:
                str += "%s:%s;"%("margin_rate",self.margin_rate)
            if self.market_tplus:
                str += "%s:%s;"%("market_tplus",self.market_tplus)
            if self.exchange:
                str += "%s:%s;"%("exchange",self.exchange)
            if self.product:
                str += "%s:%s;"%("product",self.product)
            if self.industry_name:
                str += "%s:%s;"%("industry_name",self.industry_name)
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
            if self.instruId:
                str += "%s:%s;"%("instruId",self.instruId)
            if self.blpSym:
                str += "%s:%s;"%("blpSym",self.blpSym)
            if self.sym:
                str += "%s:%s;"%("sym",self.sym)
            if self.trading_hours:
                str += "%s:%s;"%("trading_hours",self.trading_hours)
            if self.underlying_order_book_id:
                str += "%s:%s;"%("underlying_order_book_id",self.underlying_order_book_id)
        return str

    def __expr__(self):
        return "VpsInstru["+self.toString()+"]"

    def __str__(self):
        return self.__expr__()

#--code--
    def isStock(self):
        return (self.secType ==  'CS')

    def isFuture(self):
        return (self.secType ==  'Future')

    def getMetaId(self):
        if self.isStock():
            return self.permid
        else:
            return self.metaId
        
    def hasDominantDates(self):
        if self.isStock():
            return True
        elif self.dominant_date == 'NA' or self.dominant_date != self.dominant_date:  #test nan
            return False
        else:
            return True

    def get_domDt(self, debug=False):
        from dateutil.parser import parse as dtparse

        if self.permid == 'IH1512': #RQ dataset issue
            self.dominant_date = dtparse('2015-11-14').date()

        #If dominant_date start on Monday, then we need to filter from previous Friday
        #if (self.dominant_date == dtparse('20210201').date()):
        #    self.dominant_date = dtparse('20210130').date()

        if self.dominant_date is not None:
            # if type(self.dominant_date) == type(str()):
            #     print(f'get_domDt {self.dominant_date}', file=sys.stderr)
            # el
            if self.dominant_date.weekday() == 0:
                new_dominant_date = self.dominant_date + pd.Timedelta(-2, unit='day')
                if debug:
                    print('INFO: shift Monday dominant date: {}, {} to {}'.format(self.permid, self.dominant_date, new_dominant_date), file=sys.stderr)
                self.dominant_date = new_dominant_date

        return self.dominant_date

    def get_dedomDt(self):
        return self.de_dominant_date

    def exchSym(self):
        exchSym = self.permid

        if self.exchange == 'CZCE':
            exchSym = '{}{}'.format(exchSym[:-4], exchSym[-3:])
        elif self.exchange == 'SHFE' or self.exchange == 'DCE' or self.exchange == 'INE' or self.exchange == 'GFEX':
            exchSym = '{}{}'.format((exchSym[:-4]).lower(), exchSym[-4:])

        return exchSym

    # def filterByDominantDates(self, df, lastEndDtm, mkt, debug=False):
    #     #print(self.__dict__)
    #     import pandas as pd

    #     if not self.hasDominantDates():
    #         if debug:
    #             print(f'ERROR: filterByDominantDates cannot find dominant_dates for {self.permid}', file=sys.stderr)
    #         return None

    #     if self.permid == 'IH1512': #RQ dataset issue
    #         self.dominant_date = '2015-11-14'
    #     begDtm = pd.Timestamp(self.dominant_date) + pd.Timedelta(-3, unit='h')
    #     endDtm = pd.Timestamp(self.de_dominant_date) + pd.Timedelta(17, unit='h')
    #     lapseDays = 0
    #     if mkt in lastEndDtm:
    #         lapseDays = (begDtm - lastEndDtm[mkt]).days
    #         if lapseDays > 5:
    #             print(f'============================================================================ filterByDominantDates large lapse days: {self.permid} lapse {lapseDays}')
    #     lastEndDtm[mkt] = endDtm
    #     df = df[begDtm:endDtm]
    #     if debug:
    #         print(f'filterByDominantDates: {self.permid} domDt={self.dominant_date}, deDomDt={self.de_dominant_date} :  {begDtm}, {endDtm}, shape: {df.shape}, lapse: {lapseDays}')
    #         print_df(df, rows=20, title=self.permid)
    #     return(df)
