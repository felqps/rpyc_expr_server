#!/usr/bin/env python

import sys

from optparse import OptionParser
import numpy as np
#from platform_helpers import *
import pandas as pd
from dateutil.parser import parse as dtparse
from collections import defaultdict
from shared_cmn import *

class ExchSessions:
    def __init__(self, name='SN_CF_DNShort', debug=False):
        self._name = name
        self._sft_num = 0
        self._tm2offset = defaultdict(None)
        self._sft_delta = defaultdict(None)
        self._sft_dict = defaultdict(dict)
        self._bar_delta = defaultdict(None)
        self._open_periods = []

        #default to stock session beg/endTm
        self._begTm = '09:30:00'
        self._endTm = '15:00:00'

        if name == 'SN_CF_DNShort':
            self._session_cfg = {('09:00:00', '10:15:00'):15,  #:15 means close for 15min
                                ('10:30:00', '11:30:00'):120, 
                                ('13:30:00', '15:00:00'):360, 
                                ('21:00:00', '23:00:00'):600}
            self._begTm = '09:00:00'
            self._endTm = '23:00:00'

        elif name == 'SN_CF_DAY':
            self._session_cfg = {('09:00:00', '10:15:00'):15, 
                                ('10:30:00', '11:30:00'):120, 
                                ('13:30:00', '15:00:00'):18*60}
            self._begTm = '09:00:00'
            self._endTm = '15:00:00'

        elif name == 'SN_CS_DAY':
            self._session_cfg = {('09:30:00', '11:30:00'):90, 
                                ('13:00:00', '15:00:00'):18.5*60}
            self._begTm = '09:00:00'
            self._endTm = '15:00:00'

        self._sessions = {}

        adjBegT = pd.to_datetime('00:00:00', format='%H:%M:%S')
        adjV = 0
        if debug:
            for k,v in self._session_cfg.items():
                (begT, endT) = k
                begT = pd.to_datetime(begT, format='%H:%M:%S')
                endT = pd.to_datetime(endT, format='%H:%M:%S')

            self._open_periods.append((begT, endT))

            self._sft_dict[(adjBegT, begT)] = pd.Timedelta(adjV, unit='m')
            adjBegT = endT
            adjV = v #minutes to next trading start
        self._sft_dict[(adjBegT, pd.to_datetime('23:59:59', format='%H:%M:%S'))] = pd.Timedelta(adjV, unit='m')
        
        for k,v in self._sft_dict.items():
            if False:
                qps_print(f'ExchSession info: {k[0].time()} - {k[1].time()} : {v}', file=sys.stderr)

    @property
    def name(self):
        return self._name

    @property
    def begTm(self):
        return self._begTm

    @property
    def endTm(self):
        return self._endTm    

    def sft_deprecated(self, bar, num=1, fldType='Pred'):
        self._sft_num = num
        barLen = int(bar[:-1]) 
        barUnit = bar[-1]
        qps_print(f'ExchSessions sft({barLen}{barUnit}, {num})')
        self._sft_delta[bar] = pd.Timedelta(barLen * num, unit=barUnit)
        self._bar_delta[bar] = pd.Timedelta(barLen, unit=barUnit)
        self._tm2offset[bar] = None #force recalc
        self._zero_delta = pd.Timedelta(0, unit='m')
        return(self)

    def is_open(self, tm):
        for (begT, endT) in self._open_periods:
            if tm>=begT and tm<=endT:
                return True
        return False

    def _sft_tm_deprecated(self, bar, tm):
        # if self._sft_num == 0:
        #     return tm
        sftTm = tm + self._sft_delta[bar]
        for k,v in self._sft_dict.items():
            # if (sftTm>k[0] and sftTm<=k[1]):
            #     sftTm = sftTm + v - self._bar_delta
            if (sftTm>=k[0] and sftTm<k[1]):
                sftTm = sftTm + v #+ self._bar_delta
        return (sftTm)

    def get_offset_deprecated(self, bar, tm):
        if self._tm2offset[bar] == None:
            self._tm2offset[bar] = {}
    
            t = pd.to_datetime('00:00:00', format='%H:%M:%S')
            endT = pd.to_datetime('23:59:59', format='%H:%M:%S')
            while t < endT:
                if self.is_open(t):
                    self._tm2offset[bar][t.time()] = self._sft_tm(bar, t) - t
                t = t + self._bar_delta[bar]

        if tm.time() in self._tm2offset[bar]:
            return self._tm2offset[bar][tm.time()]
        else:
            return self._zero_delta

    def sft_by_deprecated(self, pld, bar, num):
        self.sft(bar, num)
        def func(tm, ts=self):
            #qps_print(f'sft_by {tm}, ts={ts}')
            sfttm = tm + ts.get_offset(bar, tm)
            if sfttm.date() != tm.date() and tm.dayofweek == 4:
                #qps_print('============== sft_by weekend ===================', tm, tm.dayofweek)
                sfttm += pd.Timedelta(2, unit='day')
            return sfttm
            #return tm + pd.DateOffset(minutes=1)
        pld.index = pld.index.map(func)
        return(pld)

    def _between_dates_deprecated(self, pld, begDt, endDt):
        qps_print('between_dates', type(pld), dtparse(begDt), dtparse(f'{endDt} 23:59:59'))
        return pld.loc[(pld.index>=dtparse(begDt)) & (pld.index<=dtparse(endDt))]

    def _between_time_deprecated(self, pld, begTm='9:00', endTm='23:00'):
        return pld.between_time(begTm, endTm)

    def __str__(self):
        return f'ExchSessions: name={self._name}'

if __name__ == '__main__':
    
    for name in ['SN_CF_DNShort', 'CF_DAY']:
        for bar in ['5m']:
            for num in [0,1,2]:
                f = open(f'/Fairedge_dev/app_QpsData/tests/logs/ExchSessions.{name}.{bar}.{num}.ref', 'w')
                ts = ExchSessions(name=name).sft(bar=bar, num=num)
                qps_print(ts, file=f)
                t = pd.to_datetime('00:00:00', format='%H:%M:%S')
                endT = pd.to_datetime('23:59:59', format='%H:%M:%S')
                while t < endT:
                    if ts.is_open(t):
                        qps_print(f'{t.time()} + {ts.get_offset(bar, t)} : {(t+ts.get_offset(bar, t)).time()}', file=f)
                    t = t + pd.Timedelta(5, unit='m')








