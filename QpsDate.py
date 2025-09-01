#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 12:11:08 2020

@author: shuser
"""

import os
import re
import time
import datetime
from QpsSys import *
from QpsDatetime import gethhmmss
import toNative
import glob
from common_ctx import gettoday
from common_files import open_readlines

def is_valid_date(str):
    try:
        time.strptime(str, "%Y%m%d")
        return True
    except:
        return False

def is_valid_time(str):
    try:
        time.strptime(str, "%Y%m%d_%H%M%S")
        return True
    except:
        return False

def getDayNumFile(tz, debug=0):
    dayNumFile = find_first_available([
        toNative.getNativePath(r'/qpsdata/config/day_num_%s.txt' % (tz.lower())),
        r'q:/config/day_num_%s.txt' % (tz.lower())
    ])
    if (dayNumFile is None):
        print("ERR: can not find %s"%(dayNumFile))
    else:
        if debug:
            print("INFO: reading daynum file %s"%(dayNumFile))
    return dayNumFile

DayNumList = {}
def getDayList(tzList, DayNumList = DayNumList, debug = False):
    #global DayNumList
    
    #tzList can be "CN,US" so that it supports multiple zones
    if debug:
        print('getDayList tzlist=%s'%(tzList))
    if not (tzList in DayNumList.keys()):
        DayNumList[tzList] = {}
    
        for tz in tzList.split(","):
            DayNumList[tz] = {}
            #print("getDayList reading", getDayNumFile(tz))
            f = open(getDayNumFile(tz),'rb')
            for line in f:                
                dt = line.split()[0] #dt = re.split(r'\s+',line.strip())[0]
                dt = dt.decode("ascii")
                
                DayNumList[tz][dt] = 1
                DayNumList[tzList][dt] = 1
            
    #days = DayNumList[tzList].keys()
    days = [str(x) for x in DayNumList[tzList].keys()]
    days.sort()
    return days

DayNumDict = None
def getDayDict(tz):
    global DayNumDict
    if DayNumDict == None:
        DayNumDict = {}
        f = open(getDayNumFile(tz),'rb')
        for line in f:
            (dt, idx, fdom, dow) = re.split(r'\s+',line.decode('utf-8').strip())
            DayNumDict[dt] = 1
    return DayNumDict

DayIndexCache = {}
def findDayIndex(dayList, yyyymmdd):
    global DayIndexCache
    yyyymmdd = int(yyyymmdd)
    if yyyymmdd in DayIndexCache:
        return DayIndexCache[yyyymmdd]
    idx = -1
    for x in dayList:
        idx += 1
        DayIndexCache[int(x)]=idx
        if(int(x)>=yyyymmdd):
            break
    return idx
    
def getDayRangeList(tz, begDate, endDate, begOffset=0, endOffset=0, includeEndDate=1):
    dayList = getDayList(tz)
    begDateIndex = findDayIndex(dayList,begDate)
    endDateIndex = findDayIndex(dayList,endDate)

    if 0:
        print >> sys.stderr, "INFO: begDate= %s; endDate= %s; bIdx= %d; eIdx= %d "%(begDate, endDate, begDateIndex, endDateIndex)

    for (dt,idx) in [(begDate,begDateIndex), (endDate,endDateIndex)]:
        if idx<0:
            print >>sys.stderr,'ERR: Date %s is out of range!'%(dt)

    return dayList[begDateIndex+begOffset: endDateIndex+includeEndDate+endOffset]

# def gettoday():
#     now = datetime.datetime.today()
#     return now.strftime("%Y%m%d")

def getyesterday(days=1):
    now = datetime.datetime.today()
    oneday = datetime.timedelta(days=days)
    return (now - oneday).strftime("%Y%m%d")

def gettomorrow():
    now = datetime.datetime.today()
    oneday = datetime.timedelta(days=1)
    return (now + oneday).strftime("%Y%m%d")

_dayList = {}
#def getexchdate(tz="CN", today=gettoday(), debugHHMMSS=160000):
#def getexchdate(tz="CN", today='20210510', debugHHMMSS=None):
#def getexchdate(tz="CN", today='20210509', debugHHMMSS=None):

def cvt2businessdate(today, tz="CN", offset=0):
    global _dayList
    if not tz in _dayList:
        _dayList[tz] = getDayList(tz)

    #print(_dayList[tz])
    dt = _dayList[tz][-1]
    idx = 0
    today = today.replace("-", '')
    while(dt > today and idx < len(_dayList[tz])):
        idx += 1
        dt = _dayList[tz][-idx]


    #print(f"DEBUG: cvt2businessdate {today} -> {dt}")
    return _dayList[tz][int(-idx+offset)]

def getexchdate(tz="CN", today=gettoday(), debugHHMMSS=None):
    global _dayList
    if not tz in _dayList:
        _dayList[tz] = getDayList(tz)

    #print(_dayList[tz])
    dt = _dayList[tz][0]
    idx = 0
    while(dt < today and idx < len(_dayList[tz])):
        idx += 1
        dt = _dayList[tz][idx]

    tzAdj = {"CN": 150000, "US": 220000}

    if debugHHMMSS is None:
        hhmmss = gethhmmss()
    else:
        hhmmss = debugHHMMSS

    if (int(hhmmss)> tzAdj[tz]):
        if today >= _dayList[tz][idx]: #Only add post-close 1-day adjust if today is a trade-date, not on weekend
            dt = _dayList[tz][idx+1]

    #print(f'INFO: getexchdate {today} {hhmmss} {dt}')
    return(dt)

# def gettradedate(exch="CN"):
#     hhmmss = gethhmmss()
#     if (int(hhmmss)> 180000):
#         return gettoday()
#     else:
#         return getyesterday()

def days_between_with_daylist(dayList, fromDt, toDt):
    fromIdx = findDayIndex(dayList, fromDt)
    toIdx = findDayIndex(dayList, toDt)
    daysBtw = int(toIdx) - int(fromIdx)    
    return daysBtw

def days_between_with_tz(tz, fromDt, toDt):
    dayList = getDayList(tz)
    return days_between_with_daylist(dayList, fromDt, toDt)

def businessday_count(tz,begDate,endDate):
    dayList = getDayRangeList(tz, begDate, endDate, 0, 0)
    return len(dayList)

def getPrevNextBusinessDay(today="", skipDaysFn="", region="CN"): #region="CN,US"):
    if today == "":
        today = getexchdate("CN")
    #return('20210510', '20210511')
    businessDayList = getDayList(region)
    skipDays = {}
    if skipDaysFn != "" and os.path.exists(skipDaysFn):
        skipDays = getListInfo(skipDaysFn)
        #print "INFO: skipDays=%s"%(",".join(skipDays.keys()))

    prevBusinessDay = businessDayList[businessDayList.index(today)-1]
    while(prevBusinessDay in skipDays):
        prevBusinessDay = businessDayList[businessDayList.index(prevBusinessDay)-1]

    nextBusinessDay = businessDayList[businessDayList.index(today)+1]
    while(nextBusinessDay in skipDays):
        nextBusinessDay = businessDayList[businessDayList.index(nextBusinessDay)+1]

    return (prevBusinessDay, nextBusinessDay)

def getNPrevBusinessDay(n_prev, today="", region="CN"): #region="CN,US"):
    if today == "":
        today = getexchdate("CN")
    businessDayList = getDayList(region)

    nPrevBusinessDay = businessDayList[businessDayList.index(today)-n_prev]
    return nPrevBusinessDay

def getPrevBusinessDay(today="", skipDaysFn="", region="CN"):
    return getPrevNextBusinessDay(today, skipDaysFn=skipDaysFn, region=region)[0]

def getNextBusinessDay(today="", skipDaysFn="", region="CN"):
    return getPrevNextBusinessDay(today, skipDaysFn=skipDaysFn, region=region)[1]

def getLastLimtPriceDate():
    trydt = getexchdate()
    trycnt = 10
    while trycnt>0 and not os.path.exists(toNative.getNativePath("c:/fe/simu/pub/chna/data/xy_ctp_handler_NORMAL.%s.limt_price.log"%(trydt))):
        trydt = getPrevNextBusinessDay(trydt)[0]
        trycnt -= 1
    return trydt

def getSetupEndDate(enddate, real_trading):
    setupEnddt = enddate
    if real_trading:
        if setupEnddt > gettoday(): 
            setupEnddt = gettoday()
    else:
        if setupEnddt > getLastLimtPriceDate():
            setupEnddt = getLastLimtPriceDate()    
    return setupEnddt

yyyymmddRe = re.compile(r'(\d\d\d\d\d\d\d\d)')
def find_dates_in_str(str):
    return [dt for dt in yyyymmddRe.findall(str)]

MktOMSDatesCfg = {}
def getDatesCfg(datename, cfg_name='download', sectype='CS', asofdate='download', verbose=False, debug=False):
    funcn = 'getDatesCfg'
    if cfg_name in ['T', 'U', 'A']:
        cfg_name = "prod1w"
    fns = []
    #debug=True
    cfg_files = ["/qpsdata/config/MktOMSDates/%s/%s/%s.cfg"%(asofdate, cfg_name, sectype), "q:/config/MktOMSDates/%s/%s/%s.cfg"%(asofdate, cfg_name, sectype)]
    
    for x in cfg_files:
        if debug:
            print(f"DEBUG: {getDatesCfg.__name__} glob({x})", file=sys.stderr)
        fns.extend(glob.glob(x))

    #print(f"cfg_files= {fns}", file=sys.stderr)

    if len(fns)>0: #found corresponding  MktOMSDates file
        fn = fns[-1]

        if debug:
            print(f"DEBUG: {funcn}, asofdate= {asofdate}, cfg_name= {cfg_name}, sectype= {sectype}, selected= {fn}, fns=", fns, file=sys.stderr)

        global MktOMSDatesCfg
        if cfg_name not in MktOMSDatesCfg:
            if debug:
                print(f"INFO: getDtsCfg reading {fn} for cfg {cfg_name}")
            MktOMSDatesCfg[cfg_name] = {}

        cfg = MktOMSDatesCfg[cfg_name]
        cfg['today'] = gettoday()

        if fileChged(asofdate,fn):
            if False or verbose:
                print(f"INFO: {funcn} reading %s"%(fn), file=sys.stderr)

            for ln in open_readlines(fn):
                (k,v) = ln.split("=")
                cfg[k] = v
    else:
        cfg = {}
        cfg["E"] = {"begDt": "20000101", "endDt": "20110101"}
        cfg["F"] = {"begDt": "20100101", "endDt": "20210101"}
        cfg["G"] = {"begDt": "20200101", "endDt": "20220101"}
        cfg["prod1w"] = cfg["G"]

        tmp = {}
        tmp['dbegdate'] = cfg[cfg_name]['begDt']
        tmp['denddate'] = cfg[cfg_name]['endDt']
        tmp['pre2date'] = cfg[cfg_name]['endDt']
        tmp['prevdate'] = cfg[cfg_name]['endDt']
        tmp['asofdate'] = cfg[cfg_name]['endDt']
        tmp['dcsndate'] = cfg[cfg_name]['endDt']
        tmp['tradedate'] = cfg[cfg_name]['endDt']
        tmp['nextdate'] = cfg[cfg_name]['endDt']
        tmp['today'] = cfg[cfg_name]['endDt']

        cfg = tmp


    return cfg[datename]


if __name__=='__main__':
    print("asofdate=%s, prevdate=%s, tradedate=%s, nextdate=%s"%(
        getDatesCfg("asofdate"), getDatesCfg("prevdate"), 
        getDatesCfg("tradedate"), getDatesCfg("nextdate")))

    for dt in ['20201202', '20220101', '20220316']:
        print(f"{dt} => {cvt2businessdate(dt)} => {getPrevBusinessDay(cvt2businessdate(dt))}")

    print(getPrevBusinessDay(getPrevBusinessDay('20201202')))

    print(getexchdate("CN"))

    print(getNPrevBusinessDay(5, "20220725"))
    print(getDayRangeList("CN", "20100101", "20230517"), len(getDayRangeList("CN", "20100101", "20230517")))
    open("/home/shuser/test2.txt", "w").write("\n".join(getDayRangeList("CN", "20100101", "20230517")))

