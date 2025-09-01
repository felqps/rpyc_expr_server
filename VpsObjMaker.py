# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 15:21:11 2020

@author: shuser
"""
from numpy import nan as NaN
from numpy import nan

import copy
from collections import OrderedDict

from VpsObj import VpsObj
from VpsInstru import  VpsInstru
from VpsOrd import VpsOrd
from VpsQuote import VpsQuote
from VpsPos import VpsPos
from VpsBook import VpsBook
from VpsOrdReq import VpsOrdReq
from VpsOrdSnd import VpsOrdSnd
from VpsFilRpt import VpsFilRpt
from VpsCxlReq import VpsCxlReq
from VpsCxlSnd import VpsCxlSnd
from VpsCxlAck import VpsCxlAck
from VpsCxlDne import VpsCxlDne
from VpsReqAck import VpsReqAck
from VpsSndAck import VpsSndAck
from VpsSndRjt import VpsSndRjt
from VpsReqRjt import VpsReqRjt

from ExtOrdNew import ExtOrdNew
from ExtPosTgt import ExtPosTgt
from ExtNewAck import ExtNewAck
from ExtTgtAck import ExtTgtAck
from ExtFilRpt import ExtFilRpt
from ExtNewRjt import ExtNewRjt
from ExtTgtRjt import ExtTgtRjt
from VpsTgtSnd import VpsTgtSnd
from VpsTgtAck import VpsTgtAck
from VpsCxlRjt import VpsCxlRjt

from VpsCmsn import VpsCmsn
from VpsBar import VpsBar

protoTypes = OrderedDict()

def add(o):
    protoTypes['.'+o.msgType()+'.'] = o

def makeObj(subject):
    if subject.find('.')<0:
        subject = ".%s."%(subject)
    if len(protoTypes)<=0:
        add(VpsQuote())
        add(VpsInstru())
        add(VpsOrd())
        add(VpsPos())
        add(VpsBook())
        add(VpsOrdReq())
        add(VpsOrdSnd())
        add(VpsFilRpt())
        add(VpsCxlReq())
        add(VpsCxlSnd())
        add(VpsCxlAck())
        add(VpsCxlDne())
        add(VpsCxlRjt())
        add(VpsReqAck())    
        add(VpsSndAck())
        add(VpsSndRjt())
        add(VpsReqRjt())
        add(VpsTgtSnd())
        add(VpsTgtAck())

        add(ExtOrdNew())
        add(ExtPosTgt())
        add(ExtNewAck())
        add(ExtTgtAck())
        add(ExtFilRpt())
        add(ExtNewRjt())
        add(ExtTgtRjt())
        
        add(VpsCmsn())
        add(VpsBar())

    for msgType in protoTypes.keys():
        if subject.find(msgType)>=0:
            #print("Making ", msgType)
            return copy.deepcopy(protoTypes[msgType])
        
    return None
        
        
if __name__ == "__main__":
    makeObj("eb.qt.test")
    for msgType in protoTypes.keys():
        print(msgType, makeObj(msgType))
