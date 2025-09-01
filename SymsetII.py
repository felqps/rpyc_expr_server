#!/usr/bin/env python

import sys

import traceback
from optparse import OptionParser
import numpy as np
#from platform_helpers import *
import pandas as pd
import rqdatac as rc

from QDData import *
from QDF import *
import InstruHelper
#from getSymset import *
from shared_func import *
from QpsUtil import sym2indu

class SymsetII:
    def __init__(self, ssn, qdfRoot, begDt, endDt, force=False, instru_force=False): #ssn = symset name
        # if True:
        #     print(f'Symset({ssn}, {qdfRoot}, {begDt}, {endDt})')

        # (QDF, DataCfg) = config_qdf(datacfg)
        # self._datacfg = datacfg        
        self._ssn = ssn
        #self._mkts = getSymset(ssn)
        self._iis = {}
        self._qdfRoot = qdfRoot
        self._begDt = begDt
        self._endDt = endDt
        #self.secType = sectype
        self._force = force
        self._instru_force = instru_force
        self._init_flag = False
        if False:
            print(f'Symset.__init__() %s'%(self), file=sys.stderr)

    def init(self, debug=False):
        if self.ssn == 'Test':
            return #need to boostrap by InstruHelper

        if self._init_flag == True:
            return(self)

        secFilters = ['active', 'cont', 'main']
        if self.secType in ['stocks', 'CS']:
            secFilters = ['active']

        for secFilter in secFilters:
            if secFilter not in self._iis:
                if debug:
                    print(f"DEBUG: symset.init qdfRoot {self._qdfRoot}")
                self._iis[secFilter] = InstruHelper.get_iis_for_mkt(self.ssn, self._qdfRoot, self._begDt, self._endDt, 
                        self.secType, secFilter, force=self._force, instru_force=self._instru_force)
        
        if self.secType in ['stocks', 'CS']:
            self._iis['main'] = {}
            for k in self._iis['active']:
                #if self._ssn != sym2indu(k) and False: #XXX do not check indu here as sometimes indu file is not up-to-day and new stocks are not pulled in
                if self._ssn != sym2indu(k): #XXX this is needed as some stocks move indu, and old indu data is not complete. If they not-complete one processed asfter completed one, wrong results
                    #print(f"DEBUG: Symset.init() ignore indu-chged stock {k}, indu= {self._ssn}, assigned to {sym2indu(k)}", file=sys.stderr)
                    continue
                else:
                    self._iis['main'][k] = self._iis['active'][k]

            self._iis['cont'] = self._iis['main']
            self._iis['active'] = self._iis['main']

        if False:
            print('Symset.init()', self)
            #traceback.print_stack()

        self._init_flag = True

        return(self)

    @property
    def ssn(self):
        return self._ssn

    @property
    def name(self):
        return self._ssn

    @property
    def svr_ssn(self): #backward issues. data is stored differently on server, v.s. client side
        if self.ssn.find('CF_')>=0:
            return 'futures'
        else:
            return self.ssn
    
    @property
    def secType(self):
        if self.ssn.find('CF_')>=0:
            return 'futures'
        else:
            return 'stocks'

    def mkts(self, suffix=''):
        self.init()
        if self.secType in ['futures', 'CF']:
            return sorted(list(set([x[:-4]+suffix for x in self._iis['main'].keys()])))
        else: 
            return sorted(self._iis['main'].keys())
        #return sorted(self._mkts)

    def iis(self, secFilter='active'):
        self.init()
        return self._iis[secFilter]

    def get_vps_instru_qdf(self, secFilter, debug=False):
        funcn = 'get_vps_instru_qdf'
        tgtFn = None
        if self.secType in ['stocks', 'CS']:
            tgtFn = self._qdfRoot + f'vps_instru.{self._ssn}.db'
        else:
            tgtFn = self._qdfRoot + f'{self._secFilter}.vps_instru.db'
        tgtFn = tgtFn.switchToQSF(self._ssn)    
        if debug:
            print(f"DEBUG: {funcn}", tgtFn.fp())
        return tgtFn

    def pmid2ii(self, dtsRng, mkt=None, secFilter='active', debug=False):
        (begDt, endDt) = dtsRng
        iid = {}
        #print(f'pmid2ii self.iis({secFilter}) {self.iis(secFilter).keys()}', file=sys.stderr)
        for pmid in self.iis(secFilter).keys():
            if pmid.find("3011") >= 0 and False:
                print(f"DEBUG: pmid2ii {secFilter} {pmid}", file=sys.stderr)
            ii = self.iis(secFilter)[pmid]
            if debug:
                print('ii_detail:', ii.__dict__, file=sys.stderr)
                print('ii:', ii, file=sys.stderr)

            #print(f'{pmid} done', file=sys.stderr)
            if mkt != None:
                if ii.isFuture():
                    if ii.getMetaId() != mkt:
                        continue
                elif ii.permid != mkt:
                    continue

            if not ii.hasDominantDates():
                if debug:
                    print(f'pmid2ii missing dominantDates:', ii)
                continue

            activeFlg = '---'
            dominFlg = '---'

            if ii.isStock():
                iid[pmid] = ii
            elif secFilter == 'cont':
                iid[pmid] = ii
            elif secFilter == 'active':
                if (ii.listed_date.replace('-','')>endDt) or (ii.de_listed_date.replace('-','')<begDt):
                    pass
                else:
                    iid[pmid] = ii
                    activeFlg = '***'
            elif secFilter == 'main':
                #print('main', ii, type(ii.get_domDt()), type(ii.get_dedomDt()), file=sys.stderr)
                if ii.get_domDt() != ii.get_domDt(): #nan
                    pass
                elif (ii.get_domDt()>dtparse(endDt).date() or ii.get_dedomDt()<dtparse(begDt).date()):
                    pass
                else:
                    #print('====', ii.get_dedomDt(), dtparse(begDt).date())
                    iid[pmid] = ii
                    dominFlg = '***'
            else:
                assert False, f'Invalid pmid2ii arguments'

            if debug:
                print(f'pmid2ii: {pmid}, dtsRng: {dtparse(begDt).date()},{dtparse(endDt).date()};')
                    # f'active: {ii.listed_date},{ii.de_listed_date}; {activeFlg}',
                    # f'dominant: {ii.get_domDt()},{ii.get_dedomDt()}; {dominFlg}')

        #print('DEBUG: iid=', iid)
        return iid

    def mkt2multi(self, to_dataframe=True):
        res = {}
        for pmid,ii in self.iis('active').items():
            res[ii.getMetaId()] = ii.factor

        if to_dataframe:
            multi = pd.DataFrame(list(res.items()), columns=['mkt','Multi'])            
            multi.set_index('mkt', inplace=True)
            return multi
        else:
            return res

    def mkt2main_ii(self, dtsRng, mkt=None, to_dataframe=True):
        res = defaultdict(set)
        for pmid,ii in self.pmid2ii(dtsRng, secFilter='main', mkt=mkt).items():
            #print(ii)
            res[ii.getMetaId()].add(ii)
        return(res)

    def mkt2main_pmid(self, dtsRng, mkt=None, to_dataframe=False):
        res = defaultdict(set)
        for mkt,iis in self.mkt2main_ii(dtsRng, mkt=mkt, to_dataframe=to_dataframe).items():
            for ii in iis:
                res[mkt].add(ii.permid)
            res[mkt] = sorted(list(res[mkt]))

        if to_dataframe:
            df = pd.DataFrame(list(res.items()), columns=['mkt','MainCtr'])            
            df.set_index('mkt', inplace=True)
            return df
        else:
            return res

    def mkt2main_name(self, dtsRng, mkt=None, to_dataframe=True):
        res = defaultdict(set)
        for mkt,iis in self.mkt2main_ii(dtsRng, mkt=mkt, to_dataframe=to_dataframe).items():
            for ii in iis:
                res[mkt].add(ii.lclName) 
            res[mkt] = sorted(list(res[mkt]))

        if to_dataframe:
            df = pd.DataFrame(list(res.items()), columns=['mkt','Name'])            
            df.set_index('mkt', inplace=True)
            return df
        else:
            return res

    def pmids(self, dtsRng, mkt, secFilter):
        rc = sorted(self.pmid2ii(dtsRng, mkt=mkt, secFilter=secFilter).keys())
        #print(f"DEBUG: Symset.pmids {rc}")
        return rc

    def __str__(self):
        return f"Symset({self._ssn}, {self._qdfRoot}, {self._begDt}, {self._endDt}, len={len(self._iis['main']) if 'main' in self._iis else 'un_init'}), type={self.secType}"


def gen_symset(ssn, qdfRoot, begDt, endDt, force=False):
    ss = SymsetII(ssn, qdfRoot, begDt, endDt, force=force)
    print(ss)

    for secFilter in ['cont', 'active', 'main']:
        for dt in ['fullhist', '20200104', '20200918', '20201103']:
            for mkt in ss.mkts():
                print('%-12s %-6s %6s %-6s'%(ss.name, secFilter, mkt, dt), 
                    ss.pmids((begDt, endDt), mkt, secFilter)[:3], 
                    '...', 
                    ss.pmids((begDt, endDt), mkt, secFilter)[-3:])
            print('\n')

    return(ss)

if __name__ == '__main__':
    parser = OptionParser(description="Symset()")

    parser.add_option("--cfg",
                      dest="cfg",
                      help="cfg (default: %default)",
                      metavar="cfg",
                      default="all")

    parser.add_option("--symset",
                      dest="symset",
                      help="symset (default: %default)",
                      metavar="symset",
                      default='CF_TEST01')    
            
    parser.add_option("--force",
					  dest="force",
					  type="int",
					  help="force (default: %default)",
					  metavar="force",
					  default=0)

    parser.add_option("--regtest",
					  dest="regtest",
					  type="int",
					  help="regtest (default: %default)",
					  metavar="regtest",
					  default=0)

    parser.add_option("--dryrun",
					  dest="dryrun",
					  type="int",
					  help="dryrun (default: %default)",
					  metavar="dryrun",
					  default=0)

    parser.add_option("--asofdate",
                      dest="asofdate",
                      help="asofdate (default: %default)",
                      metavar="asofdate",
                      default="download")
    options, _ = parser.parse_args()

    ssnList = []
    if options.symset != '':
        ssnList = [options.symset]
    else:
        ssnList = get_ssns('CF')[:]

    #print(QDFile(fp=f"{rootuser()}/che/data_rq.20100101_uptodate/CF/SN_CF_DNShort/prod_20200901_20210127"))
    #exit(0)

    for ssn in ssnList:
        ss = gen_symset(ssn, QDFile(fp=f"{dd('usr')}/CF/SN_CF_DNShort/prod_20200901_20210127"), '20200901', '20210127', options.force)
        print('1-day', ss.mkt2main_pmid(('20210127', '20210127')))



