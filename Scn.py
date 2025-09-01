import sys

#from pathlib import Path
#from typing import Optional, List, Set, Iterable, Union
from collections import defaultdict, deque
from dateutil.parser import parse as dtparse
from QpsDate import getDatesCfg
# import QpsUtil
from QDF import config_qdf, QSF, QCF
from QDFile import QDFile
from ExchSessions import ExchSessions      
from SymsetII import SymsetII
from qdb_options import get_options_jobgraph
# from getSymset import get_ssns
from shared_cmn import *
from common_basic import *
from common_colors import *
from common_ctx import *

def get_dts_cfg_data(opt, ssn, dcsnDt=None): #dts config is symset-dependent
    dtaBegDt = QCF().begDt
    sectype = 'CS'
    if ssn.find("CF_")>=0: 
        sectype = 'CF'
        #dtaBegDt = '20150101'
    dtaBegDt = getDatesCfg('dbegdate', cfg_name='download', sectype=sectype, verbose=True)
    researchEndDt = getDatesCfg('denddate', cfg_name='download', sectype=sectype, verbose=True)
    #else:
        #researchEndDt = '20201130'
        #researchEndDt = '20201201'
        #researchEndDt = '20201202'


    #Begin/End dates are different for different symset
    if dcsnDt is None:
        dcsnDt = getDatesCfg('dcsndate', cfg_name=opt.asofdate, verbose=True)

    #print(f"DEBUG: dcsnDt {dcsnDt}")
    dts_cfg_data = {}
    
    dts_cfg_data['fullhist'] = (QCF().begDt, dcsnDt)

    dts_cfg_data['rschhist'] = (dtaBegDt, researchEndDt)
    dts_cfg_data['train'] = (dtaBegDt, '20181231')
    dts_cfg_data['validate'] = ('20190101', '20191231')
    dts_cfg_data['paper'] = ('20200101', researchEndDt)

    dts_cfg_data['regtest'] = ('20201201', '20210129')
    
    dts_cfg_data['prod'] = ('20200901', getDatesCfg('dcsndate', cfg_name='20211115', verbose=opt.verbose))
    #dts_cfg_data['prod'] = (researchEndDt, dcsnDt)

    #dts_cfg_data['prod1w'] = ('20210323', dcsnDt)
    dts_cfg_data['prod1w'] = ('20210810', dcsnDt)
    dts_cfg_data['dcsndate'] = (dcsnDt, dcsnDt)
    return(dts_cfg_data)

def dts_cfg_names(opt=None):
    return ['rschhist', 'prod', 'prod1w', 'dcsndate', 'paper', 'validate', 'train', 'fullhist', 'regtest']

def asofdate4dts_cfg(dts_cfg):
    if dts_cfg == 'prod1w':
        return 'download'
    elif dts_cfg == 'prod':
        return '20211115'
    else:
        assert False, f"ERROR: invalid dts_cfg {dts_cfg}"
    

def session_names(opt):
    if ssn.find("CF_")>=0:
        return ['SN_CF_DNShort']
        #return ['SN_CF_DAY', 'SN_CF_DNShort']
    else:
        return ['SN_CS_DAY']

class Scn:
    def __init__(self, opt, ssn, dts_cfg, snn, asofdate=None, force=False, instru_force=False, dcsnDt=None, debug=False, toplevel=True):
        #print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", dts_cfg)
        if toplevel:
            setDataEnv(dts_cfg)
        self._opt = opt
        self._ssn = ssn #Symset Name
        self._snn = snn #Exch session name
        self._sectype = 'CS'
        if self._ssn.find("CF_")>=0:
            self._sectype = 'CF'
            if self._snn.find('CS_'):
                self._snn = self._snn.replace('CS_', 'CF_')
        dts_cfg = cc(dts_cfg)
        self._dts_cfg = dts_cfg
        if self._dts_cfg in ['uptodate', 'download'] or self._dts_cfg.find("202") >= 0:
            self._dts_cfg = 'prod1w'
        self._sesn = ExchSessions(name=snn)
        #self._dates = {}

        (QDF, DataCfg) = config_qdf(opt.qdb_cfg)

        if asofdate is None:
            asofdate = opt.asofdate

        (self._begTm, self._endTm) = ('21:00:00', '15:00:00')
        self._begDt = getDatesCfg('dbegdate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate)
        self._endDt = getDatesCfg('denddate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate)
        self._dcsnDt = getDatesCfg('dcsndate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate)
        self._tradeDt = getDatesCfg('tradedate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate)  
        self._nextDt = getDatesCfg('nextdate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate) 
        self._prevDt = getDatesCfg('prevdate', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate) 
        self._pre2Dt = getDatesCfg('pre2date', cfg_name=dts_cfg, sectype=self._sectype, verbose=opt.verbose, asofdate=asofdate)


        if dts_cfg == 'T':
            self._endDt = self._tradeDt        

        if hasattr(opt, 'model') and opt.model.find('Ngt')>=0:
            self._runDt = self._dcsnDt #QpsUtil.now_date()
        else:
            self._runDt = self._tradeDt

        #self._qdfRoot = QCF() + f'{ssn}' + f'{snn}' + f'{self._dts_cfg}_{self._begDt}_{self._endDt}'
        self._qdfRoot = QDFile(fp=f"{dd('usr', dataEnv=dts_cfg)}/{ssn}") + f'{snn}' + f'{self._dts_cfg}_{self._begDt}_{self._endDt}'
        if debug:
            print("DEBUG: Scn.init", self._qdfRoot, ssn)

        self._ss = SymsetII(ssn, self._qdfRoot, self._begDt, self._endDt, force=force, instru_force=instru_force)
        self._dtaLeadDays = 126

        if opt.debug:
            print("Scn.init", self, file=sys.stdout)

        if toplevel:
            ctx_set_scn(self)

        self._prevScn = None
    
    def set_prev_scn(self, dts_cfg):
        #only W/T does not contain long enough hist, we need to append to prev scn data
        self._prevScn = Scn(self._opt, self._ssn, dts_cfg, self._snn, toplevel=False)
        print(f"INFO: set_prev_scn {dts_cfg}")
        #config_print_df()
        #print(self._prevScn)

    def get_prev_scn(self):
        if ctx_debug(1):
            config_print_df()
            if self._prevScn is not None:
                print("DEBUG_9345: get_prev_scn", self._prevScn)        
        return self._prevScn

    def __str__(self):
        df = pd.DataFrame.from_dict(self.as_dict(), orient='index', columns=['SCN'])
        return f"{CYAN}{df}{NC}"
        #return f"{CYAN}{self.as_dict()}{NC}"
        #return f"{df.T}"
        
    def as_dict(self):
        dta =  {
            "ssn": self._ssn,
            "snn": self._snn,
            "dts_cfg": self.dts_cfg,
            "sectype": self.sectype,
            "begDt": self.begDt,
            "endDt": self.endDt,
            "dcsnDt": self.dcsnDt, 
            "trdDt": self.trdDt,
            "runDt": self.runDt,
            "dtaBegDtm": self.dtaBegDtm,
            "dtaEndDtm": self.dtaEndDtm,
            "qdfRoot": self.qdfRoot
            }
        dta.update(getDataEnv())
        dta['md5'] = buf2md5(f"{dta}")[-4:]
        return dta
    
    def md5(self):
        return self.as_dict()['md5']
    
    def sid(self): #scn id
        d = self.as_dict()
        return f"{d['ssn']}.{d['dts_cfg'].replace('prod1w', 'W')}.{d['md5']}"


    def init(self):
        self._ss.init()
        return(self)
        
    @property
    def opt(self):
        return self._opt

    @property
    def ss(self):
        return self._ss

    @property
    def snn(self):
        return self._snn

    @property
    def dts_cfg(self):
        return self._dts_cfg

    @property
    def sectype(self):
        return self._sectype

    @property
    def dts_cfg_expanded(self):
        return f"{self.dts_cfg}_{self._begDt}_{self._endDt}"

    # @property 
    # def dts_cfg_data(self):
    #     return self._dts_cfg_data

    @property
    def symset(self):
        return self.ss._ssn

    # @property
    # def svr_ssn(self): #backward issues. data is stored differently on server, v.s. client side
    #     if self.ssn.find('CF_')>=0:
    #         return 'futures'
    #     else:
    #         return self.ssn

    @property
    def sesn(self):
        return self._sesn

    @property
    def qdfRoot(self):
        return self._qdfRoot

    @property
    def begDt(self):
        return self._begDt

    @property
    def endDt(self):
        return self._endDt

    @property
    def dcsnDt(self):
        return self._dcsnDt

    @property
    def trdDt(self):
        return self._tradeDt

    @property
    def nextDt(self):
        return self._nextDt

    @property
    def prevDt(self):
        return self._prevDt
    
    @property
    def pre2Dt(self):
        return self._pre2Dt

    @property
    def runDt(self):
        return self._runDt

    @property
    def dtaBegDtm(self):
        return dtparse(f'{self.begDt} 00:00:00')

    @property
    def dtaBegDtmWithLead(self):
        return dtparse(f'{self.begDt} 00:00:00') - pd.Timedelta(self._dtaLeadDays, unit='day')

    @property
    def dtaEndDtm(self):
        # endDt = dtparse(f'{self.endDt} 23:59:59')
        # if endDt.weekday() == 4:
        #     endDt = endDt + pd.Timedelta(3, unit='day')
        # else:
        #     endDt = endDt + pd.Timedelta(1, unit='day') #Add one extra day to include current day for real trading
        endDt = dtparse(f'{self._tradeDt} 23:59:59')
        return endDt

    def dtsRng(self):
        return (self._begDt, self._endDt)

    def filter_dates(self, pld, withLead=True):
        begDtm = self.dtaBegDtmWithLead
        if not withLead:
            begDtm = self.dtaBegDtm
        endDtm = self.dtaEndDtm
        print('Scn filter_dates', type(pld), begDtm, endDtm, file=sys.stderr)
        keep = pld[(pld.index >= begDtm) & (pld.index <= endDtm)]
        pld.drop(index=pld.index[~pld.index.isin(keep.index)], inplace=True)
        return pld

    def filter_time(self, pld, begTm='9:00', endTm='23:00'):
        keep = pld.between_time(begTm, endTm)
        print('scn filter_time', type(pld), begTm, endTm)
        #print('before filter_time', pld.shape, keep.shape, type(keep), type(pld), file=sys.stderr)
        pld.drop(index=pld.index[~pld.index.isin(keep.index)], inplace=True)
        #print('after filter_time', pld.shape, keep.shape, type(pld), file=sys.stderr)
        return pld

    def pmid2ii(self, secFilter='active', debug=False):
        return self.ss.pmid2ii(self.dtsRng(), secFilter=secFilter, debug=debug)

    def id(self):
        return f'{self._ssn}_{self._begDt}_{self._endDt}'

if __name__ ==  '__main__':
    (opt, args) = get_options_jobgraph()
    opt.asofdate='download'
    print(f"INFO: asofdate= {opt.asofdate}", file=sys.stderr)

    scn = Scn(opt, 'CF_NS_LARGE', 'prod1w', 'SN_CF_DNShort', asofdate=opt.asofdate)
    for secFilter in ['active', 'main', 'cont']:
        print(f'--- {secFilter} ---')
        iids = scn.pmid2ii(secFilter=secFilter, debug=False)
        for pmid in sorted(iids.keys()):
            print(pmid, iids[pmid])

    
    for ssn in ['E47']:
        print('='*36)
        for dts_cfg in dts_cfg_names(opt)[:4]:
            scn = Scn(opt, ssn, dts_cfg, 'SN_CS_DAY', asofdate='download')
            print(scn)
    print('='*36)

    for dts_cfg in ['prod1w', 'dcsndate']:
        scn = Scn(opt, "CF_NS_NHQH_AGRI", dts_cfg, 'SN_CF_DNShort', asofdate=opt.asofdate)
        print(scn)
        for secFilter in ['active', 'main', 'cont']:
            mkt="A"
            iis = scn.ss.pmid2ii(scn.dtsRng(), mkt=mkt, secFilter=secFilter)
            pmids = sorted(list(iis.keys()))
            print(f"DAILY: pmids for {mkt}, dts_cfg= {dts_cfg}, dtsRng= {scn.dtsRng()}, secFilter= {secFilter}, pmids= {pmids}")




