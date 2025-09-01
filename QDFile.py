import sys

import re,math,os
import pickle
from copy import deepcopy
import rqdatac as rc
import pandas as pd
from matplotlib import pyplot
import glob
import QpsSys
from QpsDate import getDatesCfg, getPrevBusinessDay
from filelock import Timeout, FileLock
import logging
from shared_cmn import *
from dateutil.parser import parse as dtparse
from QpsUtil import buf2md5
from QpsUtil import mkdir
from common_basic import *
# from FldGraph import GraphManager
# from QDData import MemoryFiles

logging.getLogger('filelock').setLevel(logging.ERROR)

#from getSymset import *

class QDFile:
    CFG_DEFAULT=None

    def __init__(self, rootDir=f'{rootdata()}/raw/data_rq', begDt='2010-01-01', endDt='2020-09-29', svrMode=False, fp: str = ''):
        # if endDt == 'uptodate':
        #     rootDir = rootDir.replace('raw/data_rq', 'raw/lnk_data_rq')  #Make it different so we do not accidentally the 'uptodate' version
        #     #print(f'INFO: QDFile rootDir overwrite {rootDir}')
        if fp:
            self.rootDir, remains =  fp.split(r'rq.')
            self.rootDir = f"{self.rootDir}rq"
            #print(f"DEBUG_3450: {fp}, {self.rootDir}", file=sys.stderr)
            self.begDt, self.endDt = remains.split(r'/')[0].split(r'_')
            self.labels = []
            self.label('/'.join(remains.split(r'/')[1:]))
            self.rootDir = f"{self.rootDir}.{self.begDt}_{self.endDt}"
            #print(f"DEBUG: QDFile rootDir= {self.rootDir}")
        else:
            self.begDt=begDt.replace('-','')
            self.endDt=endDt.replace('-','')
            self.rootDir = f"{rootDir}.{self.begDt}_{self.endDt}"
            self.labels = []
        self.svrMode=svrMode

    @property
    def remotely(self) -> bool:
        return False
        # return self.fp().startswith(f'{rootdata()}/')

    @property
    def exch(self) -> str:
        ssn = self.labels[0]
        if ssn.find('CF')>=0:
            return 'CF'
        else:
            return 'CS'

    @property
    def svr_file(self) -> bool:
        return self.rootDir.find('qpsdata')>=0

    @property
    def cli_file(self) -> bool:
        return self.rootDir.find('qpsdata')<0    
    
    #Typical path example: f"{rootuser()}/che/data_rq.20100101_uptodate/CF/SN_CF_DNShort/regtest_20201201_20210129/5m/Pred_ohlcv_pre_5m_1/NI.db.txt"
    @property
    def varName(self):
        return(self.fp().split(r'/')[-2])

    @property
    def bar(self):
        return(self.fp().split(r'/')[-3])

    @property
    def endDtAct(self):
        return int(self.fp().split(r'/')[-4].split(r'_')[-1])

    # @property
    # def begDt(self):
    #     return int(self.fp().split(r'/')[-4].split(r'_')[-2])

    @property
    def dts_cfg(self):
        return int(self.fp().split(r'/')[-4].split(r'_')[0])

    @property
    def ssn(self):
        return int(self.fp().split(r'/')[-5])

    @property
    def secType(self):
        return int(self.fp().split(r'/')[-6])
    
    @property
    def dataType(self):
        return self.fp().split(r'/')[-1].split(r'.')[-1]

    def fp(self, lockfile=False, txt=False):
        if self.svr_file:
            v = f'{self.rootDir}/%s'%('/'.join(self.labels))
        else:
            if len(self.labels)>1:
                v = f'{self.rootDir}/%s/%s'%(self.exch, '/'.join(self.labels[1:]))
            else:
                v = f'{self.rootDir}'
        if txt:
            v = v.replace('.dbx', '.txt')
            v = v.replace('.db', '.txt')

        if v.find('None')>=0:
            raise Exception('QDFile symset None')

        if lockfile == True:
            md5 = buf2md5(v)
            vDir = f"{rootdata()}/flocks/{md5[:2]}"
            if not os.path.exists(vDir):
                os.mkdir(vDir)
            v = f"{vDir}/{buf2md5(v)}"
            #v = v.replace(self.labels[-1], f'.{self.labels[-1]}.lock')
        return(v)

    def fp_short(self, txt=False):
        # fp = self.fp(txt)
        # return '/'.join(fp.split(r'/')[4:])
        
        #Disabled. Partial path is hard to use as cut/paste can not open the file
        return self.fp(txt)

    def fdir(self):
        return '/'.join(self.fp().split(r'/')[:-1])

    def fdir_short(self):
        fdir = self.fdir()
        return '/'.join(fdir.split(r'/')[4:])

    def switchUser(self, curUser, newUser):
        newQdf = deepcopy(self)
        newQdf.rootDir = newQdf.rootDir.replace(curUser, newUser)
        return newQdf

    def switchToQSF(self, ssn, debug=False):
        newQdf = deepcopy(self)
        newQdf.svrMode=True
        newQdf.rootDir = f"{rootdata()}/%s"%('/'.join((newQdf.rootDir.split(r'/')[3:])))
        #print(newQdf.rootDir, self.labels)
        newQdf.labels = [(x if x not in ['CF', 'CS', ''] else ssn) for x in self.labels]
        if False or debug:
            print(f'switchToQSF(): {self.fp()} => {newQdf.fp()}')
        return newQdf

    def __str__(self):
        return(self.fp())

    def __repr__(self):
        return(self.fp())

    def expandMacro(self, macros):
        for k,v in macros.items():
            self.labels = [s.replace(k,v) for s in self.labels]
            #qps_print(self.labels)

    def ls(self, macroName, values, debug=False):
        qdfs = []
        macroName += '.db'
        for v in values:

            v += '.db'
            qdf = deepcopy(self)
            if debug:
                qps_print('ls(pre )+++', qdf)
            qdf.expandMacro({macroName:v})
            qdfs.append(qdf)
            if debug:
                qps_print('ls(post)+++', qdf, v)
        return(qdfs)

    def range(self, batchNum=10):
        res = []
        for i in range(batchNum):
            macros={'BATCHNO':'%d'%(i)}
            #qps_print('macros %s'%(macros))
            qdf = deepcopy(self)
            qdf.expandMacro(macros)
            res.append(qdf)
        return(res)

    def _begLabel(self, v): #deprecated
        self.labels = []
        return(self.label(v))

    def label(self,v):
        if len(self.labels)>0:
            if self.labels[-1].find('.db')>=0 or self.labels[-1].find('.txt')>=0:
                #Remove the ending filename if already assigned
                self.labels = self.labels[:-1]
        #self.labels.append(v)
        self.labels.extend(v.split(r'/'))
        return(self)

    def __add__(self, v):
        new = deepcopy(self)
        new.label(v)
        return new

    def _load(self, uselock=False, debug = False):
        debug = False
        pld = None
        try:
            if uselock and self.fp().find("futures/ohlcv_1d_pre.db")<0:
                if debug:
                    print(f"DEBUG: _load locking {self.fp()}", file=sys.stderr)
                sys.stderr.flush()
                with FileLock(f'{self.fp(lockfile=True)}', timeout=30):
                    #pld = pickle.load(open(self.fp(), 'rb'))
                    pld = smart_load(self.fp(), debug=debug, title="QDFile._load(lock)")
            else:
                #pld = pickle.load(open(self.fp(), 'rb'))
                pld = smart_load(self.fp(), debug=debug, title="QDFile._load(nolock)")

        except EOFError as e:
            print(f'EOFError for {self.fp()}, {e}', file=sys.stderr)
            import traceback
            traceback.print_stack()

        return(pld)

    def _dump(self, pld, uselock=False, debug=False, verbose=False):
        funcn = "QDFile._dump"
        if uselock:
            if debug: 
                print(f"DEBUG: _dump locking {self.fp()}", file=sys.stderr)
            sys.stderr.flush()
            with FileLock(f'{self.fp(lockfile=True)}', timeout=30):
                smart_dump(pld, self.fp(), verbose=verbose, title=funcn) 
        else:
            smart_dump(pld, self.fp(), verbose=verbose, title=funcn) 
        if debug:
            print(f"DEBUG_8711: {funcn} {self.fp()}")

    def dump(self, pld, debug=False, dump_txt=False, verbose=1):
        funcn = "QDFile.dump"
        if not self.svrMode and self.fp().find('qpsdata/raw/data_rq')>=0:
            qps_print(f'WARNING: only can save to server in server mode {self.fp()}', always=True)
            # import traceback
            # traceback.print_stack()
            return(self)
        #qps_print('/'.join(self.fp().split(r'/')[:-1]))
        #QpsSys.mkdir(['/'.join(self.fp().split(r'/')[:-1])])
        mkdir([self.fdir()])

        if False or debug:
            print(f'DEBUG: {funcn} {self.fp()}', file=sys.stderr)
            
        self._dump(pld, verbose=verbose, debug=debug)

        if dump_txt and (self.fp().find('regtest_')>=0 or self.fp().find('prod_')>=0) and (self.fp().find('CF_TEST01')>=0):
            txtFn = self.fp() + '.txt'
            print('>'*50, 'dump_txt', txtFn, self.begDt, self.endDtAct, self.dataType, self.bar, file=sys.stderr)
            # print_df(df_filter_by_dates(pld, self.begDt, self.endDtAct, data_type=self.dataType, bar=self.bar), 
            #     rows=1000000, show_head=False, show_body=False, file=open(txtFn, 'w'))
            print_df(pld, rows=1000000, show_head=False, show_body=False, file=open(txtFn, 'w'))

        if debug:
            qps_print(f'INFO: QDFile.dump %-128s %12d bytes'%(self.fp(), os.path.getsize(self.fp())), file=sys.stderr)

        return(self)

    def load(self, debug=False):
        from FldGraph import GraphManager
        from QDData import MemoryFiles
        if GraphManager.memory_mode:
            pld = MemoryFiles.load(self.fp())
            if pld is not None:
                return pld

        pld = None
        if os.path.exists(self.fp()):
            if os.path.getsize(self.fp())<=0:
                print(f'WARNING: QDFile.load zero file size {self.fp()} {os.path.getsize(self.fp())}')
            else:
                if debug:
                    print(f'INFO: QDFile.load {self.fp()} {os.path.getsize(self.fp())}')
                pld = self._load(debug=debug)
        else:
            if True or debug:
                qps_print(f'QDFile.load cannot find {self.fp()}')

        if GraphManager.memory_mode:
            MemoryFiles.dump(self.fp(), pld)
        return pld

    def open(self, mode, txt=False):
        if not os.path.exists(os.path.dirname(self.fp(txt))):
            os.makedirs(os.path.dirname(self.fp(txt)))
        print(f'QDFile.open {self.fp(txt)}')
        return(open(self.fp(txt), mode))

    def cat(self, txt=True):
        return(''.join(open(self.fp(txt), 'r').readlines()))

    def exists(self, chk_all_cfgs = False):
        rc = os.path.exists(self.fp())
        fp = self.fp()
        if rc == False and chk_all_cfgs == True:
            globStr = '/'.join([x if x.find('data_rq')<0 else 'data_rq.*_20*' for x in self.fp().split(r'/')])
            if False:
                print(f"DEBUG: QDFile.exists {globStr}")
            matchedFiles = glob.glob(globStr)
            if len(matchedFiles)>0:
                if ctx_verbose(1):
                    qps_print(f'chk_all_cfgs found existing {matchedFiles}')
                rc = True
                fp = matchedFiles[-1]
            else:
                rc = False

        if chk_all_cfgs:
            return (rc, fp)
        else:
            return (rc, self.fp())
            #return(rc)  #TODO: Need to fix this different return type issue, backward comp


def list_cmds():
    pass

if __name__ == '__main__':
    from qdb_options import get_options_jobgraph
    (opt, args) = get_options_jobgraph(list_cmds)
    if opt.debug:
        pld  = {'foo': 'bar'}
        qf = QDFile(svrMode=True).label('I65').label('test.db')
        qps_print(qf.fp())
        qf.dump(pld)
        pld = qf.load()
        qps_print(pld)

        qdfTmpl = QDFile(svrMode=True).label('foo').label('bar').label('test_PMID.db')
        qdfs = qdfTmpl.ls('PMID', ['A', 'ZC'])
        qps_print(qdfs)
        for qdf in qdfs:
            qps_print(qdf.fp())
            qps_print(qdf.fp(lockfile=True))

    if opt.regtest:
        print(QDFile(fp=f"{dd('raw')}"))

    exit(0)


