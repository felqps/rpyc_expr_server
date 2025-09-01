#!/usr/bin/env python

import sys

from QDFile import *
from rq_init import *
from common_basic import *
from QpsDate import getDatesCfg

QDF_CFG_DEFAULT = "c"

def getCfg(_cfg, cfg, root_dir):
    if cfg in _cfg:
        return _cfg[cfg]
    else:
        if cfg.find('202')<0:
            cfg = getDatesCfg('dcsndate', cfg_name="prod1w", sectype='CS', verbose=False, asofdate="download") 
        return {
            'rootDir': root_dir,
            #'begDt': dtStr(getPrevBusinessDay(getPrevBusinessDay(cfg))), #get two business days ago
            'begDt': dtStr(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(cfg)))), # 3 days ago
            #'begDt': "20210801",
            #'begDt': dtStr(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(cfg))))), # 4 days ago
            #'begDt': dtStr(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(getPrevBusinessDay(cfg)))))), # 5 days ago
            #'begDt': '2021-02-10',
            'endDt': dtStr(cfg),
            #'begDt': getDatesCfg('prevdate'),
            #'endDt': getDatesCfg('tradedate'), 
            'tickBegIdx': 0,
            'tickEndIdx': -1
        }

#@deprecated
class QSF(QDFile):  # Qps Svr File
    ROOT_DIR = QSF_ROOT_DIR
    CFG_DEFAULT = QDF_CFG_DEFAULT
    _cfg = {}
    _cfg["all"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2010-01-01",
        "endDt": "uptodate",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["R"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2010-01-01",
        "endDt": "uptodate",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["W"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2021-08-10",
        "endDt": "uptodate",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["E"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2000-01-01",
        "endDt": "2011-01-01",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["F"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2010-01-01",
        "endDt": "2021-01-01",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["G"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2020-01-01",
        "endDt": "2022-01-01",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }
    _cfg["T"] = {
        "rootDir": QSF_ROOT_DIR,
        "begDt": "2021-08-10",
        "endDt": QpsUtil.gettoday(use_dash=True),
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }

    def __init__(self, cfg=None, svrMode=False):
        if cfg == None:
            cfg = QSF.CFG_DEFAULT
        QDFile.__init__(
            self,
            rootDir = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["rootDir"],
            begDt = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["begDt"],
            endDt = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["endDt"],
            svrMode=svrMode,
        )
        #print(f"DEBUG_2134: {self.fp()}", file=sys.stderr)

    def __str__(self):
        return f"QSF: {self.fp()}"

class QCF(QDFile):  # Qps Client File
    ROOT_DIR = QCF_ROOT_DIR
    CFG_DEFAULT = QDF_CFG_DEFAULT
    _cfg = {}
    _cfg["all"] = {
        "rootDir": QCF_ROOT_DIR,
        "begDt": "2010-01-01",
        "endDt": "uptodate",
        "tickBegIdx": -1,
        "tickEndIdx": -1
    }

    def __init__(self, cfg=None):
        if cfg == None:
            cfg = QCF.CFG_DEFAULT
        QDFile.__init__(
            self,
            rootDir = getCfg(QCF._cfg, cfg, QCF_ROOT_DIR)["rootDir"],
            begDt = getCfg(QCF._cfg, cfg, QCF_ROOT_DIR)["begDt"],
            endDt = getCfg(QCF._cfg, cfg, QCF_ROOT_DIR)["endDt"],
            svrMode=False,
        )
        #print(f"DEBUG_2137: {self.fp()}", file=sys.stderr)
    def __str__(self):
        return f"QCF: {self.fp()}"

class QpsDataCfg:
    def __init__(self, cfg=QSF.CFG_DEFAULT):
        self.begDt = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["begDt"]
        #self.begDt = '2021-02-09'
        #self.begDt = '2021-04-29'
        self.endDt = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["endDt"]
        self.tickBegIdx = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["tickBegIdx"]
        self.tickEndIdx = getCfg(QSF._cfg, cfg, QSF_ROOT_DIR)["tickEndIdx"]
        self.qryBegDt = self.begDt
        self.qryEndDt = self.endDt
        if self.qryEndDt == "uptodate":
            self.qryEndDt = getDatesCfg('dcsndate', cfg_name='prod1w', sectype="CS", verbose=False, asofdate="download")

        self.symset = "None"

    def __expr__(self):
        return f"QpsDataCfg: begDt={self.begDt}, endDt={self.endDt}({self.qryEndDt}), tickBegIdx={self.tickBegIdx}, tickEndIdx={self.tickEndIdx}"

    def __str__(self):
        return self.__expr__()


# global DataCfg
# DataCfg = QpsDataCfg()

# def QDF(symset, label):
#     return QSF().label(symset).label(label)

def config_qdf(cfg):
    QDFile.CFG_DEFAULT=cfg
    QSF.CFG_DEFAULT=cfg
    QCF.CFG_DEFAULT=cfg
    def QDF(symset, label):
        f =  QSF(svrMode=True).label(symset).label(label) 
        #print(f"DEBUG_2134: {f.fp()}", file=sys.stderr)
        return f
    DataCfg = QpsDataCfg(cfg)
    return(QDF, DataCfg)


if __name__ == "__main__":
    for cfg in ["all", "20220317", 'E', 'F', 'G']:
        (QDF, DataCfg) = config_qdf(cfg)
        print(f"DataCfg('{cfg:<8}')", DataCfg)

    # qps_print(DataCfg, QCF('all').fp())

    # (QDF, DataCfg) = config_qdf('b')
    # qps_print(DataCfg, QCF('b').fp())

    # f = QDF('futures', '%s/%s/%s.%s.db'%('tick', '20201104', 'AG2101', 'pre'))
    # qps_print(f'{f} exists(chk_all_cfgs=False)= {f.exists(chk_all_cfgs=False)}')
    # qps_print(f'{f} exists(chk_all_cfgs=False)= {f.exists(chk_all_cfgs=True)}')


