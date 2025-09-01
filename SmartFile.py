#!/home/shuser/anaconda3/bin/python

import sys

import os
import pickle
import re
import glob
import pandas as pd
from optparse import OptionParser
# from common import *

from common_basic import *
import QpsUtil

MetaData = type("MetaData", (), {"type":None, "columns":None, "keys":None, "element_metadata":None})

def set_properties(instance, data):
    instance.type = type(data)
    instance.columns = data.columns if hasattr(data, "columns") else None
    instance.keys = list(data.keys()) if hasattr(data, "keys") else None
    instance.element_metadata = MetaData()

class BaseSmartFile():
    def __init__(self, fn=''):
        self._fn = fn
        self._fnNew = fn
        self._data = None

    def fn(self):
        return(self._fn)

    @property
    def path(self):
        return(self._fn)

    @path.setter
    def path(self, v):
        self._fn = v

    def delete(self):
        pass
        if os.path.exists(self.fn()):
            os.remove(self.fn())

    @property
    def data(self, debug=False):
        if not self._data:
            if debug:
                print(f"INFO: BaseSmartFile load {self.path}")
            self._data = QpsUtil.smart_load(self.path, title='SmartFile')
        return self._data

    @data.setter
    def data(self, v):
        self._data = v

    def dump(self, data=None):
        if data is None:
            QpsUtil.smart_dump(self._data, self.path)
        else:
            QpsUtil.smart_dump(data, self.path)

    def exists(self):
        return os.path.exists(self._fn)

class SmartFile(BaseSmartFile):
    def __init__(self, fn, force_meta=False):
        super().__init__(fn)
        #self._fn = fn
        self._fn_metadata = f"{fn}.metadata"

        self._metadata = MetaData()
        self.init(force_meta)

    def init(self, force_meta):
        assert os.path.exists(self.path), f"No such file: {self.path}"

        if os.path.exists(self._fn_metadata) and not force_meta:
            self._metadata = pickle.load(open(self._fn_metadata, "rb"))
        else:
            set_properties(self._metadata, self.data)

            first_value = list(self.data.values())[0]
            if type(self.data) == type({}) and (type(first_value) == type(pd.Series(dtype='float64')) or type(first_value) == type(pd.DataFrame())):
                set_properties(self._metadata.element_metadata, first_value)

            pickle.dump(self._metadata, open(self._fn_metadata, "wb"))
    
    @property
    def type(self):
        return self._metadata.type

    @property
    def columns(self):
        return self._metadata.columns

    @property
    def keys(self):
        return self._metadata.keys



    @property
    def element_metadata(self): #used when data is a dictionary of dataframe
        return self._metadata.element_metadata


if __name__ == '__main__':
    parser = OptionParser(description="SmartFile")

    parser.add_option("--regtests",
                    dest="regtests",
                    type="int",
                    help="regtests (default: %default)",
                    metavar="regtests",
                    default=1)

    parser.add_option("--delete",
                    dest="delete",
                    type="int",
                    help="delete (default: %default)",
                    metavar="delete",
                    default=0)

    parser.add_option("--debug",
                    dest="debug",
                    type="int",
                    help="debug (default: %default)",
                    metavar="debug",
                    default=0)

    parser.add_option("--asofdate",
                    dest="asofdate",
                    help="asofdate (default: %default)",
                    metavar="asofdate",
                    default="download")

    opt, args = parser.parse_args()


    if opt.regtests:
        sf = SmartFile(f"{dd('raw')}/E47/ohlcv_1d_pre.db", True)
        print(sf.data)
        print(sf.type)
        print(sf.keys)
        print(sf.columns)
        print(sf.element_metadata.columns)
        print(sf.element_metadata.type)
        print(sf.element_metadata.keys)


        sfA = BaseSmartFile()
        sfB = BaseSmartFile()
        sfC = BaseSmartFile()

        try:
            use_if_exists(sfA, "/qpsuser.local/foobar.txt", force=False)
            sfA.data = "A"
            print(f"DOING {sf.path}")
            
        except Exception as e:
            print (e)
            
        try:
            use_if_exists(sfB, f"{sfA.path}.ext0")
            sfB.data = "B"
            print(f"DOING {sfB.path}")
            sfB.dump()
        except Exception as e:
            print (e)
            
        try:
            use_if_exists(sfC, f"{sf.fn()}.ext1")
            sfC.data = "C"
            print(f"DOING {sfC.path}")
            sfC.dump()
        except Exception as e:
            print (e)
            
    else:
        reToDelete = []
        reToDelete.append(re.compile(r'/qps(data|user)/temp/.*status$'))
        reToDelete.append(re.compile(r'/qps(data|user)/temp/.*bash$'))
        reToDelete.append(re.compile(r'/qps(data|user)/.*lock$'))
        showEvery = 100000
        
        dt = "20211115"
        dtaDir = "/Fairedge_dev/app_QpsData/temp"

        for listFn in (glob.glob(f"{dtaDir}/qpsdata.{dt}.txt")):
            fnDictIn  =  {}
            fnDictOut  =  {}
            fnDictDel = {}

            pklFn =  listFn.replace('.txt', '.pkl')
            if os.path.exists(pklFn):
                print(f'INFO: reading {pklFn}')
                fnDictIn = pickle.loads(Path(pklFn).read_bytes())
            else:
                print(f'INFO: reading {listFn}')
                for ln in open(listFn, 'r').readlines():
                    if ln.find('mtime')<0:
                        print(f"DEBUG: badline {ln}")
                    else:
                        fn = ln.split()[1].split("'")[1]
                        fnDictIn[fn] = 1

            
            print(f"INFO: checking {listFn}, {len(fnDictIn)} files")

            cntChk = 0
            cntDel = 0
        
            for fn in fnDictIn.keys():
                if os.path.exists(fn):
                    fnDictOut[fn] = 1
                    cntChk += 1
                else:
                    continue
        
                if cntChk%showEvery == 0:
                    print(f'INFO: deleted {cntDel} files, chked {cntChk} files')

                for reDel in reToDelete:
                    if reDel.match(fn):
                        cntDel += 1
                        if opt.delete:
                            if opt.debug:
                                print('INFO: deleting ', fn)

                            if cntDel%showEvery == 0:
                                print(f'INFO: deleted {cntDel} files, chked {cntChk} files')

                            BaseSmartFile(fn).delete()
                            del fnDictOut[fn]
                        else:
                            if opt.debug:
                                print('INFO: to-delete', fn)
                            fnDictDel[fn] = 1
                            if cntDel%showEvery == 0:
                                print(f'INFO: to-delete {cntDel} files, chked {cntChk} files')
                        break
                            
                
            pickle.dump(fnDictOut, open(pklFn, 'wb'))
            pickle.dump(fnDictDel, open(pklFn.replace('pkl', 'del.pkl'), 'wb'))
        


