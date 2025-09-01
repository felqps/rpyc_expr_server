import traceback
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
from typing import Optional, List, Set, Iterable
from collections import defaultdict, deque
from QpsDatetime import *
from multiprocessing import Manager

from QDFile import *
from QDF import *
from filelock import Timeout, FileLock
from QDNatsClient import QDNatsClient
from QDSettings import DEFAULT_TIMEOUT, DEFAULT_NATS_URL
from getSymset import *
from shared_cmn import *
from Ops import *
from CTX import *

class MemoryFiles:
    files = {}

    @classmethod
    def clean(cls):
        cls.files.clear()

    @classmethod
    def load(cls, path: str, debug=False):
        if path.find('NI')>=0 and debug:
            print(f"MemoryFiles load '{path}' {hex(id(cls.files.get(path)))}", file=sys.stderr)
            if type(cls.files.get(path)) == type(dict()):
                for k,v in cls.files.get(path).items():
                    print(f'\t{k}, {hex(id(v))}', file=sys.stderr)
        return cls.files.get(path)

    @classmethod
    def dump(cls, path: str, content, debug=False):
        if path.find('NI')>=0 and debug:
            print(f"MemoryFiles dump '{path}' {hex(id(cls.files.get(path)))}", file=sys.stderr)
        cls.files[path] = content
    
    @classmethod
    def get_path(cls, pmid: str, bar:str):
        for k in cls.files:
            if os.path.basename(k) == f"{pmid}.db" and k.find(bar)>=0:
            # if k.endswith(f"{pmid2mkt_rq(pmid)}.db") and k.find(bar)>=0:
                return k
        return None

class Step:  # can be interpreted as  operation-step or operation
    def __init__(self, func, pmids, inFn: QDFile = None, saveas=None, name: str = None, group: float=None):
        self._func = func
        self._pmids = pmids
        self._inFn = inFn
        self._saveas = saveas
        self.group: float = group or 0
        self.name = name if name else 'None'

    def __repr__(self):
        return f'<Step:{self.name}>'

    @property
    def input_path(self) -> Optional[str]:
        if self._inFn is None:
            return None
        return self._inFn.fp()

    @property
    def remotely(self) -> bool:
        return self._inFn and self._inFn.remotely

    def execute(self, ):
        return self._execute(self._func, self._pmids, inFn=self._inFn, saveas=self._saveas)

    def _execute(self, func, pmids, inFn=None, saveas=None):
        pld = None
        
        debug = True
        name = 'NA'

        #print(f"_execute {func}")
        if type(func) == type(dict()):
            if 'debug' in func:
                debug = func['debug']
            name = func['name']
            func = func['func']
        elif type(func) == type(Ops()):
            debug = func.debug
            name = func.varName
            func = func.func


        if ctx_debug(5):
            qps_print(f"-----step------ name: {name}, inFn: {inFn}, pmids: {('%s' % (pmids))[:30]} ...")

        from FldGraph import GraphManager
        if inFn != None:
            if GraphManager.memory_mode and MemoryFiles.load(inFn.fp()):
                pld = MemoryFiles.load(inFn.fp())
            elif os.path.exists(inFn.fp()) and os.path.getsize(inFn.fp())>0:
                pld = inFn._load()
                # if inFn.fp().find("futures/ohlcv_1d_pre.db")<0: #only lock if mkt-specific files
                #     with FileLock(f'{inFn.fp(lockfile=True)}', timeout=30):
                #         pld = inFn.load(debug)
                # else:
                #     pld = inFn.load(debug)
                    # if GraphManager.memory_mode:
                    #     MemoryFiles.dump(inFn.fp(), pld)
        kwargs = {}
        if 'debug' not in kwargs:
            kwargs['debug'] = debug 
        #qps_print(f'_execute kwargs= {kwargs}')

        #res = None
        #if pld is not None: #  and len(pld.keys())>0:
        res = func(pmids, pld, **kwargs)
        return saveas, res


class Steps:
    def __init__(self, steps: List[Step], outFn: QDFile):
        self.steps = steps
        self.outFn = outFn

    @property
    def remotely(self):
        return self.steps and self.steps[0].remotely

    def run(self):
        saveas, payload = self.execute()
        if 0:
            out_path = Path(self.outFn.fp())
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # During calculation, each step as data to the payload with key 'saveas'.
            # When writing results out, do not keep the saveas key.
            # This is consistent with existing data files. For example, industries can be save as a list directly,
            # not as dict with a list value and 'indu' key.
            out_path.write_bytes(pickle.dumps(payload[saveas]))
            # if saveas:
            #     out_path.write_bytes(pickle.dumps(payload[saveas]))
            # else:
            #     out_path.write_bytes(pickle.dumps(payload))
        elif self.steps:
            from FldGraph import GraphManager
            if GraphManager.memory_mode:
                MemoryFiles.dump(self.outFn.fp(), payload[saveas])
            else:
                self.outFn.dump(payload[saveas], dump_txt=True)
        return payload

    def execute(self):
        payload = defaultdict(dict)
        # data generated at each step will be accumlated, not overwritten
        saveas = None
        for step in self.steps:
            saveas, res = step.execute()
            if not isinstance(res, dict):
                payload[saveas] = res
            else:
                payload[saveas].update(res)
        return saveas, payload


class QDData:
    _nats_client: Optional[QDNatsClient] = None

    def __init__(self, *, nats_url: str = DEFAULT_NATS_URL, timeout: int = DEFAULT_TIMEOUT):
        # if not self._nats_client:
        #     self._nats_client = QDNatsClient(nats_url=nats_url, timeout=timeout)
        self.pld = defaultdict(dict)
        self.steps = []

    def step(self, func, pmids, inFn=None, saveas=None, name: str = None, group: float = 0):
        if 0 and inFn:
            qps_print('step', inFn.fp())

        self.steps.append(Step(func, pmids, inFn=inFn, saveas=saveas, name=name, group=group))
        return (self)

    def run(self, outFn=None, cache_mode='yes'):
        if cache_mode != 'no' and os.path.exists(outFn.fp()):
            qps_print(f'QDData.run reading {outFn}')
            payload = pickle.loads(Path(outFn.fp()).read_bytes())
        else:
            steps = Steps(self.steps, outFn)
            # if steps.remotely:
            #     reply = self._nats_client.request(steps)
            #     if reply.data != b'OK':
            #         raise Exception(f'Request execute failed, server stack trace: {reply.data.decode()}')
            #     payload = pickle.loads(Path(outFn.fp()).read_bytes())
            #     # if cache_mode == 'no':
            #     # delete after use
            #     # Path(outFn.fp()).unlink()
            # else:
            payload = steps.run()

        return payload


if __name__ == '__main__':
    inFn = QSF('_a_').label('futures').label('dominant_future.db')
    ssn = 'CF_NS_LARGE'
    outFn = QCF('_a_').label('futures').label(ssn).label('main_ctr.db')
    qps_print(inFn, outFn)
    dta = QDData()
    dta.step(getMainCtr, getSymset(ssn), inFn, saveas='main_ctr')
    dta.run(outFn=outFn, cache_mode='no')
    qps_print(dta.pld)

    exit(0)

    ctr = QDData().src('rq').market('futures')
    ctr.step(getContCtr, getSymset(ssn), saveas='cont_ctr')
    ctr.lable(ssn).tgtDB('cont_ctr.db').run(cache_mode='no')

    dta = QDData().src('rq').market('futures').lable(ssn).tgtDB('ohlcv_1m_pre.db')
    for i in range(10):
        inFn = f"{rootdata()}/raw/data_rq/futures/ohlcv_1m_pre.%s.db" % (i)
        dta.step(getCtrData, ctr.pld['cont_ctr'], inFn=inFn, saveas='bar1m')
        qps_print(inFn)
    dta.run(cache_mode='no')


