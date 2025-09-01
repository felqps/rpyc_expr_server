import traceback
from contextlib import redirect_stdout, redirect_stderr
import random
from io import StringIO
from pathlib import Path
from typing import Optional, List, Set, Iterable, Union
from collections import defaultdict, deque
from dateutil.parser import parse as dtparse
from QDFile import *
from QDData import *

class Fld:
    def __init__(self, outFn: QDFile, cache_mode='yes', qd_data: QDData = None, debug = False):
        self.qd_data = qd_data if qd_data else QDData()
        self.outFn = outFn
        if debug:
            qps_print(f'Fld(): {outFn.fp()}')
        self.cache_mode = cache_mode
        self.dependencies = []
        self.graph = None

    @property
    def steps(self) -> List[Step]:
        return self.qd_data.steps

    def bind(self, graph: 'Graph'):
        self.graph = graph

    def __lt__(self, other: 'Fld'):
        return str(self) < str(other)

    def add_dependencies(self, *flds: 'Fld'):
        if not self.graph:
            for fld in flds:
                if fld.graph:
                    self.bind(fld.graph)
        assert self.graph, 'Must bind graph to fld before adding dependencies'
        self.graph.add_fld(self, flds)

    def __repr__(self):
        return f'Fld({self.outFn.fp()})'

    def label(self, verbose: int = 3):
        path = Path(self.outFn.fp())
        return os.path.join(*path.parts[-verbose:])

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        if self.outFn.fp() != other.outFn.fp():
            return False
        return True

    def __hash__(self):
        if not hasattr(self, 'outFn'):
            return random.randint(0, 100000000)
        return hash(self.outFn.fp())

    @staticmethod
    def __extract_input_files(qd_data: QDData) -> Set[str]:
        return {step.input_path for step in qd_data.steps if step.input_path}

    @property
    def cached(self):
        if self.cache_mode == 'no':
            return False
        if not os.path.exists(self.outFn.fp()):
            return False

        out_mtime = os.path.getmtime(self.outFn.fp())
        for input_file in self.__extract_input_files(self.qd_data):
            if not os.path.exists(input_file):
                return False
            if os.path.getmtime(input_file) > out_mtime:
                return False
        return True


    @property
    def cached_result(self):
        if self.outFn.fp().find('.db')>=0:
            return pickle.loads(Path(self.outFn.fp()).read_bytes())
        else:
            return Path(self.outFn.fp()).read_bytes()
            
    @property
    def should_cache(self) -> bool:
        return self.cache_mode == 'yes'

    def step(self, func, pmids, inFn: Union[QDFile, 'Fld']=None, saveas='__default__', name: str = None):
        if isinstance(inFn, Fld):
            self.add_dependencies(inFn)
            inFn = inFn.outFn

        self.qd_data.step(func=func, pmids=pmids, inFn=inFn, saveas=saveas, name=name)
        return self

    def run(self, force_update: bool = False):
        cache_mode = 'no' if force_update else self.cache_mode

        return self.qd_data.run(self.outFn, cache_mode=cache_mode)

    def add_depends(self, fld):
        # qps_print('-'* 200, f'add_depends {fld.outFn.fp()}')
        self.dependencies.append(fld)

    def is_resp(self):
        return self.fp_contains('Resp')

    def is_pred(self):
        return self.fp_contains('Pred')

    def fp_contains(self, v):
        return self.outFn.fp().find(v)>=0

    def fp(self):
        return self.outFn.fp()

    def fdir(self):
        return self.outFn.fdir()

    def fdir_short(self):
        return self.outFn.fdir_short()

    def pmid(self):
        return self.fp().split(r'/')[-1].split(r'.')[0]

    def dump(self, pld, verbose=1):
        return self.outFn.dump(pld, verbose=verbose)

    def load(self):
        return self.outFn.load()


