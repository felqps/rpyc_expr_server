import pathlib
import pickle
from options_helper import *


import pandas_ta as ta

class TALib:  #fake as having problem to install TAlib on windows
    def __init__(self):
        pass
    def WMA(self, x, d):
        return ta.wma(x, d)
    def EMA(self, x, d):
        return ta.ema(x, d)

talib = TALib()
#from platform_helpers import *

def is_windows():
    return False

def Path(fn, create_dir_if_missing=True):
    if create_dir_if_missing:
        pathlib.Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)
        
    return pathlib.Path(fn)

if __name__ == "__main__":
    funcn = "symset_helpers.main"
    opt, args = get_options(funcn)
    pickle.loads(Path("/qpsdata/data_rq.20210810_uptodate/convertible/all_instruments.db").read_bytes())
