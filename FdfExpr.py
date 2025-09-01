import os,sys

from fdf_helpers import *
from platform_helpers import *

class FdfExpr:
    def __init__(self, expr):
        self._expr = expr
        #print(f"FdfExpr({expr}")
    
    def md5(self):
        #print(f"{RED} {self._expr} ==> {buf2md5(self._expr.split(r':')[-1])[-8:]}")
        return buf2md5(self._expr.split(r':')[-1])[-8:]

    def save_definition(self, fn):
        Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)
        Path(fn).write_text(self._expr+'\n')

if __name__ == "__main__":
    funcn = "factor_worker.main"
    from options_helper import get_options
    opt, args = get_options(funcn)
    print(FdfExpr('CLOSE').md5())
