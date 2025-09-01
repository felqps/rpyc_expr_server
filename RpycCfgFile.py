import os,sys
from QpsSys import gethostname

class RpycCfgFile:
    def __init__(self, opt):
        if opt.do not in ["", "all"]:
            group_name = f".{opt.do}_{gethostname()}"
        else: 
            group_name = opt.group_name
        self._fn = f"/Fairedge_dev/app_qpsrpyc/rpyc_expr_server{group_name}.cfg"
        assert os.path.exists(self._fn), f"'{opt.do}', {self._fn} does not exists"
    
    def readlines(self):
        print(f"INFO: RpycCfgFile reading {self._fn}")
        lines = []
        for ln in open(self._fn, 'r').readlines():
            ln = ln.strip()
            if ln == "" or ln[0] == '#':
                continue   
            lines.append(ln)
        return lines
