import os,sys


import warnings
warnings.simplefilter("ignore", category=UserWarning)


from platform_helpers_linux import *
#from platform_helpers_win import *

if __name__ == "__main__":
    funcn = "symset_helpers.main"
    from options_helper import get_options
    opt, args = get_options(funcn)
    pickle.loads(Path("/qpsdata/data_rq.20210810_uptodate/convertible/all_instruments.db").read_bytes())
