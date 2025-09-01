import os,sys

from common_options_helper import *

if __name__ == "__main__":
    funcn = "options_helper.main"
    import pandas as pd
    opt, args = get_options(funcn)
    print_opt(opt)

