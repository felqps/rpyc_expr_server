#!/usr/bin/env python

import os,sys
from common_logging import *

import hashlib
from  fdf_colors import *
import pandas as pd
from fdf_logging import *

def buf2md5(buf):
    md5 = hashlib.md5()
    buf = buf.encode('utf-8')
    md5.update(buf)
    return md5.hexdigest()

def mkdir(dirs):
    for dir in dirs:
        if not os.path.exists(dir):
            os.system("mkdir -p %s"%(dir))
    return(dirs)

def is_file_newer(tgt,chk):
    #print(f"DEBUG_0984: {tgt}, {chk}")
    if(not os.path.isfile(tgt)):
        return False
    tgtTime = os.stat(tgt).st_mtime
    if(not os.path.isfile(chk)):
        return True
    chkTime = os.stat(chk).st_mtime
    if tgtTime < chkTime:
        return False
    return True

verbose = 1
def set_verbose(v):
    global verbose
    verbose = v

def qps_print(*args, **kwargs):
    if 'loglevel' in kwargs.keys():
        kwargs.pop('loglevel')
    doPrint = True
    if 'always' in kwargs:
        if kwargs['always'] == True:
            doPrint = True
        del kwargs['always']
    if 'quiet' in kwargs:
        if kwargs['quiet'] == False:
            doPrint = True
        del kwargs['quiet'] 

    global verbose
    if doPrint or verbose:
        print(*args, **kwargs)


if __name__ == '__main__':
    funcn = 'fdf_helpers.main'
    from options_helper import *
    opt, args = get_options(funcn)

    import pickle
    df1 = pickle.load(open("/NASQPS06.qpsdata/fdf/cs_F/1d/76/ed46f10ecd0e0206dbf05afc725225.fdf", "rb"))
    print_df(df1, show_head=False, loglevel='error', title='test_error')
    print_df(df1, show_head=False, loglevel='debug', title='test_debug')

