import os
import datetime
import sys
from common_colors import *

CMDLINE_OPT = None
def cmdline_opt():
    global CMDLINE_OPT
    return CMDLINE_OPT

def cmdline_opt_debug_on():
    opt = cmdline_opt()
    try:
        if opt:
            return opt.debug
        else:
            return False
    except Exception as e:
        return False

def cmdline_opt_verbose_on():
    opt = cmdline_opt()
    try:
        if opt:
            return opt.verbose
        else:
            return False
    except Exception as e:
        return False
    
def set_cmdline_opt(opt):
    global CMDLINE_OPT
    CMDLINE_OPT = opt

def display(item):
    opt = cmdline_opt()
    if opt is None:
        return True
    if hasattr(opt,"hide_items") and item in opt.hide_items:
        return False
    elif hasattr(opt,"show_items") and item in opt.show_items:
        return True
    else:
        if opt.debug:
            return True
        else:
            return item>=500000

def opt2dict(opt):
    return eval(f"{opt}") 

