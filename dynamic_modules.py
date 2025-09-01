import os,sys
from pathlib import Path
import time
import importlib
import inspect
import numpy as np
from common_colors import GREEN,NC
import register_cf_functions as cf
import register_cs_functions as cs
from QpsUtil import gethostname

def register_function(fw, funcn, funcp, ordinal, dimension):
    #dimension: f=float, i=integer, s=string, d=dataframe
    if fw._ctx is None:
        fw._ctx = {}
        fw._ctx['np'] = np
    fw._ctx[funcn] = funcp  #function name to function pointer map
    fw._ctx[f"{funcn}_ordinal"] = ordinal
    fw._ctx[f"{funcn}_dimension"] = dimension

    ###NOTE: lambda func wont work here!!!
    if dimension == "di":
        for d in cf.ts_func_periods():
            #print(f"Registering: {funcn}{d}")
            exec(f"""
def {funcn}{d}(f,d={d}): 
    return cf.{funcn}(f,d=d)
fw._ctx[f"{funcn}{d}"] = {funcn}{d}
""")            
            # fw._ctx[f"{funcn}{d}"] = lambda f: funcp(f,d=d)
            fw._ctx[f"{funcn}{d}_ordinal"] = 1
            fw._ctx[f"{funcn}{d}_dimension"] = 'd'
    
    if dimension == "ddi":
        for d in cf.ts_func_periods():
            exec(f"""
def {funcn}{d}(a,b,d={d}): 
    return cf.{funcn}(a,b,d=d)
fw._ctx[f"{funcn}{d}"] = {funcn}{d}
""")      
            #fw._ctx[f"{funcn}{d}"] = lambda a,b: funcp(a,b,d=d)
            fw._ctx[f"{funcn}{d}_ordinal"] = 2
            fw._ctx[f"{funcn}{d}_dimension"] = 'dd'

def print_registered_functions(fw):
    ctx = fw._ctx
    opt = fw._opt
    if not opt.group_name:
        opt.group_name = "GROUPNAME"
    register_function_md_file = f"/Fairedge_dev/app_qpsrpyc/registered_functions.{opt.group_name}_{gethostname()}.md"
    fp = open(register_function_md_file, "w")
    func_dict = {}
    for k in ctx.keys():
        if k.find("ordinal") >= 0 or k.find("dimension") >= 0:
            continue
        else:
            if f"{k}_ordinal" in ctx.keys():
                print(f"{k}:", ctx[f"{k}_ordinal"], ctx[f"{k}_dimension"], file=fp)

FACTOR_WORKER = None
def register_factor_worker(fw):
    global FACTOR_WORKER
    FACTOR_WORKER = fw

def get_factor_worker():
    global FACTOR_WORKER
    return FACTOR_WORKER

def fun_list(module_name):
    module = sys.modules[module_name]
    fun_list = [fun for fun in dir(module) if "__" not in fun]
    print(f"module list: {fun_list}")
    for i, fun in enumerate(fun_list):
        func = getattr(module, fun, None)
        if callable(func):
            func(str(i))

def reload_module(module_name, module_path):
    sys.path = [module_path, *sys.path]
    #print(sys.path)

    if module_name in sys.modules:  
        del sys.modules[module_name]
    new_module = importlib.import_module(module_name)
    globals()[module_name] = new_module

    #The following is similar to "from module_name import *"
    for func_name, func in inspect.getmembers(new_module, inspect.isfunction):  
        globals()[func_name] = func
    
    sys.path = sys.path[1:]

    return new_module

def load_dynamic_modules(fn):
    modules = []
    module_dict = eval(Path(fn).read_text())
    for module_name, module_path in module_dict.items():
        print(f"{GREEN}INFO: module_list_file= {fn}; load_dynamic_module= {module_name}; module_path= {module_path}{NC}")
        try:
            modules.append(reload_module(module_name, module_path))
        except Exception as e:
            print(e)
            pass
    for md in modules:
        md.register_dynamic_module_functions()

if __name__ == "__main__":
    while 1:
        #fun_list('example_module')
        load_dynamic_modules(fn="/qpsdata/config/dynamic_modules/modules_list.txt")    
        time.sleep(3)
