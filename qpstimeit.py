import time
import sys
from functools import wraps
from common_colors import *

def timeit_real(my_func):
    @wraps(my_func)
    def timed(*args, **kw):    
        tstart = time.time()
        output = my_func(*args, **kw)
        tend = time.time()
        tmstr = 'timeit   func= {:42s} finished in {:7.3f} seconds'.format(my_func.__name__, (tend - tstart))
        if True:
            print(f"{GREEN}{tmstr}{NC}", file=sys.stderr)
        return output
    return timed

def timeit_m_real(my_func):
    def timed(ref, *args, **kw):
        tstart = time.time()
        output = my_func(ref, *args, **kw)
        tend = time.time()
        class_name = ref.__class__.__name__
        tmstr = 'timeit_m func= {:42s} finished in {:7.3f} seconds'.format(f"{class_name}.{my_func.__name__}", (tend - tstart))
        if True:
            print(f"{GREEN}{tmstr}{NC}", file=sys.stderr)
        #print('timeit_m "{}" took {:.3f} ms'.format(my_func.__name__, (tend - tstart) * 1000), file=sys.stderr)
        return output
    return timed

def timeit(my_func): #overwrite if not using it
    #return my_func
    return timeit_real(my_func)

def timeit_m(my_func):
    #return my_func
    return timeit_m_real(my_func)


