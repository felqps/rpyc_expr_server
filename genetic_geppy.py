
#!/usr/bin/env python

import sys

from common_basic import *
import QpsUtil
from main_che import *
import geppy as gep
from deap import creator, base, tools
import numpy as np
import random

import operator 
import math
import datetime
from register_script_functions import *


# def div(x1, x2):
#     # if abs(x2) < 1e-6:
#     #     return 1
#     rc = x1/x2
#     # print(x1.head(5))
#     # print(x2.head(5))
#     # print(rc.head(5))
#     #print(f"DEBUG_2345: protected_div({type(x1)}, {type(x2)}) => {type(rc)}")
#     return rc

HALL_OF_FAME_FORMULA = {}
def print_hof(opt, hof):
    if opt.gep_cfg.find("unittest")>=0: #no need to print out hof using unittest
        return 
    global HALL_OF_FAME_FORMULA
    for ind in hof:
        formulaStr = f"{ind}"
        if formulaStr not in HALL_OF_FAME_FORMULA:
            print(f"INFO_5000: HOF {'*'*18}", ind, ind.fitness.values)
            HALL_OF_FAME_FORMULA[formulaStr] = ind.fitness.values

            if ind.fitness.values[0] > 0.02:
                md5str = QpsUtil.buf2md5(formulaStr)
                cacheFn = f"{rootdata()}/egen_study/genetic_func_hof/Resp_C2C.{opt.symset}.{opt.scn}.{md5str}.txt"
                print(f"{formulaStr} : {ind.fitness.values}", file=open(cacheFn, 'w'))

def fld_alpha_performance_factory(opt, task, facNm, isClassifier=False):
    perf_params = get_perf_params(
        task.scn().dts_cfg, 
        opt.symset, 
        facNm, 
        simple_report=not opt.detailed, 
        return_data=None, 
        factor_data=None, 
        is_binary_factor=isClassifier
        )  

    def fld_alpha_performance(resp, pred, params=perf_params, alphaNm="alpha_QT_ZZ500"):
        #print_df(resp.head(), title="DEBUG_2345:")
        #print_df(pred.head(), title="DEBUG_2345:")
        params['return_data'] = resp
        params['factor_data'] = pred
        params['start_date'] = None
        params['end_date'] = None
        params['display_result'] = False
        params['quiet'] = 1

        fm = AlphaPerformance(params=params)
        pfd = fm.GetPerformanceData()
        if ctx_verbose(1):     
            print(f"INFO: =========================================={alphaNm}(net)", pfd['net_performance'][alphaNm][2]['all'])
        return pfd['net_performance'][alphaNm][2]['all']['Annualized Return']
    
    return fld_alpha_performance

DNA_BEGIN_TIME = time.time()
RESULT_COUNT = 0
def genetic_geppy_basic(task, *x):
    funcn = f"genetic_geppy_basic()"
    print(f"INFO: {funcn}")
    global DNA_BEGIN_TIME
    global RESULT_COUNT
    opt = task._opt
    gep_cfg = opt.gep_cfg
    mode = opt.mode
    func_select = opt.func_select   
    fld_select = opt.fld_select
    #print(f"DEBUG_2335", "genetic_basic", file=sys.stderr)
    # for reproduction
    randseed = opt.randseed
    random.seed(randseed)
    np.random.seed(randseed)

    creator.create("FitnessMin", base.Fitness, weights=(-1,))  # to minimize the objective (fitness)
    creator.create("FitnessMax", base.Fitness, weights=(1,)) 
    creator.create("Individual", gep.Chromosome, fitness=creator.FitnessMax)

    rcCache = {}                
    globals()['Y'] = task.nsByVar()['Resp_C2C']
    # globals()['CLOSE'] = task.nsByVar()['CLOSE']
    # globals()['RETURNS'] = task.nsByVar()['RETURNS']
    # globals()['VOLUME'] = task.nsByVar()['VOLUME']

    xFlds = ""
    if fld_select == 'ohlcv':
        xFlds = "OPEN,HIGH,LOW,CLOSE,VOLUME,RETURNS,VWAP"
    elif fld_select == 'bear':
        xFlds = "CLOSE,RETURNS,VOLUME,VWAP"
    elif fld_select == 'macd':
        xFlds = "OPEN,HIGH,LOW,CLOSE,VOLUME"
    else:
        assert False, f"ERROR: invalid fld_select '{fld_select}'"


    pset = gep.PrimitiveSet('Main', input_names=xFlds.split(r','))
    geppy_register_script_functions(pset, func_select=func_select)
    if opt.rnc_len>0:
        pset.add_rnc_terminal()

    h = 30          # head length
    champs = 10     # hof count
    if opt.rnc_len>0:
        rnc_len = opt.rnc_len 
    else:
        rnc_len = 100       # length of the RNC array
    if gep_cfg in ['unittest', 'unittest02']:
        h = 6
        #h = 30
        n_pop = 100
        n_gen = 10
    elif gep_cfg == 'dev':
        h = 30
        n_pop = 50
        n_gen = 5
        champs = 3
    elif gep_cfg in [f"default{x}" for x in ["","02"]]:
        h = 30
        n_pop = opt.n_pop
        #n_gen = 30
        n_gen = opt.n_gen

    else:
        assert False, f"ERROR: invalid gep_cfg= '{gep_cfg}'"

    #total number of individuals evalulated capped at (n_pop * n_gen)

    enable_ls = False # whether to apply the linear scaling technique

    toolbox = gep.Toolbox()
    toolbox.register('rnc_gen', random.randint, a=-10, b=10)   # each RNC is random integer within [-5, 5]
    toolbox.register('gene_gen', gep.GeneDc, pset=pset, head_length=h, rnc_gen=toolbox.rnc_gen, rnc_array_length=rnc_len)

    #toolbox.register('individual', creator.Individual, gene_gen=toolbox.gene_gen, n_genes=n_genes, linker=div)

    if gep_cfg == 'unittest': #01
        n_genes = 1    # number of genes in a chromosome
        toolbox.register('individual', creator.Individual, gene_gen=toolbox.gene_gen, n_genes=n_genes)
    elif gep_cfg == 'unittest02':
        n_genes = 2    # number of genes in a chromosome
        toolbox.register('individual', creator.Individual, gene_gen=toolbox.gene_gen, n_genes=n_genes, linker=operator.mul)
    elif gep_cfg in  ['default', 'dev']:    
        n_genes = 2    # number of genes in a chromosome
        toolbox.register('individual', creator.Individual, gene_gen=toolbox.gene_gen, n_genes=n_genes, linker=operator.mul)
    elif gep_cfg in  ['default02']:    
        n_genes = 1    
        toolbox.register('individual', creator.Individual, gene_gen=toolbox.gene_gen, n_genes=n_genes)

    toolbox.register("population", tools.initRepeat, list, toolbox.individual)

    # compile utility: which translates an individual into an executable function (Lambda)
    toolbox.register('compile', gep.compile_, pset=pset)

    # # as a test I'm going to try and accelerate the fitness function
    # from numba import jit

    # @jit
    def evaluate(individual, func_perf=fld_alpha_performance_factory(task._opt, task, task._opt.facnm, isClassifier=False)):
        funcn = "genetic_geppy.evalulate"
        global DNA_BEGIN_TIME
        global RESULT_COUNT
        formulaStr = f"{individual}"
        md5str = QpsUtil.buf2md5(formulaStr)
        cacheDir = f"{rootdata()}/egen_study/genetic_func_cache/Resp_C2C.{task._opt.symset}.{task._opt.scn}"
        QpsUtil.mkdir([cacheDir])
        cacheFn = f"{cacheDir}/{md5str}.pkl"

        RESULT_COUNT =  RESULT_COUNT + 1
        if formulaStr in rcCache:
            if ctx_debug(5): # or opt.gep_cfg.find("unittest")>=0:
                print(f"DEBUG_9119: have_seen= {RESULT_COUNT}: {individual}, {rcCache[formulaStr]:9.6f}", file=sys.stderr)
            return rcCache[formulaStr],

        if task._opt.force == 0 and os.path.exists(cacheFn):
            rc = smart_load(cacheFn)
            rcCache[formulaStr] = rc
            if ctx_debug(5): # or opt.gep_cfg.find("unittest")>=0:
                print(f"DEBUG_9120: prev_eval= {RESULT_COUNT}: {individual}, {rc:9.6f}", file=sys.stderr)
            return rc,
        

        """Evalute the fitness of an individual: MAE (mean absolute error)"""
        func = toolbox.compile(individual)
        
        # below call the individual as a function over the inputs
        
        # Yp = np.array(list(map(func, X)))

        #Yp = np.array(list(map(func, CLOSE, RETURNS))) 
        print_hof(task._opt, hof)

        #print(individual)
        #We copy over the data from original because the copy sometimes are changed during func() eval
        for xFld in xFlds.split(r','):
            globals()[xFld] = task.nsByVar()[xFld].copy()
            # globals()['CLOSE'] = task.nsByVar()['CLOSE'].copy()
            # globals()['RETURNS'] = task.nsByVar()['RETURNS'].copy()
            # globals()['VOLUME'] = task.nsByVar()['VOLUME'].copy()
        func_call = f"func({xFlds})"

        DNA_END_TIME = time.time()
        EVAL_TOT_TIME = (DNA_END_TIME-DNA_BEGIN_TIME)

        if opt.gep_cfg.find("unittest")>=0:
            if ctx_debug(1):
                print(f"DEBUG_9123: RESULT_COUNT= {RESULT_COUNT}, individual= {individual}", file=sys.stderr)
            if RESULT_COUNT>200:
                exit(0)

        elif ctx_debug(1): 
            print(f"DEBUG_9127: eval_time= {EVAL_TOT_TIME}, individual= {individual}", file=sys.stderr)

        #allocated_time  = 60*30
        allocated_time  = 60*opt.gep_minutes
        if ctx_symset() in ['CS_ALL']:
            allocated_time = allocated_time * 4
        if EVAL_TOT_TIME>allocated_time:
            print(f"INFO: reach dna_eval_allocated_time {EVAL_TOT_TIME}", file=sys.stderr)
            exit(0)

        try:
            if opt.gep_cfg.find("unittest")>=0:
                rc = random.uniform(0, 1)
            else:
                Yp = eval(func_call)
                rc = func_perf(Y, Yp)
        except Exception as e:
            now = time.strftime('[%Y/%m/%d %H:%M:%S]')
            print(now, e, individual, file=sys.stderr)
            rc = -100

        if np.isnan(rc):
            rc = -100.0
        rcCache[formulaStr] = rc
        smart_dump(rc, cacheFn, title=funcn, verbose=1)
        print(f"{formulaStr} : {rc}", file = open(f"{cacheDir}/{md5str}.txt", 'w'))
        return rc,

    toolbox.register('evaluate', evaluate)

    toolbox.register('select', tools.selTournament, tournsize=3)

    # 1. general operators
    toolbox.register('mut_uniform', gep.mutate_uniform, pset=pset, ind_pb=0.05, pb=1)
    toolbox.register('mut_invert', gep.invert, pb=0.1)
    toolbox.register('mut_is_transpose', gep.is_transpose, pb=0.1)
    toolbox.register('mut_ris_transpose', gep.ris_transpose, pb=0.1)
    toolbox.register('mut_gene_transpose', gep.gene_transpose, pb=0.1)
    toolbox.register('cx_1p', gep.crossover_one_point, pb=0.3)
    toolbox.register('cx_2p', gep.crossover_two_point, pb=0.2)
    toolbox.register('cx_gene', gep.crossover_gene, pb=0.1)
    # 2. Dc-specific operators
    toolbox.register('mut_dc', gep.mutate_uniform_dc, ind_pb=0.05, pb=1)
    toolbox.register('mut_invert_dc', gep.invert_dc, pb=0.1)
    toolbox.register('mut_transpose_dc', gep.transpose_dc, pb=0.1)
    # for some uniform mutations, we can also assign the ind_pb a string to indicate our expected number of point mutations in an individual
    toolbox.register('mut_rnc_array_dc', gep.mutate_rnc_array_dc, rnc_gen=toolbox.rnc_gen, ind_pb='0.5p', pb=1.00)
    toolbox.pbs['mut_rnc_array_dc'] = 1  # we can also give the probability via the pbs property

    stats = tools.Statistics(key=lambda ind: ind.fitness.values[0])
    stats.register("avg", np.mean)
    stats.register("std", np.std)
    stats.register("min", np.min)
    stats.register("max", np.max)

    pop = toolbox.population(n=n_pop) # 
    hof = tools.HallOfFame(champs)   # only record the best three individuals ever found in all generations

    startDT = datetime.datetime.now()
    print ("INFO: Start time", str(startDT), file=sys.stderr)

    # start evolution
    pop, log = gep.gep_simple(pop, toolbox, n_generations=n_gen, n_elites=2, stats=stats, hall_of_fame=hof, verbose=True)

    print ("Evolution times were:\n\nStarted:\t", startDT, "\nEnded:   \t", str(datetime.datetime.now()))

    for ind in hof:
        print(ind, f"fitness= {ind.fitness.values}")
        #print(gep.simplify(ind), f"fitness= {ind.fitness.values}")
    return 1.0

