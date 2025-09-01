#!/usr/bin/env python

import sys
import re
import os
from common_paths import *
from ssn2mkts import ssn2mkts

def symsets_cf_cont():
    return " ".join([f"{k}_cont" for k in ssn2mkts.keys()])

def symsets_str_paper():
    #return "I65 C35 C20 C25 C38 C39 CS_ALL"
    return "CS_ALL C39 C35 C38 I65 C25 C20"

def symsets_paper():
    return symsets_str_paper().split()

def symsets_str_rsch():
    rschIndus = []
    for indu in cnstk_indus(plus_all=True):
        if indu in ['E47', 'K70']:
            continue
        d = fn_formula_dir(indu)
        if os.path.exists(d):
            rschIndus.append(indu)
    return ' '.join(rschIndus)
    #return "CS_ALL C39 C35 C26 C38 I65 C25 C20"

def symsets_rsch():
    return symsets_str_rsch().split()

def cnstk_indus(plus_all=False):
    indus = "A01 A02 A03 A04 A05 B06 B07 B08 B09 B10 B11 C13 C14 C15 C17 C18 C19 C20 C21 C22 C23 C24 C25 C26 C27 C28 C29 C30 C31 C32 C33 C34 C35 C36 C37 C38 C39 C40 C41 C42"
    indus = f"{indus} D44 D45 D46 E47 E48 E49 E50 F51 F52 G53 G54 G55 G56 G58 G59 G60 H61 H62 I63 I64 I65 J66 J67 J68 J69 K70 L71 L72 M73 M74 M75 N77 N78 O80 P82 Q83 R85 R86 R87 R88 S90"
    #indus = f"{indus} Unknown"
    if plus_all:
        indus = f"{indus} CS_ALL"
    #return indus.split()
    return [x for x in indus.split() if x >= '']

def get_symsets(scn, symset, all_indus = False, plus_all=True):
    funcn = "get_symsets"
    
    if symset in ['CS', 'NA', 'ALL', 'INDU']:
        if scn in ["T"]:
            symsets = symsets_paper()
        elif not all_indus:
            symsets = symsets_rsch()
        else:
            symsets = cnstk_indus(plus_all=plus_all)
    else:
        symsets = [symset]

    #print(f"DEBUG: {funcn}, {scn}, {symset}, symsets= {symsets}") 
    return symsets

def get_symsets_str(scn, symset, all_indus = False, plus_all=True):
    return ' '.join(get_symsets(scn, symset, all_indus, plus_all))

symset_weights_dict = None
def get_symset_weights_dict():
    global symset_weights_dict
    if symset_weights_dict is None:
        symset_weights_dict = {}
        lncnt = 1
        for symset in re.findall(r"\s(\S+).db", '\n'.join(open('/Fairedge_dev/app_QpsData/bgen_symset_weights.txt', 'r').readlines())):
            #print(f"======================= {symset}", file=sys.stderr)
            symset_weights_dict[symset] = lncnt
            lncnt = lncnt + 1
        symset_weights_dict['CS_ALL'] = 0
        #print("DEBUG_4501", symset_weights_dict.keys())
        #exit(0)
    return symset_weights_dict

def get_symset_weight(symset):
    if symset not in get_symset_weights_dict().keys():
        get_symset_weights_dict()[symset] = 80
    return get_symset_weights_dict()[symset]

if __name__=='__main__':
    funcn = sys.argv[1]
    print(eval(funcn))
