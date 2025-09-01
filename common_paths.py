import sys
from common_ctx import *
from CTX import *

def fn_fgen_flds_cfg(sectype='CS'):
    #/qpsdata/config/fgen_flds.CS.cfg
    return f"/qpsdata/config/fgen_flds.{sectype}.cfg"

def fn_all_instru(sectype='CS'):
    #/qpsdata/config/all_instru_CS.txt
    return f"/qpsdata/config/all_instru_{sectype}.txt"

def fn_symset(symset, scn='W'):
    return f"/qpsdata/config/symsets/{scn}/{symset}.pkl"

def fn_formula_dir(indu):
    return f"/Fairedge_dev/proj_JVrc/Resp_C2C.{indu}"

def redis_db_hashkey(fldnm, dt, symset='CS_ALL', src='rq', period='1d'):
    return f"{fldnm}:{src}:{period}:{symset}:{dt}"

def dts_cfg_longform(v):
    d = {'W': 'prod1w', 'P': 'prod', 'R': 'rschhist'}
    if v in d:
        return d[v]
    else:
        return v

def dts_cfg_shortform(v):
    d = {'prod1w': 'W', 'prod': 'P', 'rschhist': 'R'}
    if v in d:
        return d[v]
    else:
        return v

def dd_raw(dts_cfg):
    return(dd('raw', dts_cfg))

def dd_usr(dts_cfg):
    return(dd('usr', dts_cfg))

def dd_fld(dts_cfg, snn="SN_CS_DAY", p='1d'):
    # from CTX import ctx_dts_cfg_expanded
    return f"{dd_usr(dts_cfg)}/{snn.split(r'_')[1]}/{snn}/{ctx_dts_cfg_expanded()}/{p}"

def formula_search_ver():
    return "formula_search_05"

def formula_build_ver():
    return "formula_builder_05"

def formula_search_dir():
    return f"/qpsdata/egen_study/{formula_search_ver()}"

def formula_builder_dir():
    return f"/qpsdata/egen_study/{formula_build_ver()}"

def ctx_fldnm2fp(fldNm, symset=None):
    if symset is None:
        fldFp = f"{ctx_qdfroot()}/{fldnm2bar(fldNm)}/{fldNm}/{ctx_symset()}.db"
    else:
        fldFp = f"{ctx_qdfroot()}/{fldnm2bar(fldNm)}/{fldNm}/{symset}.db"
    return fldFp

def getStudyName(do, study_name):
    if study_name == "NA":
        return formula_build_ver()
    else:
        return study_name

if __name__ == '__main__':
    if len(sys.argv)>1:
        for x in sys.argv[1:]:
            print(eval(x))
    else:
        for x in [
            "scn_list()",
            *[f"dd_raw('{x}')" for x in scn_list()],
            *[f"dd_usr('{x}')" for x in scn_list()],
            *[f"dd_fld('{x}', 'SN_CS_DAY')" for x in scn_list()],
            *[f"dd_fld('{x}', 'SN_CF_DNShort')" for x in scn_list()],
            "fn_symset('C39')",
            "formula_search_dir()",
            "formula_builder_dir()",
            "ctx_fldnm2fp('Resp_lnret_C2C_1d_1')"
        ]:
            print(f"{x:<36}=:", eval(x))
        

