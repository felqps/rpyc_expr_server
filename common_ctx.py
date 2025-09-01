
import datetime
import os

def gettoday(use_dash=False):
    now = datetime.datetime.today()
    if use_dash:
        return now.strftime("%Y-%m-%d")
    return now.strftime("%Y%m%d")

CTX = {}
DETACHED=None
def detached():
    global DETACHED
    if DETACHED is None:
        if "DETACHED_MODE" not in os.environ:
            DETACHED = 0
        else:
            DETACHED = int(os.environ["DETACHED_MODE"])
    return DETACHED

def rootdata():
    if not detached():
        return "/qpsdata"
    else:
        return "/qpsdata.local"

def rootuser():
    if not detached():
        return "/qpsuser"
    else:
        return "/qpsuser.local"

def ctx_set_scn(scn):
    global CTX
    CTX['scn'] = scn
    CTX['tradedate'] = scn._tradeDt

def ctx_set_opt(opt):
    global CTX
    CTX['opt'] = opt

CUSTOMIZE_OPTIONS = None
def set_customize_options(f):
    global CUSTOMIZE_OPTIONS
    CUSTOMIZE_OPTIONS=f

def get_customize_options():
    global CUSTOMIZE_OPTIONS
    return(CUSTOMIZE_OPTIONS)

__dataEnvCfg = {
    "rschhist": {
        "raw": f"{rootdata()}/data_rq.20100101_uptodate",
        "usr": f"{rootuser()}/che/data_rq.20100101_uptodate"
    },
    "prod": {
        "raw": f"{rootdata()}/data_rq.20100101_uptodate",
        "usr": f"{rootuser()}/che/data_rq.20100101_uptodate"
    },
    "prod1w": {
        "raw": f"{rootdata()}/data_rq.20210810_uptodate",
        "usr": f"{rootuser()}/che/data_rq.20210810_uptodate"
    },
    "dcsndate": {
        "raw": f"{rootdata()}/data_rq.20210810_uptodate",
        "usr": f"{rootuser()}/che/data_rq.20210810_uptodate"
    },
    "E": {
        #"raw": f"{rootdata()}/data_rq.20000101_20110101",
        "raw": f"{rootdata()}/data_rq.E",
        "usr": f"{rootuser()}/che/data_rq.20000101_20110101"
    },
    "F": {
        #"raw": f"{rootdata()}/data_rq.20100101_20210101",
        "raw": f"{rootdata()}/data_rq.F",
        "usr": f"{rootuser()}/che/data_rq.20100101_20210101"
    },
    "G": {
        #"raw": f"{rootdata()}/data_rq.20200101_20220101",
        "raw": f"{rootdata()}/data_rq.G",
        "usr": f"{rootuser()}/che/data_rq.20200101_20220101"
    },
}

__dataEnvCfg['W'] = __dataEnvCfg["prod1w"]
__dataEnvCfg['P'] = __dataEnvCfg["prod"]
__dataEnvCfg['R'] = __dataEnvCfg["rschhist"]
__dataEnvCfg['W'] = __dataEnvCfg["prod1w"]
__dataEnvCfg['T'] = __dataEnvCfg["prod1w"]
__dataEnvCfg['U'] = __dataEnvCfg["prod1w"]
__dataEnvCfg['A'] = __dataEnvCfg["prod1w"]
__dataEnv = "prod1w"

    
def cc(name, data={'W': 'prod1w', 'P': 'prod', 'R': 'rschhist'}):
    global __dataEnv
    if name in data:
        return data[name]
    else:
        return name

def getDataEnvName():
    global __dataEnv
    return cc(__dataEnv)

def getDataEnv():
    global __dataEnvCfg
    global __dataEnv
    return __dataEnvCfg[__dataEnv]    

def setDataEnv(env, debug=False):
    global __dataEnvCfg
    global __dataEnv

    if cc(__dataEnv) != cc(env):
        if debug:
            print(f"INFO: setDataEnv from_env= {cc(__dataEnv)}, to_env={cc(env)},  {__dataEnvCfg[env]}", file=sys.stderr)
    else:
        return __dataEnv

    if env in __dataEnvCfg:
        __dataEnv = env
    else:
        __dataEnv = "prod1w"

STUDY_RESP = None
def studyResp(v="Resp_C2C"):
    global STUDY_RESP
    if STUDY_RESP is None:
        STUDY_RESP = v
    
    return STUDY_RESP


def scn_list(default=('F','G','W')):
    if default:
        return default
    global __dataEnvCfg
    return sorted(__dataEnvCfg.keys())

def dd(name, dataEnv=None):
    global __dataEnvCfg
    global __dataEnv
    if dataEnv is None:
        dataEnv = __dataEnv
    return __dataEnvCfg[dataEnv][name]

