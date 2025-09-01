from QpsNotebookShared import *
import os
from collections import defaultdict
from qdb_options import get_options_sgen
import traceback
import bgen_helpers as bh
import QpsUtil
from common_basic import *
from common_colors import *
from EgenStudy import *
from CTX import *

def make_tasks(opt, rt, studies, studyName, symset, scn, resp, force=False, save_task_data=False):
    funcn = 'run_study.make_tasks'
    task_dirs = []
    for runName in studies[studyName].keys():
        study = studies[studyName][runName]
        study['tasks'] = {}
        for paramsIn in study['paramsL'][:]:
            bar = runName.split(r'_')[-1]
            paramsIn.update({"BAR": bar})
            paramsIn.update({"VERSION": done_version()})
            models = study['models'] if 'models' in study else None
            for params in expand_params(studyName, paramsIn):
                task = EgenStudy(opt, studyName, symset, scn, lines=study['script'], run_name=runName, params=params, models=models)
                #task.save()
                study['tasks'][task.task_name()] = task

                if ctx_verbose(1):
                    print(f"INFO: task_dir {task.task_dir()}")
                task_dirs.append((task.task_dir(), task))

                if not opt.debug:
                   continue 

                fn = f"{task.task_dir()}/{symset}.fldPaths.pkl"
                if os.path.exists(fn):
                    fldPaths = smart_load(fn)
                    if fldPaths is not None:
                        for fp in fldPaths:
                            if fp:
                                desc = smart_load(fp.replace(".egen", ".desc"))
                                print(f"INFO: {funcn} desc info fp= {fp}\n\tdesc= {desc}")
    return task_dirs

def expand_params(studyName, paramsIn):
    if 'FACTOR' not in paramsIn:
        return [paramsIn]
    elif studyName.find("rq_factors")<0:
        return [paramsIn]
    else:
        paramsExp = []
        flddb = smart_load(f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db")
        for row_dict in flddb.to_dict(orient="records"):
            upp = paramsIn.copy()
            for k,v in row_dict.items():
                upp[k.upper()] = v
            if 'FACTOR' in upp:
                upp['FACTOR'] = upp['FACTOR'].replace('FLDNM', upp['NM'])

            if upp['CATEGORY'] in ['overbought', 'momentum', 'super', 'WorldQuant'] or upp['CATEGORY'].find("_1d")>=0:
                if upp['CLASSIFIER'] == 0:
                    paramsExp.append(upp)
            else:
                if False:
                    print(upp)
        print(f"INFO: expanded params count {len(paramsExp)}")
        return paramsExp[:]

    return []

def run_task(opt, task_dir, by_indu, rt=None):
    funcn = "run_task"
    sty = EgenStudy(opt=opt).init_from_task(task_dir) #init a study using params from task
    if rt is None:
        rt = get_runtime(opt, sty.study_name())
    print(f"INFO: {funcn}", sty.study_name(), sty.run_name(), sty.task_name(), rt)
    saveargs = []
    if rt["save_task_data"]:
        saveargs.append("data")
    finishedTasks = sty.run(symsets=rt['symset'], scn=rt['scn'], debug=rt['debug'], force=rt['force'], 
                            by_indu=by_indu, saveargs=saveargs)
    for fT in finishedTasks:
        # if rt["save_task_data"]:
        #     fT.save("data")

        if "eval_factor" in rt["models"]:
            print(f"INFO: eval_factor  task= {fT} {fT.getGenFlds()}")
            for predNm in fT.genFldNms():
                eval_predictor(opt, rt, predNm, sty.study_name())

    return finishedTasks

def eval_predictor(opt, rt, params, study_name, models=["q10", "cx"], detailed=False, is_binary_factor=False):
    funcn = "eval_predictor"
    paramsL = []
    predNm = params['PRED']
    params['RESP'] = rt['resp']
    if params['RESP'].find('_')>0:
        params['RESP'] = params['RESP'].split(r'_')[1]

    predCode = '_'.join(predNm.split(r'_')[1:-1])
    print(f"INFO: {funcn} respNm= {rt['resp']}, predNm= {predNm}({predCode})")

    script = '\n'.join(QpsUtil.open_readlines(f"/Fairedge_dev/app_QpsData/predict_eval_{params['PREFIX']}.txt"))

    if ctx_verbose(5) or True:
        print(f"{NC}INFO: eval script: {script}{NC}")
                        
    mdlTasks = make_tasks(
        opt, 
        rt, 
        {study_name: {predCode: {"paramsL": [params], "script": script, "models": models}}}, #Fake a study dictionary struct
        studyName=study_name, 
        symset=rt['symset'], 
        scn=rt['scn'],
        resp=rt['resp'], 
        force=False, 
        save_task_data=False
        )

    for (mdltask_dir, mdltask) in mdlTasks:
        def need_run(task, force=rt['force'], models = models, detailed=detailed):
            needRun = False
            if force:
                return True
            for modelNm in models:
                fn = f"{task.dta_dir(cache=False)}/{modelNm}/{done_version()}.{detailed}.pkl"
                if not os.path.exists(fn) or ctx_force():
                    needRun = True
                else:
                    print(f"INFO: {funcn} {fn} exists, skipping ...")
            return (needRun)

        mdltask.run_study(symsets=rt['symset'], scn=rt['scn'], debug=rt['debug'], by_indu=False, need_run=need_run, detailed=detailed, is_binary_factor=is_binary_factor, force=rt['force'])

        if opt.show:
            mdltask.show()

    return mdlTasks

def get_runtime(opt, studyNm, default=None):
    rtD = {}

    cmn = {'symset': opt.symset, 'scn': opt.scn, 'debug': False, 'calc': 'worker', "force": opt.force}

    for studyNmPre in ["worldquant", "super", "momentum", "overbought", "prcvol", "univ"]:
        rt = cmn.copy()
        rt.update({'resp': opt.resp,  "by_indu": False,  'save_task_data': False, "models": []})
        rtD[f"{studyNmPre}_factors_02"] = rt

    for studyNmPre in ["univ"]:
        rt = cmn.copy()
        rt.update({'resp': opt.resp, "by_indu": False, 'save_task_data': False, "models": []})
        rtD[f"{studyNmPre}_factors_02"] = rt 

    for studyNmPre in ["formula"]:
        rt = cmn.copy()
        rt.update({'resp': opt.resp, "by_indu": False, 'save_task_data': False, "models": []})
        rtD[f"{studyNmPre}_search_01"] = rt 
        rtD[f"{studyNmPre}_search_02"] = rt 
        rtD[f"{studyNmPre}_search_03"] = rt 

    for studyNmPre in ["talib", "rq"]:
        rt = cmn.copy()
        rt.update({'resp': opt.resp, "by_indu": False, 'save_task_data': False, "models": ["eval_factor"]})
        rtD[f"{studyNmPre}_factors_02"] = rt
    
    for studyNmPre in ["select_predictors_01"]:
        rt = cmn.copy()
        rt.update({'resp': opt.resp, "by_indu": False, 'save_task_data': False, "models": []})
        rtD[studyNmPre] = rt

    for studyNmPre in ["select_predictors_02"]:
        rt = cmn.copy()
        #by_indu True otherwise file too large
        rt.update({'resp': opt.resp, "by_indu": True, 'save_task_data': False, "models": []})
        rtD[studyNmPre] = rt

    if studyNm in rtD:
        rt = rtD[studyNm]
    else:
        rt = rtD[default]
    rt['study_name'] = studyNm
    return rt

STUDIES_DB = None
def get_studies_db():
    global STUDIES_DB
    if STUDIES_DB is None:
        STUDIES_DB = defaultdict(dict)
        study_scripts_dir = f"/Fairedge_dev/app_QpsData/study_scripts"
        for scriptNm in ["ATR_001", "MACD_001"][:]:
            STUDIES_DB["talib_factors_02"].update({scriptNm: eval("".join(open(f"{study_scripts_dir}/{scriptNm}.py", 'r').readlines()))})

        for scriptNm in ["Rq_001"][:]:
            STUDIES_DB["rq_factors_02"].update({scriptNm: eval("".join(open(f"{study_scripts_dir}/{scriptNm}.py", 'r').readlines()))})

        for scriptNm in ["UnivTOPxxx_001", "UnivStSts_001", "UnivCNxxx_001"][:]:
            STUDIES_DB["univ_factors_02"].update({scriptNm: eval("".join(open(f"{study_scripts_dir}/{scriptNm}.py", 'r').readlines()))})

        for scriptNm in ["GenJvData_001"][:]:
            STUDIES_DB["select_predictors_01"].update({scriptNm: eval("".join(open(f"{study_scripts_dir}/{scriptNm}.py", 'r').readlines()))})

        for scriptNm in ["GenRData_001"][:]:
            STUDIES_DB["select_predictors_02"].update({scriptNm: eval("".join(open(f"{study_scripts_dir}/{scriptNm}.py", 'r').readlines()))})

    return STUDIES_DB

def get_study_names():
    return [x for x in sorted(get_studies_db().keys()) if x.find("select")<0]

def get_run_names(studyNm):
    return sorted(get_studies_db()[studyNm].keys())

def get_study_run_list():
    studyRunList = []
    for studyNm in get_study_names():
        for scriptNm in get_run_names(studyNm):
            studyRunList.append([studyNm, scriptNm])
    return studyRunList

def customize_run_study_options(parser): 
    parser.add_option("--show",
                    dest = "show",
                    type = "str",
                    help = "--show (default: %default)",
                    metavar = "--show",
                    default = 'cx')

    parser.add_option("--export_tag",
                    dest = "export_tag",
                    type = "str",
                    help = "--export_tag (default: %default)",
                    metavar = "--export_tag",
                    default = '')

    parser.add_option("--export_dir",
                    dest = "export_dir",
                    type = "str",
                    help = "--export_dir (default: %default)",
                    metavar = "--export_dir",
                    default = f'/NASQPS08.qpsdata/research/performance_eval_server/rkan5')

if __name__ == '__main__':
    funcn = 'run_study.main'
    (opt, args) = get_options_sgen(list_cmds, customize_options=customize_run_study_options)

    if opt.do == "NA":
        opt.do = "build"

    try:
        if len(args)<=0:
            from itertools import product
            studyNms = []
            if opt.study_name == "NA":
                studyNms = get_study_names()
            else:
                studyNms = [opt.study_name]

            lst = list(product(
                opt.scn, 
                studyNms
                ))
            print(f"INFO: scn/studyNm list {lst}")
            cmds = []

            for (scn, studyNm) in lst:
                opt.scn = scn #support running multiple scn's
                opt.study_name = studyNm
                setDataEnv(scn)

                assert opt.study_name != "NA", f"ERROR: must specify --study_name"
                rt = get_runtime(opt, opt.study_name)
                print(f"INFO: {funcn} {opt.study_name} runtime= {rt}")

                studies = defaultdict(dict)
                
                studies_db = get_studies_db()[opt.study_name]

                for scriptNm in studies_db.keys():
                    if opt.debug:
                        print(f"DEBUG: {funcn} adding script= {scriptNm}")
                    studyCfg = eval("".join(open(f"/Fairedge_dev/app_QpsData/study_scripts/{scriptNm}.py", 'r').readlines()))
                    #print(studyCfg)
                    for params in studyCfg['paramsL']:
                        params["STUDY_NAME"] = opt.study_name
                        params["SCRIPT_NAME"] = scriptNm
                    studies[opt.study_name].update({scriptNm: studyCfg})

                print(f"INFO: {funcn} added {opt.study_name} {studies[opt.study_name].keys()}")

                matched_grep = bh.funcgen_matched_grep(opt)

                for studyName in [opt.study_name]:
                    for symset in rt['symset'].split(r','):
                        assert symset != "", f"ERROR: must specify '--symset' "
                        for scn in [opt.scn]:
                            for resp in rt['resp'].split(r','):
                                task_dirs = make_tasks(opt, rt, studies, studyName, symset, scn, resp, force=opt.force, save_task_data=True)
                                if opt.debug:
                                    print(task_dirs)
                                if opt.do.find("run") >=0:
                                    for (taskdir, task) in task_dirs:
                                        tasks = run_task(opt, taskdir, by_indu=rt['by_indu'])
                                        if opt.export_tag != "":
                                            tasks[0].export(tag=opt.export_tag, dir=opt.export_dir)
                                else:
                                    for (taskdir, task) in task_dirs:
                                        bashTmpl = f'/Fairedge_dev/app_QpsData/FC.tmpl'
                                        cmd = f"$PY -u /Fairedge_dev/app_QpsData/run_study.py --asofdate {opt.asofdate} --scn {scn} --symset {symset} {taskdir}"
                                        #print(f"DEBUG: cmd(before) {cmd}")
                                        depTag = f"EGEN_STUDY.70.05"
                                        assigned_host = "any"
                                        cmdAfter = f"{bh.expandBashTmp(opt, symset, bashTmpl, cmd, runfromdir='/Fairedge_dev/app_egen', depTag=depTag, quiet=opt.quiet)} = hostname {assigned_host}"
                                        if matched_grep(cmdAfter):
                                            cmds.append(cmdAfter)  

            if len(cmds)>0:
                print("DEBUG_7122:", "\n".join(cmds))
                bh.run(opt, cmds)                
                
        else:
            for task_dir in args:
                study_name = task_dir.split(r'/')[3]
                rt = get_runtime(opt, study_name)
                finishedTasks = run_task(opt, task_dir, by_indu=rt['by_indu'], rt=rt)
                # for task in finishedTasks:
                #     task.save("data")
                
    except KeyboardInterrupt:
        exit(1)
    except Exception as e:
        print("Exception", e)
        print(f"CMD: {' '.join(sys.argv)}")
        traceback.exc()

