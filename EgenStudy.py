#!/home/shuser/anaconda3/bin/python

import sys


from common import *
from EgenNew import *
from common_basic import *
from SmartFile import *
#from QpsNotebookShared import analyze_qSummary
from funcs_fld import *
from funcs_series import *
from model_cx import *
from model_cx_fast import *
from model_q10 import *


def run_egen_async(egen):
    egen.run(force=True, debug=True)
    return(egen)  

def get_classifier_flds():
    fldInfoDf = smart_load(f"{rootdata()}/egen_study/flds_summary/fldInfoDf.db")
    classifierFlds = list(fldInfoDf[fldInfoDf['classifier']!=0].index.values)
    classifierFlds = ['_'.join(x.split(r'_')[-3:-1]) for x in classifierFlds]
    return classifierFlds


class EgenStudy:
    @timeit_m
    def __init__(self, opt=None, study_name=None, symsets=None, scn=None, lines=None, params=None, run_name=None, models=None):
        funcn = "EgenColl.init()"
        self._study_name = study_name
        self._dts_cfg_lookup = {'W': 'prod1w', 'P': 'prod', 'R': 'rschhist'}
        self._opt = opt
        self._symsets = symsets #This is a comma sperated list of all symset to run when run the study
        self._symset = symsets #This is the symset for the current run, as study can run muliple symsets
        # self._scn = scn
        self._dts_cfg = scn
        self._lines = lines
        self._params = params
        self._egens = {} #key is symset
        self._run_name = run_name
        self._models = models
        self._df = None
        self._qSummary = None
        self._task_name = "NA"
        # self._genFlds = None

        if params is not None:
            if 'RESP' in params:
                self._run_name = f"{params['RESP']}.{self._run_name}"
            if 'TASK_NAME' in params:
                self._task_name = params['TASK_NAME']
            else:
                self._task_name = buf2md5(f"{pickle.dumps(self._params)}")[-6:]
            self.set_dirs()

    def set_dirs(self):
        # self._run_dir = f"{self._rootDir()}/{self._run_name}"
        # self._task_dir = f"{self._run_dir}/{self._task_name}"
        # self._unit_tests_dir = self._run_dir.replace(f"{rootdata()}", "/Fairedge_dev/qpsdata/unit_tests")
        pass

    def qSummary(self):
        return format_qsummary(self._qSummary)

    def predNm(self):
        return self._params['PRED']
    
    def respNm(self):
        # if len(list(self._egens.values()))<=0:
        #     assert 
        # else:

        return list(self._egens.values())[0].respNm()

    def genFlds(self):
        if len(list(self._egens.values())) > 0:
            return list(self._egens.values())[0].getGenFlds()
        else:
            return None

    @deprecated_m
    def init_from_task(self, task_dir, models = ["q10", "cx"]):
        #/qpsdata.local/egen_study/super_factors_01/C2C.sbp/28a037
        (study_name, run_name, task_name) = task_dir.split(r'/')[-3:]
        print(study_name, run_name, task_name)
        self._study_name = study_name
        self._run_name = run_name
        self._task_name = task_name
        self.set_dirs()
        print(f"INFO: reading fldlst {self.task_dir(cache=False)}/fldlst.txt")
        self._lines = '\n'.join(open(f"{self.task_dir(cache=False)}/fldlst.txt", 'r').readlines())
        self._params = pickle.loads(Path(f"{self.task_dir(cache=False)}/params.pkl").read_bytes())
        self._symsets = pickle.loads(Path(f"{self.task_dir(cache=False)}/symsets.pkl").read_bytes())
        #self._opt = pickle.loads(Path(f"{self.task_dir(cache=False)}/opt.pkl").read_bytes())
        #self._scn = pickle.loads(Path(f"{self.task_dir(cache=False)}/scn.pkl").read_bytes())
        self._models = pickle.loads(Path(f"{self.task_dir(cache=False)}/models.pkl").read_bytes())
        # if os.path.exists(f"{self.task_dir(cache=False)}/genFlds.pkl"):
        #     self._genFlds = pickle.loads(Path(f"{self.task_dir(cache=False)}/genFlds.pkl").read_bytes())
        if self._models is not None:
            self._models = models.copy()

        return self

    def pre_run(self, debug=False):
        funcn = "EgenStudy.pre_run"
        self._dts_cfg = cc(self._dts_cfg)        
        if debug:
            print(f"{funcn}", self._dts_cfg, self._dts_cfg_lookup)
    
    def return_egen(self, egen):
        print(f"DEBUG: return_egen {egen}", file=sys.stderr)
        self.add(egen)
    
    def return_err(self, err):
        print(f"ERROR: EgenStudy run error: {err}")

    def make_egen(self, symset="", scn="", lines=None, params=None, debug=False, force=False):
        funcn = "EgenStudy.make_egen"
        self._symset = symset
        if scn != "":
            self._dts_cfg = scn

        if lines is not None:
            self._lines = lines

        if params is not None:
            self._params = params

        self.pre_run()

        self._df = None #so it is regenerated


        #for symset in self.symsets(symsets)[:]:
        if debug:
            print(f"INFO: EgenStudy.run('{symset}')", file=sys.stderr)

        # scnW = Scn(opt, symset, "prod1w", "SN_CS_DAY")
        # scnP = Scn(opt, symset, "prod", "SN_CS_DAY")
        # scnR = Scn(opt, symset, "rschhist", "SN_CS_DAY")
        scn = Scn(self._opt, symset, self._dts_cfg, "SN_CS_DAY", asofdate=self._opt.asofdate)
        if ctx_verbose(1):
            #config_print_df()
            print(f"INFO: {funcn}:\n{scn}")
            print(f"INFO: runDir= {self.run_dir(cache=False)} runDir(cache)= {self.run_dir(cache=True)}")

        egen = EgenNew(self._opt, sty=self, scn=scn, lines=lines, params=params, task_name=self._task_name, outDir=self.run_dir(cache=True), run_dir=self.run_dir(cache=False))
        #egen.run(debug=debug) #print(timeit.timeit(lambda: egen.run(True), number = 1))
        self.add(egen)
        return(egen)

    def symsets(self, symsets, by_indu=False):  #Study can potentially run multiple symsets
        if symsets  != "":
            self._symsets = symsets
        if ctx_debug(1):
            print(f"DEBUG: EgenStudy.symsets() {self._symsets}")
        if by_indu:
            return str2ssn_list(self._symsets)
        else:
            return self._symsets.split(",")
        
    def symset(self, shorthand=False):
        if shorthand:
            if self._symset.find('sector_')>=0:    
                return self._symset.split(r'_')[-1]
        return self._symset

    # def genFlds(self):
    #     if self._genFlds is None:
    #         self._genFlds = smart_load(f"{self.task_dir()}/genFlds.pkl", debug=True)
    #     return self._genFlds

    @timeit_m_real
    def run_study(self, symsets="", scn="", lines=None, params=None, debug=False, force=False, by_indu=False, opt=None, saveargs=[], need_run=None, detailed=False, is_binary_factor=False, gen_flds=True, genetic_algor=False):
        funcn = "EgenStudy.run_study"
        
        # po = Pool(5)
        finishedTasks = []
        for symset in self.symsets(symsets, by_indu):
            params=params if params else self._params
            params.update({'TASK_NAME': self._task_name})
            lines = lines if lines else self._lines
            scn = scn if scn else self._dts_cfg
            
            egen = self.make_egen(symset=symset, scn=scn, lines=lines if lines else self._lines, params=params, debug=debug, force=force)
            if need_run and not need_run(egen):
                continue

            task = egen.run_egen(debug=debug, force=force, gen_flds=gen_flds, genetic_algor=genetic_algor)
            
            if task is None:
                continue

            #task.save(what='setup')

            finishedTasks.append(task)
            # self._genFlds = task.getGenFlds()
            #po.apply_async(self.run_egen, (symset, scn, lines, params, debug, force), error_callback=self.return_err, callback=self.return_egen)

            if symset == 'E47':
                egen.save_unit_tests(self.unit_tests_dir())

            for savewhat in saveargs:
                task.save(savewhat)

            if False and debug:
                print(f"INFO: {funcn} #ofMkts= {len(self.mkts())}, symsets= {self.indus()}, rootDir= {self.rootDir()}")
        # po.close()
        # po.join()

        if self._models is not None:
            for task in finishedTasks:
                for modelNm in self._models:
                    outputTag = f"{modelNm}.{task._scn.dts_cfg}"
                    for symset in self.symsets(symsets):
                        self.run_model(task, modelNm, symset, outputTag, force=force, detailed=detailed, is_binary_factor=is_binary_factor)

        return finishedTasks
                        
    @timeit_m
    def run_model(self, task, modelNm, symset, outputTag, force=False, detailed=False, is_binary_factor=False):
        funcn = "EgenStudy.run_model"
        print(f"INFO: {funcn} modelNm= {modelNm}, symset= {symset}, respNm= {self.respNm()}, predNm= {self.predNm()}")
        if modelNm in ['q5', 'q9']:
            quantile_model(task, modelNm, self.symset(shorthand=True), outputTag, self.respNm(), self.predNm(), force=force)
        elif modelNm in ['q10']:
            decile_model(task, modelNm, self.symset(shorthand=True), outputTag, self.respNm(), self.predNm(), force=force)
        elif modelNm in ['cx']:
            cross_section_model(task, modelNm, self.symset(shorthand=True), outputTag, self.respNm(), self.predNm(), force=force, write_pred_file=False )
        elif modelNm in ['cx_fast']:
            cross_section_fast_model(task, modelNm, self.symset(shorthand=True), outputTag, self.respNm(), self.predNm(), force=force, write_pred_file=False, detailed=detailed, is_binary_factor=is_binary_factor)
        
            
    def add(self, egen):
        self._egens[f"{egen.tag()}.{egen.symset()}"] = egen

    def mkts(self):
        mktsL = []
        print(self._egens)
        for k,t in self._egens.items():
            mktsL.extend(t.mkts())
        return sorted(mktsL)

    def indus(self):
        indus = [k.split(r'.')[-1] for k in self._egens.keys()]
        return indus

    def len(self):
        return len(self._egens)

    def rootDir(self, cache=False):
        d = f"{rootdata()}/egen_study/{self._study_name}"
        if cache:
            d = f"{rootdata()}.local/egen_study/{self._study_name}"
        if not os.path.exists(d):
            QpsUtil.mkdir([d])
        return d

    def opt(self):
        return self._opt

    def study_name(self):
        return self._study_name
    
    def run_name(self):
        return self._run_name

    def task_name(self):
        return self._task_name

    def scn(self):
        return self._egens.values()[0].scn()

    def study_dir(self):
        #contains study params, and raw data for each symset/scn combo
        return self.rootDir()

    def run_dir(self, cache=False):
        #contains predictor raw calc
        return f"{self.rootDir(cache)}/{self._run_name}"

    def task_dir(self, cache=False):
        #contains predictor transformations
        return f"{self.run_dir(cache)}/{self._task_name}"

    def dta_dir(self, cache=False):
        return list(self.egens().values())[0].dta_dir(cache)

    def unit_tests_dir(self, cache=False):
        return self.run_dir(cache).replace(f"{rootdata()}", "/Fairedge_dev/qpsdata/unit_tests")

    def cal_avg(self, cols=['OUpnl', 'OU'], indus=None, debug=False):
        df = self.df()

        grp = df[cols].groupby(cols[1])
        sum = grp.sum()
        cnt = grp.count()
        avg = (sum/cnt)*10000.0 #bps
        #avg = avg[avg.index!=0] #Currently 0 can be dominated by IPO
        avg[avg.index==0] = 0.0 #Currently 0 can be dominated by IPO
        
        #print(avg)
        fn = f"{self.run_dir(cache=True)}/{self._dts_cfg}/avg/{indu}.db"
        if debug:
            print(f"INFO: dumping {fn}")
        QpsUtil.smart_dump(avg, fn)

        avg.loc[-10:10,].plot.bar(ylim=(-75,75), title=f"{indu} {time.strftime('[%Y/%m/%d %H:%M:%S]')}").get_figure().savefig(fn.replace('db', 'png'))

    @func_m_called
    def save(self, what='setup', debug=False):
        funcn = f"EgenStudy.save(what={what})"
        if what in ['setup']:
            QpsUtil.mkdir([self.dta_dir(cache=True), self.dta_dir(cache=False), self.unit_tests_dir()])
            if debug:
                print(f"INFO: Study.save {what} {self}")

            fn = f"{self.dta_dir(cache=False)}/fldlst.txt"
            #print(f"INFO: save fldlst {fn}")
            open(fn, 'w').write(self._lines)
            QpsUtil.smart_dump(self._params, f"{self.dta_dir(cache=False)}/params.pkl", debug=debug, title=funcn, verbose=1)
            QpsUtil.smart_dump(self._symsets, f"{self.dta_dir(cache=False)}/symsets.pkl", debug=debug, title=funcn, verbose=1)
            QpsUtil.smart_dump(self._opt, f"{self.dta_dir(cache=False)}/opt.pkl", debug=debug, title=funcn, verbose=1)
            #QpsUtil.smart_dump(self._scn, f"{self.dta_dir(cache=False)}/scn.pkl", debug=debug, title=funcn, verbose=1)
            QpsUtil.smart_dump(self._models, f"{self.dta_dir(cache=False)}/models.pkl", debug=debug, title=funcn, verbose=1)
            if self.genFlds() is not None:
                QpsUtil.smart_dump(self.genFlds(), f"{self.dta_dir(cache=False)}/genFlds.pkl", debug=debug, title=funcn, verbose=1)

        if what in ['data']:
            self.save_data()

    def show(self):
        for task in self._egens.values():
            task.show()

    def egens(self):
        return (self._egens)

    def export(self, tag):
        for task in self._egens.values():
            task.export(tag)

    def nsByVar(self):      
        return self._egens.values()[0].nsByVar()

    @timeit_m
    def save_data(self):
        funcn = "EgenStudy.save_data"
        ths = []

        # po = Pool(1) #Writing does not seems to help much
        for k in self._egens.keys():
            # (tag, symset) = k.split(r'.')
            taskDataFn = f"{self.task_dir(cache=False)}/{self._scn}/{self.symset()}.task.egen.pkl"
            QpsUtil.mkdir([os.path.dirname(taskDataFn)])
            print(f"INFO: {funcn} to {taskDataFn}")
            #QpsUtil.smart_dump(self._egens[k].dfMktDtmByFlds(), taskDataFn)
            df = self._egens[k].dfMktDtmByFlds(title=f"{funcn} {k}")
            df.reset_index(inplace=True)
            #df["RqInduCode"] = symset
            df = df.rename(columns={"Mkt":"mkt", "Dtm": "datetime"})
            smart_dump(df, taskDataFn)
            #pickle.dump(df, open(taskDataFn), 'wb'), protocol=4)
            #df.to_feather(taskDataFn.replace(".pkl", ".ftr"))
            #po.apply_async(QpsUtil.smart_dump, (self._egens[k].dfMktDtmByFlds(), taskDataFn), error_callback=self.return_err)
        # po.close()
        # po.join()

        # df = self.df()
        # print(funcn, df.shape, file=sys.stderr)
        # for symset in self.symsets.split(r','):
        #     if symset.find("ALL")>=0:
        #         QpsUtil.smart_dump(df, f"{self.task_dir}/unstacked.{self.dts_cfg}.{symset}.egen")

    def save_unit_tests(self, mkts, dts):
        funcn = 'save_unit_tests'
        df = self.df()
        pd.set_option('display.width', 1000)
        for mkt in mkts:
            if mkt not in df:
                continue
            for dt in dts:
                if dt in df.loc[mkt].index:
                    fn = f"{self.unit_tests_dir()}/{mkt}.{dt}.txt"
                    print(f"INFO: {funcn} writing {fn}")
                    open(fn, 'w').write(f"{df.loc[mkt].loc[dt]}")

    def __str__(self):
        return f"EgenStudy: (task_dir= {self.task_dir(cache=False)}, task_dir(cache)= {self.task_dir(cache=True)})"


def list_cmds(opt):
    for dts_cfg in "WPR":
        for tgt in getRespFldnms():
            print(f"python /Fairedge_dev/app_egen/EgenStudy.py --dts_cfg {dts_cfg} {tgt}")


if __name__ == '__main__':
    (opt, args) = get_options_sgen(list_cmds)

    for rundir in args:
        #studyName = rundir.split(r'/')[-2]
        print(f"INFO: study {rundir}")
        sty = EgenStudy().load(rundir)
        sty.run(symsets = opt.symset, scn = opt.scn)
        print(sty)

        # if studyName.find("_")>=0:

            # fldlstFn = f"{rundir}/fldlst.txt"
            # paramsFn = f"{rundir}/params.pkl"
            # for symset in opt.symset.split(r','):
            #     sty = EgenStudy(opt, studyName, symset, opt.scn)
            #     lines = '\n'.join(open(fldlstFn, 'r').readlines())
            #     params = pickle.loads(Path(paramsFn).read_bytes())
            #     print(f"INFO: params {params}")
            #     sty.run(lines, params)

            #     if studyName.find("return_streak")>=0:
            #         sty.cal_avg()

            #     sty.save()

    exit(0)





