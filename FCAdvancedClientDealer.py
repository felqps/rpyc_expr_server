import sys
import os
import zmq
import uuid
import pickle
import re
import time
import threading
from pathlib import Path
from optparse import OptionParser
from QpsSys import gethostname,gethostip
from multiprocessing import Pool,Manager
import queue
from FCWorker import load_addr_cfg
import QpsUtil
from qdb_options import get_FCAdvancedClient_options
import pandas as pd
from shared_cmn import *
from QpsLogging import Logger
from common_basic import *


def get_identity(priority="Z"):
    identity = f"C_{gethostname()}_{priority}_{uuid.uuid1()}"
    return identity

def left_align(df: pd.DataFrame):
    pd.set_option('display.max_colwidth', None)
    df.style.set_properties(**{'text-align': 'left'}).set_table_styles([dict(selector='th', props=[('text-align', 'left')])])
    # left_aligned_df = df.style.set_properties(**{'text-align': 'left'})
    # left_aligned_df = left_aligned_df.set_table_styles(
    #    [dict(selector='th', props=[('text-align', 'left')])]
    #)
    return df

class FCAdvancedClientDealer():
    def __init__(self, options):
        self.opt = options
        load_addr_cfg(self.opt)
        self.jobs = {}
        self.job_queue = Manager().Queue(-1)  #If maxsize is <= 0, the queue size is infinite
        self.all_is_completed = False
        self.cnt_completed = 0

        log_dir = "/Fairedge_dev/app_qpscloud/run"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        # self.logger = Logger(f'{log_dir}/FCAdvancedClientDealer_{QpsUtil.dt_suffix()}.log').logger
        self.logger = Logger(f'{log_dir}/FCAdvancedClientDealer_{QpsUtil.dt_suffix()}.log').getFileLogger("FCAdvancedClientDealer")

    def run_cmd_in_worker(self, job:dict, job_queue:"Queue"):
        assert type(job["hosts"]) == type([]), "Hosts must be list."
        client_url = "tcp://%s:%s"%(self.opt.addr, self.opt.client_port)
        context = zmq.Context.instance()
        socket = context.socket(zmq.DEALER)

        socket.identity = get_identity(self.opt.priority).encode("utf-8")
        socket.connect(client_url)
        socket.send_multipart([pickle.dumps({"hosts":job["hosts"], "heavy_score":job["heavy_score"], "timeout":job["timeout"], "cmd":job["cmd"], "flag": "JOB"})])
        # job["status"] = "Running"
        # job_queue.put(job, block=False)
        reply = socket.recv()
        try:
            result = pickle.loads(reply)
        except Exception as err:
            result = {"result": err}
            self.logger.exception(err)

        job["status"] = "Completed"
        # job["result"] = result
        job.update(result)
        job_queue.put(job)
        socket.close()
        return job

    def check_worker_available(self, hosts:list=[], heavy_score:int=0):
        assert type(hosts) == type([]), "Hosts must be list."
        client_url = "tcp://%s:%s"%(self.opt.addr, self.opt.client_port)
        context = zmq.Context.instance()
        socket = context.socket(zmq.DEALER)

        socket.identity = get_identity(self.opt.priority).encode("utf-8")
        socket.connect(client_url)
        socket.send_multipart([pickle.dumps({"hosts":hosts, "heavy_score":heavy_score, "flag": "CHECK_WORKER_AVAILABLE"})])
        reply = socket.recv()
        try:
            result = int(reply)
        except Exception as err:
            result = -1
            self.logger.exception(err)
        socket.close()
        return result

    def remove_all_jobs(self):
        client_url = "tcp://%s:%s"%(self.opt.addr, self.opt.client_port)
        context = zmq.Context.instance()
        socket = context.socket(zmq.DEALER)

        socket.identity = get_identity(self.opt.priority).encode("utf-8")
        socket.connect(client_url)
        socket.send_multipart([pickle.dumps({"flag": "REMOVE", "hostname": gethostname()})])
        reply = socket.recv()
        try:
            result = reply.decode("utf-8")
        except Exception as err:
            result = err
            self.logger.exception(err)

        socket.close()
        return result

    def load_jobs(self, lines):
        self.cnt_total = len(lines)
        jobs_group = {}
        for line in lines:
            if not line:
                continue

            ele = re.split(r'\=', line)
            group_level, cmd, cmd_content = ele[:3]
            group_name, level_1, level_2 = group_level.strip().split(r'.')

            args = {}
            for s in ele[3:]:
                k_v = re.split(r'\s+', s.strip())
                if s and len(k_v) == 2:
                    args[k_v[0]] = k_v[1]

            hosts = []
            if "hostname" in args and args.get("hostname").find("any") < 0:
                hosts = args.get("hostname").split(r',')

            heavy_score = 20
            if "heavy_score" in args:
                heavy_score = int(args.get("heavy_score"))

            timeout = 60*60
            if "timeout" in args:
                timeout = int(args.get("timeout"))

            if group_name not in jobs_group:
                jobs_group[group_name] = []

            jobs_group[group_name].append({
                "group_name": group_name,
                "level": f"{level_1}.{level_2}",
                "cmd": cmd.strip(),
                "heavy_score": heavy_score,
                "cmd_content": cmd_content,
                "hosts": hosts,
                "timeout": timeout,
                "status": "Waiting",  #Waiting,Running,Error,Completed
                "dependencies": [],
            })
        jobs = self.build_job_dependencies(jobs_group)
        self.jobs = jobs
        # [print(f"{k}:{job}") for k,job in self.jobs.items()]
        # [print(f"{k}:{job['group_name']}:{job['level']}:{len(job['dependencies'])}") for k,job in self.jobs.items()]
        # exit()

    def build_job_dependencies(self, jobs_group:dict):
        all_jobs = {}

        zzz_dependencies = []
        other_dependencies = []
        for job_group in jobs_group.values():
            level_list = sorted(list(set([job["level"] for job in job_group])))
            for job in job_group:
                if job["level"] == level_list[-1] and job["group_name"] != "ZZZ" and job["group_name"] != "AAA":
                    zzz_dependencies.append(job["cmd"])
                if job["group_name"] == "AAA" and job["level"] == level_list[-1]:
                    other_dependencies.append(job["cmd"])
        
        for job_group in jobs_group.values():
            level_list = sorted(list(set([job["level"] for job in job_group])))
            for job in job_group:
                upper_level = get_upper_level(level_list, job["level"])
                # upper_level = job["level"] - 1
                if upper_level:
                    job["dependencies"] = [j["cmd"] for j in job_group if j["level"] == upper_level]
                
                if job["level"] == level_list[0]:
                    if job["group_name"] == "ZZZ":
                        job["dependencies"].extend(zzz_dependencies)
                
                    if job["group_name"] != "ZZZ" and job["group_name"] != "AAA":
                        job["dependencies"].extend(other_dependencies)

                all_jobs[job["cmd"]] = job

        # without_aaa_zzz = [job["cmd"] for job_group in jobs_group.values() for job in job_group if job["group_name"] not in  ["ZZZ", "AAA"]]
        # all_aaa = [job["cmd"] for job_group in jobs_group.values() for job in job_group if job["group_name"] == "AAA"]

        # for job_group in jobs_group.values():
        #     level_list = sorted(list(set([job["level"] for job in job_group])))
        #     for job in job_group:
        #         upper_level = get_upper_level(level_list, job["level"])
        #         # upper_level = job["level"] - 1
        #         if upper_level:
        #             job["dependencies"] = [j["cmd"] for j in job_group if j["level"] == upper_level]
                
        #         if job["group_name"] == "ZZZ" and job["level"] == level_list[0]:
        #             job["dependencies"].extend(without_aaa_zzz)
                
        #         if job["level"] == level_list[0] and job["group_name"] != "ZZZ" and job["group_name"] != "AAA":
        #             job["dependencies"].extend(all_aaa)
        #             # print(job)
        #             # print(len(job["dependencies"]))

        #         all_jobs[job["cmd"]] = job


        # l_t = sorted(all_jobs.items(), key=lambda x:x[1]["level"], reverse=False)
        # all_jobs = {t[0]:t[1] for t in l_t}
        return all_jobs

    def run_client(self, po):
        all_is_completed = True
        # self.logger.info("&&"*20)
        self.update_job()
        for k,job in self.jobs.items():
            if job["status"] != "Completed":
                all_is_completed = False

            if self.is_ready(job):
                result = self.check_worker_available(job["hosts"], job["heavy_score"])
                # self.logger.info(f"{result} {job['hosts']} { job['heavy_score']}")
                # self.logger.info(job)
                if result == 0:
                    self.logger.warning(f"No such workers: {job['hosts']}")
                elif result == 1:
                    job["status"] = "Running"
                    po.apply_async(self.run_cmd_in_worker, (job, self.job_queue), callback=self.cb_function, error_callback=lambda e:print(e))
                    # result = po.apply_async(self.run_cmd_in_worker, (self.jobs[k], self.job_queue), callback=self.cb_function)
                    # result.get()
                    time.sleep(0.1)
                elif result == -1:
                    pass

        if all_is_completed:
            self.all_is_completed = True

    def is_ready(self, job):
        _is_ready = True
        if job["status"] != "Waiting":
            return False

        for d in job["dependencies"]:
            if self.jobs[d]["status"] != "Completed":
                _is_ready = False
                break
        return _is_ready

    def start(self):
        try:
            assert self.opt.pool_size > 0, f"Pool size must be greater than 0."
            self.logger.info(f"Pool size: {self.opt.pool_size}")
            po = Pool(self.opt.pool_size)
            
            while not self.all_is_completed:
                self.run_client(po)
                time.sleep(0.01)
            
            po.close()
            po.join()
            self.logger.info(f"Complete {len([v for v in self.jobs.values() if v['status'] == 'Completed'])} jobs.")
        except KeyboardInterrupt:
            self.logger.info("="*80)
            result = self.remove_all_jobs()
            self.logger.warning(result)
            exit(1)
        except Exception as e:
            self.logger.exception(e)

    def update_job(self):
        while True:
            try:
                job = self.job_queue.get(block=False)
                self.jobs[job["cmd"]] = job
            except queue.Empty as e:
                break
            except Exception as e:
                self.logger.exception(e)
                continue
        # while self.job_queue.qsize() != 0:
        #     job = self.job_queue.get()
        #     self.jobs[job["cmd"]] = job

    def cb_function(self, result):
        try:
            info = {
                'group_name': result["group_name"],
                'level': result["level"],
                'cmd': result["cmd"],
                'cmd_content': result["cmd_content"],
                'status': 'NA',
                'host': 'NA',
                'heavy_score': result["heavy_score"],
                'tm': round(int(result['tm'])/1000)
            }


            if "result" in result:
                if type(result["result"]) == type(dict()):
                    info.update(result["result"])
                else:
                    info['status'] = result["result"]

            # df = pd.DataFrame([info])
            # df = df[['group_name', 'level', 'host', 'status', 'cmd']]

            #logging.info(info)
            #print(left_align(df))
            self.cnt_completed = self.cnt_completed + 1
            #print("%s %s %s %s %s"%(info['group_name'], info['level'], info['host'], info['status'], str_minmax(info['cmd'])))
            if info['cmd'].find("python")>=0:
                info['cmd'] = info['cmd'][info['cmd'].find("python"):]
            # self.logger.info("%s %s %s %s %s %s %4d/%04d %s"%(info['group_name'], info['level'], info['host'], info['heavy_score'], info['status'], 
            #     str_minmax(info['cmd'], use_only=['=p', '=P', '=T', '=A', '=U']), self.cnt_completed, self.cnt_total, info['tm']))
            print(info['status'])
            self.logger.info(" %s %s %s %s %s %4d/%04d %s"%(info['group_name'], info['level'], info['host'], info['heavy_score'], 
                str_minmax_without_status(info['cmd']), self.cnt_completed, self.cnt_total, info['tm']))        
        except Exception as e:
            self.logger.exception(e)

def get_upper_level(level_list:list, value:str):
    index = level_list.index(value) - 1
    upper_level = ""
    if index >= 0:
        upper_level = level_list[index]
    return upper_level

def list_cmds():
    cmds = [
        "python /Fairedge_dev/app_qpscloud/FCAdvancedClientDealer.py --addr_cfg /qpsdata/config/FCScheduler.cfg --pool_size 10 /qpsdata/temp/ace.txt",
    ]
    print('\n'.join(cmds))


if __name__ == "__main__":
    (options, args) = get_FCAdvancedClient_options(list_cmds)
    if options.list_cmds:
        list_cmds()
        exit(0)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)
    fc_client = FCAdvancedClientDealer(options)

    lines = []
    if len(args)>=1:
        for arg in args:
            jobs = QpsUtil.open_readlines(arg)
            print(f"Load {len(jobs)} job from: {arg}")
            lines.extend(jobs)
    else:
        lines = [ln.strip() for ln in sys.stdin.readlines() if ln.find('=')>=0]

    print('\n'.join(lines))
    fc_client.load_jobs(lines)

    fc_client.start()
