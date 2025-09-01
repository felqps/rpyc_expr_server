import sys
import os
import zmq
import uuid
import pickle
import random
import time
import datetime
import telnetlib
from pathlib import Path
from optparse import OptionParser
from multiprocessing import Pool,Process
from QpsLogging import logging
from QpsSys import gethostname,gethostip
import QpsUtil
import subprocess
import io
import signal
from common_basic import list_modules
from pynats import NATSClient
from VpsCfg import VpsCfg
import threading
import psutil
import subprocess


def get_FCWorker_options():
    parser = OptionParser(description="FCWorker")
    parser.add_option("--addr_cfg",
                    dest = "addr_cfg",
                    type = "str",
                    help = "addr_cfg (default: %default)",
                    metavar = "addr_cfg",
                    default = "/qpsdata/config/FCScheduler.cfg")
    parser.add_option("--worker_cfg",
                    dest = "worker_cfg",
                    type = "str",
                    help = "worker_cfg (default: %default)",
                    metavar = "worker_cfg",
                    default = "/qpsdata/config/FCWorker.cfg")
    parser.add_option("--pool_size",
                    dest = "pool_size",
                    type = "int",
                    help = "pool_size (default: %default)",
                    metavar = "pool_size",
                    default = 0)
    parser.add_option("--vps_cfg",
                    dest="vps_cfg",
                    type="str",
                    help="vps_cfg (default: %default)",
                    metavar="vps_cfg",
                    default="qpscloud",)
    parser.add_option("--heartbeat_interval",
                    dest="heartbeat_interval",
                    type="int",
                    help="heartbeat_interval (default: %default)",
                    metavar="heartbeat_interval",
                    default=60,)
    parser.add_option("--list_cmds",
                    dest="list_cmds",
                    type = "str",
                    help="list_cmds (default: %default)",
                    metavar="list_cmds",
                    default="")
    parser.add_option("--list_modules",
                      dest="list_modules",
                      type="str",
                      help="list_modules (default: %default)",
                      metavar="list_modules",
                      default="")

    opt, args = parser.parse_args()
    #from common_basic import list_modules
    if opt.list_modules:
        list_modules()
        exit(0)

    return (opt, args)

def run_worker(worker_url, index, vps_cfg):
    try:
        # context = zmq.Context.instance()
        # socket = context.socket(zmq.REQ)
        # socket.identity = get_identity().encode("utf-8")
        # socket.connect(worker_url)
        socket = init_socket(worker_url, index)
        
        # Tell the broker we are ready for work
        socket.send_multipart([gen_reply_msg(flag="READY")])
        th = None
        proc_id = {"proc_id": None}
        while True:
            try:
                data, = socket.recv_multipart()
                data = pickle.loads(data)
                client_id = data.get("client_id")
                flag = data.get("flag")
                worker_id = get_identity(index=index)
                if flag == "ERROR":
                    logging.error(data)
                    continue
                elif flag == "job_info":
                    p = psutil.Process(os.getpid())
                    info = {
                        "cpu": p.cpu_percent(),
                        "memory": p.memory_percent(),
                    }
                    socket.send_multipart([gen_reply_msg(client_id, flag, info)])
                elif flag == "abort_job" or flag == "restart_job":
                    if th and hasattr(th, "is_alive") and th.is_alive():
                        p = proc_id["proc_id"]
                        os.killpg(p.pid, signal.SIGTERM)
                        # QpsUtil.terminator_thread(th)
                        socket.send_multipart([gen_reply_msg(client_id, flag, format_output(worker_id, flag, f"{'Restart job success' if flag == 'restart_job' else 'Abort job success'}"))])
                    else:
                        socket.send_multipart([gen_reply_msg(client_id, flag, format_output(worker_id, flag, "No such running job"))])
                else:
                    cmd = data.get("cmd")
                    logging.info(f"{socket.identity} receive from {client_id.decode('utf-8')}")
                    logging.info(f"Cmd: {cmd}")
                    # result = run_cmd(cmd)
                    th = threading.Thread(target=run_cmd, args=(cmd, worker_id, vps_cfg, socket, client_id, proc_id))
                    th.start()
            except Exception as err:
                logging.exception(err)
                socket.send_multipart([gen_reply_msg(client_id, "ERROR", f"Exception in worker '{socket.identity}', with error: {err}")])
    except Exception as e:
        logging.exception(f"Worker '{socket.identity}' exception error: {e}")

def check_heartbeat(worker_url, heartbeat_interval, recv_timeout=10*1000):
    socket = init_socket(worker_url=worker_url, index=1000, recv_timeout=recv_timeout)
    socket.send_multipart([gen_reply_msg(flag="Heartbeat")])
    while True:
        try:
            _ = socket.recv_multipart()
        except Exception as e:
            logging.exception(e)
        time.sleep(heartbeat_interval)
        socket.send_multipart([gen_reply_msg(flag="Heartbeat")])

def init_socket(worker_url, index, recv_timeout=None):
    context = zmq.Context.instance()
    # socket = context.socket(zmq.REQ)
    socket = context.socket(zmq.DEALER)
    if recv_timeout:
        socket.setsockopt(zmq.RCVTIMEO, int(recv_timeout))

    socket.identity = get_identity(index=index).encode("utf-8")
    socket.connect(worker_url)
    return socket

def clear_old_workers(worker_url):
    socket = init_socket(worker_url, 1001)
    socket.send_multipart([gen_reply_msg(flag="CLEAR")])
    # status, text = socket.recv_multipart()
    data, = socket.recv_multipart()
    data = pickle.loads(data)
    logging.info(data.get("notes"))

def start_workers(opt):
    assert opt.pool_size > 0, f"Pool size must be greater than 0."
    logging.info(f"Pool size: {opt.pool_size}")
    worker_url = "tcp://%s:%s"%(opt.addr, opt.worker_port)
    logging.info(f"Connet to {worker_url}")
    vps_cfg = VpsCfg(opt.vps_cfg)

    #clear old workers
    process = Process(target=clear_old_workers, args=(worker_url,))
    process.start()
    process.join()
    
    #register new workers
    po = Pool(opt.pool_size + 1)
    for i in range(int(opt.pool_size)):
        po.apply_async(run_worker, (worker_url, i+1, {"nats_oms":vps_cfg.nats_oms, "nats_prefix":vps_cfg.nats_prefix}), error_callback=lambda err:print(err))

    po.apply_async(check_heartbeat, (worker_url,opt.heartbeat_interval), error_callback=lambda err:print(err))

    while 1:
        try:
            telnetlib.Telnet(host=opt.addr, port=opt.worker_port)
            if opt.sche_closed:
                po.terminate()
                po.join()
                opt.sche_closed = False
                break
        except Exception as err:
            if not opt.sche_closed:
                logging.warning(f"FCScheduler is closed")
            opt.sche_closed = True
        time.sleep(0.1)

    time.sleep(20)
    logging.info(f"Will reconnect to scheduler")
    start_workers(opt)


    # po.close()
    # po.join()

def run_cmd_test(cmd):
    cmd_result = os.popen(cmd)
    result_fn = ""
    for line in cmd_result.readlines():
        line = line.strip()
        if not line:
            continue
        if "resultFn" in line:
            result_fn = line.split(":")[1].strip()

    if os.path.exists(result_fn):
        result = pickle.load(open(result_fn, "rb"))
    else:
        result = ""

    return result

def run_cmd(cmd, worker_id, vps_cfg, socket, client_id, proc_id):
    if os.path.exists("/home/shuser/FCWorkerTestSwitch"):
        return run_cmd_test(cmd)
    else:
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=-1, preexec_fn=os.setsid)
        proc_id["proc_id"] = proc
        stdout, stderr  = proc.communicate()
        proc.wait()

        rt = (stdout + stderr).decode("utf-8").strip()
        # rt = "\n".join([f"{worker_id}:{cmd} >>>>> {ln}" for ln in rt.split("\n")])
        rt = format_output(worker_id, cmd, rt)
        if not rt:
            rt = f"{worker_id}:{cmd} >>>>> no log"

        send_info_to_nats(vps_cfg, worker_id, cmd, stderr.decode("utf-8").strip())
        socket.send_multipart([gen_reply_msg(client_id, "REPLY", rt)])

def format_output(worker_id, cmd, result, add_tm=False, filter=""):
    if add_tm:
        return "\n".join([f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:{worker_id}:{cmd} >>>>> {ln}" for ln in result.split("\n")])
    else:
        return "\n".join([f"{worker_id}:{cmd} >>>>> {ln}" for ln in result.split("\n")])

def send_info_to_nats(vps_cfg, worker_id, cmd, info, flag="error"):
    try:
        # print("=="*20, vps_cfg)
        with NATSClient(vps_cfg.get("nats_oms")) as client:
            subject = f"{vps_cfg.get('nats_prefix')}.FCWorker.{flag}"
            if not info:
                info = 'Success'
            info = format_output(worker_id, cmd, info, True)
            client.publish(subject, payload=pickle.dumps(info))
    except Exception as e:
        logging.exception(e)


def get_identity(hostname=gethostname(), index=random.randint(1000,10000)):
    identity = f"W_{hostname}_{index}"
    return identity

def load_addr_cfg(opt):
    assert os.path.exists(opt.addr_cfg), f"No such file: {opt.addr_cfg}"

    cfg = eval(Path(opt.addr_cfg).read_text())
    opt.addr = cfg.get("addr")
    opt.worker_port = cfg.get("worker_port")
    opt.client_port = cfg.get("client_port")

def gen_reply_msg(client_id="", flag="", result=""):
    msg = {
        "client_id": client_id,
        "flag": flag,
        "result": result,
    }
    # print(msg)
    return pickle.dumps(msg)

def list_cmds():
    cmds = [
        "python /Fairedge_dev/app_qpscloud/FCWorker.py --addr_cfg /qpsdata/config/FCScheduler.cfg --pool_size 3",
    ]
    print('\n'.join(cmds))


if __name__ == "__main__":
    (opt, args) = get_FCWorker_options()
    if opt.list_cmds:
        list_cmds()
        exit(0)

    if opt.pool_size <= 0:
        POOLSIZECFG = eval(Path(opt.worker_cfg).read_text())
        hostnm = gethostname()
        if hostnm in POOLSIZECFG:
            opt.pool_size = POOLSIZECFG[hostnm]
        else:
            opt.pool_size = 1

    load_addr_cfg(opt)
    opt.sche_closed = False
    start_workers(opt)

