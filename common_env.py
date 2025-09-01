#!/cygdrive/c/Anaconda3/python.exe

import sys
import os

from optparse import OptionParser
from common_env_vps import srcs_vps
from common_env_apps import srcs_apps

def getOptions():
    parser = OptionParser(description="common_env")
    parser.add_option("--pycver",
                    dest="pycver",
                    help="pycver",
                    metavar="pycver",
                    type="str",
                    default="lnx321")
                
    parser.add_option("--list",
                      dest="list",
                      help="list",
                      metavar="list",
                      type="str",
                      default=None)

    parser.add_option("-d",
                      "--debug",
                      dest="debug",
                      help="debug",
                      metavar="debug",
                      type="int",
                      default=0)

    parser.add_option("-f",
                      "--force",
                      dest="force",
                      help="force rerun",
                      metavar="force",
                      type="int",
                      default=0)
                      
    parser.add_option("-p",
                      "--postrun",
                      dest="postrunOnly",
                      help="postrun only",
                      metavar="postrunonly",
                      type="int",
                      default=0)
                     
    parser.add_option("-c",
                      "--clean",
                      dest="clean",
                      help="clean runtime",
                      metavar="cleanruntime",
                      type="int",
                      default=0)
                      
    (opt, args) = parser.parse_args()
    return (opt, args)

def get_verbose_delta(fp):
    if fp is None:
        return 1

    if fp.find(".desc") < 0 and \
        fp.find(".XSH") < 0 and \
        fp.find("/params") < 0 and \
        fp.find("/pars") < 0 and \
        fp.find("/opt") < 0 and \
        fp.find("/genFlds") < 0 and \
        fp.find("/models") < 0 and \
        fp.find(".fldPaths") < 0 and \
        fp.find("/symsets") < 0 and \
        fp.find("/raw") < 0 and \
        fp.find("/20") < 0:
        return 0
    else:
        return 1

ENV = None
def env():
    global ENV
    if ENV is None:
        ENV = Env()
    return ENV

class Env:
    def __init__(self):
        self.SMART_LOAD_QUIET = {}
        self.SMART_LOAD_QUIET['title'] = True
        self.SMART_LOAD_QUIET['get_symset_sws'] = True
        self.SMART_LOAD_QUIET['generator_vps_instru'] = True
        self.SMART_LOAD_QUIET['all_cs_db'] = True
        self.SMART_LOAD_QUIET['gen_symsets'] = True
        self.SMART_LOAD_QUIET['all_stock_0'] = True
        self.SMART_LOAD_QUIET['all_stock_1'] = True
        self.SMART_LOAD_QUIET['patch_fld'] = True
        self.SMART_LOAD_QUIET = {} #disable load quiet

        self._print_smart_load = 0
        if "PRINT_SMART_LOAD" in os.environ:
            self._print_smart_load = int(os.environ["PRINT_SMART_LOAD"])

        self._print_smart_dump = 0
        if "PRINT_SMART_DUMP" in os.environ:
            self._print_smart_dump = int(os.environ["PRINT_SMART_DUMP"])

        self._print_get_fld_fgw = 0
        if "PRINT_GET_FLD_FGW" in os.environ:
            self._print_get_fld_fgw = int(os.environ["PRINT_GET_FLD_FGW"])

    def print_smart_load(self, fn=None, title=None):
        return self._print_smart_load

    def print_smart_dump(self, fn=None, title=None):
        return self._print_smart_dump + get_verbose_delta(fn)

    def print_get_fld_fgw(self):
        return self._print_get_fld_fgw

def apps_using_src(apptype='nopyc'):
    pysrc = {}
    if apptype in ['nopyc', 'apps']:
        srcs_apps(pysrc)
    
    if apptype in ['nopyc', 'vps']:
        srcs_vps(pysrc)

    return pysrc

def write_env(opt, envFn, flag, pycver=None):
    print(f"INFO: env_file= {envFn}", file=sys.stderr)
    fp = open(envFn, 'w')
    print(f"#print flags", file=fp)
    for flagFn in ["PRINT_SMART_LOAD", "PRINT_SMART_DUMP", "PRINT_GET_FLD_FGW"]:
        print(f"export {flagFn}={flag}", file=fp)

    #/Fairedge_rel/factcomb/bin/lnx307/factcomb.pyc

    for apptype in ['apps', 'vps']:
        print(f"#{apptype}", file=fp)
        pysrc = apps_using_src(apptype)
        for app in sorted(pysrc.keys()):
            if pysrc[app].find(':')>=0:
                (src, ver) = pysrc[app].split(r':')
                if pycver is not None:
                    ver = pycver
            else:
                src = pysrc[app]
                ver = None
            if ver is None:
                print(f"export cmd_{app}={src}", file=fp)
            else:
                print(f"export cmd_{app}=/Fairedge_rel/{app}/bin/{ver}/{app.split(r'__')[0]}.pyc", file=fp)

if __name__ == '__main__':
    (opt, args) = getOptions()
    if opt.list is not None:
        if opt.list in ['vps']:
            # apps = [k for k,v in apps_using_src('vps').items() if k.find("Vps")==0 or k.find("Pgan")>=0]
            # apps.extend(['adjust_tgts', 'send_target_fac', 'PortTgtsSend'])
            apps = [k for k,v in apps_using_src('vps').items()]
        else:
            apps = [k for k,v in apps_using_src('apps').items()]

        apps.sort()
        print(' '.join(apps))
        exit(0)

    flag = 0
    if opt.debug:
        flag = -1

    envFn = "/Fairedge_dev/app_common/env.bash"
    write_env(opt, envFn, flag)
    write_env(opt, "/Fairedge_dev/app_common/env_debug.bash", -1)
    write_env(opt, "/Fairedge_dev/app_common/env_release.bash", 0)
    write_env(opt, "/Fairedge_dev/app_common/env_dev.bash", 0, pycver=opt.pycver)


