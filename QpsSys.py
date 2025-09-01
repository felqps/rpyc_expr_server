import subprocess
import os
import sys
import re
import glob
import toNative
import socket
import ctypes
import inspect
import datetime
#from common_basic import *

#System related

def exists(fn):
    return fn!="" and os.path.exists(dos_style(fn))

def gethostname():
    return socket.gethostname()

def gethostip(): 
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def getuser():
    #getpass.getuser()
    p = run_command("whoami", waitFinish=1)
    for ln in p:
        ln = ln.strip()
        if sys.platform=="win32":	#windows
            splitList = ln.decode("utf-8").split(r'\\')
            return splitList[1] if len(splitList)>1 else splitList[0]
        else:	#unixlike
            return ln

def touch(fname, verbose=0):
    if(verbose):
        print >> "touch ",fname
    try:
        os.utime(fname, None)
    except:
        open(fname, 'a').close()

def touchList(fnList, verbose=0):
    for fn in fnList:
        touch(fn, verbose)

def Open(fn, opt):
    dir = os.path.dirname(fn)
    mkdir([dir])
    return open(fn, opt)

def is_empty_line(ln):
    tmp = ln.replace(' ', '')
    tmp = tmp.replace('\t', '')
    tmp = tmp.replace('\r', '')
    tmp = tmp.replace('\n', '')
    return tmp == "" or (len(tmp)>0 and tmp[0] == '#')

def pipe_cmd(cmd, dryrun=0, pipebash=0):
    if dryrun:
        print >> sys.stderr, "INFO(DRYRUN): %s"%(cmd)
    else:
        if (cmd.find(".bash")>=0) and pipebash==0:
            print >> sys.stderr, "INFO: pipe(ignored_for_bash) %s"%(cmd)
            run_cmd(cmd)
        else:
            print >> sys.stderr, "INFO: pipe(run_command) %s"%(cmd)
            return run_command(cmd, waitFinish=0)

def isCmdStarted(cmd,startcmd):
    if sys.platform=="win32":	#windows
        return cmd.find("%s.exe"%startcmd)>=0
    else:	#unixlike
        return cmd.startswith("%s "%startcmd)

def isCmdContained(cmd,startcmd):
    if sys.platform=="win32":	#windows
        return cmd.find("%s.exe"%startcmd)>=0
    else:	#unixlike
        return cmd.find(" %s "%startcmd)>=0

def run_cmd(cmd, dryrun=0, debug=0, expandHat=1, quiet=0, bash=0, keepbackslash=0):
    #print >>sys.stderr,cmd
    if (quiet):
        if keepbackslash:
            cmd = '%s -c "%s 1> %s 2> %s"'%(toNative.getCmdBin("bash"),cmd,toNative.getTempOutFile("c:/temp/dummy_stdout.txt"),toNative.getTempOutFile("c:/temp/dummy_stderr.txt"))
        else:
            cmd = '%s -c "%s 1> %s 2> %s"'%(toNative.getCmdBin("bash"),cmd.replace("\\", "/"),toNative.getTempOutFile("c:/temp/dummy_stdout.txt"),toNative.getTempOutFile("c:/temp/dummy_stderr.txt"))

    if (bash):
        if keepbackslash:
            cmd = '%s -c "%s"'%(toNative.getCmdBin("bash"),cmd)
        else:
            cmd = '%s -c "%s"'%(toNative.getCmdBin("bash"),cmd.replace("\\", "/"))
            
    if sys.platform=="win32" and ((expandHat and cmd.find("bash")<0) or (isCmdContained(cmd,"cp"))):
        if cmd.find('^^') < 0:
            cmd = cmd.replace('^', '^^')

    if dryrun:
        print("DRYRUN: QpsUtil.run_cmd() %s"%(cmd))

    if 0 or debug:
        print("INFO(debug): QpsUtil.run_cmd() %s"%(cmd))

    if not dryrun:
        ret = os.system(cmd)
        return ret

    return True

def macro_expand(inFn, outFn, macroFn, kvOverwrite={}, dryrun=False, verbose=False, debug=False, quiet=False):
    if verbose and not quiet:
        print >> sys.stderr, "INFO: QpsUtil.macro_expand\t in= %s\n\tout= %s\n\tmacro= %s\n\tkvoverwrite= %s"%(inFn, outFn, macroFn, kvOverwrite)
    kv = file2kv(macroFn)
    for k in kvOverwrite:
        kv[k]=kvOverwrite[k]
        
    macroExpand = kv2macroexpand(kv)
    sed_replace(macroExpand, inFn, outFn, verbose=debug)

    if debug:
        print >> sys.stderr, "INFO: macro_expand() running %s"%(outFn)
        for ln in open_readlines(outFn):
            print >> sys.stderr, '\t', ln

    if not dryrun:
        run_bash_tmpl(outFn)

def find_and_run(inDir, globPatterns, params=[], inBackground="", dryrun=0, verbose=0, usepipe=0, exe2py=True, use_freeze_runner=False):
    for globStr in globPatterns.split(r';'):
        if verbose:
            print >> sys.stderr, "INFO: QpsUtil.find_and_run --glob %s"%(globStr)

        bashFound = 0
        for bashTmpl in glob.glob("%s/%s"%(inDir, globStr)):
            run_bash_tmpl(bashTmpl, params, inBackground, dryrun, verbose, usepipe, exe2py, use_freeze_runner)
            bashFound += 1
        if bashFound <= 0:
            print >> sys.stderr, "WARN: Cannot find executable matching \'%s\' in directory %s"%(globPatterns, inDir)


def run_bash_tmpl(bashTmpl, params=[], inBackground="", dryrun=0, verbose=0, usepipe=0, exe2py=True, use_freeze_runner=False):
    if 1:
        if 1:
            param = " ".join(params)
            bashTmpl = bashTmpl.replace("\\", "/")
            if (bashTmpl.find(".make.bash")>=0):
                scnDir = bashTmpl.split()[-1].split(r'/')[-4]
                if scnDir.find("daily")<0:
                    print >> sys.stderr, "INFO: --do make ignore intraday scenario %s, %s"%(scnDir, bashTmpl)
                    return

            #Add a step to further processing these bash files
            #For example, we might choose to run .py instead of .exe or add an extra commandline option not in the bash 
            bashReal = "%s.from_tmpl"%(bashTmpl)

            if verbose:
                print >> sys.stderr, "INFO: QpsUtil.find_and_run bash template %s"%(bashTmpl)
                print >> sys.stderr, "INFO: QpsUtil.find_and_run bash executed  %s"%(bashReal)

            lns = []
            mkCmd="time make -j 8 -k -f "
            for ln in open_readlines(bashTmpl):
                ln = ln.strip()
                if exe2py==True and ln.find(".bn")>=0:
                    lnOrig = ln
                    ln = ln.replace(".exe", ".py")
                    for v in ("202","203","204","205","206","207","208"):
                        ln = ln.replace(".bn%s"%(v), "")
                    print >> sys.stderr, "INFO: QpsUtil.run_bash_tmpl exe2py overwrite,  in: %s"%(lnOrig)
                    print >> sys.stderr, "INFO: QpsUtil.run_bash_tmpl exe2py overwrite, out: %s"%(ln)

                if use_freeze_runner==True and ln.find(mkCmd)>=0:
                    ln = ln.replace(mkCmd, "")
                    ln = ln.replace(" ", "")
                    ln = '%s%s\"'%('curl "192.168.20.119:28101/make?do=run&file=', ln)
                lns.append(ln)

            if sys.platform!="win32":
                open(bashReal, 'w').write(("\n".join(lns)).replace("c:/", "/"))
            else:
                open(bashReal, 'w').write("\n".join(lns))

            if sys.platform!="win32":
                print >> sys.stderr, "os.chmod: %s"%(bashReal)
                try:
                    os.chmod(bashReal, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
                except:
                    print >> sys.stderr, "os.chmod failed"
            else:
                run_command("dos2unix %s"%(bashReal))
                
            if usepipe:
                return pipe_cmd("%s -c \"%s%s\""%(toNative.getCmdBin("bash"), bashReal, "%s"%(inBackground)), dryrun, pipebash=1)
            else:
                run_cmd("%s -c \"%s%s %s\""%(toNative.getCmdBin("bash"),bashReal, "%s"%(inBackground), param), dryrun)


def get_bn_cfg(verbose=False):
    bnCfg = {"strategies":"", "vts":""}
    searchDirs = [toNative.getNativePath("c:/quanpass/programs/python/strategies")]
    for d in searchDirs:
        for x in bnCfg.keys():
            fn = "%s/%s.bn"%(d, x)
            if os.path.exists(fn):
                for ln in open(fn, 'r').readlines():
                    ln = ln.strip()
                    if is_empty_line(ln) == False:
                        bnCfg[x] = "%s;%s"%(ln, fn)

    if (verbose):
        for x in bnCfg.keys():
            print("INFO: get_bn_cfg %s %s\n"%(x, bnCfg[x].replace(";", " from ")))

    return bnCfg

def get_bn_cmd(cmd):
    bnCfg = get_bn_cfg()

    if cmd.find("vts")>=0:
        if bnCfg["vts"] != "":
            cmd = re.sub(r'\d\d\d\.\S+\/', bnCfg["vts"].split(r';')[0]+'/', cmd)

    if cmd.find("strategies")>=0:
        if bnCfg["strategies"] != "":
            cmd = re.sub(r'bn\d+', bnCfg["strategies"].split(r';')[0], cmd)
        else:
            cmd = re.sub(r'\.bn\d+', "", cmd).replace(".exe", ".py")

    return cmd

def run_python(cmd, dryrun=0, debug=0, expandHat=1, quiet=0):
    #cmd = get_bn_cmd(cmd)
    if (quiet):
        cmd = '%s -c "%s 2> %s"'%(toNative.getCmdBin("bash"),cmd,toNative.getNativePath("c:/temp/dummy.txt"))
    elif (expandHat):
        cmd = cmd.replace('^', '^^')

    if cmd.find(".py")>0 and not isCmdStarted(cmd,"python"):
        cmd = "%s %s"%(toNative.getPythonBin(),cmd)

    if dryrun:
        print("DRYRUN: %s"%(cmd))
    else:
        if debug:
            print("INFO: %s"%(cmd))
        ret = os.system(cmd)
        return ret
    return True

def run_python_pipe(cmd, dryrun=0, waitFinish=0, debug=0):
    cmd = get_bn_cmd(cmd)

    if dryrun:
        print >> sys.stderr, "INFO(DRYRUN): %s"%(cmd)

    if cmd.find(".py")>0 and not isCmdStarted(cmd,"python"):
        cmd = "%s %s"%(toNative.getPythonBin(),cmd)

    p = subprocess.Popen(cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT)
    s = iter(p.stdout.readline,r'')
    if (waitFinish):
        p.wait()
    return s

def cleandir(dirs):
    for dir in dirs:
        cmd = "%s -f %s/*.*"%(toNative.getCmdBin("rm"),dir)
        print("INFO: %s"%(cmd))
        run_cmd(cmd)

def removeFiles(inDir, fileList, dryrun = 0):
    for fn in fileList:
        cmd = "%s -f %s/%s"%(toNative.getCmdBin("rm"),inDir, fn)
        print("INFO: %s"%(cmd))
        if(dryrun == 0):
            run_cmd(cmd)

def chmod(fileList, mode = "+666"):
    for fn in fileList:
        run_cmd("chmod %s %s"%(mode, fn), quiet=0)

def rm(fileList, title='QpsSys'):
    for fn in fileList:
        print(f"INFO: {title}, deleting file {fn}", file=sys.stderr)
        run_cmd("%s -f %s"%(toNative.getCmdBin("rm"),fn), quiet=0)

def rmdirs(fileList):
    for fn in fileList:
        print(f"INFO: rm dir {fn}", file=sys.stderr)
        cmd = "%s -rf %s"%(toNative.getCmdBin("rm"),fn)
        run_cmd(cmd, quiet=0)


def is_junction_dir(dirIn):
    rc = False
    for dir in dir_chain(dos_style(dirIn)):
        p = run_command("c:/FE/job.net/junction.exe %s"%(dir), waitFinish=1)
        for ln in p:
            if ln.find('Substitute Name')>=0:
                rc = True
        if  rc:
            break
    return rc

def create_junction_dir(fromDir, toDir, verbose = 0):
    cmd = "c:/FE/job.net/junction.exe %s %s"%(dos_style(fromDir), dos_style(toDir))
    if verbose:
        print("INFO: %s"%(cmd))
    rc = run_cmd(cmd, quiet=1)
    return rc

def delete_junction_dir(toDir, verbose = 0):
    cmd = "c:/FE/job.net/junction.exe -d %s"%(dos_style(toDir))
    if verbose:
        print >> sys.stderr, "INFO: %s"%(cmd)
    rc = run_cmd(cmd, quiet=1)
    return rc

def dos_style(dirPath, use=False):
    if use:
        dirPath = dirPath.replace("\\", "/")
        for x in ['c', 'h', 'd', 'm', 'n', 'e', 'f', 'r', 't', 's']:
            dirPath = dirPath.replace("/%s/"%(x.upper()), "/%s/"%(x))
            dirPath = dirPath.replace('/cygdrive/%s/'%(x), '%s:/'%(x))
        dirPath = dirPath.replace("C:", "c:")
    return dirPath

def unix_style(dirPath):
    dirPath = dirPath.replace("C:", "c:")
    dirPath = dirPath.replace('c:/', '/cygdrive/c/')
    dirPath = dirPath.replace('\\', '/')
    return dirPath

def is_newer(tgt,chks,usecache=1, debug=False):
    if usecache>=1:
        if(not os.path.isfile(tgt)):
            return False
        tgtTime = os.stat(tgt).st_mtime
        for chk in chks:
            if(not os.path.isfile(chk)):
                continue
            chkTime = os.stat(chk).st_mtime
            if tgtTime < chkTime:
                if debug:
                    print(f"DEBUG_2342:\n tgt: {tgt} {tgtTime}\n chk: {chk} {chkTime}")
                return False
        return True
    else:
        return False

def fix_file(fn, rplRegExp, rplValue, verbose=1):
    content = open(fn, 'r').readlines()
    newContent = re.sub(rplRegExp, rplValue, "".join(content))
    if newContent!="" and newContent != content:
        open(fn, 'w').write(newContent)
        print >> sys.stderr, "INFO: fixing  %s"%(fn)
    else:
        print >> sys.stderr, "INFO: skiping %s"%(fn)


def append_write(fn):
    if os.path.exists(fn):
        return open(fn, 'a')
    else:
        return open(fn, 'w')


def dirHierarchyArray(dirName):
    elems = dirName.split("/")
    arr = []
    while(len(elems)>0):
        arr.append("/".join(elems))
        elems.pop()
    return arr

def recursiveFindFile(dirIn, fn, verbose=False):
    foundFn = ""
    for dirName in dirHierarchyArray(dirIn):
            foundFn = "%s/%s"%(dirName, fn)
            if exists(foundFn):
                if verbose:
                    print >> sys.stderr, "WARN: found %s"%(foundFn)
                break
            else:
                foundFn = ""
    return foundFn


def clrFileHandleCache():
    global FileHandleCache
    for fp in FileHandleCache.values():
        fp.flush()
        fp.close()
    FileHandleCache = {}


def file2line(fn, sep=";"):
    return sep.join(open_readlines(fn)).replace('\n','')

LastFileChgTime = {}
def fileChgedTime(asofdate,fn):
    global LastFileChgTime	
    if not os.path.exists(fn):
        return None

    lastReadKey = f"{asofdate},{fn}"

    if not lastReadKey in LastFileChgTime:
        LastFileChgTime[lastReadKey] = os.stat(fn).st_mtime
        return None #first time see it
    
    return LastFileChgTime[lastReadKey]

def resetFileChgedTime(asofdate,fn):
    global LastFileChgTime	
    lastReadKey = f"{asofdate},{fn}"
    if lastReadKey in LastFileChgTime:
        del LastFileChgTime[lastReadKey]

def fileChged(asofdate,fn):
    lastFileChgTime = fileChgedTime(asofdate,fn)
    if lastFileChgTime is None or lastFileChgTime != os.stat(fn).st_mtime:
        return True
    else:
        return False
    
def fileOlderThan(asofdate, fn, now, secs=60):
    lastFileChgTime = fileChgedTime(asofdate,fn)
    if lastFileChgTime is None or lastFileChgTime != os.stat(fn).st_mtime:
        return True
    else:
        print(f"INFO: lastFileChgTime= {lastFileChgTime}, now= {now.timestamp()}, diff = {now.timestamp()-lastFileChgTime}")
        if now.timestamp()-lastFileChgTime > secs:
            resetFileChgedTime(asofdate,fn)
            return True
        return False    


def copy(frm, to):
    run_cmd("%s -R %s %s" % (toNative.getCmdBin("cp"), frm, to), quiet=0, debug=0)


prevFind = {}
def find_first_available(fileList, begat=0, debug=0):
    for fn in fileList[begat:]:
        if fn in prevFind:
            return fn
        if (os.path.exists(fn)):
            prevFind[fn] = 1
            if debug:
                print("INFO: find_first_available %s" % (fn))
            return fn
    return None

def cygwin_glob(pattern, verbose=0):
    matches = glob.glob(pattern)
    if len(matches)<=0 and verbose>0:
        print >> sys.stderr, "WARNING: cygwin_glob Cannot find matching files \'%s\'"%(pattern)

    if verbose:
        print >> sys.stderr, "INFO: find matching %d files \'%s\'"%(len(matches), pattern)
    return [x.replace('\\', '/') for x in matches]

def search_base_filename(dir, fileext, debug):
    matches = glob.glob("%s/*.%s"%(dir, fileext))
    if(matches):
        basename = os.path.splitext(os.path.basename(matches[0]))[0]
        if debug:
            print("finding %s, --%s %s" % (matches[0], fileext, basename))
        return basename
    return "na"

def __async_raise(thread_Id, exctype):
    thread_Id = ctypes.c_long(thread_Id)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_Id, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_Id, None)
        raise SystemError("PyThreadState_SEtAsyncExc failed")

def terminator_thread(thread):
    #terminator thread
    __async_raise(thread.ident, SystemExit)

def running_remotely():
    funcn = "running_remotely"
    rc = gethostname() in ['MSI03']
    print(f"INFO: {funcn} {gethostname()}= {rc}") 
    return rc

if __name__=='__main__':
    #print(cygwin_glob("c:/Fairedge_dev/app_common/*.py"))
    print(running_remotely())
