import toNative
import re
import hashlib
from QpsDate import *

def file2md5(fn):
    buf = "".join(open_readlines(fn))
    buf = re.sub(r'\s', '', buf)
    md5 = buf2md5(buf)
    return md5

def md52int(md5):
    ret = 0
    for i in md5:
        if i.isdigit():
            ret += int(i)
    return ret

def getFileMd5(fn):
    for ln in run_command("md5sum.exe \"%s\""%(fn)):
        md5sum = ln.split()[0]
        return md5sum
    return "unknown_md5sum"

def buf2md5(buf):
    md5 = hashlib.md5()
    buf = buf.encode('utf-8')
    md5.update(buf)
    return md5.hexdigest()

def flist2md5(fl):
    lines = []
    for fn in sorted(fl):
        lines.extend(open(fn, 'r').readlines())
    buf = "".join(lines)
    buf = re.sub(r'\s', '', buf)
    md5 = hashlib.md5()
    md5.update(buf)
    return md5.hexdigest()

def params2md5(params, ignoredKv = {}):
    paramsArr = params.split(r'~')
    paramsUse = []
    for kv in paramsArr:
        if kv.find('^')<0:
            paramsUse.append(kv)
            continue
        (k, v) = kv.split(r'^')
        if not k in ignoredKv:
            paramsUse.append(kv)
        else:
            if 0:
                print >> sys.stderr, "INFO: ignore %s in params2md5 generation"%(k)

    md5 = hashlib.md5()
    md5.update('~'.join(paramsUse).encode('utf-8'))
    return md5.hexdigest()

def kv2params(kv, keysToIgnore = []):
    params = ""
    for k in kv.keys():
        if (k in keysToIgnore):
            continue

        if (k=='X'):
            continue
        if (params == ""):
            params = "%s^%s"%(k, kv[k])
        else:
            params = "%s~%s^%s"%(params, k, kv[k])
    return params

def kv2qpstbl(kv, klist):
    qpstbl = ""
    for k in klist:
        qpstbl = "%s %s= %s"%(qpstbl, k, kv[k])
    return qpstbl


def kv2options(kv):
    params = ""
    for k in kv.keys():
        if (params == ""):
            params = "--%s \"%s\""%(k, kv[k])
        else:
            params = "%s --%s \"%s\" "%(params, k, kv[k])
    return params

def kv2str(kv, keysToIgnore = []):
    params = ""
    for k in kv.keys():
        if (k in keysToIgnore):
            continue
        if (params == ""):
            params = "%s=%s"%(k, kv[k])
        else:
            params = "%s\n%s=%s"%(params, k, kv[k])
    return params


def params2kv(params, pairSep='~', kvSep='^'):
    kv = {}
    if params != "":
        for kvpair in params.split(pairSep):
            (k, v) = kvpair.split(kvSep)
            k = k.replace('oger_', 'oger:')
            kv[k] = v.rstrip()
    return kv

def kv2macroexpand(kv, debug=False):
    macroExpand = ""
    for k in sorted(kv.keys(), reverse=True):
        v = kv[k]
        macroExpand = "%ss/%s/%s/g;"%(macroExpand, k, v.replace('/', r'\/'))

    if debug:
        print("DEBUG: macroExpand= %s"%(macroExpand), file = sys.stderr)
    return macroExpand


#QPS related
def genTaskParamFile(taskDir, params):
    pf = open("%s/params.txt"%(taskDir), "w")
    pf.write(params+"\n")
    pf.close()

def mkdirStudyRunTask(studyDir, runDir, taskDir, tags):
    mkdir([studyDir, runDir, taskDir])
    for tag in tags.split(";"):
        run_cmd("touch %s/.%sstudy"%(studyDir, tag), 0)
        run_cmd("touch %s/.%srun"%(runDir, tag), 0)
        run_cmd("touch %s/.%stask"%(taskDir, tag), 0)


def addIfNotHas(kv, k, v, overwrite=0):
    if not k in kv or overwrite>0:
        kv[k] = v

def mergeKvFromFile(fn, kv, overwrite=0):
    for ln in open(fn, 'r').readlines():
        if(ln[0]=='#' or is_empty_line(ln)):
            continue
        ln = ln.rstrip()
        (k, v) = ln.split(r'=')
        addIfNotHas(kv, k, v, overwrite=overwrite)
    return kv

def getCfgInfo(fn, sep=r'\s+', minlines=5):
    fn = dos_style(fn)
    if(not os.path.exists(fn)):
        print >> sys.stderr, "ERROR: can not open cfg file %s"%(fn)
        return None
    kv = {}
    validLineCnt = 0
    for ln in open(fn, 'r').readlines():
        if(ln[0]=='#'):
            continue
        ln = ln.rstrip()
        ln = ln.replace(' ','')
        #print ln
        cols = ln.split(sep)
        if len(cols)>1:
            kv[cols[0]] = cols[1]
            validLineCnt = validLineCnt + 1

    if (0 and validLineCnt<minlines):
        print >> sys.stderr, "WARNING: cfg file contain too few lines: %s, %d lines"%(fn, validLineCnt)
    return kv

def file2kv(fn):
    return getCfgInfo(fn, sep='=')

def getTacticInfo(fn, sep='=', simu_user=''):
    if not exists(fn):
        fn = toNative.getNativePath("c:/Quanpass/programs/python/strategies/TacticInfo.txt")
    ti = getCfgInfo(fn, sep)
    if 'simu_user' in ti:
        simu_user=ti['simu_user']

    #XXX
    return ti

    if not 'simu_root' in ti:
        ti['simu_root'] = toNative.getNativePath('c:/fe/simu/%s/%s'%(simu_user, ti['simu_domain']))

    if not 'rsch_dir' in ti:
        ti['rsch_dir'] = '%s/%s/%s/date^RSCHDATE'%(ti['simu_root'], ti['study_name'], ti['run_name'])
        ti['dcsn_dir'] = '%s/%s/%s/date^DCSNDATE'%(ti['simu_root'], ti['study_name'], ti['run_name'])

    if ti['rsch_dir'].find('RSCHDATE')<0: #new format config file
        ti['rsch_dir'] = '%s/%s/%s/date^RSCHDATE'%(ti['rsch_dir'], ti['study_name'], ti['run_name'])
        ti['dcsn_dir'] = '%s/%s/%s/date^DCSNDATE'%(ti['dscn_dir'], ti['study_name'], ti['run_name'])

    return ti

def getListInfo(fn, sep=r'\s+'):
    kv = {}
    fn = dos_style(fn)
    if(not os.path.exists(fn)):
        print >> sys.stderr, "ERROR: can not open cfg file %s"%(fn)
        return kv

    for ln in open(fn, 'r').readlines():
        ln = ln.rstrip()
        if(ln[0]=='#'):
            continue
        cols = ln.split(sep)
        if len(cols)>=1:
            kv[cols[0]] = 1
    return kv

def readIni(fn, m4defines = "", m4predefFn = "", verbose=False):
    kv = {}
    #for ln in open(fn, 'r').readlines():
    cmd = "%s %s %s %s"%(toNative.getCmdBin("m4"), m4defines, m4predefFn, fn)
    #print >> sys.stderr, "IFO: %s"%(cmd)
    if (verbose):
        print >> sys.stderr, "IFO: %s"%(cmd)

    for ln in run_command(cmd, waitFinish=1):
        #print ln
        if (ln[0]=='#'):
            continue
        ln = ln.replace(' ','')
        ln = ln.replace('\n','')
        ln = ln.replace('\r','')
        if (not is_empty_line(ln)):
            (k,v) = ln.split(r':')
            if (not k in kv):
                kv[k] = v
            else:
                kv[k] = "%s;%s"%(kv[k],v)
    return kv

def getStatus(fn):
    if (not os.path.exists(fn)):
        return "na"
    content = open(fn, 'r').read()
    return content.split(r'\n')[-2]

def writeStatus(fn,line):
    out = open(fn, "a")
    out.write(line)
    out.close()

def checkInclude(content, include):
    return checkLambda((lambda x: x<0), content, include)

def checkExclude(content, include):
    return checkLambda((lambda x: x>=0), content, include)

def checkLambda(f,content,include):  #use or
    for tag in include.split(r'|'):
        idx = content.find(tag)
        #print "%s %d"%(tag, idx)
        if f(idx):
            return False
    return True

def scn_file(opt, begat=0):
    return find_first_available(["%s/%s.scn" % (query_dir(opt),opt.scn ),"%s/scenarios/%s.scn"%(user_dir(opt.user), opt.scn), "%s/scenarios/%s.scn"%(user_dir("pub"), opt.scn)], begat,opt.debug)

def set_file(opt, begat=0):
    return find_first_available(["%s/%s.set" % (query_dir(opt),opt.set ),"%s/symsets/%s.set"%(user_dir(opt.user), opt.set), "%s/symsets/%s.set"%(user_dir("pub"), opt.set)], begat,opt.debug)

def qry_file(opt, begat=0):
    return find_first_available(["%s/%s.qry" % (query_dir(opt),opt.qry ),"%s/queries/%s.qry"%(user_dir(opt.user), opt.qry), "%s/queries/%s.qry"%(user_dir("pub"), opt.qry)], begat,opt.debug)

def query_file(opt):
    return "%s/%s"%(query_dir(opt), "query.sid")

def print_opt_info(opt):
    print("studydir    =%s"%(study_dir(opt)), file=sys.stderr)
    print("querydir  =%s"%(query_dir(opt)), file=sys.stderr)
    print("initdir =%s"%(init_dir(opt)), file=sys.stderr)
    print("extdir  =%s"%(ext_dir(opt)), file=sys.stderr)
    print("scn         =%s"%(scn_file(opt)), file=sys.stderr)
    print("symset      =%s"%(set_file(opt)), file=sys.stderr)
    print("qry         =%s"%(qry_file(opt)), file=sys.stderr)


def set_opt_from_querydir(opt, querydir):
    opt.querydir = querydir
    opt.studydir = ""

    subdirs = opt.querydir.split(r'/')

    opt.query = subdirs[-1]
    opt.run = subdirs[-2]
    opt.study = subdirs[-3]
    opt.user = subdirs[-4]
    opt.studydir = '/'.join(subdirs[:-2])

    opt.qry = search_base_filename(opt.querydir, "qry", opt.debug)
    opt.set = search_base_filename(opt.querydir, "set", opt.debug)
    opt.scn = search_base_filename(opt.querydir, "scn", opt.debug)

def serialize_by_key(varDict, varList):
    retVal = ""
    for var in varList:
        if(retVal != ""):
            retVal = "%s;"%(retVal)
        retVal = "%s%s"%(retVal,varDict[var])
    return retVal

def check_opt_info(opt):
    if(not hasattr(opt,"querydir")):
        print >> sys.stderr, "ERROR: must specify query directory"
        return 0
    return 1

def scn2fundCode(fn):
    if fn.find('NeiXin0')>=0:
        return 'NX0'
    if fn.find('NeiXin1')>=0:
        return 'NX1'
    if fn.find('NeiXin2')>=0:
        return 'NX2'
    if fn.find('NeiXin3')>=0:
        return 'NX3'
    if fn.find('NeiXin4')>=0:
        return 'NX4'
    if fn.find('NeiXin5')>=0:
        return 'NX5'
    if fn.find('ZSA')>=0:
        return 'ZSA'
    if fn.find('ZSB')>=0:
        return 'ZSB'

    return 'NA'

def getNetworkFileUsingCache(fn, remapRoot=toNative.getNativePath("c:/temp/data_cache/%s_"%(gettoday())), debug=False):
    cacheFn = remapNetworkFn(fn, remapRoot)
    if not exists(cacheFn):
        cmd = "%s %s %s"%("cp", fn, cacheFn)
        print(cmd, file=sys.stderr)
        run_cmd(cmd, debug)

    if exists(cacheFn):
        return cacheFn
    else:
        print >> sys.stderr, "ERROR: getNetworkFileUsingCache cannot find file %s"%(fn)
        return "NA"

def isNightSessionTactic(tacticInfoDir):
    return (tacticInfoDir.find('NGT')>=0 or
            tacticInfoDir.find('_ns_')>=0 or
            tacticInfoDir.find('_c2300')>=0 or            
            tacticInfoDir.find('_nsrev')>=0 or
            tacticInfoDir.find('_nsopn')>=0)

def isIntradayTactic(tacticInfoDir):
    return (tacticInfoDir.find('daily')<0)

ListnameAssignment = {}
def listname2assignment(listname):
    global ListnameAssignment
    if len(ListnameAssignment)<=0:
        for ln in open_readlines(toNative.getNativePath("c:/quanpass/programs/python/strategies/ListnameAssignment.txt")):
            ln = ln.strip()
            (listnameRoot, tacticCategory, session, tacticType) = ln.split()
            ListnameAssignment[listnameRoot] = (session, tacticType)

    bk = listname.split(r'_')[0]
    if bk[-2:] in ListnameAssignment:
        return ListnameAssignment[bk[-2:]]
    if bk[-1:] in ListnameAssignment:
        return ListnameAssignment[bk[-1:]]

    if listname[-1] != "Z":
        print >> sys.stderr, "ERROR: listname2assignment() unknown assignment for bk=%s listname=%s"%(bk, listname)
    return ("NA", "NA")

_cfg = None
_opt = None
_var = {}

def set_global_cfg_opt(cfg, opt):
    global _cfg
    global _opt
    cfg.opt = opt
    _cfg = cfg
    _opt = opt

def set_global_opt(opt):
    global _opt
    _opt = opt

def set_global_cfg(cfg):
    global _cfg
    _cfg = cfg

def set_global_var(var):
    global _var
    _var = var

def glbcfg():
    return _cfg

def glbopt():
    return _opt

def glbvar():
    return _var

def glbvarexp(fldSpec):
    for var in glbvar().keys():
        #print "before var expansion:", fldSpec, " ", var, " ", glbvar()[var]
        fldSpec = fldSpec.replace("@%s"%(var), "'%s'"%(glbvar()[var]))
        #print "after  var expansion:", fldSpec
    return fldSpec

def spliceCurrentDir(fn):

    if fn[:2] == './':
        currentDir = os.path.abspath('.')
        fn = fn.replace('.',currentDir,1)
    elif fn == '.':
        currentDir = os.path.abspath('.')
        dirlist = currentDir.split(r'\\')
        fn = '/'.join(dirlist[-2:])

    return fn

def ordTypes(exch = ""):
    return ['CL1', #short-acount buy (cover) previous closing position
            'CS1', #long-account sell previous closing position
            'CL0', #short-acount buy (cover) today's position
            'CS0', #long-account sell today's position
            'OL0', #long-account buy today
            'OS0', #short-acount sell (short) today
            ]

symRe = re.compile(r'([a-zA-Z]+)(\d*)')
def sym2root(sym):
    if sym.isdigit() or sym.isalpha():
        return sym  # stocks
    else:
        m = symRe.search(sym)
        symRoot = m.group(1)
        return symRoot

def ratio(a, b):
    if (b==0.0):
        return 0.0
    else:
        return float(a)/b


def print_error(msg, markerCnt=3, file=sys.stderr):
    if markerCnt>0:
        print("#--ERROR--#\n"*markerCnt, file=file)
    print("%s\n\n"%(msg), file=file)
    if markerCnt>0:
        print("#--ERROR--#\n"*markerCnt, file=file)

# def print_info(msg):
#     print >> sys.stderr, "INFO: %s"%(msg)

def parseTacticSpec(tacticSpec):
    (majorCat, minorCat, tacticName) = tacticSpec.split(r'/')
    inBook = minorCat
    if (majorCat.find(':')>=0):
        (inBook, majorCat) = majorCat.split(r':')
    return (inBook, majorCat, minorCat, tacticName)

def del_scn_dir(scndir):
    rmdirs = []
    for lnk in glob.glob("%s/*.lnk"%(scndir)):
        cmd = "c:/FE/job.net/junction.exe %s"%(lnk.replace('\\', '/'))
        p = run_command(cmd, waitFinish=1)
        for ln in p:
            ln = ln.strip()
            if ln.find("Substitute")>=0:
                taskdir = ln.split()[-1].replace('\\', '/')
                rmdirs.append(taskdir)
    rmdirs.append(scndir)
    for d in rmdirs:
        cmd = "rm -rf %s"%(d)
        print >> sys.stderr, "INFO: %s"%(cmd)
        run_cmd(cmd)

def isSimuFundCode(fundcode):
    if (fundcode[0] == 'N' or fundcode[0] == 'Z'):
        return False  #real fund code starts with upper case
    else:
        return True

def getProductionFundCodes():
    return ["ZSA", "ZSB", "NX5", "NX4b", "NX4", "NX3", "NX1", "NX0", "NXZQ"]

def getFundCodeFromFilePath(fn):
    for fundcode in getProductionFundCodes():
        if (fn.find(fundcode)>=0):
            #print >> sys.stderr, "INFO: fund_code %s"%(fundcode)
            return fundcode
    print >> sys.stderr, "WARNING: QpsUtil.getFundCodeFromFilePath can not decide fund_code for %s"%(fn)
    return ""

def open_readlines_scnfn(options, scnFn):
    if 1:
        #In the past, .scn contains all the scn generated at "freeze" time.
        #This is problematic if a freeze was generated with simu_period "H" and we want to simulate "L", has "L" goes back further.
        #Here we will try to build the entire list on the fly regardless simu_period used at freeze time.
        scnKv = {}
        for ln in open_readlines(scnFn):
            ln = ln.strip().replace(" tag=", ";tag=")
            if not is_empty_line(ln):
                scnKv = params2kv(ln, pairSep=";", kvSep="=")
                if options.verbose:
                    print("INFO: open_readlines_scnfn scnKv=%s"%(scnKv), file=sys.stderr)
                break

        scnLns = {}
        for ln in open_readlines(scnFn):
            ln = ln.strip()
            if ln.find('#')>=0:
                continue
            scnLns[ln] = 1
        
        for ln in open_readlines(toNative.getNativePath("c:/Quanpass/programs/python/strategies/scn.tmpl")):
            ln = ln.strip()
            ln = ln%(scnKv['bn_vts'],
                    scnKv['refdate'],
                    scnKv['tacticlist'],
                    scnKv['minenddate'],
                    scnKv['host'],
                    scnKv['tag'],
                    scnKv['user'],
                    )
            if ln in scnLns:
                scnLns[ln] = 2
            else:
                scnLns[ln] = 1

        for ln in sorted(scnLns.keys()):
            if options.debug:
                print >> sys.stderr, "INFO: %s cnt=%d"%(ln, scnLns[ln])
            
        return sorted(scnLns.keys())

MktDailyCache = {}
def getMktDailyFor(dt, debug=False):
    global MktDailyCache
    if not dt in MktDailyCache:
        fn = toNative.getNativePath("c:/fe/simu/data/chna/daily/cnfutr_%s.txt"%(dt))
        if not os.path.exists(fn): 
            print >> sys.stderr, "ERROR: getMktDailyFor missing daily prc file %s"%(fn)
            return None
        MktDailyCache[dt] = MktDaily(fn)
    return MktDailyCache[dt]


def remapFreezeFn(fn, remapRoot=toNative.getNativePath("c:/fe/stratfunds/freeze_simu_ref/")):
    fn = dos_style(fn)
    fn = fn.replace("/", "#")
    fn = fn.replace("c:#FE#simu#", remapRoot+"/")
    fn = fn.replace("c:#fe#simu#", remapRoot+"/")
    fn = fn.replace("r:#", remapRoot+"/")
    fn = fn.replace("R:#", remapRoot+"/")            
    return fn

def reverseFreezeFn(fn, remapRoot):
    fn = dos_style(fn)
    fn = fn.replace(remapRoot, "c:#FE#simu#")
    fn = fn.replace("#", "/")
    return fn



def mergeFilrptFiles(frmFn, tgtFn, debug=True):
    if debug:
        print >> sys.stderr, "INFO: mergeFilrptFiles() %s %s"%(frmFn, tgtFn)            
    lns = []
    inMtmLns = []
    mtmBegDt="30000000"
    mtmEndDt="30000000"    
    inMtmLns = [x for x in QpsUtil.open_readlines(frmFn)]
    if len(inMtmLns)>0:
        mtmBegDt=inMtmLns[0].split()[0]
        mtmEndDt=inMtmLns[-1].split()[0]
    print >> sys.stderr, "INFO: VtsFreeze.mergeTo() %s, %s->%s, copy to %s"%(frmFn, mtmBegDt, mtmEndDt, tgtFn)
    
    #Keep all trades in the existing ref file with trade date earlier than current one
    if QpsUtil.exists(tgtFn):
        for ln in QpsUtil.open_readlines(tgtFn):
            if not ln:
                continue
            dt = ln.split()[0]
            if dt<mtmBegDt:
                lns.append(ln)

    #Now append new ones using currnt mtm file
    #lns = lns +  [x for x in inMtmLns if x.split()[0]>contFrom]
    lns = lns +  [x for x in inMtmLns]
                
    open(tgtFn, 'w').write("\n".join(lns))


def getBnVts():
    defaultBnFn = toNative.getNativePath("c:/quanpass/programs/python/strategies/vts.bn")
    if (os.path.exists(defaultBnFn)):
        return open(defaultBnFn, 'r').readline().strip().replace(".rel","")
    else:
        return "790"

def getBnStrategies():
    defaultBnFn = toNative.getNativePath("c:/quanpass/programs/python/strategies/strategies.bn")
    if (os.path.exists(defaultBnFn)):
        return open(defaultBnFn, 'r').readline().strip()
    else:
        return "bn204"

def getDefaultBn():
    return getBnVts()

ExistingPrevListnames = []
def getExistingPrevListnames():
    if len(ExistingPrevListnames)>0:
        return ExistingPrevListnames

    pattern = "PREV_LISTNAME_="
    for fn in glob.glob(toNative.getNativePath("c:/quanpass/programs/python/strategies/macro_defaults_*.cfg")):
        for ln in open(fn, 'r').readlines():
            if ln.find(pattern)>=0:
                prevLN = ln.replace(pattern, '')
                ExistingPrevListnames.append(prevLN)

    ExistingPrevListnames.append('GX')
    ExistingPrevListnames.append('ZX')
    ExistingPrevListnames.append('FX')

    print("INFO: ExistingPrevListnames %s"%(" ".join(ExistingPrevListnames)), file=sys.stdout)
    return ExistingPrevListnames

FundCategory2Listnames = {}
def getFundCategory2Listnames():
    if len(FundCategory2Listnames)>0:
        return FundCategory2Listnames
    aggRe = re.compile(r'book_name=(.*?)\".*book_name=(.*?)\]')
    for ln in open(toNative.getNativePath("c:/fe/config/supermon_filters.xml"), 'r').readlines():
        for kv in aggRe.findall(ln):
            (fund_category, list_names) = kv
            FundCategory2Listnames[fund_category] = list_names
            #print >> sys.stderr, "INFO: fund_category= %26s, list_names=%s"%(fund_category, list_names)
    return FundCategory2Listnames

Listname2Category = {}
def getListname2Category():
    if len(Listname2Category)>0:
        return Listname2Category
    for fund_category in getFundCategory2Listnames().keys():
        if fund_category.find("HS250")>=0:
            continue
        arr = fund_category.split(r'_')
        fund = arr[0]
        if not fund in getProductionFundCode():
            continue
        category = '_'.join(arr[1:])
        for listname in getFundCategory2Listnames()[fund_category].split(r'|'):
            listname = removePrevListname(listname)
            if listname in Listname2Category:
                if Listname2Category[listname] != category:
                    print >> sys.stderr, "ERROR: conflcting category assignment for %s, prev=%s, new=%s"%(listname, Listname2Category[listname], category)
            Listname2Category[listname] = category
    return Listname2Category

def removePrevListname(listname):
    for prevLN in getExistingPrevListnames():
        if listname.find(prevLN) == 0: #start with one of the string in list
            listname = listname[len(prevLN):]
            return listname
    return listname

ListName2TacticCategory = {}
def listname2TacticCategory(listname):
    if len(ListName2TacticCategory)<=0:
        for ln in open(toNative.getNativePath('c:/quanpass/programs/python/strategies/ListnameAssignmentNew.txt'), 'r').readlines():
            (l, c, dn, dl) = ln.split()[:4]
            ListName2TacticCategory[l] = "%s_%s_%s"%(c, dn, dl)

    listname = removePrevListname(listname.split(r'_')[0])

    if listname in ListName2TacticCategory:
        return ListName2TacticCategory[listname]
    else:
        return "Unknown_%s"%(listname)


minEndDateCfg = {}

def adjMinEndDate(minEndDateCfgFn):
    global minEndDateCfg
    if (minEndDateCfgFn==""):
        minEndDateCfg = {"L": "20150105",
                        "O": "20151008",
                        "A": "20160401",
                        "M": "20170102",
                        "S": "20170401",
                        "N": "20170703",
                        "V": "20171009",
                        "F": "20180102",
                        "G": "20180402",
                        "H": "20180702",
                        "I": "20181008",
                        "J": "20190102",
                        "K": "20190401",
                        "P": "20190701",
                        "Q": "20191007",
                        }
    else:
        for ln in open_readlines(minEndDateCfgFn):
            (simu_period, dt) = ln.split("=")
            minEndDateCfg[simu_period]=dt

adjMinEndDate("") #init at least once
if 0:
    print(minEndDateCfg, file=sys.stdout)
        
def getMinEndDate(simu_period):
    if 0:
        print >> sys.stderr, "INFO: getMinEndDate %s"%(simu_period)
    if simu_period in minEndDateCfg:
        return minEndDateCfg[simu_period]
    else:
        return minEndDateCfg['L']

def setMinEndDate(options):
    options.macro = "MINENDDATE=%s"%(getMinEndDate(options.simu_period))
    if 0:
        print >> sys.stderr, "DEBUG: simu_period %s, macro to %s"%(options.simu_period, options.macro)
    
def getSimuPeriod(begdate, simu_period):
    simu_period_list = ["L", "O", "A", "M", "S", "N", "V", "F", "G", "H", "I", "J", "K", "P", "Q"]
    index = 1
    if int(begdate):
        if int(begdate) < int(getMinEndDate('L')):
            return 'L'
        if int(begdate) > int(gettoday()):
            begdate = gettoday()
        while int(begdate) > int(getMinEndDate(simu_period_list[index])):
            index += 1
            if index == len(simu_period_list):
                break
        return simu_period_list[index - 1]
    return simu_period

def getMacroCfgFiles(args):
    macroCfgFiles = []
    for x in args:
        x = dos_style(x)
        if x.find(".cfgs") >=0 or x.find(".cfgd") >=0:  #cfgd is for daily simulation and cannot use condor
            for e in open(x, 'r').readlines():
                e = e.strip()
                if e != "" and e[0]!= '#' and not is_empty_line(e):
                    macroCfgFiles.append(e)
        else:
            macroCfgFiles.append(x)
    return macroCfgFiles

def splitBnListname(bnListname):
    bn = "0"
    listname = ""
    idx = 0
    for c in bnListname:
        if c.isdigit():
            idx = idx + 1
        else:
            if idx>0:
                bn = bnListname[:idx]
            listname = bnListname[idx:]
            break
    return (bn, listname)

def copyNetworkFileUsingCache(fn, dir, remapRoot=toNative.getNativePath("c:/temp/data_cache/%s_"%(gettoday())), debug=False):
    cacheFn = remapNetworkFn(fn, remapRoot)
    if not exists(cacheFn):
        cmd = "%s %s %s"%("cp", fn, cacheFn)
        print(cmd, file=sys.stdout)
        run_cmd(cmd, debug)

    if 0:
        print("DEBUG: copyNetworkFileUsingCashe %s %s"%(cacheFn, dir), file=sys.stdout)

    if exists(cacheFn):
        cmd =  "%s %s %s/%s"%(toNative.getCmdBin("cp"), cacheFn, dir, fn.split(r'/')[-1])
        run_cmd(cmd, debug)
    else:
        print("ERROR: copyNetworkFileUsingCache cannot find file %s"%(fn), file=sys.stderr)


FileHandleCache = {}
def getFileHandleFor(fn, mode='w', output_tag= "INFO: rollpos gen", debug=False):
    global FileHandleCache
    if fn in FileHandleCache:
        fp = FileHandleCache[fn]
    else:
        if not exists(os.path.dirname(fn)):
            mkdir([os.path.dirname(fn)])
        fp = open(fn, mode)
        if debug:
            print("%s %s"%(output_tag, fn), file=sys.stderr)
        FileHandleCache[fn] = fp
    return fp


#Copy from network drive can be slow. So introduce cache similar to GFS cache logic
def remapNetworkFn(fn, remapRoot=toNative.getNativePath("c:/temp/data_cache/%s_"%(gettoday()))):
    if not exists(toNative.getNativePath("c:/temp/data_cache")):
        mkdir([toNative.getNativePath("c:/temp/data_cache")])
    fn = dos_style(fn)
    fn = fn.replace("/", "#")
    fn = fn.replace("N:", remapRoot)
    if fn.startswith('#n#'):
        fn = remapRoot + fn[2:]
    return fn

def get_commission(sz, pri, commission_ratio=1.5, stamp_duty=5):
    amt = abs(sz*pri)
    cmsn = 0
    if commission_ratio:
        if sz > 0:
            cmsn = amt*commission_ratio/10000
        elif sz < 0:
            cmsn = amt*(stamp_duty+commission_ratio)/10000
    return cmsn
