
def check_condor_error(subtaskDir):
    subtaskDir = dos_style(subtaskDir)
    errFn = "%s/run/condor_0.error.txt"%(subtaskDir)
    logFn = "%s/run/condor_0.log.txt"%(subtaskDir)
    rc = 0
    if not os.path.exists(logFn):
        rc += 1 #no log file
    if not os.path.exists(errFn):
        rc += 2 #no err file

    if rc > 0:
        return rc

    execOnLn = ""
    errorLn = ""
    findPocoException = False
    findMktNA = False
    for fn in (errFn, logFn):
        for ln in open_readlines(fn):
            ln = ln.strip()

            if ln.find("Job executing on host")>=0:
                execOnLn = ln

            if ln.find("held")>=0 or (ln.find("Abort")>=0 and ln.find("core dumped")<0) or (ln.find("gfs")>=0 and ln.find("already exist")<0) or ln.find("Exception")>=0:
                errorLn = ln
                rc += 16

            if ln.find("No such file") >= 0 and ln.find("/study_sub") < 0:
                errorLn = ln
                rc += 32

            if ln.find("Poco::SystemException") >= 0:
                findPocoException = True

            if ln.find("NA.LTQ") >= 0 or ln.find("NA.ltq") >= 0:
                findMktNA = True

    if findPocoException or findMktNA:
        rc = 0				

    if rc>10:
        print >> sys.stderr, "INFO(condor error): rc=%d \n\tsubdir= %s \n\terror= %s \n\texecOn= %s"%(rc, subtaskDir, errorLn, execOnLn)

    return rc
