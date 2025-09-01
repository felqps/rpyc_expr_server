import sys
import os

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

def is_empty_line(ln):
    tmp = ln.replace(' ', '')
    tmp = tmp.replace('\t', '')
    tmp = tmp.replace('\r', '')
    tmp = tmp.replace('\n', '')
    return tmp == "" or (len(tmp)>0 and tmp[0] == '#')

def open_readlines(fn, verbose=False, warning=False, note=os.path.basename(sys.argv[0]).split(".py")[0]):
    lines = []
    fn = fn.replace('\\', '/')
    if not os.path.exists(fn):
        if warning:
            print("WARNING: cannot find %s"%(fn))
        return lines
    
    if verbose:
        print(f"INFO: open_readlines({note}, {verbose}) file {fn}", file=sys.stderr)

    for ln in open(dos_style(fn), 'r').readlines():
        ln = ln.strip()
        if ln.find("#include")>=0:
            inclFilter = ""
            cols = ln.split()
            inclFn = cols[1]
            if len(cols)>=3:
                inclFilter = cols[2]
            if (inclFilter == ""):
                lines.extend(open(inclFn, 'r').readlines())
            else:
                lines.extend([ln.strip() for ln in open(inclFn, 'r').readlines() if ln.find(inclFilter)>=0])
        else:
            if not is_empty_line(ln):
                lines.append(ln)
    return lines

# def open_readlines(fn, verbose=False, warning=False, note=os.path.basename(sys.argv[0]).replace(".py","")):
#     lines = []
#     fn = fn.replace('\\', '/')
#     if not os.path.exists(fn):
#         if warning:
#             print("WARNING: cannot find %s"%(fn))
#         return lines
    
#     if verbose:
#         print("INFO: open_readlines(%s) file %s"%(note, fn), file=sys.stderr)

#     for ln in open(dos_style(fn), 'r').readlines():
#         ln = ln.strip()
#         if ln.find("#include")>=0:
#             inclFilter = ""
#             cols = ln.split()
#             inclFn = cols[1]
#             if len(cols)>=3:
#                 inclFilter = cols[2]
#             if (inclFilter == ""):
#                 lines.extend(open_readlines(inclFn))
#             else:
#                 lines.extend([ln.strip() for ln in open_readlines(inclFn) if ln.find(inclFilter)>=0])
#         else:
#             if not is_empty_line(ln):
#                 lines.append(ln)
#     return lines
