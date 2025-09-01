#!/cygdrive/c/Python27/python.exe

import os
import re
import glob
import sys
import time
import datetime


def getNativePath(winpath):
    if sys.platform=="win32":	#windows
        if winpath[0]=="/" and (not winpath.startswith("/cygdrive/")):
            winpath = "c:%s"%(winpath)
        return winpath
    else:	#unixlike
        if winpath[0:8].lower()=="c:/temp/":
            return "/tmp/%s"%winpath[8:]
        if winpath[1]==":":
            return winpath[2:].replace("/FE/","/fe/").replace("/Quanpass/", "/quanpass/")
        if winpath[0:10].lower()=="/cygdrive/":
            return winpath[11:].replace("/FE/","/fe/").replace("/Quanpass/", "/quanpass/")
        
        return winpath.replace("/FE/","/fe/")

def getNetPath(winpath):
    if sys.platform=="win32":	#windows
        if winpath[0]=="/" and (not winpath.startswith("/cygdrive/")):
            winpath = "c:%s"%(winpath)
        return winpath
    else:	#unixlike
        if winpath[1]==":":
            return "/%s%s"%(winpath[0].lower(),winpath[2:])
        if winpath[0:10].lower()=="/cygdrive/":
            return winpath[9:]
        
        return winpath

def getPythonBin():
    if sys.platform=="win32":	#windows
        return "c:/Python27/python.exe"
    else:	#unixlike
        return "python"

def getTempRootPath():
    if sys.platform=="win32":	#windows
        return "c:/temp"
    else:	#unixlike
        return "/tmp"

def getCmdBin(cmd):
    if sys.platform=="win32":	#windows
        return "%s.exe"%cmd
    else:	#unixlike
        return cmd

def getPy(winpath):
    if sys.platform=="win32":	#windows
        return winpath
    else:	#unixlike
        return re.sub(r'\.bn[0-9][0-9][0-9]\/', '/', getNativePath(winpath)).replace(".exe",".py")

def getTempOutFile(winpath):
    if sys.platform=="win32":	#windows
        return winpath
    else:	#unixlike
        return "~/%s"%winpath[8:]



