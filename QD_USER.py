import sys
from QpsSys import gethostip
from common_ctx import *

ip = gethostip() if not detached() else None

if False and ip == "192.168.20.45":
    QD_USER = 'axiong'
elif ip == "192.168.20.141":
    QD_USER = 'jli'
elif ip in ["192.168.20.184", "192.168.20.185", "192.168.20.186", "192.168.20.187", "192.168.20.188"]:
    QD_USER = 'che'
else:
    QD_USER = 'che'

