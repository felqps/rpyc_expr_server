#!/home/shuser/anaconda3/bin/python

import sys

import os
import pickle
import socket
from optparse import OptionParser
from common_basic import list_modules


def get_statusfile_options():
    parser = OptionParser(description="StatusFile")
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
class StatusFile():
    def __init__(self, fn, cmd='NA', host='NA', status='NA'):
        self.fn = fn
        self.dta = {'cmd': cmd, 'host': host, 'status': status}
        if os.path.exists(fn) and os.path.getsize(fn)>0:
            try:
                self.dta = pickle.load(open(fn, 'rb'))
            except Exception as e:
                print(f"ERROR: StatusFile cmd= {cmd}, fn= {fn}, e= {e}", file=sys.stderr)

    def set_status(self, v):
        if self.fn == 'NA':
            return
            
        if self.dta['cmd'] == 'NA':
            self.dta['cmd'] = (' '.join(sys.argv))

        
        self.dta['host'] = socket.gethostname()
        self.dta['status'] = v
        #print(f"INFO: {self.dta['host']} => {self.dta['cmd']}", file=sys.stderr)
        pickle.dump(self.dta, open(self.fn, 'wb'))

    def start(self):
        self.set_status('start')
        return self
    
    def success(self):
        self.set_status('success')
        return self

    def __str__(self):
        # return f"INFO: StatusFile {self.dta['cmd']}, {self.dta['host']} {self.dta['status']}"
        #'resultFn:' is needed by FCWorker 
        return f"resultFn: {self.fn}"

if __name__ == "__main__":
    (options, args) = get_statusfile_options()
    (cmd, fn) = sys.argv[-2:]
    print(StatusFile(fn, cmd=cmd).success())
