#!/usr/bin/env python

import os,sys
from symset_helpers import *

class FDFFile:
    servers=[f"NASQPS0{x}" for x in [4,6,7,8,9]]
    missing_fdf_sym_list = []
    encode_keys = ['cfgnm', 'symIn', 'symOut', 'rootdir', 'begdt', 'enddt', 'fldnm']
    min_valid_file_size = 10
    def __init__(self, spec):
        self._spec = spec

    def id(self):
        spec_for_id = {k:self._spec[k] for k in self.encode_keys}
        cfg = self._spec['cfgnm'].split(r'_')[-1]
        if cfg in ['T', 'S']: #realtime updated
            #print(f"fldnm= {self._spec['fldnm']}")
            # if self._spec['fldnm'].find('Factor__')>=0:
            #     pass
            # elif self._spec['fldnm'].find('FdfDict')>=0: 
            #     pass
            #elif self._spec['fldnm'].find("(")>=0: #expression
            if not is_symset(self._spec['symOut']):
                pass
            else:
                spec_for_id['update_dtm'] = open(f"/qpsdata/egen_study/factcomb.T/factcomb.{cfg}.timestamp", 'r').readlines()[0].strip()

        self._spec['id'] = buf2md5(f"{spec_for_id}")
        return self._spec['id']

    def nm_ord(self):
        ord_value = 0
        for x in self.id()[0]:
            ord_value += ord(x)
        self._spec['nm_ord'] = ord_value
        return ord_value

    def cfg(self):
        return self._spec['cfg']

    def symIn(self):
        return self._spec['symIn']


    def symOut(self):
        return self._spec['symOut']

    def begdt(self):
        return self._spec['begdt']

    def enddt(self):
        return self._spec['enddt']

    def fdir(self):
        return '/'.join(self.fpath().split(r'/')[:-1])

    def fsize(self):
        if os.path.exists(self.fpath()):
            return os.path.getsize(self.fpath())
        return 0
        
    def fpath_467(self, nm_ord):
        fpstrRoot = self._spec['rootdir']%(f"{FDFFile.servers[nm_ord]}")
        fpstr = f"{fpstrRoot}/{self.id()[:2]}/{self.id()[2:]}.fdf"
        if is_symset(self.symOut()):
            fpstr = fpstr.replace(".fdf", ".fds") #fd for symset
        if self.fldnm().find("FdfDict")>=0:
            fpstr = fpstr.replace(".fdf", ".fdd").replace(".fds", ".fdd") #fd for symset
        return fpstr

    def fpath_46789(self, nm_ord):
        fpstrRoot = self._spec['rootdir']%(f"{FDFFile.servers[nm_ord]}")
        fpstr = f"{fpstrRoot}/{self.id()[:2]}/{self.id()[2:]}.fdf"
        if is_symset(self.symOut()):
            fpstr = fpstr.replace(".fdf", ".fds") #fd for symset
        if self.fldnm().find("FdfDict")>=0:
            fpstr = fpstr.replace(".fdf", ".fdd").replace(".fds", ".fdd") #fd for symset
        return fpstr

    def fpath(self, qpsnas_cfg='46789'): #old version is qpsnas_cfg=467
        fpstr = self.fpath_467(self.nm_ord()%3)
        if os.path.exists(fpstr) or qpsnas_cfg=='467':
            return fpstr
        else:
            #print(f"DEBUG_4441: missing ", self.toString(show_size=False, path='-'))
            fpstr = self.fpath_46789(self.nm_ord()%5)
            #print(f"DEBUG_2345: using fpath_46789= {fpstr}", file=sys.stderr)
            return fpstr

    def fpath_bash(self):
        return self.fpath().replace(".fdf", ".bash").replace(".fds", ".bash").replace(".fdd", ".bash")

    def ftype(self):
        return self.fpath().split(r'.')[-1]

    def toString(self, show_size=True, path=None):
        #printInfo = {k:v for k,v in self._spec.items() if k not in ['cfgnm', 'rootdir', 'id', 'nm_ord', 'cfg', 'debug', 'force_regen']}
        printInfo = {k:self._spec[k] for k in self.encode_keys}
        if path is not None:
            l = [path, f"{printInfo}"]
        else:
            l = [self.fpath(), f"{printInfo}"]
        if show_size:
            l.append(f"file_size: {self.fsize()}")
        return ' '.join(l)

    def __str__(self):
        return self.toString(show_size=False)

    def print(self):
        print(f"{self}")

    def isComposit(self):
        return is_symset(self.symOut())

    def debug(self):
        return self._spec['debug']

    def force_regen(self):
        return self._spec['force_regen']

    def fldnm(self):
        return self._spec['fldnm']

    def constituentList(self):
        fdfs = []
        logger().debug(f"self, constituentListSyms= {ssn2mkts()[self._spec['symIn']]}")
        for sym in ssn2mkts()[self._spec['symIn']]:
            ss = self.cfg().ss(sym)
            logger().debug(f"DEBUG_889d: constituentList {sym}")
            fdf = ss.fdf(f"{self.fldnm()}")
            if fdf is not None:
                fdfs.append(fdf)
            logger().debug(f"DEBUG_889o: fdf= {fdf}")
        logger().debug(f"DEBUG_889f: constituentList len= {len(fdfs)}")
        return fdfs

    def constituentNotChangedSince(self):
        rc = False
        if not self.isComposit(): #has not constintuents
            rc = True
        for cf in self.constituentList():
            if not is_file_newer(self.fpath(), cf.fpath()):
                rc = False
        logger().debug(f"DEBUG_889g: constituentNotChangedSince= {rc}")
        return True

    def need_regen(self):
        rc = self.fsize()<=FDFFile.min_valid_file_size or (self.isComposit() and (self.force_regen() or (not self.constituentNotChangedSince())))
        logger().debug(f"DEBUG_8834: regen {rc} for symset {self._spec['symIn']}, {self}")
        return rc

    def get(self, debug=1, force=False):
        assert (self._spec['symIn'] is not None) and (self._spec['symOut'] is not None), f"ERROR: missing symIn/symOut, {fdf}"

        if not os.path.exists(self.fdir()):
            mkdir([self.fdir()])
        pld = None
        
        # print("="*30, self, self.constituentList())
        if self.need_regen() and not force:
            self.unlink()
            logger().debug(f"DEBUG_9945: FDFFile.get {self}")
            if self.isComposit():
                flds = []
                #print("="*30, self, self.constituentList())
                for fdf in self.constituentList():
                    #print(f"DEBUG_9008:", fdf)
                    mktPld = fdf.get()
                    if mktPld is not None:
                        flds.append(mktPld)
                        logger().debug(f"DEBUG_9450: {self._spec['symIn']} constituent {fdf.symOut()}: {mktPld.shape} {mktPld.tail(5)}")

                if len(flds)<=0:  
                    #print(f"{ERR}ERROR: empty flds for cmd: {' '.join(sys.argv)}{NC}, {self} missing")
                    return None
                
                pld = pd.concat(flds, axis=1)
                if self.debug():
                    print("DEBUG_9451", self.fpath())
                    print("DEBUG_9452", pld)
                # print(f"INFO_9453: dumping FDF {self.fpath()}")
                # pickle.dump(pld, open(self.fpath(), 'wb'))
                self.dump(pld)
            else:
                if not os.path.exists(self.fpath()):
                    pld = None
                    FDFFile.missing_fdf_sym_list.append(self.symIn())
                    if self.debug():
                        print(f"{BROWN}INFO: missing FDF file {self}{NC}", file=sys.stderr)
                # print(f"DEBUG_9988:", self.fsize(), self.isComposit(), self.constituentNotChangedSince(), self.force_regen(), "===", self)
                # assert False, f"ERROR: missing FDF file {self}"
        else:
            if not os.path.exists(self.fpath()):
                pld = None
                print(f"INFO: missing FDF file {self}", file=sys.stderr)
            else:
                #pld = pickle.loads(Path(self.fpath()).read_bytes())
                pld = self.load()
                if self.ftype() not in ['fdd']:
                    logger().debug(f"INFO: loading FDF file {self}, shape: {pld.shape}")

        if len(FDFFile.missing_fdf_sym_list)>0:
            if False:
                print(f"DEBUG_4531: missing_fdf count {len(FDFFile.missing_fdf_sym_list)}", file=sys.stderr)
        return pld

    def load(self, verbose=False):
        if verbose:
            logger().info(f"{BLUE}INFO: FDFFile.load {self}{NC}")
        pld = pickle.loads(Path(self.fpath()).read_bytes())
        return pld
    
    def unlink(self):
        p = Path(self.fpath())
        if p.exists():
            p.unlink()

    def dump(self, pld, verbose=True):
        #print('\n'.join(pld.columns), file=open('tmp.txt','w'))
        try:
            if self.fpath().find(".fdd")>=0:
                shapeStr = f"keys= {len(pld)}"
            else:
                if pld is None:
                    print(f"{WRN}WARNING: pld is None; cmd= {' '.join(sys.argv)}; fdf= {self}{NC}")
                    return
                shapeStr = f"shape= {pld.shape}"
            if verbose:
                logger().info(f"{GREEN}INFO: FDFFile.dump {self}, {shapeStr}{NC}")
            if not os.path.exists(os.path.dirname(self.fpath())):
                os.system(f"mkdir -p {os.path.dirname(self.fpath())}")
            pickle.dump(pld, open(self.fpath(), 'wb'))
            open(self.fpath_bash(), 'w').write(f"python {' '.join(add_quotes_to_cmdline(sys.argv))}\n")
        except Exception as e:
            print(e, f"{' '.join(sys.argv)}")

        
    def exists(self):
        return os.path.exists(self.fpath())
