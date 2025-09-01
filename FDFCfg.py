#!/usr/bin/env python

import sys
from macro_helper import *
from FDFFile import *
from get_fld import get_fld_FGW
from common_basic import symset2sectype

class FDFCfg:
    def __init__(self, cfgnm, opt=None):
        cfgnm = fdf_expand_macros(cfgnm)
        self._cfg = {}
        self._cfg['cfgnm'] = cfgnm
        self._cfg['symIn'] = None
        self._cfg['symOut'] = None
        self._cfg['debug'] = 0
        self._cfg['force_regen'] = 0
        if opt is not None:
            self._cfg['debug'] = opt.debug 
            self._cfg['force_regen'] = opt.force_regen or opt.force

        fn_cfg = smart_path(f"/Fairedge_dev/app_fdf/cfgs/fdf.{cfgnm}")
        for ln in open_readlines(fn_cfg):
            (k,v) = ln.split("=")
            self._cfg[k]=v

        self._opt = opt

        # if self._cfg['enddt'] == 'tradedate':
        #     import datetime
        #     self._cfg['enddt'] = datetime.date.today().strftime('%Y%m%d')
            
    def __STR__(self):
        s = f"FDFCfg: cfg={self._cfg}"
        return s

    def cfgnm(self):
        return self._cfg['cfgnm']
    
    def fldnm(self):
        return self._cfg['fldnm']
    
    def expr(self):
        return self.fldnm()

    def ss(self, ssn):
        self._cfg['symIn'] = ssn
        self._cfg['symOut'] = ssn
        # if symOut is not None:
        #     self._cfg['symOut'] = symOut
        # else:
        #     self._cfg['symOut'] = symIn    
        return self                

    def fdf(self, fldnm):
        spec = self._cfg.copy()
        fldnm = fdf_expand_macros(fldnm)
        if fldnm.find("FdfDict")>=0:
            fldnm = fldnm.replace("'","")
            fldnm = fldnm.replace("\"","")

        spec['fldnm'] = fldnm #fdf_expand_macros(fldnm)
        spec['cfg'] = self

        fdf = FDFFile(spec)
        return fdf

    def debug(self):
        return self._cfg['debug']

    def mkts4symset(self, ssn, ssn_only=False):
        if ssn_only:
            assert is_symset(ssn), f"ERROR: can not find symset {ssn}"

        if is_symset(ssn):
            return ssn2mkts()[ssn]
        else:
            return [ssn]

    def ctx(self, opt=None):
        loc = {}
        loc['Factor'] = self.genfunc_Factor(fake_data = 0 if opt is None else opt.fake_data, output_fp=opt.output_fp if opt is not None else None)
        return loc

    def get_mkts(self, ssn):
        if is_symset(ssn):
            return ssn2mkts()[ssn]
        else:
            return [ssn]

    def genfunc_Factor(self, fake_data=0, debug=0, output_fp=None):
        funcn  = "FDFCfg.Factor()"
        def Factor(fldnm, shift=0):
            pld = None
            if symset2sectype(self._opt.symset) in ['stocks', 'CS'] and self._opt.domain in ['A']:
                fldnm2realname = {
                    'open': 'Pred_OHLCV_O_1d_1',
                    'high': 'Pred_OHLCV_H_1d_1',
                    'low': 'Pred_OHLCV_L_1d_1',
                    'close': 'Pred_OHLCV_C_1d_1',
                    'volume': 'Pred_OHLCV_V_1d_1',
                    'uclose': 'Pred_OHLCV_uC_1d_1',
                    'total_turnover': 'Pred_Rqraw_RqTotalTurnover_1d_1',
                    'market_cap': 'Pred_Rqraw_RqMarketCap_1d_1'
                }
                            
                pld = get_fld_FGW(self._opt, fldnm2realname[fldnm], self._opt.symset, self._opt.domain)

            if pld is None:
                if shift == 0:
                    pld = self.fdf(f"Factor('{fldnm}')").get()
                else:
                    pld = self.fdf(f"Factor('{fldnm}', shift={shift})").get()
            elif shift!=0:
                pld.shift(shift)

            if debug:
                if output_file is None:
                    print_df(pld, title=funcn)
                else:
                    print_df(pld, cols=10, title=funcn, file=output_file)
            if not fake_data:
                return pld
            else:
                if fldnm == 'open':
                    return 3.0
                elif fldnm == 'high':
                    return 5.0
                elif fldnm == 'low':
                    return 1.0
                elif fldnm == 'close':
                    return 4.0
                elif fldnm == 'volume':
                    return 100.0
                else:
                    return 0.0

        return Factor   

    def eval(self, stmt, opt=None):
        ctx = self.ctx(opt)
        return eval(stmt, ctx)

