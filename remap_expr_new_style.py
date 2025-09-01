#!/usr/bin/env python

import sys,os

def remap_expr_new_style(expr, debug_exprs=None):
    if expr.find("regout(fld_fgw('Pred_SRCH8f7ffc_FmtlEp_1d_1',CLOSE), fld_fgw('Pred_mysql_sbp_1d_1',CLOSE))")>=0:
        print(f"++++++++++++++++++++++++++++++++++ remap_expr_new_style {expr}")
        expr="regout(regout(1/fld_fgw('RqPeRatioTtm', CLOSE), log(MARKET_CAP)), fld_fgw('Pred_mysql_sbp_1d_1',CLOSE))"

    if expr.find("log(MARKET_CAP)")>=0:
        debug_exprs.append("log(MARKET_CAP)")
        debug_exprs.append("1/fld_fgw('RqPeRatioTtm',CLOSE)")
    
    return expr


if __name__ == '__main__':
    for expr in ["regout(fld_fgw('Pred_SRCH8f7ffc_FmtlEp_1d_1',CLOSE), fld_fgw('Pred_mysql_sbp_1d_1',CLOSE))"]:
        print(remap_expr_new_style(expr))



