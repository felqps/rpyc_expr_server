#!/usr/bin/env python

import sys,os

def remap_expr_old_style(stmt, debug_stmts):
    if stmt.find("signedpower")>=0:
        stmt = stmt.replace("ns['signedpower1']", "signedpower1")
        stmt = stmt.replace("ns['signedpower2']", "signedpower2")
        stmt = stmt.replace("ns['signedpower3']", "signedpower3")

    if stmt.find("DNA_b8d7ca")>=0:
        debug_stmts.append("ns['Pred_SRCH8f7ffc_DNA_b8d7ca_1d_1'] = scale(ns['VWAP'])")
        debug_stmts.append("ns['Pred_SRCH8f7ffc_DNA_b8d7ca_1d_1'] = signedpower3(scale(ns['VWAP']))")
        debug_stmts.append("ns['Pred_SRCH8f7ffc_DNA_b8d7ca_1d_1'] = decay_linear1(decay_linear120(ts_argmin60(ts_sum90(ns['RETURNS']))))")
        debug_stmts.append("ns['Pred_SRCH8f7ffc_DNA_b8d7ca_1d_1'] = mul(decay_linear1(decay_linear120(ts_argmin60(ts_sum90(ns['RETURNS'])))),inv(signedpower3(scale(ns['VWAP']))))")
    
    return stmt


if __name__ == '__main__':
    for expr in ['foobar']:
        print(remap_expr_old_style(expr))



