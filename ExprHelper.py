import os,sys
import re
from common_smart_load import buf2md5
from common_colors import *
import pandas as pd
import glob
from FdfExpr import FdfExpr
from PostgreSqlTools import postgres_instance
from common_ctx import gettoday
from QpsUtil import open_readlines
from common_basic import use_cache_if_exists

def gen_sql_in_clause(exprnm_list):
    single_quote = [f"'{x}'" for x in exprnm_list]
    return f"({','.join(single_quote)})"

def query_filter_fgprod_for_detail_analysis(symset, exprnm_list):
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    sql = f"select exprnm, symset, \"ir.cs_1d_F\", \"ir.cs_1d_G\", \"ir.cs_1d_prod\" from filter_fgprod_for_detailed_analysis where exprnm in {gen_sql_in_clause(exprnm_list)} and symset = '{symset}'"
    print(f"INFO: sql_statment= {sql}")
    (column_types, rows) = postgres.query_data(sql)
    if len(rows)>0:
        df = pd.DataFrame.from_dict(rows, orient='columns')
        df.sort_values(by=['symset', 'exprnm'], ascending=[1,1], inplace=True)
    else:
        df = None
    print(df)
    return(rows)

def query_expr_eval_by_corr_for_exprnm_list(symset, exprnm_list):
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    #sql = f"select exprnm, \"alphaQT_net_IR\" from performance_eval_details where exprnm in {gen_sql_in_clause(exprnm_list)}"
    sql = f"select exprnm, symset, cfg, ir from expr_eval_by_corr where exprnm in {gen_sql_in_clause(exprnm_list)} and symset = '{symset}'"
    print(f"INFO: sql_statment= {sql}")
    (column_types, rows) = postgres.query_data(sql)

    if len(rows)>0:
        df = pd.DataFrame.from_dict(rows, orient='columns')
        df.sort_values(by=['symset', 'exprnm', 'cfg'], ascending=[1,1,1], inplace=True)
    else:
        df = None
    print(f"INFO: eval_by_corr for {symset}")
    print(df)
    return rows
    # for row in rows:
    #     print(row)
        #print(row['exprnm'], row['alphaQT_net_IR'])

def query_expr_for_exprnm(exprnm):
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    (column_types, rows) = postgres.query_data(f"select expr from view_exprnm_expr_lookup where exprnm = '{exprnm}'")
    if len(rows)>=1:
        return rows[0]['expr']
    else:
        return None

def query_large_negative_ir(symset='CS_ALL'):
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    (column_types, rows) = postgres.query_data(f"select expr from expr_eval_by_corr where ir<=-1.50 and symset = '{symset}' and cfg = 'cs_1d_F' order by id")
    return [x['expr'] for x in rows]

def query_large_positive_ir(symset='CS_ALL'):
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    (column_types, rows) = postgres.query_data(f"select expr from expr_eval_by_corr where ir>=1.50 and symset = '{symset}' and cfg = 'cs_1d_F' order by id")
    res = [x['expr'] for x in rows]
    #print(res[:5])
    return res

def query_expr_candidates_for_industry_expansion(symset='CS_ALL'):
    funcn = "query_expr_candidates_for_industry_expansion"
    postgres = postgres_instance("/qpsdata/config/PostgreSql.cfg")
    (column_types, rows) = postgres.query_data(f"select expr from expr_eval_by_corr where symset = '{symset}' and exprnm in (select exprnm from filter_fgprod_for_detailed_analysis where symset = '{symset}') order by id")
    res = [x['expr'] for x in rows]
    #print(res[:5])
    print(f"{funcn}: returned {len(res)} rows", file=sys.stderr)
    return res

EXPRNM_OLDSTYLE_2_EXPR_DICT = {}
def exprnm_oldstyle_2_expr_dict(pattern="/Fairedge_dev/proj_JVrc/Resp_C2C.*/formula_genetic.99.txt"):
    global EXPRNM_OLDSTYLE_2_EXPR_DICT
    if len(EXPRNM_OLDSTYLE_2_EXPR_DICT.keys())>0: #initialized
        return EXPRNM_OLDSTYLE_2_EXPR_DICT
    for fn in glob.glob(pattern):
        for ln  in open_readlines(fn):
            ln = ln.replace(' ','')
            if ln.find("=")<0:
                continue
            if ln.find("#")>=0:
                ln = ln.split('#')[0]
            (exprnm, expr) = ln.split("=")
            EXPRNM_OLDSTYLE_2_EXPR_DICT[exprnm] = expr
    return EXPRNM_OLDSTYLE_2_EXPR_DICT


def symset_2_strat001_exprnm_oldstyle_list(fn="/Fairedge_dev/app_formula_search/exprnm_list.STR_strat001.txt"):
    exprnm_oldstyle_dict = exprnm_oldstyle_2_expr_dict(pattern="/Fairedge_dev/proj_JVrc/Resp_C2C.*/formula_genetic.99.txt")
    res = {}
    for ln in open_readlines(fn):
        ln = ln.strip()
        (symset, exprnm_oldstyle) = ln.split()
        if exprnm_oldstyle in exprnm_oldstyle_dict:
            if symset not in res.keys():
                res[symset] = []
            res[symset].append(exprnm_oldstyle_dict[exprnm_oldstyle])
        else: 
            print(f"ERROR: cannot field old_style exprnm '{exprnm_oldstyle}'")
    return(res)

def gen_all_prev_strategy_factors(fn="/Fairedge_dev/app_formula_search/exprnm_list.STR_strat001.txt"):
    expr_dict = symset_2_strat001_exprnm_oldstyle_list(fn)
    el = []
    for scn in ['F', 'G', 'prod']:
        for k, v in expr_dict.items():
            for e in v:
                expr = f"cs_1d_{scn}:{k}:{e}"
                el.append(expr)
    return el

def symset_2_strat001_exprnm_list(fn="/Fairedge_dev/app_formula_search/exprnm_list.STR_strat001.txt"):
    res = {}
    for k,v in symset_2_strat001_exprnm_oldstyle_list(fn).items():
        res[k] = [buf2md5(expr)[-8:] for expr in v]
    return res

VARNM_2_EXPR = {}
def translate_expr_style_to_new(expr, varnm=None):
    global VARNM_2_EXPR
    inner = re.findall(r"U\*\((.+)\)", expr)
    if len(inner) > 0:
        expr = inner[0]

    funcns = re.findall(r"([A-Z0-9\_]+)\(", expr)
    if len(funcns)>0:
        for funcn in funcns:
            expr = expr.replace(f"{funcn}(", f"{funcn.lower()}(")

    varins = re.findall(r"(Alpha[0-9a-z\_]+)", expr)
    if len(varins)>0:
        for varin in varins:
            if varin in VARNM_2_EXPR:
                expr = expr.replace(varin, VARNM_2_EXPR[varin])
            else:
                print(f"ERROR: unknown Alpha {varin} for expr: {expr}")

    if varnm is not None:
        VARNM_2_EXPR[varnm] = expr

    ## disable secondary expressions like ma(Alpha003) initially
    if len(varins)>0:
        expr = None

    return expr


def formula_generator_file_based(pattern="/Fairedge_dev/proj_JVrc/Resp_C2C.C25/formula_genetic.99.txt"):
    exprs = []
    for fn in glob.glob(pattern):
        print(f"INFO: formula_generator_file_based reading {fn}")
        for ln  in open_readlines(fn):
            ln = ln.replace(' ','')
            expr = None
            varnm = None
            if ln.find("=")>=0:
                varnm = ln.split("=")[0]
                expr = ln.split("=")[1]
            if expr is not None and expr.find("#")>=0: #comment
                expr = expr.split("#")[0]

            if expr is not None:
                expr = expr.strip()

                if pattern.find("formula_alpha101"):
                    #remap to new function style
                    if ln.find("054")>=0 and False:
                        print("DEBUG: ", varnm, expr)
                    expr = translate_expr_style_to_new(expr, varnm)

                if expr is not None and expr not in exprs:
                    exprs.append(expr)
    return exprs

def format_for_eval_by_corr(exprs, verbose=False):
    expr_full_list = []
    for x in exprs:
        helper = ExprHelper(x)
        if verbose:
            print(helper.to_dataframe())
        expr_full_list.append(helper.expr_full(eval_by_corr=True))

    for x in expr_full_list:
        if verbose:
            print(f"{BLUE}{x}{NC}")
    
    return expr_full_list

STATUS_FILE_DICT = None
def get_status_file_dict():
    global STATUS_FILE_DICT

    if STATUS_FILE_DICT is None:
        STATUS_FILE_DICT = {}
        fn = "/Fairedge_dev/app_qpsrpyc/status_file_list.txt"
        if os.path.exists(fn):
            for ln in open_readlines(fn):
                STATUS_FILE_DICT[ln] = 1

    return STATUS_FILE_DICT


class ExprHelper:
    def __init__(self, expr):
        expr = expr.replace(' ', '')
        self._data = {}
        self._data['resp'] = "Resp1_1" #default
        if expr.find(":")<0:
            expr = f"cs_1d_G:C25:{expr}" #default if not specify cfg and symset

        self._data['expr_full'] = expr
        self._data['cfg'] = expr.split(':')[0]
        self._data['symset'] = expr.split(':')[1]
        self._data['expr'] = expr.split(':')[2]
        self._data['expr_server_side'] = self._data['expr'] #serverside do not manipulate eval_by_corr
        self._data['ex'] = self._data['cfg'].split('_')[0]
        self._data['periodicity'] = self._data['cfg'].split('_')[1]
        self._data['scn'] = self._data['cfg'].split('_')[2]
        
        if self._data['expr'].find("eval_by_corr")>=0:
            self._data['expr'] = self._data['expr'].replace("eval_by_corr","")
            self._data['resp'] = re.findall(r"Resp\w+\_\w+", self._data['expr'])[0]
            self._data['expr'] = re.findall(r"\((\S+)\,", self._data['expr'])[0]
            #self._data['expr_full'] = self._data['expr_full'].replace(self._data['resp'], f"TRADE_AT_CLOSE*{self._data['resp']}") #FIXing the limit-up/limit-down issue

        #self._data['exprnm'] = buf2md5(self._data['expr'])[-8:]

    def expr_full(self, eval_by_corr=False):
        eval_by_corr = eval_by_corr or self._data['expr_full'].find("eval_by_corr")>=0
        if eval_by_corr:
            return f"{self._data['ex']}_{self._data['periodicity']}_{self._data['scn']}:{self._data['symset']}:eval_by_corr({self._data['expr']}, TRADE_AT_CLOSE*{self._data['resp']})"
        else:
            return self._data['expr_full']
        
    def flip_sign(self):
        self._data['expr'] = f"-{self._data['expr']}"
        return self

    def scn(self):
        return self._data['scn']

    def exprnm(self):
        return self.md5()
        
    def expr(self):
        return self._data['expr']

    def expr_server_side(self):
        return self._data['expr_server_side']
    
    def symset(self):
        return self._data['symset']
    
    def cfg(self):
        return self._data['cfg']
        
    def md5(self):
        expr = self.expr()
        if self.scn in ['T','A']:
            expr = expr.replace(f"_{self.scn}", gettoday())

        return buf2md5(expr)[-8:]

    def save_definition(self, fn):
        Path(os.path.dirname(fn)).mkdir(parents=True, exist_ok=True)
        Path(fn).write_text(self._expr+'\n')

    def to_dataframe(self):
        return pd.DataFrame.from_dict(self.to_dict(), orient='index', columns=['Value'])
    
    def to_dict(self):
        self._data['exprnm'] = self.md5()
        return self._data

    def __str__(self):
        return self.expr_full()
    
    def wkr_id(self):
        return f"{self._data['cfg']}.{self._data['symset']}"
    
    def rpyc_expr_status_file(self):
        #fdfexpr = FdfExpr(self._data['expr_full'])
        #status_dir = "/qpsdata/temp/rpyc_expr"
        status_dir = use_cache_if_exists("/NASQPS08.qpsdata/research/rpyc_expr_server/temp/rpyc_expr", cache_dir="/NASQPS08.qpsdata.cache/research/rpyc_expr_server/temp/rpyc_expr")
        #return f"{status_dir}/{self.wkr_id()}/{fdfexpr.md5()}.db"
        return f"{status_dir}/{self.wkr_id()}/{self.md5()}.db"
    
    def pending_status_file(self):
        return f"{self.rpyc_expr_status_file()}.pending"

    
    def status_file_exists(self, force=False):
        if force:
            return False
        return self.rpyc_expr_status_file() in get_status_file_dict() or os.path.exists(self.rpyc_expr_status_file())

class ExprGenerator:
    def __init__(self, expr_list):
        self._expr_list = expr_list

    def generate(self, cfg_list=["F"], eval_by_corr=False):  #["F", "G", "prod"]
        expr_list = [] 
        if eval_by_corr:
            expr_list_G = format_for_eval_by_corr([x for x in self._expr_list])
        else:
            # print("DDDDD", len(self._expr_list), '\n'.join(self._expr_list[:5]))
            expr_list_G = [ExprHelper(x).expr_full() for x in self._expr_list]
        
        for cfg in cfg_list:
            if cfg not in ['G']:
                expr_list.extend([x.replace('_G:', f'_{cfg}:') for x in expr_list_G if x.find("_G:")>=0])
            else:
                expr_list.extend(expr_list_G)

        self._expr_list = expr_list
        return self
    
    def symset_list(self, symset_list):
        assert type(symset_list) == type(list()), f"ERROR: symset_list must be a list"
        temp = []
        for symset in symset_list:
            temp.extend([x.replace("C25", symset) for x in self._expr_list])
        self._expr_list = temp
        return self

    def scn_list(self, scn_list):
        assert type(scn_list) == type(list()), f"ERROR: symset_list must be a list"
        temp = []
        for scn in scn_list:
            temp.extend([x.replace("_F", f"_{scn}") for x in self._expr_list])
        self._expr_list = temp
        return self
    
    def flip_sign(self):
        self._expr_list = [ExprHelper(x).flip_sign().expr_full() for x in self._expr_list]
        return self
    
    def expr_list(self):
        return self._expr_list

    def exprnm_by_symset(self):
        d = {}
        for x in self.expr_list():
            eh = ExprHelper(x)
            if eh.symset() not in d:
                d[eh.symset()] = {}
            d[eh.symset()][eh.exprnm()] = 1

        for k,v in d.items():
            d[k] = list(v.keys())

        return d

if __name__ == "__main__":
    funcn = "factor_worker.main"
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 5000)

    if 0:
        sample_list = [
            "cs_1d_G:CS_ALL:eval_by_corr(mul(decay_linear1(decay_linear120(ts_argmin60(ts_sum90(RETURNS)))),inv(signedpower3(scale(VWAP)))), Resp1_1)",
            "cs_1d_G:CS_ALL:eval_by_corr(RETURNS, Resp1_1)",
            "cs_1d_G:CS_ALL:RETURNS",
            "CLOSE",
        ]
        format_for_eval_by_corr(sample_list)
        
        exprs = ExprGenerator(formula_generator_file_based(pattern="/Fairedge_dev/proj_JVrc/Resp_C2C.C25/formula_genetic.99.txt"))\
            .generate(eval_by_corr=True).symset_list(["CS_ALL"]).expr_list()[:10]
        print(f"INFO: exprs len (A)= {len(exprs)}")
        print("\n".join(exprs))

        exprs = ExprGenerator(formula_generator_file_based(pattern="/Fairedge_dev/proj_JVrc/Resp_C2C.C25/formula_genetic.99.txt")).generate(eval_by_corr=False).expr_list()[:10]
        print(f"INFO: exprs len (B)= {len(exprs)}")
        for x in exprs:
            print(ExprHelper(x).rpyc_expr_status_file(), "=>", x)

        exprs = ExprGenerator(query_large_negative_ir(symset='CS_ALL')).generate(eval_by_corr=True).symset_list(["CS_ALL"]).flip_sign().expr_list()[:10]
        print(f"INFO: exprs len (C)= {len(exprs)}")
        for x in exprs:
            print(ExprHelper(x).rpyc_expr_status_file(), "=>", x)    

        exprs = ExprGenerator(query_large_positive_ir(symset='CS_ALL')[:5]).generate(eval_by_corr=True).symset_list(["I65","C25"]).expr_list()[:]
        print(f"INFO: exprs len (D)= {len(exprs)}")
        for x in exprs:
            print(ExprHelper(x).rpyc_expr_status_file(), "=>", x)  

        exprs = ExprGenerator(query_large_positive_ir(symset='CS_ALL')[:5]).generate(eval_by_corr=True).symset_list(["CS_ALL"]).scn_list(["G","prod"]).expr_list()[:]
        print(f"INFO: exprs len (E)= {len(exprs)}")
        for x in exprs:
            print(ExprHelper(x).rpyc_expr_status_file(), "=>", x)  

    query_expr_candidates_for_industry_expansion()

    for k,v in symset_2_strat001_exprnm_oldstyle_list().items():
        print(f"INFO: strat001 expr symset {GREEN}{k}{NC}: {v}")

    # for k,v in symset_2_strat001_exprnm_list().items():
    #     print(k, ":", gen_sql_in_clause(v))
    
    for k,v in symset_2_strat001_exprnm_list().items():
        query_expr_eval_by_corr_for_exprnm_list(k,v)

    if 0:
        for k,v in symset_2_strat001_exprnm_list().items():
            query_filter_fgprod_for_detail_analysis(k, v)

    f = open("database_data.exprnm_for_strat001.csv", "w")
    print("#id,symset,exprnm", file=f)
    for k,v in symset_2_strat001_exprnm_list().items():
        for exprnm in v:
            id = f"{k}.{exprnm}"
            print(f"{id},{k},{exprnm}", file=f)




