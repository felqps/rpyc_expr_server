import os,sys

# from pathlib import Path
from optparse import OptionParser,OptionConflictError

from fdf_basic import *
from fdf_logging import *
from macro_helper import *
from platform_helpers import *
from cmdline_opt import *
from qpstimeit import *
from common_paths import formula_search_dir

def list_modules():
    for k,v in sys.modules.items():
        vstr = f"{v}"
        #print(v)
        if vstr.find("/Fairedge_dev")>=0:
            print(vstr)

OPTIONS_DEFAULTS_FDF = None
def get_option_defaults(what):
    global OPTIONS_DEFAULTS_FDF
    if OPTIONS_DEFAULTS_FDF is None:
        opts = {}
        exec(Path("/Fairedge_dev/app_fdf/options_helper.txt").read_text(), locals())
        OPTIONS_DEFAULTS_FDF = opts

    if what in OPTIONS_DEFAULTS_FDF:
        return OPTIONS_DEFAULTS_FDF[what]
    else:
        return None

def add_options_logging(parser):
    parser.add_option("--loglevel",
                    dest="loglevel",
                    type=str,
                    help="loglevel (default: %default)",
                    metavar="loglevel",
                    default="info")

    parser.add_option("--logfile",
                    dest="logfile",
                    type=str,
                    help="logfile (default: %default)",
                    metavar="logfile",
                    default="")
    
    parser.add_option("--logformat",
                    dest="logformat",
                    type=str,
                    help="logformat (default: %default)",
                    metavar="logformat",
                    default="%(asctime)s;%(name)s;%(levelname)s: %(message)s")

def add_options_cfg(parser):
    parser.add_option("--begdt",
                    dest="begdt",
                    type="string",
                    help="begdt (default: %default)",
                    metavar="begdt",
                    default="")

    parser.add_option("--enddt",
                    dest="enddt",
                    type="string",
                    help="enddt (default: %default)",
                    metavar="enddt",
                    default="")

    parser.add_option("--exch",
                    dest="exch",
                    type="str",
                    help="exch (default: %default)",
                    metavar="exch",
                    default="cs")

    parser.add_option("--period",
                    dest="period",
                    type="str",
                    help="period (default: %default)",
                    metavar="period",
                    default="1d")

    parser.add_option("--domain",
                    dest="domain",
                    type="str",
                    help="domain (default: %default)",
                    metavar="domain",
                    default="")

    #cfg={exch}_{period}_{domain}
    try:
        parser.add_option("--cfg",
                        dest="cfg",
                        type="string",
                        help="cfg (default: %default)",
                        metavar="cfg",
                        default="")
    except OptionConflictError:
        pass

    parser.add_option(
                    "--cfgnm",
                    dest="cfgnm",
                    type="str",
                    help="cfgnm (default: %default)",
                    metavar="cfgnm",
                    default="")
    
    parser.add_option(
                    "--exprnm",
                    dest="exprnm",
                    type="str",
                    help="exprnm (default: %default)",
                    metavar="exprnm",
                    default="NA")
    
    try:
        parser.add_option(
                        "--asofdate",
                        dest="asofdate",
                        type="str",
                        help="asofdate (default: %default)",
                        metavar="asofdate",
                        default="uptodate") #default=datetime.date.today().strftime('%Y%m%d'))
    except OptionConflictError:
        pass

    try:
        parser.add_option(
                        "--factor_concat_adj",
                        dest="factor_concat_adj",
                        type="str",
                        help="factor_concat_adj (default: %default)",
                        metavar="factor_concat_adj",
                        default="") #none,by_ratio
    except OptionConflictError:
        pass
    
    try:
        parser.add_option(
                        "--use_ver",
                        dest="use_ver",
                        type="int",
                        help="use_ver (default: %default)",
                        metavar="use_ver",
                        default=0) #0 or 1
    except OptionConflictError:
        pass

    parser.add_option(
                    "--mkt_dates_cfg",
                    dest="mkt_dates_cfg",
                    type="str",
                    help="mkt_dates_cfg (default: %default)",
                    metavar="mkt_dates_cfg",
                    default=f"/qpsdata/config/MktOMSDates/{datetime.date.today().strftime('%Y%m%d')}/CS.cfg")
                    #default="/qpsdata/config/MktOMSDates.uptodate.cfg")

    

def add_options_basic(parser):
    try:
        parser.add_option("--list_modules",
                        dest="list_modules",
                        type="str",
                        help="list_modules (default: %default)",
                        metavar="list_modules",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--debug",
                        dest="debug",
                        type="int",
                        help="debug (default: %default)",
                        metavar="debug",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--debug_dump",
                        dest="debug_dump",
                        type="str",
                        help="debug_dump (default: %default)",
                        metavar="debug_dump",
                        default="debug_dump")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--dryrun",
                        dest="dryrun",
                        type="int",
                        help="dryrun (default: %default)",
                        metavar="dryrun",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--verbose",
                        dest="verbose",
                        type="int",
                        help="verbose (default: %default)",
                        metavar="verbose",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--force",
                        dest="force",
                        type="int",
                        help="force (default: %default)",
                        metavar="force",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--standalone",
                        dest="standalone",
                        type="str",
                        help="standalone (default: %default)",
                        metavar="standalone",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--regtest",
                        dest="regtest",
                        type="str",
                        help="regtest (default: %default)",
                        metavar="regtest",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--qdb_cfg",
                        dest="qdb_cfg",
                        type="str",
                        help="qdb_cfg (default: %default)",
                        metavar="qdb_cfg",
                        default="all")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--regtest_begdate",
                        dest="regtest_begdate",
                        type="str",
                        help="regtest_begdate (default: %default)",
                        metavar="regtest_begdate",
                        default="20100101")
        parser.add_option("--regtest_enddate",
                        dest="regtest_enddate",
                        type="str",
                        help="regtest_enddate (default: %default)",
                        metavar="regtest_enddate",
                        default="20240308")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--test",
                        dest="test",
                        type="string",
                        help="test (default: %default)",
                        metavar="test",
                        default="all")
    except OptionConflictError:
        pass                        

    try:
        parser.add_option("--step",
                        dest="step",
                        type="string",
                        help="step (default: %default)",
                        metavar="step",
                        default="all")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--server_id",
                        dest="server_id",
                        type="string",
                        help="server_id (default: %default)",
                        metavar="server_id",
                        default="server_id")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--server_addr",
                        dest="server_addr",
                        type="string",
                        help="server_addr (default: %default)",
                        metavar="server_addr",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--server_port",
                        dest="server_port",
                        type="string",
                        help="server_port (default: %default)",
                        metavar="server_port",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--models",
                        dest="models",
                        type="string",
                        help="models (default: %default)",
                        metavar="models",
                        default="models")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--show_items",
                        dest="show_items",
                        type="string",
                        help="show_items (default: %default)",
                        metavar="show_items",
                        default="100200")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--hide_items",
                        dest="hide_items",
                        type="string",
                        help="hide_items (default: %default)",
                        metavar="hide_items",
                        default="100200")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--outdir",
                        dest = "outdir",
                        type = "str",
                        help = "outdir (default: %default)",
                        metavar = "outdir",
                        default = "")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--scn",
                        dest = "scn",
                        type = "str",
                        help = "scn (default: %default)",
                        metavar = "scn",
                        default = "W")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--grep",
                        dest = "grep",
                        type = "str",
                        help = "grep (default: %default)",
                        metavar = "grep",
                        default = ".*")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--stratid",
                        dest = "stratid",
                        type = "str",
                        help = "stratid (default: %default)",
                        metavar = "stratid",
                        default = "STR_strat001")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--symset_filter",
                        dest="symset_filter",
                        type="str",
                        help="symset_filter (default: %default)",
                        metavar="symset_filter",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--filter_name",
                        dest="filter_name",
                        type="str",
                        help="filter_name (default: %default)",
                        metavar="filter_name",
                        default="initial")
    except OptionConflictError:
        pass

    try:                
        parser.add_option("--comment",
                        dest = "comment",
                        type = "str",
                        help = "comment (default: %default)",
                        metavar = "comment",
                        default = "NA")
    except OptionConflictError:
        pass

    try:    
        parser.add_option("-S",
                        "--status_file",
                        dest="status_file",
                        type="str",
                        help="status_file (default: %default)",
                        metavar="status_file",
                        default='NA')
    except OptionConflictError:
        pass
    
    try:
        parser.add_option("--resp",
                        dest = "resp",
                        type = "str",
                        help = "resp (default: %default)",
                        metavar = "resp",
                        default = "Resp_C2C")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--formula_search_ver",
                        dest="formula_search_ver",
                        type="str",
                        help="formula_search_ver (default: %default)",
                        metavar="formula_search_ver",
                        default="formula_search_05")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--formula_build_ver",
                        dest="formula_build_ver",
                        type="str",
                        help="formula_build_ver (default: %default)",
                        metavar="formula_build_ver",
                        default="formula_builder_05")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--select_predictors_ver",
                        dest="select_predictors_ver",
                        type="str",
                        help="select_predictors_ver (default: %default)",
                        metavar="select_predictors_ver",
                        default="select_predictors_05")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--bar",
                    dest="bar",
                    help="bar (default: %default)",
                    metavar="bar",
                    default='1d')
    except OptionConflictError:
        pass 

    try:
        parser.add_option("--pred_tag",
                        dest = "pred_tag",
                        type = "str",
                        help = "pred_tag (default: %default)",
                        metavar = "pred_tag",
                        default = "all")
    except OptionConflictError:
        pass 

    try:
        parser.add_option("--resp_tag",
                        dest = "resp_tag",
                        type = "str",
                        help = "resp_tag (default: %default)",
                        metavar = "resp_tag",
                        default = "all")
    except OptionConflictError:
        pass 

    try:
        parser.add_option("-c",
                        "--dts_cfg",
                        dest="dts_cfg",
                        help="dts_cfg (default: %default)",
                        metavar="dts_cfg",
                        default="NA")
                        #default="fullhist")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--bookid",
                    dest="bookid",
                    type="str",
                    help="bookid (default: %default)",
                    metavar="bookid",
                    default="strat001")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--last_n_days",
            dest = "last_n_days",
            type = "int",
            help = "--last_n_days (default: %default)",
            metavar = "--last_n_days",
            default = 1)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--now",
                        dest = "now",
                        type = "str",
                        help = "now (default: %default)",
                        metavar = "now",
                        default = f"{datetime.datetime.today().date()}".replace('-',''))
    except OptionConflictError:
        pass

    try:
        parser.add_option("--lazy",
                    dest="lazy",
                    type="int",
                    help="lazy (default: %default)",
                    metavar="lazy",
                    default=1)
    except OptionConflictError:
        pass
    
    try:
        parser.add_option("--detailed",
                    dest = "detailed",
                    type = "int",
                    help = "--detailed (default: %default)",
                    metavar = "--detailed",
                    default = 0)
    except OptionConflictError:    
        pass
    
    try:
        parser.add_option("--formula_file",
                    dest = "formula_file",
                    type = "str",
                    help = "--formula_file (default: %default)",
                    metavar = "--formula_file",
                    default = None)
    except OptionConflictError:    
        pass

    try:
        parser.add_option("--eval",
                        dest = "eval",
                        type = "str",
                        help = "--eval (default: %default)",
                        metavar = "--eval",
                        default = 'cx_fast')
    except OptionConflictError:
        pass

    try:
        parser.add_option("--evaluation_model",
                        dest = "evaluation_model",
                        type = "str",
                        help = "--evaluation_model (default: %default)",
                        metavar = "--evaluation_model",
                        default = 'cx')
    except OptionConflictError:
        pass

    try:
        parser.add_option("--show",
                        dest = "show",
                        type = "str",
                        help = "--show (default: %default)",
                        metavar = "--show",
                        default = 'none')
    except OptionConflictError:
        pass

    try:
        parser.add_option("--sectype",
                        dest="sectype",
                        help="sectype (default: %default)",
                        metavar="sectype",
                        default='CS') 
    except OptionConflictError:
        pass

    parser.add_option("--facnm",
                    dest = "facnm",
                    type = "str",
                    help = "--facnm (default: %default)",
                    metavar = "--facnm",
                    default = "NA")
    
    parser.add_option("--results_dir",
                    dest = "results_dir",
                    type = "str",
                    help = "--results_dir (default: %default)",
                    metavar = "--results_dir",
                    default = f"{formula_search_dir()}")

    parser.add_option("--argpar_use_default",
                    dest = "argpar_use_default",
                    type = "int",
                    help = "--argpar_use_default (default: %default)",
                    metavar = "--argpar_use_default",
                    default = 0)  

    parser.add_option("--parmd5",
                    dest = "parmd5",
                    type = "str",
                    help = "--parmd5 (default: %default)",
                    metavar = "--parmd5",
                    default = "NA")

    parser.add_option("--par_set_per_run",
                    dest = "par_set_per_run",
                    type = "int",
                    help = "--par_set_per_run (default: %default)",
                    metavar = "--par_set_per_run",
                    default = 3)
    
    parser.add_option("--server_mode",
                    dest = "server_mode",
                    type = "str",
                    help = "--server_mode (default: %default)",
                    metavar = "--server_mode",
                    default = "")

    parser.add_option("--export_tag",
                    dest = "export_tag",
                    type = "str",
                    help = "--export_tag (default: %default)",
                    metavar = "--export_tag",
                    default = 'NA')

    parser.add_option("--mode",
                    dest = "mode",
                    type = "str",
                    help = "--mode (default: %default)",
                    metavar = "--mode",
                    default = "normal")
    
    parser.add_option("--table_name",
                    dest = "table_name",
                    type = "str",
                    help = "--table_name (default: %default)",
                    metavar = "--table_name",
                    default = "normal")

    try:
        parser.add_option("--dec_tag",
                        dest = "dec_tag",
                        type = "str",
                        help = "dec_tag (default: %default)",
                        metavar = "dec_tag",
                        default = "NA")
    except OptionConflictError:
        pass 

    try:
        parser.add_option("--dec_type",
                        dest = "dec_type",
                        type = "str",
                        help = "dec_type (default: %default)",
                        metavar = "dec_type",
                        default = "paper")
    except OptionConflictError:
        pass 

    try:
        parser.add_option("--dec_unit",
                        dest = "dec_unit",
                        type = "int",
                        help = "dec_unit (default: %default)",
                        metavar = "dec_unit",
                        default = 30000)
    except OptionConflictError:
        pass 

    try:
        parser.add_option("--remote",
                        dest = "remote",
                        type = "int",
                        help = "remote (default: %default)",
                        metavar = "remote",
                        default = 0)
    except OptionConflictError:
        pass 

def add_options_output(parser):
    try:
        parser.add_option("--show",
                        dest="show",
                        type=int,
                        help="show (default: %default)",
                        metavar="show",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--output_file",
                        dest="output_file",
                        type="str",
                        help="output_file (default: %default)",
                        metavar="output_file",
                        default="")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--rows",
                        dest="rows",
                        type="int",
                        help="rows (default: %default)",
                        metavar="rows",
                        default=5)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--localize",
                        dest="localize",
                        type="str",
                        help="localize (default: %default)",
                        metavar="localize",
                        default="cn")
    except OptionConflictError:
        pass

    parser.add_option("--filetype",
                    dest="filetype",
                    type="str",
                    help="filetype (default: %default)",
                    metavar="filetype",
                    default="fdf")

def add_options_ls(parser):
    parser.add_option("--list_size",
                    dest="list_size",
                    type="int",
                    help="list_size (default: %default)",
                    metavar="list_size",
                    default=0)

    parser.add_option("--lenmax",
                    dest="lenmax",
                    type="int",
                    help="lenmax (default: %default)",
                    metavar="lenmax",
                    default=100)

    parser.add_option("--list_non_zero",
                    dest="list_non_zero",
                    type="int",
                    help="list_non_zero (default: %default)",
                    metavar="list_non_zero",
                    default=-1)

    parser.add_option("--list_zero",
                    dest="list_zero",
                    type="int",
                    help="list_zero (default: %default)",
                    metavar="list_zero",
                    default=-1)

    parser.add_option("--list_cp_to",
                    dest="list_cp_to",
                    type="str",
                    help="list_cp_to (default: %default)",
                    metavar="list_cp_to",
                    default="")

def add_options_rpt(parser):
    parser.add_option("--stratnm",
                    dest="stratnm",
                    type="str",
                    help="stratnm (default: %default)",
                    metavar="stratnm",
                    default="neu001")
    try:
        parser.add_option("--benchmark",
                        dest="benchmark",
                        type="str",
                        help="benchmark (default: %default)",
                        metavar="benchmark",
                        default="000300.XSHG")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--benchmark_indices",
                        dest="benchmark_indices",
                        type=str,
                        help="benchmark_indices (default: %default)",
                        metavar="benchmark_indices",
                        default="")
    except OptionConflictError:
        pass

def add_options_qpscloud(parser):
    try:
        parser.add_option("--addr_cfg",
                        dest = "addr_cfg",
                        type = "str",
                        help = "addr_cfg (default: %default)",
                        metavar = "addr_cfg",
                        default = f"qpsdata/config/FCScheduler.cfg")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--pool_size",
                        dest="pool_size",
                        type= "int",
                        help="pool_size (default: %default)",
                        metavar="pool_size",
                        default=3)
    except OptionConflictError:
        pass
    
    try:
        parser.add_option("--priority",
                        dest = "priority",
                        type = "str",
                        help = "priority (default: %default)",
                        metavar = "priority",
                        default = "Z")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--bash",
                        dest="bash",
                        type="str",
                        help="bash (default: %default)",
                        metavar="bash",
                        default="echo")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--quiet",
                        dest="quiet",
                        type="int",
                        help="quiet (default: %default)",
                        metavar="quiet",
                        default=0)
    except OptionConflictError:
        pass

    try:
        parser.add_option("--formula_search_batch_size",
                        dest="formula_search_batch_size",
                        type="int",
                        help="formula_search_batch_size (default: %default)",
                        metavar="formula_search_batch_size",
                        default=1)
        pass
    except OptionConflictError:
        pass

    try:
        parser.add_option("--minor_threshold",
                        dest="minor_threshold",
                        type="int",
                        help="minor_threshold (default: %default)",
                        metavar="minor_threshold",
                        default=100)
        pass
    except OptionConflictError:
        pass

    try:
        parser.add_option("--use_fds",
                        dest="use_fds",
                        type="int",
                        help="use_fds (default: %default)",
                        metavar="use_fds",
                        default=0)
        pass
    except OptionConflictError:
        pass

    try:
        parser.add_option("--univ",
                        dest = "univ",
                        type = "str",
                        help = "--univ (default: %default)",
                        metavar = "--univ",
                        default = 'Full')
        pass
    except OptionConflictError:
        pass

    try:
        parser.add_option("--study_name",
                        dest = "study_name",
                        type = "str",
                        help = "study_name (default: %default)",
                        metavar = "study_name",
                        default = "NA")
        pass
    except OptionConflictError:
        pass

    try:
        pass
    except OptionConflictError:
        pass

def add_options_webcmd(parser):
    try:
        parser.add_option("--chart_type",
                        dest = "chart_type",
                        type = "str",
                        help = "chart_type (default: %default)",
                        metavar = "chart_type",
                        default = f"line")
    except OptionConflictError:
        pass
    try:
        parser.add_option("--webcmd_cfg",
                        dest = "webcmd_cfg",
                        type = "str",
                        help = "webcmd_cfg (default: %default)",
                        metavar = "webcmd_cfg",
                        default = f"/qpsdata/config/WebCmd.cfg")
    except OptionConflictError:
        pass
    try:
        parser.add_option("--sym",
                        dest = "sym",
                        type = "str",
                        help = "sym (default: %default)",
                        metavar = "sym",
                        default = "")
    except OptionConflictError:
        pass

    try:
        pass
    except OptionConflictError:
        pass

def add_options_jq_download(parser):
    try:
        parser.add_option("--data_dir",
                        dest = "data_dir",
                        type = "str",
                        help = "data_dir (default: %default)",
                        metavar = "data_dir",
                        default = "/NASQPS09.qpsdata/raw")
    except OptionConflictError:
        pass
    # try:
    #     parser.add_option("--jq_username",
    #                     dest = "jq_username",
    #                     type = "str",
    #                     help = "jq_username (default: %default)",
    #                     metavar = "jq_username",
    #                     default = f"11111100074")
    # except OptionConflictError:
    #     pass
    # try:
    #     parser.add_option("--jq_password",
    #                     dest = "jq_password",
    #                     type = "str",
    #                     help = "jq_password (default: %default)",
    #                     metavar = "jq_password",
    #                     default = "95kC8dRs")
    # except OptionConflictError:
    #     pass
    
    try:
        parser.add_option("--jq_begdt",
                        dest = "jq_begdt",
                        type = "str",
                        help = "jq_begdt (default: %default)",
                        metavar = "jq_begdt",
                        default = "")
    except OptionConflictError:
        pass
    try:
        parser.add_option("--jq_enddt",
                        dest = "jq_enddt",
                        type = "str",
                        help = "jq_enddt (default: %default)",
                        metavar = "jq_enddt",
                        default = "")
    except OptionConflictError:
        pass
    try:
        parser.add_option("--data_provider",
                        dest = "data_provider",
                        type = "str",
                        help = "data_provider (default: %default)",
                        metavar = "data_provider",
                        default = "JQ")
    except OptionConflictError:
        pass
    
    try:
        parser.add_option(
                        "--tdx_ip",
                        dest = "tdx_ip",
                        type = "str",
                        help = "tdx_ip (default: %default)",
                        metavar = "tdx_ip",
                        default = "tdx.xmzq.com.cn"
        )
    except OptionConflictError:
        pass
    
    try:
        parser.add_option(
                        "--tdx_port",
                        dest = "tdx_port",
                        type = "int",
                        help = "tdx_port (default: %default)",
                        metavar = "tdx_port",
                        default = 7709
        )
    except OptionConflictError:
        pass

    
def add_options_mysql(parser):
    try:
        parser.add_option("--db_cfg",
                        dest = "db_cfg",
                        type = "str",
                        help = "db_cfg (default: %default)",
                        metavar = "db_cfg",
                        default = "/qpsdata/config/mysql_cf_0.56.cfg")
    except OptionConflictError:
        pass
    try:
        parser.add_option("--postgres_cfg_file",
                        dest = "postgres_cfg_file",
                        type = "str",
                        help = "postgres_cfg_file (default: %default)",
                        metavar = "postgres_cfg_file",
                        default = "/qpsdata/config/PostgreSql.cfg")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--api_key",
                        dest = "api_key",
                        type = "str",
                        help = "api_key (default: %default)",
                        metavar = "api_key",
                        default = "")
    except OptionConflictError:
        pass
    # try:
    #     parser.add_option("--data_cache_dir",
    #                     dest = "data_cache_dir",
    #                     type = "str",
    #                     help = "data_cache_dir (default: %default)",
    #                     metavar = "data_cache_dir",
    #                     default = "")
    # except OptionConflictError:
    #     pass

def set_options_fdf(parser):
    try:
        parser.add_option("--do",
                        dest="do",
                        type="string",
                        help="do (default: %default)",
                        metavar="do",
                        default="all")
    except OptionConflictError:
        pass

    try:
        parser.add_option("--symset",
                        dest="symset",
                        type="string",
                        help="symset (default: %default)",
                        metavar="symset",
                        default="NA")
    except OptionConflictError:
        pass

    parser.add_option("--toret",
                    dest="toret",
                    type="int",
                    help="toret (default: %default)",
                    metavar="toret",
                    default=0)

    parser.add_option("--cumsum",
                    dest="cumsum",
                    type="int",
                    help="cumsum (default: %default)",
                    metavar="cumsum",
                    default=0)

    parser.add_option("--fake_data",
                    dest="fake_data",
                    type="int",
                    help="fake_data (default: %default)",
                    metavar="fake_data",
                    default=0)

    parser.add_option("--force_regen",
                    dest="force_regen",
                    type="int",
                    help="force_regen (default: %default)",
                    metavar="force_regen",
                    default=0)

    parser.add_option("--fpath",
                    dest="fpath",
                    type="str",
                    help="fpath (default: %default)",
                    metavar="fpath",
                    default="")

    parser.add_option("--min_mean",
                    dest="min_mean",
                    type="float",
                    help="min_mean (default: %default)",
                    metavar="min_mean",
                    default=0.02)
                
    parser.add_option("--max_mean",
                    dest="max_mean",
                    type="float",
                    help="max_mean (default: %default)",
                    metavar="max_mean",
                    default=0.07)

    parser.add_option("--runid",
                    dest="runid",
                    type="str",
                    help="runid (default: %default)",
                    metavar="runid",
                    default="")

    parser.add_option("--expr",
                    dest="expr",
                    type="str",
                    help="expr (default: %default)",
                    metavar="expr",
                    default="")
    
    parser.add_option("--expr_md5",
                    dest="expr_md5",
                    type="str",
                    help="expr_md5 (default: %default)",
                    metavar="expr_md5",
                    default="")
    
    parser.add_option("--nats_servers",
                    dest="nats_servers",
                    type="str",
                    help="nats_servers (default: %default)",
                    metavar="nats_servers",
                    #default="nats://192.168.20.94:5222,nats://192.168.20.95:5223,nats://192.168.20.98:5224")
                    default="nats://192.168.20.94:5232,nats://192.168.20.95:5233,nats://192.168.20.98:5234")
    parser.add_option("--job_limit",
                    dest="job_limit",
                    type="int",
                    help="job_limit (default: %default)",
                    metavar="job_limit",
                    default=5)
    parser.add_option(
                    "--use_jetstream",
                    dest="use_jetstream",
                    type="int",
                    help="use_jetstream (default: %default)",
                    metavar="use_jetstream",
                    #default=0,
                    default=1,
    )
    
    parser.add_option("--worker_id",
                    dest="worker_id",
                    type="str",
                    help="worker_id (default: %default)",
                    metavar="worker_id",
                    default="",
    )

    parser.add_option("--refresh_last_n_results",
                    dest="refresh_last_n_results",
                    type="int",
                    help="refresh_last_n_results (default: %default)",
                    metavar="refresh_last_n_results",
                    default=10
    )

    parser.add_option("--expr_requests_chunk_size",
                    dest="expr_requests_chunk_size",
                    type="int",
                    help="expr_requests_chunk_size (default: %default)",
                    metavar="expr_requests_chunk_size",
                    default=50
    )

    parser.add_option(
                    "--subject_prefix",
                    dest="subject_prefix",
                    type="str",
                    help="subject_prefix (default: %default)",
                    metavar="subject_prefix",
                    default="exprworker",
    )

    parser.add_option(
                    "--worker_name",
                    dest="worker_name",
                    type="str",
                    help="worker_name (default: %default)",
                    metavar="worker_name",
                    default="",
    )
    parser.add_option(
                    "--group_name",
                    dest="group_name",
                    type="str",
                    help="group_name (default: %default)",
                    metavar="group_name",
                    default="",
    )
    try:
        parser.add_option("--expr_regex",
                        dest="expr_regex",
                        type="string",
                        help="expr_regex (default: %default)",
                        metavar="expr_regex",
                        default="")
    except OptionConflictError:
        pass

    parser.add_option("--forecast_eval",
                    dest="forecast_eval",
                    type="str",
                    help="forecast_eval (default: %default)",
                    metavar="forecast_eval",
                    default="NA")
    
    parser.add_option("--workspace_home",
                    dest="workspace_home",
                    type="str",
                    help="workspace_home (default: %default)",    
                    metavar="workspace_home",
                    default="/qpsdata/FactorWorker")

    parser.add_option("--workspace_tmpl",
                    dest="workspace_tmpl",
                    type="str",
                    help="workspace_tmpl (default: %default)",
                    metavar="workspace_tmpl",
                    default="/Fairedge_dev/app_factor_worker/tmpl.cs_001.wk")

    parser.add_option("--workspace_name",
                    dest="workspace_name",
                    type="str",
                    help="workspace_name (default: %default)",
                    metavar="workspace_name",
                    default="")

    parser.add_option("--normalize_by",
                    dest="normalize_by",
                    type="str",
                    help="normalize_by (default: %default)",
                    metavar="normalize_by",
                    default="no") #no normalization

    parser.add_option("--fldnm",
                    dest="fldnm",
                    type="str",
                    help="fldnm (default: %default)",
                    metavar="fldnm",
                    default="")

    parser.add_option("--max_eval_num",
                    dest="max_eval_num",
                    type="int",
                    help="max_eval_num (default: %default)",
                    metavar="max_eval_num",
                    default=500000)

    parser.add_option("--br_per_mkt",
                    dest="br_per_mkt",
                    type="int",
                    help="br_per_mkt (default: %default)",
                    metavar="br_per_mkt",
                    default=0)

    # parser.add_option(
    #                 "--is_none_adjust",
    #                 dest="is_none_adjust",
    #                 type="int",
    #                 help="is_none_adjust (default: %default)",
    #                 metavar="is_none_adjust",
    #                 default=0)

    parser.add_option(
                    "--root_dirs",
                    dest = "root_dirs",
                    type = "str",
                    help = "root_dirs (default: %default)",
                    metavar = "root_dirs",
                    # default = "/qpsdata/data_rq.20100101_uptodate,/qpsdata/data_rq.20210810_uptodate",
                    default = "")
    parser.add_option(
                    "--fields_cfg_file",
                    dest = "fields_cfg_file",
                    type = "str",
                    help = "fields_cfg_file (default: %default)",
                    metavar = "fields_cfg_file",
                    default = "")
    parser.add_option(
                    "--data_type",
                    dest = "data_type",
                    type = "str",    
                    help = "data_type (default: %default)",
                    metavar = "data_type",
                    default = "ohlcv")
    parser.add_option(
                    "--del_fdf_file",
                    dest = "del_fdf_file",
                    type = "int",
                    help = "del_fdf_file (default: %default)",
                    metavar = "del_fdf_file",
                    default = 0)
    parser.add_option(
                    "--combine_exists",
                    dest = "combine_exists",
                    type = "int",
                    help = "combine_exists (default: %default)",
                    metavar = "combine_exists",
                    default = 1)
    parser.add_option(    
                    "--align_index",
                    dest = "align_index",
                    type = "int",
                    help = "align_index (default: %default)",
                    metavar = "align_index",    
                    default = 1)
    parser.add_option(
                    "--qpsnas_cfg",
                    dest = "qpsnas_cfg",
                    type = "str",
                    help = "qpsnas_cfg (default: %default)",
                    metavar = "qpsnas_cfg",
                    default = "467")    
    parser.add_option(
                    "--backup_tag",
                    dest = "backup_tag",
                    type = "str",
                    help = "backup_tag (default: %default)",
                    metavar = "backup_tag",
                    default = "backup")
    parser.add_option(    
                    "--tactic_id",
                    dest = "tactic_id",
                    type = "str",
                    help = "tactic_id (default: %default)",
                    metavar = "tactic_id",
                    default = '')
    parser.add_option(    
                    "--nopos",
                    dest = "nopos",
                    type = "int",
                    help = "nopos (default: %default)",
                    metavar = "nopos",
                    default = 1)
    parser.add_option(
                    "--src",
                    dest = "src",
                    type = "str",
                    help = "src (default: %default)",
                    metavar = "src",
                    default = "")
    parser.add_option(
                    "--tgt",
                    dest = "tgt",
                    type = "str",
                    help = "tgt (default: %default)",
                    metavar = "tgt",
                    default = "")
    parser.add_option(
                    "--gen_backward_lookup_fn",
                    dest = "gen_backward_lookup_fn",
                    type = "int",
                    help = "gen_backward_lookup_fn (default: %default)",
                    metavar = "gen_backward_lookup_fn",
                    default = 0)
    parser.add_option(
                    "--gen_research_db",
                    dest = "gen_research_db",
                    type = "int",
                    help = "gen_research_db (default: %default)",
                    metavar = "gen_research_db",
                    default = 0)
    parser.add_option(
                    "--dominant_contract_name",
                    dest = "dominant_contract_name",
                    type = "str",
                    help = "dominant_contract_name (default: %default)",
                    metavar = "dominant_contract_name",
                    default = "dominant_future")
    parser.add_option(    
                    "--gen_fdd_A",
                    dest = "gen_fdd_A",
                    type = "int",
                    help = "gen_fdd_A (default: %default)",
                    metavar = "gen_fdd_A",
                    default = 0)
    add_options_basic(parser)
    add_options_cfg(parser)
    add_options_logging(parser)
    add_options_output(parser)
    add_options_ls(parser)
    add_options_rpt(parser)
    add_options_qpscloud(parser)
    add_options_webcmd(parser)
    add_options_jq_download(parser)
    add_options_mysql(parser)

def post_parse_fdf(opt):
    if hasattr(opt, 'benchmark_indices'):
        if not opt.benchmark_indices:
            opt.benchmark_indices = '000300.XSHG,000905.XSHG,399006.XSHE,000016.XSHG,000852.XSHG,8841431.WI'
            # if hasattr(opt, 'benchmark'):
            #     opt.benchmark_indices = opt.benchmark

def post_parse_cfg(opt):
    if hasattr(opt, 'cfg'):
        if opt.cfg:
            pass
        elif (opt.exch and opt.period and opt.domain):    
            opt.cfg = f"{opt.exch}_{opt.period}_{opt.domain}"
        else:
            opt.cfg = "cs_1d_prod"

        if opt.cfg:
            opt.cfg = fdf_expand_macros(opt.cfg)

            (exch, period, domain) = opt.cfg.split(r'_')    
            opt.exch = exch
            opt.period = period
            opt.domain = domain
        
        #From lib_QpsData/QDF.py
        if domain in ["F"]:
            opt.begdt = '20100101'
            opt.enddt = '20210101'    
        elif domain in ["G"]:
            opt.begdt = '20200101'
            opt.enddt = '20220101'

        opt.cfgnm = opt.cfg

        if not opt.workspace_name:
            if opt.workspace_tmpl:
                tmpl_base_fn = os.path.basename(opt.workspace_tmpl)
                opt.workspace_name = tmpl_base_fn.replace('tmpl', f"{opt.workspace_home}/workspace/{opt.cfg}.{opt.symset}").replace(".wk", "/config.wk")

                # print(tmpl_base_fn, opt.workspace_home, opt.workspace_name)
                # exit(0)

                opt.workspace_dir = os.path.dirname(opt.workspace_name)
                #os.system(f"mkdir -p {opt.workspace_dir}")
                Path(os.path.dirname(opt.workspace_name)).mkdir(parents=True, exist_ok=True)
                #open(opt.workspace_name, 'w').write(fdf_expand_macros(''.join(open(opt.workspace_tmpl, 'r').readlines()).replace('SYMSET', opt.symset).replace('CFG', opt.cfg)))
                Path(opt.workspace_name).write_text(fdf_expand_macros(''.join(open(opt.workspace_tmpl, 'r').readlines()).replace('SYMSET', opt.symset).replace('CFG', opt.cfg)))

        fn_cfg = smart_path(f"/Fairedge_dev/app_fdf/cfgs/fdf.{opt.cfg}")
        for ln in open_readlines(fn_cfg):
            (k,v) = ln.split("=")
            if k == 'begdt':
                if not opt.begdt:
                    opt.begdt = v
            if k == 'enddt':
                if not opt.enddt:
                    opt.enddt = v

        # if opt.enddt == 'tradedate':
        #     import datetime
        #     opt.enddt = datetime.date.today().strftime('%Y%m%d')

        if opt.br_per_mkt<=0:
            if opt.symset.find("EQTY")>=0:
                #opt.br_per_mkt = 2000000
                opt.br_per_mkt = 5500000
            else:
                #opt.br_per_mkt = 50000*7.5
                opt.br_per_mkt = 5500000

    post_parse_fdf(opt)

    set_cmdline_opt(opt)
    # global CMDLINE_OPT
    # CMDLINE_OPT = opt

def add_quotes_to_cmdline(argv):
    newargv = []
    for v in argv:
        if (v.find("'")>=0 or v.find('(')>=0) and v.find("\"")<0:
            newargv.append(f"\"{v}\"")
        else:
            newargv.append(v)
    return newargv

def get_options(funcn="NA", parser=None, customize_options=None, delay_post_parse_cfg=False):
    if parser is None:
        parser = OptionParser(description=funcn)

    set_options_fdf(parser)

    if customize_options is not None:
        customize_options(parser)

    opt, args = parser.parse_args()

    opt.output_fp = None

    
    opt.show_items = [int(x) for x in opt.show_items.split(r',')]
    opt.hide_items = [int(x) for x in opt.hide_items.split(r',')]

    if cmdline_opt_debug_on():
        print(f"INFO: opt.show_items= {opt.show_items}")
        print(f"INFO: opt.hide_items= {opt.hide_items}")

    
    if not delay_post_parse_cfg:
        post_parse_cfg(opt)


    init_logger(level=opt.loglevel, filename=opt.logfile, format=opt.logformat, name=os.path.basename(sys.argv[0]).split(r'.')[0])

    print(f"{BROWN}INFO: run_cmd= python {' '.join(add_quotes_to_cmdline(sys.argv))}{NC}", file=sys.stderr)

    dbg(f"DEBUG_dsdf: cfgnm= {opt.cfgnm}, begdt= {opt.begdt}, enddt= {opt.enddt}")

    if opt.list_modules:
        list_modules()
        exit(0)

    return(opt, args)

if __name__ == "__main__":
    # funcn = "options_helper.main"
    # import pandas as pd
    # opt, args = get_options(funcn)
    # print_opt(opt)
    for k,v in sys.modules.items():
        print(k, v)
    

